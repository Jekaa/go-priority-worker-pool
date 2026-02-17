package prioritypool

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Priority определяет уровень приоритета задачи
type Priority int

const (
	HighPriority Priority = iota
	NormalPriority
	LowPriority
)

func (p Priority) String() string {
	switch p {
	case HighPriority:
		return "high"
	case NormalPriority:
		return "normal"
	case LowPriority:
		return "low"
	default:
		return "unknown"
	}
}

// Оптимизация 1: SpinLock для высоконагруженных сценариев
type spinLock struct {
	locked uint32
}

func (s *spinLock) Lock() {
	// Используем exponential backoff при конкурентном доступе
	backoff := 1
	for !atomic.CompareAndSwapUint32(&s.locked, 0, 1) {
		// Экспоненциальный backoff для уменьшения конкуренции
		for i := 0; i < backoff; i++ {
			runtime.Gosched()
		}
		if backoff < 16 {
			backoff <<= 1
		}
	}
}

func (s *spinLock) Unlock() {
	atomic.StoreUint32(&s.locked, 0)
}

// Оптимизация 2: RingBuffer для очередей
type ringBuffer struct {
	buffer []func()
	head   uint64
	tail   uint64
	mask   uint64
	_      [56]byte // padding для предотвращения false sharing
}

func newRingBuffer(size int) *ringBuffer {
	// Округляем до степени двойки для оптимизации маски
	actualSize := 1
	for actualSize < size {
		actualSize <<= 1
	}

	return &ringBuffer{
		buffer: make([]func(), actualSize),
		mask:   uint64(actualSize - 1),
	}
}

// Enqueue добавляет задачу в буфер (lock-free)
func (r *ringBuffer) Enqueue(task func()) bool {
	head := atomic.LoadUint64(&r.head)
	tail := atomic.LoadUint64(&r.tail)

	if tail-head >= uint64(len(r.buffer)) {
		return false // переполнение
	}

	pos := tail & r.mask

	// Используем atomic для записи, чтобы гарантировать видимость
	// Но саму функцию сохраняем через указатель для избежания копирования
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&r.buffer[pos]))
	atomic.StorePointer(ptr, unsafe.Pointer(&task))

	atomic.AddUint64(&r.tail, 1)
	return true
}

// Dequeue забирает задачу из буфера (lock-free)
func (r *ringBuffer) Dequeue() (func(), bool) {
	head := atomic.LoadUint64(&r.head)
	tail := atomic.LoadUint64(&r.tail)

	if head >= tail {
		return nil, false // пусто
	}

	pos := head & r.mask
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&r.buffer[pos]))
	taskPtr := atomic.SwapPointer(ptr, nil)

	if taskPtr == nil {
		return nil, false
	}

	task := *(*func())(taskPtr)
	atomic.AddUint64(&r.head, 1)
	return task, true
}

// Len возвращает текущую длину очереди
func (r *ringBuffer) Len() uint64 {
	head := atomic.LoadUint64(&r.head)
	tail := atomic.LoadUint64(&r.tail)
	return tail - head
}

// PriorityQueue объединяет кольцевой буфер с метаданными
type PriorityQueue struct {
	buffer   *ringBuffer
	priority Priority
	_        [48]byte // padding
}

// PriorityPool управляет пулом воркеров с приоритезацией задач
type PriorityPool struct {
	queues        map[Priority]*PriorityQueue
	queueSizes    map[Priority]int
	workers       int
	maxWorkers    int
	activeWorkers atomic.Int32

	// Оптимизация 3: Кэширование reflect.Value
	queueReflectValues map[Priority]reflect.Value

	// Оптимизация 4: sync.Pool для select cases
	casePool sync.Pool

	// Метрики
	metrics struct {
		submitted atomic.Int64
		completed atomic.Int64
		waiting   map[Priority]*atomic.Int64
		executing atomic.Int32
		temporary atomic.Int32
	}

	wg          sync.WaitGroup
	closeOnce   sync.Once
	shutdown    chan struct{}
	shutdownCtx context.Context
	cancelFunc  context.CancelFunc

	// Используем spinLock вместо обычного мьютекса
	workerSpinLock spinLock
}

// NewPriorityPool создает новый приоритетный пул воркеров
func NewPriorityPool(workers int, queueSizes map[Priority]int) *PriorityPool {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	// Инициализируем очереди с кольцевыми буферами
	queues := make(map[Priority]*PriorityQueue)
	queueReflectValues := make(map[Priority]reflect.Value)
	metricsWaiting := make(map[Priority]*atomic.Int64)

	for p, size := range queueSizes {
		if size <= 0 {
			size = 10
		}
		queues[p] = &PriorityQueue{
			buffer:   newRingBuffer(size),
			priority: p,
		}
		// Кэшируем reflect.Value для каждого приоритета
		queueReflectValues[p] = reflect.ValueOf(queues[p])
		metricsWaiting[p] = &atomic.Int64{}
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &PriorityPool{
		queues:             queues,
		queueSizes:         queueSizes,
		queueReflectValues: queueReflectValues,
		workers:            workers,
		maxWorkers:         workers * 2,
		shutdown:           make(chan struct{}),
		shutdownCtx:        ctx,
		cancelFunc:         cancel,
	}

	pool.metrics.waiting = metricsWaiting

	// Инициализируем пул объектов для select cases
	pool.casePool = sync.Pool{
		New: func() interface{} {
			return make([]reflect.SelectCase, 0, 3)
		},
	}

	// Запускаем основных воркеров
	for i := 0; i < workers; i++ {
		pool.startWorker(false)
	}

	// Запускаем диспетчер приоритетов
	pool.startDispatcher()

	return pool
}

// Submit добавляет задачу в пул с указанным приоритетом
func (p *PriorityPool) Submit(ctx context.Context, priority Priority, task func()) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.shutdownCtx.Done():
		return errors.New("pool is shutting down")
	default:
	}

	// Для high-priority задач пытаемся создать временного воркера
	if priority == HighPriority {
		if err := p.trySpawnTemporaryWorker(ctx, task); err == nil {
			return nil
		}
	}

	// Пытаемся отправить в соответствующую очередь
	queue := p.queues[priority]
	if queue.buffer.Enqueue(task) {
		p.metrics.submitted.Add(1)
		p.metrics.waiting[priority].Add(1)
		return nil
	}

	// Очередь переполнена
	if priority == HighPriority {
		return p.spawnTemporaryWorker(ctx, task)
	}

	return fmt.Errorf("queue for %s priority is full", priority)
}

// trySpawnTemporaryWorker пытается создать временного воркера
func (p *PriorityPool) trySpawnTemporaryWorker(ctx context.Context, task func()) error {
	p.workerSpinLock.Lock()
	defer p.workerSpinLock.Unlock()

	current := p.activeWorkers.Load()
	if current >= int32(p.maxWorkers) {
		return errors.New("max workers limit reached")
	}

	p.startWorker(true)

	// Запускаем задачу напрямую
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.shutdownCtx.Done():
		return errors.New("pool is shutting down")
	default:
		p.wg.Add(1)
		p.metrics.executing.Add(1)
		p.metrics.temporary.Add(1)

		go func() {
			defer p.wg.Done()
			defer p.metrics.executing.Add(-1)
			defer p.metrics.temporary.Add(-1)
			defer p.metrics.completed.Add(1)

			p.executeTask(task)
		}()

		p.metrics.submitted.Add(1)
		return nil
	}
}

// spawnTemporaryWorker создает временного воркера для high-priority задачи
func (p *PriorityPool) spawnTemporaryWorker(ctx context.Context, task func()) error {
	p.workerSpinLock.Lock()
	defer p.workerSpinLock.Unlock()

	current := p.activeWorkers.Load()
	if current >= int32(p.maxWorkers) {
		// Пробуем отправить в очередь high-priority
		if p.queues[HighPriority].buffer.Enqueue(task) {
			p.metrics.submitted.Add(1)
			p.metrics.waiting[HighPriority].Add(1)
			return nil
		}
		return errors.New("all workers busy and queues full")
	}

	p.startWorker(true)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.shutdownCtx.Done():
		return errors.New("pool is shutting down")
	default:
		p.wg.Add(1)
		p.metrics.executing.Add(1)
		p.metrics.temporary.Add(1)

		go func() {
			defer p.wg.Done()
			defer p.metrics.executing.Add(-1)
			defer p.metrics.temporary.Add(-1)
			defer p.metrics.completed.Add(1)

			p.executeTask(task)
		}()

		p.metrics.submitted.Add(1)
		return nil
	}
}

// startWorker запускает нового воркера
func (p *PriorityPool) startWorker(temporary bool) {
	p.activeWorkers.Add(1)
	p.wg.Add(1)

	go func() {
		defer p.wg.Done()
		defer p.activeWorkers.Add(-1)

		var idleTimer *time.Timer
		if temporary {
			idleTimer = time.NewTimer(5 * time.Second)
			defer idleTimer.Stop()
		}

		for {
			select {
			case <-p.shutdownCtx.Done():
				return
			default:
			}

			// Получаем cases из пула
			cases := p.casePool.Get().([]reflect.SelectCase)
			cases = p.buildSelectCases(cases[:0])

			chosen, value, ok := reflect.Select(cases)

			// Возвращаем cases в пул
			p.casePool.Put(cases[:0])

			select {
			case <-p.shutdownCtx.Done():
				return
			default:
			}

			if temporary && idleTimer != nil {
				if !idleTimer.Stop() {
					select {
					case <-idleTimer.C:
					default:
					}
				}
				idleTimer.Reset(5 * time.Second)
			}

			if !ok {
				continue
			}

			// Получаем очередь и задачу
			queue := value.Interface().(*PriorityQueue)
			task, ok := queue.buffer.Dequeue()
			if !ok || task == nil {
				continue
			}

			priority := p.getPriorityFromIndex(chosen)
			p.metrics.waiting[priority].Add(-1)
			p.metrics.executing.Add(1)

			p.executeTask(task)

			p.metrics.executing.Add(-1)
			p.metrics.completed.Add(1)

			if temporary && idleTimer != nil {
				select {
				case <-idleTimer.C:
					return
				default:
				}
			}
		}
	}()
}

// buildSelectCases создает случаи для reflect.Select с использованием кэшированных значений
func (p *PriorityPool) buildSelectCases(cases []reflect.SelectCase) []reflect.SelectCase {
	priorities := []Priority{HighPriority, NormalPriority, LowPriority}

	for _, priority := range priorities {
		if queue, exists := p.queues[priority]; exists {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: p.queueReflectValues[priority],
				Send: reflect.Value{}, // zero value для recv случаев
			})
			// Используем queue для компилятора, чтобы избежать предупреждения
			_ = queue
		}
	}

	return cases
}

// getPriorityFromIndex определяет приоритет по индексу
func (p *PriorityPool) getPriorityFromIndex(index int) Priority {
	priorities := []Priority{HighPriority, NormalPriority, LowPriority}
	if index >= 0 && index < len(priorities) {
		return priorities[index]
	}
	return LowPriority
}

// executeTask выполняет задачу с восстановлением после паники
func (p *PriorityPool) executeTask(task func()) {
	defer func() {
		if r := recover(); r != nil {
			// В production здесь должно быть логирование
			_ = fmt.Sprintf("task panicked: %v", r)
		}
	}()

	if task != nil {
		task()
	}
}

// startDispatcher запускает диспетчер
func (p *PriorityPool) startDispatcher() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-p.shutdownCtx.Done():
				return
			case <-ticker.C:
				p.preventPriorityInversion()
			}
		}
	}()
}

// preventPriorityInversion предотвращает приоритетную инверсию
func (p *PriorityPool) preventPriorityInversion() {
	highWaiting := p.metrics.waiting[HighPriority].Load()
	lowWaiting := p.metrics.waiting[LowPriority].Load()
	executing := p.metrics.executing.Load()

	if highWaiting > 0 && lowWaiting > 0 && executing >= int32(p.workers) {
		p.workerSpinLock.Lock()
		current := p.activeWorkers.Load()
		if current < int32(p.maxWorkers) {
			p.startWorker(true)
		}
		p.workerSpinLock.Unlock()
	}
}

// Shutdown останавливает пул
func (p *PriorityPool) Shutdown(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.cancelFunc()
	})

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// Metrics возвращает текущие метрики пула
func (p *PriorityPool) Metrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"submitted": p.metrics.submitted.Load(),
		"completed": p.metrics.completed.Load(),
		"executing": p.metrics.executing.Load(),
		"workers":   p.activeWorkers.Load(),
		"temporary": p.metrics.temporary.Load(),
		"waiting":   make(map[string]int64),
	}

	waiting := metrics["waiting"].(map[string]int64)
	for priority, counter := range p.metrics.waiting {
		waiting[priority.String()] = counter.Load()
	}

	// Добавляем информацию о длинах очередей
	for priority, queue := range p.queues {
		metrics["queue_"+priority.String()+"_len"] = queue.buffer.Len()
	}

	return metrics
}
