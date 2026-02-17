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
)

// Priority определяет уровень приоритета задачи
type Priority int

const (
	// HighPriority - задачи, которые должны выполняться максимально быстро
	HighPriority Priority = iota
	// NormalPriority - задачи со стандартным приоритетом
	NormalPriority
	// LowPriority - задачи, которые могут ждать
	LowPriority
)

// String возвращает строковое представление приоритета
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

// PriorityPool управляет пулом воркеров с приоритезацией задач
type PriorityPool struct {
	queues        map[Priority]chan func() // каналы для разных приоритетов
	queueSizes    map[Priority]int
	workers       int
	maxWorkers    int
	activeWorkers atomic.Int32

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

	mu       sync.RWMutex
	workerMu sync.Mutex // для управления временными воркерами
}

// NewPriorityPool создает новый приоритетный пул воркеров
func NewPriorityPool(workers int, queueSizes map[Priority]int) *PriorityPool {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	// Инициализируем очереди для каждого приоритета
	queues := make(map[Priority]chan func())
	metricsWaiting := make(map[Priority]*atomic.Int64)

	for p, size := range queueSizes {
		if size <= 0 {
			size = 10 // значение по умолчанию
		}
		// Буферизированные каналы для каждой очереди
		queues[p] = make(chan func(), size)
		metricsWaiting[p] = &atomic.Int64{}
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &PriorityPool{
		queues:      queues,
		queueSizes:  queueSizes,
		workers:     workers,
		maxWorkers:  workers * 2, // максимум временных воркеров
		shutdown:    make(chan struct{}),
		shutdownCtx: ctx,
		cancelFunc:  cancel,
	}

	pool.metrics.waiting = metricsWaiting

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

	// Для high-priority задач пытаемся создать временного воркера,
	// если все основные заняты
	if priority == HighPriority {
		if err := p.trySpawnTemporaryWorker(ctx, task); err == nil {
			return nil
		}
	}

	// Пытаемся отправить в соответствующую очередь
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.shutdownCtx.Done():
		return errors.New("pool is shutting down")
	case p.queues[priority] <- task:
		p.metrics.submitted.Add(1)
		p.metrics.waiting[priority].Add(1)
		return nil
	default:
		// Очередь переполнена
		if priority == HighPriority {
			// Для high-priority создаем временного воркера
			return p.spawnTemporaryWorker(ctx, task)
		}
		return fmt.Errorf("queue for %s priority is full", priority)
	}
}

// trySpawnTemporaryWorker пытается создать временного воркера, если есть свободные ресурсы
func (p *PriorityPool) trySpawnTemporaryWorker(ctx context.Context, task func()) error {
	p.workerMu.Lock()
	defer p.workerMu.Unlock()

	current := p.activeWorkers.Load()
	if current >= int32(p.maxWorkers) {
		return errors.New("max workers limit reached")
	}

	// Создаем временного воркера
	p.startWorker(true)

	// Отправляем задачу напрямую
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.shutdownCtx.Done():
		return errors.New("pool is shutting down")
	default:
		// Запускаем задачу в отдельной горутине, чтобы не блокировать Submit
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
	p.workerMu.Lock()
	defer p.workerMu.Unlock()

	current := p.activeWorkers.Load()
	if current >= int32(p.maxWorkers) {
		// Если достигли лимита, пробуем отправить в очередь high-priority
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p.queues[HighPriority] <- task:
			p.metrics.submitted.Add(1)
			p.metrics.waiting[HighPriority].Add(1)
			return nil
		default:
			return errors.New("all workers busy and queues full")
		}
	}

	p.startWorker(true)

	// Отправляем задачу напрямую
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

		// Создаем таймер для автоматического завершения временных воркеров
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

			// Используем reflect.Select для неблокирующего выбора из всех очередей
			cases := p.buildSelectCases()

			chosen, value, ok := reflect.Select(cases)

			// Проверяем завершение
			select {
			case <-p.shutdownCtx.Done():
				return
			default:
			}

			// Для временных воркеров сбрасываем таймер при получении задачи
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
				// Канал закрыт
				continue
			}

			// Получаем задачу из выбранного канала
			task, ok := value.Interface().(func())
			if !ok || task == nil {
				continue
			}

			// Обновляем метрики ожидания для соответствующего приоритета
			priority := p.getPriorityFromIndex(chosen)
			p.metrics.waiting[priority].Add(-1)
			p.metrics.executing.Add(1)

			// Выполняем задачу
			p.executeTask(task)

			p.metrics.executing.Add(-1)
			p.metrics.completed.Add(1)

			// Для временных воркеров проверяем таймер простоя
			if temporary && idleTimer != nil {
				select {
				case <-idleTimer.C:
					// Воркер завершается из-за бездействия
					return
				default:
				}
			}
		}
	}()
}

// buildSelectCases создает случаи для reflect.Select
func (p *PriorityPool) buildSelectCases() []reflect.SelectCase {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Сортируем приоритеты: High, Normal, Low
	priorities := []Priority{HighPriority, NormalPriority, LowPriority}
	cases := make([]reflect.SelectCase, 0, len(priorities))

	for _, priority := range priorities {
		ch, exists := p.queues[priority]
		if !exists {
			continue
		}

		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		})
	}

	return cases
}

// getPriorityFromIndex определяет приоритет по индексу в select case
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
			// В production-коде здесь должно быть логирование
			_ = fmt.Sprintf("task panicked: %v", r)
		}
	}()

	if task != nil {
		task()
	}
}

// startDispatcher запускает диспетчер для мониторинга и предотвращения приоритетной инверсии
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
	// Проверяем, не заблокированы ли high-priority задачи low-priority
	highWaiting := p.metrics.waiting[HighPriority].Load()
	lowWaiting := p.metrics.waiting[LowPriority].Load()
	executing := p.metrics.executing.Load()

	// Если есть ожидающие high-priority задачи и много low-priority в выполнении
	if highWaiting > 0 && lowWaiting > 0 && executing >= int32(p.workers) {
		// Создаем временного воркера для high-priority задач
		p.workerMu.Lock()
		current := p.activeWorkers.Load()
		if current < int32(p.maxWorkers) {
			p.startWorker(true)
		}
		p.workerMu.Unlock()
	}
}

// Shutdown останавливает пул и ждет завершения всех задач
func (p *PriorityPool) Shutdown(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.cancelFunc()

		// Закрываем все очереди
		for _, ch := range p.queues {
			close(ch)
		}
	})

	// Ожидаем завершения с учетом контекста
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

	return metrics
}
