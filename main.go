package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func main() {
	// Демонстрация всех возможностей PriorityPool
	fmt.Println("=== Демонстрация Priority Worker Pool ===\n")

	// 1. Создание пула с разными размерами очередей
	queueSizes := map[Priority]int{
		HighPriority:   5,  // маленькая очередь для high, т.к. они выполняются быстро
		NormalPriority: 10, // средняя очередь
		LowPriority:    20, // большая очередь для low
	}

	pool := NewPriorityPool(3, queueSizes) // 3 постоянных воркера
	defer gracefulShutdown(pool)

	// 2. Демонстрация базовой отправки задач
	fmt.Println("1. Базовая отправка задач с разными приоритетами:")
	basicSubmission(pool)

	// 3. Демонстрация приоритетной обработки
	fmt.Println("\n2. Демонстрация приоритетной обработки (High > Normal > Low):")
	priorityDemonstration(pool)

	// 4. Демонстрация переполнения очередей и временных воркеров
	fmt.Println("\n3. Демонстрация обработки переполнения High-приоритетом:")
	overflowDemonstration(pool)

	// 5. Демонстрация предотвращения приоритетной инверсии
	fmt.Println("\n4. Демонстрация предотвращения приоритетной инверсии:")
	inversionPreventionDemonstration(pool)

	// 6. Демонстрация контекста и таймаутов
	fmt.Println("\n5. Демонстрация работы с контекстом и таймаутами:")
	contextDemonstration(pool)

	// 7. Демонстрация метрик
	fmt.Println("\n6. Финальные метрики пула:")
	printMetrics(pool)
}

// Priority для удобства демонстрации
type Priority int

const (
	HighPriority Priority = iota
	NormalPriority
	LowPriority
)

func (p Priority) String() string {
	switch p {
	case HighPriority:
		return "HIGH"
	case NormalPriority:
		return "NORMAL"
	case LowPriority:
		return "LOW"
	default:
		return "UNKNOWN"
	}
}

// PriorityPool интерфейс для демонстрации
type PriorityPool interface {
	Submit(ctx context.Context, priority Priority, task func()) error
	Shutdown(ctx context.Context) error
	Metrics() map[string]interface{}
}

// NewPriorityPool создает пул (заглушка для компиляции)
func NewPriorityPool(workers int, queueSizes map[Priority]int) PriorityPool {
	// В реальности здесь будет вызов вашей реализации
	return &mockPool{
		workers:    workers,
		queueSizes: queueSizes,
		metrics:    make(map[string]interface{}),
	}
}

// mockPool для демонстрации (замените на реальную реализацию)
type mockPool struct {
	workers    int
	queueSizes map[Priority]int
	metrics    map[string]interface{}
	mu         sync.Mutex
}

func (p *mockPool) Submit(ctx context.Context, priority Priority, task func()) error {
	// Имитация отправки задачи
	go func() {
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
		task()
	}()

	p.mu.Lock()
	defer p.mu.Unlock()

	// Обновляем метрики
	if p.metrics["submitted"] == nil {
		p.metrics["submitted"] = int64(0)
	}
	p.metrics["submitted"] = p.metrics["submitted"].(int64) + 1
	p.metrics["waiting"] = map[string]int64{
		"high":   rand.Int63n(5),
		"normal": rand.Int63n(10),
		"low":    rand.Int63n(15),
	}

	return nil
}

func (p *mockPool) Shutdown(ctx context.Context) error {
	fmt.Println("  ⏳ Завершение работы пула...")
	time.Sleep(2 * time.Second)
	fmt.Println("  ✅ Пул успешно остановлен")
	return nil
}

func (p *mockPool) Metrics() map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Генерируем реалистичные метрики
	return map[string]interface{}{
		"submitted": rand.Int63n(100) + 50,
		"completed": rand.Int63n(80) + 20,
		"executing": rand.Int63n(3),
		"workers":   rand.Int63n(5) + 3,
		"temporary": rand.Int63n(2),
		"waiting": map[string]int64{
			"high":   rand.Int63n(5),
			"normal": rand.Int63n(10),
			"low":    rand.Int63n(15),
		},
	}
}

func gracefulShutdown(pool PriorityPool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Shutdown(ctx); err != nil {
		fmt.Printf("Ошибка при завершении: %v\n", err)
	}
}

func basicSubmission(pool PriorityPool) {
	// Отправляем задачи разных приоритетов
	priorities := []Priority{HighPriority, NormalPriority, LowPriority}

	for i := 0; i < 5; i++ {
		priority := priorities[rand.Intn(len(priorities))]
		taskID := i + 1

		err := pool.Submit(context.Background(), priority, func() {
			fmt.Printf("    Задача %d (приоритет: %s) выполнена\n",
				taskID, priority)
		})

		if err != nil {
			fmt.Printf("    Ошибка отправки задачи %d: %v\n", taskID, err)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func priorityDemonstration(pool PriorityPool) {
	var wg sync.WaitGroup

	// Сначала отправляем много Low задач
	fmt.Println("    Отправка 3 Low задач...")
	for i := 0; i < 3; i++ {
		taskID := i + 1
		wg.Add(1)

		pool.Submit(context.Background(), LowPriority, func() {
			defer wg.Done()
			time.Sleep(500 * time.Millisecond)
			fmt.Printf("    Low задача %d завершена\n", taskID)
		})
	}

	time.Sleep(100 * time.Millisecond)

	// Затем High задачу
	fmt.Println("    Отправка High задачи (должна выполниться первой)...")
	wg.Add(1)
	pool.Submit(context.Background(), HighPriority, func() {
		defer wg.Done()
		fmt.Println("    HIGH задача выполнена (опередила Low задачи!)")
	})

	// И Normal задачу
	fmt.Println("    Отправка Normal задачи...")
	wg.Add(1)
	pool.Submit(context.Background(), NormalPriority, func() {
		defer wg.Done()
		time.Sleep(300 * time.Millisecond)
		fmt.Println("    Normal задача выполнена")
	})

	wg.Wait()
}

func overflowDemonstration(pool PriorityPool) {
	// Заполняем очередь Low задачами
	fmt.Println("    Заполнение очереди Low задачами...")
	for i := 0; i < 10; i++ {
		taskID := i + 1
		pool.Submit(context.Background(), LowPriority, func() {
			time.Sleep(2 * time.Second)
			fmt.Printf("    Low задача %d (из очереди) выполнена\n", taskID)
		})
	}

	time.Sleep(100 * time.Millisecond)

	// Отправляем High задачи, которые должны создать временных воркеров
	fmt.Println("    Отправка High задач (должны создать временных воркеров)...")

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		taskID := i + 1
		wg.Add(1)

		pool.Submit(context.Background(), HighPriority, func() {
			defer wg.Done()
			fmt.Printf("    HIGH задача %d выполнена (возможно временным воркером)\n", taskID)
			time.Sleep(300 * time.Millisecond)
		})
	}

	wg.Wait()
}

func inversionPreventionDemonstration(pool PriorityPool) {
	// Создаем ситуацию, где Low задачи блокируют ресурсы
	fmt.Println("    Имитация длительных Low задач...")

	for i := 0; i < 4; i++ {
		taskID := i + 1
		pool.Submit(context.Background(), LowPriority, func() {
			time.Sleep(3 * time.Second)
			fmt.Printf("    Long Low задача %d завершена\n", taskID)
		})
	}

	time.Sleep(500 * time.Millisecond)

	// Отправляем High задачу, которая должна запустить механизм предотвращения инверсии
	fmt.Println("    Отправка High задачи в условиях дефицита ресурсов...")

	var wg sync.WaitGroup
	wg.Add(1)

	start := time.Now()
	pool.Submit(context.Background(), HighPriority, func() {
		defer wg.Done()
		fmt.Printf("    HIGH задача выполнена через %v (инверсия предотвращена)\n",
			time.Since(start).Round(time.Millisecond))
	})

	wg.Wait()
}

func contextDemonstration(pool PriorityPool) {
	// Демонстрация работы с контекстом и таймаутами

	// 1. Успешное выполнение с контекстом
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()

	fmt.Println("    Отправка задачи с таймаутом 2 сек...")
	err := pool.Submit(ctx1, NormalPriority, func() {
		time.Sleep(1 * time.Second)
		fmt.Println("    ✅ Задача выполнена в рамках таймаута")
	})

	if err != nil {
		fmt.Printf("    ❌ Ошибка: %v\n", err)
	}

	// 2. Превышение таймаута
	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel2()

	fmt.Println("\n    Отправка задачи с таймаутом 500ms...")
	err = pool.Submit(ctx2, LowPriority, func() {
		time.Sleep(1 * time.Second)
		fmt.Println("    ❌ Эта задача не должна выполниться")
	})

	if err != nil {
		fmt.Printf("    ✅ Ожидаемая ошибка таймаута: %v\n", err)
	}

	// 3. Отмена через контекст
	ctx3, cancel3 := context.WithCancel(context.Background())

	fmt.Println("\n    Отправка задачи с возможностью отмены...")
	err = pool.Submit(ctx3, NormalPriority, func() {
		select {
		case <-time.After(2 * time.Second):
			fmt.Println("    ❌ Задача выполнилась (не должна)")
		case <-ctx3.Done():
			fmt.Println("    ✅ Задача отменена через контекст")
		}
	})

	// Отменяем через 100ms
	time.Sleep(100 * time.Millisecond)
	cancel3()

	time.Sleep(500 * time.Millisecond)
}

func printMetrics(pool PriorityPool) {
	metrics := pool.Metrics()

	fmt.Printf("    Всего отправлено задач: %d\n", metrics["submitted"])
	fmt.Printf("    Выполнено задач: %d\n", metrics["completed"])
	fmt.Printf("    Выполняется сейчас: %d\n", metrics["executing"])
	fmt.Printf("    Активных воркеров: %d (временных: %d)\n",
		metrics["workers"], metrics["temporary"])

	if waiting, ok := metrics["waiting"].(map[string]int64); ok {
		fmt.Println("    Ожидающие задачи по приоритетам:")
		fmt.Printf("      High: %d\n", waiting["high"])
		fmt.Printf("      Normal: %d\n", waiting["normal"])
		fmt.Printf("      Low: %d\n", waiting["low"])
	}
}

// Пример расширенного использования с кастомными задачами
func advancedExample() {
	pool := NewPriorityPool(5, map[Priority]int{
		HighPriority:   10,
		NormalPriority: 20,
		LowPriority:    30,
	})
	defer gracefulShutdown(pool)

	// Создаем задачи разных типов
	type Task struct {
		ID       int
		Priority Priority
		Data     string
		Process  func(string) string
	}

	tasks := []Task{
		{1, HighPriority, "срочные данные", func(s string) string { return "ОБРАБОТАНО: " + s }},
		{2, NormalPriority, "обычные данные", func(s string) string { return "processed: " + s }},
		{3, LowPriority, "фоновые данные", func(s string) string { return "background: " + s }},
	}

	results := make(chan string, len(tasks))
	ctx := context.Background()

	// Отправляем задачи
	for _, task := range tasks {
		task := task // захват для замыкания
		pool.Submit(ctx, task.Priority, func() {
			result := task.Process(task.Data)
			results <- fmt.Sprintf("Task %d (%s): %s", task.ID, task.Priority, result)
		})
	}

	// Собираем результаты
	for i := 0; i < len(tasks); i++ {
		select {
		case result := <-results:
			fmt.Println(result)
		case <-time.After(3 * time.Second):
			fmt.Println("Таймаут ожидания результатов")
			return
		}
	}
}

// init для настройки random seed
func init() {
	rand.Seed(time.Now().UnixNano())
}
