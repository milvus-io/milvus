package wab

import (
	"sync"
	"time"
)

// MaintenanceManager periodically evicts expired messages from all registered
// write-ahead buffers with one shared background goroutine.
type MaintenanceManager struct {
	mu      sync.Mutex
	buffers map[*WriteAheadBuffer]struct{}
	closed  bool

	interval  time.Duration
	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewMaintenanceManager creates a shared write-ahead buffer maintenance manager.
func NewMaintenanceManager(interval time.Duration) *MaintenanceManager {
	if interval <= 0 {
		panic("write-ahead buffer maintenance interval must be positive")
	}

	manager := &MaintenanceManager{
		buffers:  make(map[*WriteAheadBuffer]struct{}),
		interval: interval,
		closeCh:  make(chan struct{}),
	}
	manager.wg.Add(1)
	go manager.background()
	return manager
}

func (m *MaintenanceManager) register(buffer *WriteAheadBuffer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		panic("register write-ahead buffer to closed maintenance manager")
	}
	m.buffers[buffer] = struct{}{}
}

func (m *MaintenanceManager) unregister(buffer *WriteAheadBuffer) {
	m.mu.Lock()
	delete(m.buffers, buffer)
	m.mu.Unlock()
}

func (m *MaintenanceManager) background() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.maintain()
		case <-m.closeCh:
			return
		}
	}
}

func (m *MaintenanceManager) maintain() {
	m.mu.Lock()
	buffers := make([]*WriteAheadBuffer, 0, len(m.buffers))
	for buffer := range m.buffers {
		buffers = append(buffers, buffer)
	}
	m.mu.Unlock()

	for _, buffer := range buffers {
		buffer.evictExpiredMessages()
	}
}

// Close stops the shared maintenance goroutine.
func (m *MaintenanceManager) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		clear(m.buffers)
		m.mu.Unlock()
		close(m.closeCh)
		m.wg.Wait()
	})
}
