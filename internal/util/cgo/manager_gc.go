package cgo

import "go.uber.org/atomic"

const (
	defaultGCPoolSize = 10
)

type gcFutureManager struct {
	pending      []basicFuture
	pendingCount *atomic.Int64
	register     chan basicFuture
	publisher    chan basicFuture
}

func newGCManager() *gcFutureManager {
	manager := &gcFutureManager{
		pending:      make([]basicFuture, 0),
		pendingCount: atomic.NewInt64(0),
		register:     make(chan basicFuture, defaultRegisterBuf),
		publisher:    make(chan basicFuture, defaultGCPoolSize),
	}
	return manager
}

func (m *gcFutureManager) PendingCount() int64 {
	return m.pendingCount.Load()
}

func (m *gcFutureManager) Register(c basicFuture) {
	m.register <- c
}

func (m *gcFutureManager) Run() {
	go m.doGC()
	for i := 0; i < defaultGCPoolSize; i++ {
		go m.gcWork()
	}
}

func (m *gcFutureManager) doGC() {
	var publisher chan basicFuture
	var nextGCFuture basicFuture
	for {
		if len(m.pending) > 0 && nextGCFuture == nil {
			nextGCFuture = m.pending[0]
			m.pending = m.pending[1:]
			publisher = m.publisher
		}

		select {
		case newFuture := <-m.register:
			m.pending = append(m.pending, newFuture)
			m.pendingCount.Inc()
		case publisher <- nextGCFuture:
			publisher = nil
			nextGCFuture = nil
		}
	}
}

func (m *gcFutureManager) gcWork() {
	for f := range m.publisher {
		f.releaseWhenUnderlyingDone()
		m.pendingCount.Dec()
	}
}
