package observe

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// Observer receives QueryView observability events from owner-layer code.
type Observer interface {
	Observe(context.Context, Event)
}

// Registry fanouts QueryView observability events to registered observers.
type Registry struct {
	mu        sync.RWMutex
	observers []Observer
}

func NewRegistry(observers ...Observer) *Registry {
	copied := make([]Observer, len(observers))
	copy(copied, observers)
	return &Registry{observers: copied}
}

func (r *Registry) Register(observer Observer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observers = append(r.observers, observer)
}

func (r *Registry) Observe(ctx context.Context, event Event) {
	r.mu.RLock()
	observers := make([]Observer, len(r.observers))
	copy(observers, r.observers)
	r.mu.RUnlock()

	for _, observer := range observers {
		observer.Observe(ctx, event)
	}
}

var defaultRegistry = NewRegistry(LogObserver{})

func Register(observer Observer) {
	defaultRegistry.Register(observer)
}

func Observe(ctx context.Context, event Event) {
	defaultRegistry.Observe(ctx, event)
}

// LogObserver writes QueryView events to mlog.
type LogObserver struct{}

func (LogObserver) Observe(ctx context.Context, event Event) {
	level := event.LogLevel()
	if !mlog.LevelEnabled(level) {
		return
	}
	mlog.Log(ctx, level, "query view event", FieldEvent(event))
}
