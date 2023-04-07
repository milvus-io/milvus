package inproc

import (
	"github.com/milvus-io/milvus/internal/registry"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
)

type watcher[T types.Component] struct {
	inner registry.ServiceWatcher[T]

	evtCh     chan registry.SessionEvent[T]
	closeCh   chan struct{}
	component string
	s         *inProcServiceDiscovery
}

func newWatcher[T types.Component](s *inProcServiceDiscovery, inner registry.ServiceWatcher[T], component string) *watcher[T] {
	w := &watcher[T]{
		inner:     inner,
		component: component,
		s:         s,
		closeCh:   make(chan struct{}),
		evtCh:     make(chan registry.SessionEvent[T], 100),
	}
	w.start()
	return w
}

func (w *watcher[T]) Watch() <-chan registry.SessionEvent[T] {
	return w.inner.Watch()
}

func (w *watcher[T]) Stop() {
	w.inner.Stop()
}

func (w *watcher[T]) start() {
	go w.work()
}

func (w *watcher[T]) work() {
	for {
		select {
		case <-w.closeCh:
		case evt, ok := <-w.inner.Watch():
			if !ok {
				log.Warn("inproc watcher inner channel closed")
				return
			}
			w.processEvt(evt)
		}
	}
}

func (w *watcher[T]) processEvt(evt registry.SessionEvent[T]) {
	// only apply replace client when session type is `Add`
	if evt.EventType != registry.SessionAddEvent {
		w.evtCh <- evt
		return
	}
	service, ok := getComponent[T](w.s, w.component)
	if ok {
		//TODO add address check
		evt.Client = service
	}

	w.evtCh <- evt
}
