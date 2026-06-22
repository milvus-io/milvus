package recovery

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type liveObserverRegistry struct {
	mu        sync.Mutex
	observers map[string]map[walview.VChannelLiveObserver]struct{}
}

func newLiveObserverRegistry() *liveObserverRegistry {
	return &liveObserverRegistry{
		observers: make(map[string]map[walview.VChannelLiveObserver]struct{}),
	}
}

func (r *liveObserverRegistry) Register(vchannel string, observer walview.VChannelLiveObserver) {
	if observer == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.observers[vchannel] == nil {
		r.observers[vchannel] = make(map[walview.VChannelLiveObserver]struct{})
	}
	r.observers[vchannel][observer] = struct{}{}
}

func (r *liveObserverRegistry) Dispatch(ctx context.Context, msg message.ImmutableMessage) {
	r.DispatchEvent(ctx, msg.VChannel(), walview.VChannelResourceEvent{Message: msg})
}

func (r *liveObserverRegistry) DispatchEvent(ctx context.Context, vchannel string, event walview.VChannelResourceEvent) {
	for _, observer := range r.snapshot(vchannel) {
		if observer.ObserveEvent(ctx, event) {
			continue
		}
		r.unregister(vchannel, observer)
	}
}

func (r *liveObserverRegistry) CloseVChannel(vchannel string) {
	observers := r.take(vchannel)
	for _, observer := range observers {
		observer.Close()
	}
}

func (r *liveObserverRegistry) snapshot(vchannel string) []walview.VChannelLiveObserver {
	r.mu.Lock()
	defer r.mu.Unlock()
	registered := r.observers[vchannel]
	if len(registered) == 0 {
		return nil
	}
	observers := make([]walview.VChannelLiveObserver, 0, len(registered))
	for observer := range registered {
		observers = append(observers, observer)
	}
	return observers
}

func (r *liveObserverRegistry) take(vchannel string) []walview.VChannelLiveObserver {
	r.mu.Lock()
	defer r.mu.Unlock()
	registered := r.observers[vchannel]
	if len(registered) == 0 {
		return nil
	}
	observers := make([]walview.VChannelLiveObserver, 0, len(registered))
	for observer := range registered {
		observers = append(observers, observer)
	}
	delete(r.observers, vchannel)
	return observers
}

func (r *liveObserverRegistry) unregister(vchannel string, observer walview.VChannelLiveObserver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	registered := r.observers[vchannel]
	if len(registered) == 0 {
		return
	}
	delete(registered, observer)
	if len(registered) == 0 {
		delete(r.observers, vchannel)
	}
}
