package broadcaster

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// newWatcher creates a new watcher.
func newWatcher(broadcaster *broadcasterImpl) *watcherImpl {
	w := &watcherImpl{
		watcherBGNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		pendingEvents:     make(map[string]*message.BroadcastEvent, 0),
		broadcasterImpl:   broadcaster,
		version:           0,
		input:             make(chan *message.BroadcastEvent),
		output:            make(chan *message.BroadcastEvent),
	}
	go w.execute()
	return w
}

// watcherImpl implement the Watcher interface.
type watcherImpl struct {
	watcherBGNotifier *syncutil.AsyncTaskNotifier[struct{}]
	pendingEvents     map[string]*message.BroadcastEvent
	*broadcasterImpl
	version int
	input   chan *message.BroadcastEvent
	output  chan *message.BroadcastEvent
}

func (w *watcherImpl) ObserveResourceKeyEvent(ctx context.Context, ev *message.BroadcastEvent) error {
	select {
	case w.input <- ev:
		return nil
	case <-w.backgroundTaskNotifier.Context().Done():
		return w.backgroundTaskNotifier.Context().Err()
	case <-w.watcherBGNotifier.Context().Done():
		return w.watcherBGNotifier.Context().Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *watcherImpl) EventChan() <-chan *message.BroadcastEvent {
	return w.output
}

func (w *watcherImpl) execute() {
	defer func() {
		close(w.output)
		w.watcherBGNotifier.Finish(struct{}{})
	}()
	for {
		ch := w.manager.WatchAtVersion(w.version)
		select {
		case <-w.backgroundTaskNotifier.Context().Done():
			w.Logger().Info("watcher is exit because of broadcaseter is closing", zap.Int("version", w.version))
			return
		case <-w.watcherBGNotifier.Context().Done():
			w.Logger().Info("watcher is exit because of watcher itself is closing", zap.Int("version", w.version))
			return
		case <-ch:
			w.update()
		case ev := <-w.input:
			w.pendingEvents[message.UniqueKeyOfBroadcastEvent(ev)] = ev
			w.update()
		}
	}
}

func (w *watcherImpl) update() {
	w.version = w.manager.CurrentVersion()
	newPendingEvent := make(map[string]*message.BroadcastEvent, len(w.pendingEvents))
	for key, pendingEvent := range w.pendingEvents {
		switch ev := pendingEvent.Event.(type) {
		case *messagespb.BroadcastEvent_ResourceKeyAckAll:
			task, ok := w.manager.GetBroadcastTaskByResourceKey(message.NewResourceKeyFromProto(ev.ResourceKeyAckAll.ResourceKey))
			if !ok || task.IsAllAcked() {
				w.output <- pendingEvent
				continue
			}
		case *messagespb.BroadcastEvent_ResourceKeyAckOne:
			task, ok := w.manager.GetBroadcastTaskByResourceKey(message.NewResourceKeyFromProto(ev.ResourceKeyAckOne.ResourceKey))
			if !ok || task.IsAcked() {
				w.output <- pendingEvent
				continue
			}
		}
		newPendingEvent[key] = pendingEvent
	}
	w.pendingEvents = newPendingEvent
}

func (w *watcherImpl) Close() {
	w.watcherBGNotifier.Cancel()
	w.watcherBGNotifier.BlockUntilFinish()
}
