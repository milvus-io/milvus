package broadcast

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var errWatcherClosed = errors.New("watcher is closed")

// newResumingWatcher create a new resuming watcher.
func newResumingWatcher(b WatcherBuilder, backoffConfig *typeutil.BackoffTimerConfig) *resumingWatcher {
	rw := &resumingWatcher{
		backgroundTask: syncutil.NewAsyncTaskNotifier[struct{}](),
		input:          make(chan *pendingEvent),
		evs:            &pendingEvents{evs: make(map[string]*pendingEvent)},
		watcherBuilder: b, // TODO: enable local watcher here.
	}
	rw.SetLogger(logger)
	go rw.execute(backoffConfig)
	return rw
}

// resumingWatcher is a watcher that can resume the watcher when it is unavailable.
type resumingWatcher struct {
	log.Binder
	backgroundTask *syncutil.AsyncTaskNotifier[struct{}]
	input          chan *pendingEvent
	evs            *pendingEvents
	watcherBuilder WatcherBuilder
}

// ObserveResourceKeyEvent observes the resource key event.
func (r *resumingWatcher) ObserveResourceKeyEvent(ctx context.Context, ev *message.BroadcastEvent) error {
	notifier := make(chan struct{})
	select {
	case <-r.backgroundTask.Context().Done():
		return errWatcherClosed
	case <-ctx.Done():
		return ctx.Err()
	case r.input <- &pendingEvent{
		ev:       ev,
		notifier: []chan<- struct{}{notifier},
	}:
	}
	select {
	case <-r.backgroundTask.Context().Done():
		return errWatcherClosed
	case <-ctx.Done():
		return ctx.Err()
	case <-notifier:
		return nil
	}
}

func (r *resumingWatcher) Close() {
	r.backgroundTask.Cancel()
	r.backgroundTask.BlockUntilFinish()
}

func (r *resumingWatcher) execute(backoffConfig *typeutil.BackoffTimerConfig) {
	backoff := typeutil.NewBackoffTimer(backoffConfig)
	nextTimer := time.After(0)
	var watcher Watcher
	defer func() {
		if watcher != nil {
			watcher.Close()
		}
		r.backgroundTask.Finish(struct{}{})
	}()

	for {
		var eventChan <-chan *message.BroadcastEvent
		if watcher != nil {
			eventChan = watcher.EventChan()
		}

		select {
		case <-r.backgroundTask.Context().Done():
			return
		case ev := <-r.input:
			if !r.evs.AddPendingEvent(ev) && watcher != nil {
				if err := watcher.ObserveResourceKeyEvent(r.backgroundTask.Context(), ev.ev); err != nil {
					watcher.Close()
					watcher = nil
				}
			}
		case ev, ok := <-eventChan:
			if !ok {
				watcher.Close()
				watcher = nil
				break
			}
			r.evs.Notify(ev)
		case <-nextTimer:
			var err error
			if watcher, err = r.createNewWatcher(); err != nil {
				r.Logger().Warn("create new watcher failed", zap.Error(err))
				break
			}
			r.Logger().Info("create new watcher successful")
			backoff.DisableBackoff()
			nextTimer = nil
		}
		if watcher == nil {
			backoff.EnableBackoff()
			var interval time.Duration
			nextTimer, interval = backoff.NextTimer()
			r.Logger().Warn("watcher is unavailable, resuming it after interval", zap.Duration("interval", interval))
		}
	}
}

func (r *resumingWatcher) createNewWatcher() (Watcher, error) {
	watcher, err := r.watcherBuilder.Build(r.backgroundTask.Context())
	if err != nil {
		return nil, err
	}
	if err := r.evs.SendAll(r.backgroundTask.Context(), watcher); err != nil {
		watcher.Close()
		return nil, errors.Wrapf(err, "send all pending events to watcher failed")
	}
	return watcher, nil
}

type pendingEvents struct {
	evs map[string]*pendingEvent
}

// AddPendingEvent adds a pending event.
// Return true if the event is already in the pending events.
func (evs *pendingEvents) AddPendingEvent(ev *pendingEvent) bool {
	id := message.UniqueKeyOfBroadcastEvent(ev.ev)
	if existEv, ok := evs.evs[id]; ok {
		existEv.notifier = append(existEv.notifier, ev.notifier...)
		return true
	} else {
		evs.evs[id] = ev
		return false
	}
}

func (evs *pendingEvents) Notify(ev *message.BroadcastEvent) {
	id := message.UniqueKeyOfBroadcastEvent(ev)
	if existEv, ok := evs.evs[id]; ok {
		for _, notifier := range existEv.notifier {
			close(notifier)
		}
		delete(evs.evs, id)
	}
}

func (evs *pendingEvents) SendAll(ctx context.Context, w Watcher) error {
	for _, ev := range evs.evs {
		if err := w.ObserveResourceKeyEvent(ctx, ev.ev); err != nil {
			return err
		}
	}
	return nil
}

type pendingEvent struct {
	ev       *message.BroadcastEvent
	notifier []chan<- struct{}
}
