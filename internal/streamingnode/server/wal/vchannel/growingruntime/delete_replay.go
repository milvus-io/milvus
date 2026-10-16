package growingruntime

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func drainDeleteReplay(ctx context.Context, view walview.VChannelWALView) ([]*streamingpb.TransformLogEntry, error) {
	if view.BaseTransformTimeTick == 0 {
		return nil, nil
	}
	if view.TransformLogStream == nil {
		return nil, wal.ErrTransformLogVChannelUnavailable
	}
	handler := newDeleteReplayHandler()
	subscription, err := view.TransformLogStream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           view.VChannel,
		StartAfterTimeTick: view.DeleteReplayStartAfterTimeTick,
		EndTimeTick:        view.BaseTransformTimeTick,
		Handler:            handler,
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		handler.abort()
		_ = subscription.Close()
	}()

	entries := make([]*streamingpb.TransformLogEntry, 0)
	for {
		select {
		case event, ok := <-handler.events:
			if !ok {
				return entries, handler.error()
			}
			if event.Entry != nil {
				entries = append(entries, event.Entry)
			}
			if event.SyncUp != nil {
				return entries, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

type deleteReplayHandler struct {
	events  chan wal.TransformLogStreamEvent
	aborted chan struct{}

	abortOnce sync.Once
	closeOnce sync.Once
	errMu     sync.Mutex
	err       error
}

func newDeleteReplayHandler() *deleteReplayHandler {
	return &deleteReplayHandler{
		events:  make(chan wal.TransformLogStreamEvent, 16),
		aborted: make(chan struct{}),
	}
}

func (h *deleteReplayHandler) Handle(event wal.TransformLogStreamEvent) error {
	if event.Err != nil {
		h.errMu.Lock()
		if h.err == nil {
			h.err = event.Err
		}
		h.errMu.Unlock()
		return nil
	}
	select {
	case h.events <- event:
	case <-h.aborted:
	}
	return nil
}

func (h *deleteReplayHandler) Close() {
	h.closeOnce.Do(func() {
		close(h.events)
	})
}

func (h *deleteReplayHandler) abort() {
	h.abortOnce.Do(func() {
		close(h.aborted)
	})
}

func (h *deleteReplayHandler) error() error {
	h.errMu.Lock()
	defer h.errMu.Unlock()
	return h.err
}
