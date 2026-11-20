package growingruntime

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func drainDeleteReplay(ctx context.Context, view walview.VChannelWALView, apply func(*streamingpb.TransformLogEntry) error) error {
	if view.BaseTransformTimeTick == 0 {
		return nil
	}
	if view.TransformLogStream == nil {
		return wal.ErrTransformLogVChannelUnavailable
	}
	handler := newDeleteReplayHandler()
	subscription, err := view.TransformLogStream.Subscribe(ctx, wal.TransformLogSubscriptionOption{
		VChannel:           view.VChannel,
		StartAfterTimeTick: view.DeleteReplayStartAfterTimeTick,
		EndTimeTick:        view.BaseTransformTimeTick,
		Handler:            handler,
	})
	if err != nil {
		return err
	}
	defer func() {
		handler.abort()
		_ = subscription.Close()
	}()

	for {
		select {
		case event, ok := <-handler.events:
			if !ok {
				if err := ctx.Err(); err != nil {
					return err
				}
				if e := view.TransformLogStream.Error(); e != nil {
					return e
				}
				return merr.WrapErrServiceNotReadyMsg("bounded transform replay closed before its end frontier")
			}
			if event.Entry != nil {
				if err := apply(event.Entry); err != nil {
					return err
				}
			}
			if event.SyncUp != nil {
				if event.SyncUp.TimeTick < view.BaseTransformTimeTick {
					return merr.WrapErrServiceNotReadyMsg("incomplete bounded transform replay")
				}
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type deleteReplayHandler struct {
	events  chan wal.TransformLogStreamEvent
	aborted chan struct{}

	abortOnce sync.Once
	closeOnce sync.Once
}

func newDeleteReplayHandler() *deleteReplayHandler {
	return &deleteReplayHandler{
		events:  make(chan wal.TransformLogStreamEvent, 16),
		aborted: make(chan struct{}),
	}
}

func (h *deleteReplayHandler) Handle(event wal.TransformLogStreamEvent) {
	select {
	case h.events <- event:
	case <-h.aborted:
	}
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
