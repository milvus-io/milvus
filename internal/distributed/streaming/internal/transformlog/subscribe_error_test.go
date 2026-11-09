package transformlog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

func TestResumableStreamRetriesSubscriptionCreationWithoutAdvancingCursor(t *testing.T) {
	first, second := newFakeTransformLogStream(), newFakeTransformLogStream()
	first.subscribeErrByVChan["v"] = status.NewUnknownError("read unavailable")
	attempt := 0
	stream := NewResumableStream(t.Context(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
		attempt++
		if attempt == 1 {
			return first, nil
		}
		return second, nil
	})
	defer stream.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	h := newRecordingHandler()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v", StartAfterTimeTick: 10, Handler: h})
	require.NoError(t, err)
	defer sub.Close()
	require.EqualValues(t, 10, second.startAfter("v"))
	require.Empty(t, h.events)
	select {
	case <-first.Done():
	default:
		t.Fatal("failed subscription attempt was not closed")
	}
}

func TestResumableStreamStopsWhenClientCloses(t *testing.T) {
	stream := NewResumableStream(t.Context(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
		return nil, handler.ErrClientClosed
	})
	_, err := stream.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: newRecordingHandler()})
	require.ErrorIs(t, err, handler.ErrClientClosed)
	select {
	case <-stream.Done():
	case <-time.After(time.Second):
		t.Fatal("closed handler client must end resuming")
	}
	require.ErrorIs(t, stream.Close(), handler.ErrClientClosed)
}
