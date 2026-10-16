package transformlog

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestRetryableSubscriptionError(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, grpcstatus.Error(codes.Canceled, "canceled"), status.AsStreamingError(context.Canceled), status.AsStreamingError(context.DeadlineExceeded), status.NewOnShutdownError("owner closed"), status.NewChannelFenced("p"), status.NewChannelNotExist("p"), status.NewUnmatchedChannelTerm("p", 1, 2)} {
		require.True(t, isRetryableSubscriptionError(err), "%v", err)
	}
	for _, err := range []error{nil, wal.ErrTransformLogInvalidReadOption, wal.ErrTransformLogVChannelUnavailable, wal.ErrTransformLogStartPointTruncated, status.NewUnknownError("corrupt retained chunk"), status.NewUnknownError("read failed: context canceled"), status.NewInvalidArgument("invalid start")} {
		require.False(t, isRetryableSubscriptionError(err), "%v", err)
	}
}

func TestResumableStreamRetriesSubscribeCancellation(t *testing.T) {
	for _, err := range []error{context.Canceled, status.AsStreamingError(context.Canceled)} {
		t.Run(err.Error(), func(t *testing.T) {
			first := walsummary.NewStream(walsummary.NewManager(walsummary.ManagerConfig{}))
			second := walsummary.NewStream(walsummary.NewManager(walsummary.ManagerConfig{}))
			defer first.Close()
			defer second.Close()
			var original func(*walsummary.Stream, context.Context, wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error)
			patch := mockey.Mock((*walsummary.Stream).Subscribe).Origin(&original).To(func(s *walsummary.Stream, ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
				if s == first {
					return nil, err
				}
				return original(s, ctx, opt)
			}).Build()
			defer patch.UnPatch()
			calls := 0
			stream := NewResumableStream(context.Background(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
				calls++
				if calls == 1 {
					return first, nil
				}
				return second, nil
			})
			defer stream.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			handler := newRecordingHandler()
			sub, e := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v", Handler: handler})
			require.NoError(t, e)
			defer sub.Close()
			event := recvStreamEvent(t, handler.events)
			require.NoError(t, event.Err)
			require.NotNil(t, event.SyncUp)
			select {
			case <-first.Done():
			default:
				t.Fatal("failed attempt was not closed")
			}
		})
	}
}

func TestResumableStreamPreservesSemanticSubscriptionError(t *testing.T) {
	manager := walsummary.NewManager(walsummary.ManagerConfig{})
	underlying := walsummary.NewStream(manager)
	options := make(chan wal.TransformLogSubscriptionOption, 1)
	var original func(*walsummary.Stream, context.Context, wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error)
	patch := mockey.Mock((*walsummary.Stream).Subscribe).Origin(&original).To(func(s *walsummary.Stream, ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
		sub, err := original(s, ctx, opt)
		options <- opt
		return sub, err
	}).Build()
	defer patch.UnPatch()
	stream := NewResumableStream(context.Background(), "p", func(context.Context, string) (wal.TransformLogStream, error) { return underlying, nil })
	defer stream.Close()
	handler := newRecordingHandler()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v", Handler: handler})
	require.NoError(t, err)
	defer sub.Close()
	require.NotNil(t, recvStreamEvent(t, handler.events).SyncUp)
	opt := <-options
	fatal := status.NewUnknownError("corrupt retained chunk")
	require.NoError(t, opt.Handler.Handle(wal.TransformLogStreamEvent{Err: fatal}))
	require.ErrorIs(t, recvStreamEvent(t, handler.events).Err, fatal)
	select {
	case <-underlying.Done():
		t.Fatal("semantic error must not trigger transport retry")
	default:
	}
	// Exercise a successful checkpoint without manufacturing progress on the error.
	require.NoError(t, opt.Handler.Handle(wal.TransformLogStreamEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 10}}))
	require.Equal(t, uint64(10), recvStreamEvent(t, handler.events).Entry.GetTimeTick())
}

func TestResumableStreamClosesFailedConnectionAttempt(t *testing.T) {
	attempts := make(chan context.Context, 4)
	second := walsummary.NewStream(walsummary.NewManager(walsummary.ManagerConfig{}))
	calls := 0
	stream := NewResumableStream(context.Background(), "p", func(ctx context.Context, _ string) (wal.TransformLogStream, error) {
		calls++
		attempts <- ctx
		if calls == 1 {
			return nil, status.NewOnShutdownError("owner is migrating")
		}
		return second, nil
	})
	defer stream.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	handler := newRecordingHandler()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v", Handler: handler})
	require.NoError(t, err)
	defer sub.Close()
	firstCtx := <-attempts
	require.ErrorIs(t, firstCtx.Err(), context.Canceled)
	require.NotNil(t, recvStreamEvent(t, handler.events).SyncUp)
	require.NoError(t, stream.Close())
	require.ErrorIs(t, (<-attempts).Err(), context.Canceled)
	select {
	case <-stream.Done():
	default:
		t.Fatal("explicit close did not finish")
	}
}

func TestResumableStreamDoesNotResumePastRejectedEntry(t *testing.T) {
	options := make(chan wal.TransformLogSubscriptionOption, 4)
	var original func(*walsummary.Stream, context.Context, wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error)
	capture := mockey.Mock((*walsummary.Stream).Subscribe).Origin(&original).To(func(s *walsummary.Stream, ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
		sub, err := original(s, ctx, opt)
		options <- opt
		return sub, err
	}).Build()
	defer capture.UnPatch()
	rejected := status.NewUnknownError("consumer rejected entry")
	delivery := mockey.Mock((*recordingHandler).Handle).To(func(h *recordingHandler, event wal.TransformLogStreamEvent) error {
		if event.Entry != nil && event.Entry.TimeTick == 20 {
			return rejected
		}
		h.events <- event
		return nil
	}).Build()
	defer delivery.UnPatch()
	stream := NewResumableStream(context.Background(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
		return walsummary.NewStream(walsummary.NewManager(walsummary.ManagerConfig{})), nil
	})
	defer stream.Close()
	handler := newRecordingHandler()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v", Handler: handler})
	require.NoError(t, err)
	defer sub.Close()
	first := <-options
	require.NoError(t, first.Handler.Handle(wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: 10}}))
	require.ErrorIs(t, first.Handler.Handle(wal.TransformLogStreamEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 20}}), rejected)
	require.NoError(t, first.Handler.Handle(wal.TransformLogStreamEvent{Err: context.Canceled}))
	select {
	case next := <-options:
		require.Equal(t, uint64(10), next.StartAfterTimeTick)
	case <-ctx.Done():
		t.Fatal("did not resume after cancellation")
	}
}
