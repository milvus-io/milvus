package transformlog

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestSubscribePendingReportsErrorOncePerAttempt(t *testing.T) {
	for _, firstCallback := range []bool{true, false} {
		name := "synchronous_only_then_callback"
		if firstCallback {
			name = "callback_then_synchronous_only"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			stream := &resumableStream{
				ctx: ctx, pchannel: "p",
				subscriptions: make(map[int64]*resumableSubscription),
			}
			underlying := newFakeTransformLogStream()
			handler, siblingHandler := newRecordingHandler(), newRecordingHandler()
			sub := newResumableSubscription(stream, 1, wal.TransformLogSubscriptionOption{
				VChannel: "p_1v0", StartAfterTimeTick: 10, Handler: handler,
			})
			sibling := newResumableSubscription(stream, 2, wal.TransformLogSubscriptionOption{
				VChannel: "p_2v0", StartAfterTimeTick: 20, Handler: siblingHandler,
			})
			stream.subscriptions[sibling.ID()] = sibling
			defer sub.Close()
			defer sibling.Close()
			require.NoError(t, stream.subscribePending(ctx, cancel, underlying))
			stream.subscriptions[sub.ID()] = sub

			failure := status.NewUnknownError("corrupt retained chunk")
			attempt := 0
			var original func(*fakeTransformLogStream, context.Context, wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error)
			patch := mockey.Mock((*fakeTransformLogStream).Subscribe).Origin(&original).To(func(s *fakeTransformLogStream, ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
				if opt.VChannel != sub.VChannel() || attempt >= 2 {
					return original(s, ctx, opt)
				}
				callback := firstCallback
				if attempt == 1 {
					callback = !callback
				}
				attempt++
				if callback {
					// Model the remote client's error callback followed by the same
					// error returned from Subscribe, with a healthy sibling RPC.
					delivered := make(chan error, 1)
					go func() {
						delivered <- opt.Handler.Handle(wal.TransformLogStreamEvent{Err: failure})
					}()
					require.NoError(t, <-delivered)
				}
				return nil, failure
			}).Build()
			defer patch.UnPatch()

			for i := 0; i < 2; i++ {
				require.NoError(t, stream.subscribePending(ctx, cancel, underlying))
				event := recvStreamEvent(t, handler.events)
				require.ErrorIs(t, event.Err, failure)
				require.Equal(t, sub.ID(), event.SubscriptionID)
				require.Equal(t, sub.VChannel(), event.VChannel)
				require.Empty(t, handler.events, "one failure must produce one notification per attempt")
				require.Equal(t, i+1, attempt)
				require.NoError(t, sub.Error())
				require.NoError(t, sub.readyErr)
				select {
				case <-sub.ready:
				default:
					t.Fatal("logical subscription creation did not finish")
				}
				require.Same(t, sub, stream.subscriptions[sub.ID()])
				require.EqualValues(t, 10, sub.option().StartAfterTimeTick)
				require.False(t, stream.accepted.Load(), "errors must not count as accepted progress")
				require.True(t, sibling.hasRemote())
				require.NoError(t, ctx.Err())
			}

			// A later attempt can succeed and deliver data and runtime errors.
			require.NoError(t, stream.subscribePending(ctx, cancel, underlying))
			require.True(t, sub.hasRemote())
			id := underlying.subscriptionID(sub.VChannel())
			underlying.emit(wal.TransformLogStreamEvent{SubscriptionID: id, Entry: &streamingpb.TransformLogEntry{TimeTick: 11}})
			require.EqualValues(t, 11, recvStreamEvent(t, handler.events).Entry.GetTimeTick())
			underlying.emit(wal.TransformLogStreamEvent{SubscriptionID: id, SyncUp: &wal.TransformLogSyncUp{TimeTick: 12}})
			require.EqualValues(t, 12, recvStreamEvent(t, handler.events).SyncUp.TimeTick)
			underlying.emit(wal.TransformLogStreamEvent{SubscriptionID: id, Err: failure})
			require.ErrorIs(t, recvStreamEvent(t, handler.events).Err, failure)
			require.EqualValues(t, 12, sub.option().StartAfterTimeTick)
			underlying.emit(wal.TransformLogStreamEvent{SubscriptionID: underlying.subscriptionID(sibling.VChannel()), Entry: &streamingpb.TransformLogEntry{TimeTick: 21}})
			require.EqualValues(t, 21, recvStreamEvent(t, siblingHandler.events).Entry.GetTimeTick())
			require.NoError(t, sub.Error())
			require.Empty(t, handler.events)
		})
	}
}
