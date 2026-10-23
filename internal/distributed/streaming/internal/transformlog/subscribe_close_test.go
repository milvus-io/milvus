package transformlog

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

type subscribeCloseHandler struct {
	*recordingHandler
}

func (h *subscribeCloseHandler) Close() { h.recordingHandler.Close() }

func TestResumableSubscribeCloseCreationResult(t *testing.T) {
	for _, test := range []struct {
		name              string
		created           bool
		blockHandlerClose bool
	}{
		{name: "pending with ready and done closed"},
		{name: "pending while handler Close is running", blockHandlerClose: true},
		{name: "creation completed before Close", created: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Hold Subscribe before it selects, so the result cannot depend on
			// whether ready or done happens to win a scheduling race.
			registered := make(chan *resumableSubscription, 1)
			resumeSubscribe := make(chan struct{})
			var original func(*resumableStream, wal.TransformLogSubscriptionOption) *resumableSubscription
			patch := mockey.Mock((*resumableStream).newSubscription).Origin(&original).To(func(s *resumableStream, opt wal.TransformLogSubscriptionOption) *resumableSubscription {
				sub := original(s, opt)
				registered <- sub
				<-resumeSubscribe
				return sub
			}).Build()
			defer patch.UnPatch()
			closeEntered, releaseClose := make(chan struct{}), make(chan struct{})
			closePatch := mockey.Mock((*subscribeCloseHandler).Close).To(func(*subscribeCloseHandler) {
				close(closeEntered)
				<-releaseClose
			}).Build()
			defer closePatch.UnPatch()
			defer func() {
				select {
				case <-releaseClose:
				default:
					close(releaseClose)
				}
			}()
			stream := NewResumableStream(t.Context(), "p", func(ctx context.Context, _ string) (wal.TransformLogStream, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			})
			type result struct {
				sub wal.TransformLogSubscription
				err error
			}
			results := make(chan result, 1)
			go func() {
				sub, err := stream.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: &subscribeCloseHandler{newRecordingHandler()}})
				results <- result{sub, err}
			}()
			sub := receiveSubscribeClose(t, registered)
			if test.created {
				sub.markReady(nil)
			}
			require.NoError(t, stream.Close())
			receiveSubscribeClose(t, closeEntered)
			if !test.blockHandlerClose {
				close(releaseClose)
				receiveSubscribeClose(t, sub.done)
			}
			close(resumeSubscribe)
			got := receiveSubscribeClose(t, results)
			if test.created {
				require.NoError(t, got.err)
				require.Same(t, sub, got.sub)
			} else {
				require.Nil(t, got.sub)
				require.Error(t, got.err)
				require.True(t, status.AsStreamingError(got.err).IsOnShutdown())
				// A late creation completion cannot overwrite Close's result.
				sub.markReady(nil)
				require.True(t, status.AsStreamingError(sub.readyErr).IsOnShutdown())
			}
			require.NoError(t, sub.Error(), "normal shutdown is distinct from aborted creation")
			if test.blockHandlerClose {
				close(releaseClose)
				receiveSubscribeClose(t, sub.done)
			}
		})
	}
}

func receiveSubscribeClose[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("subscription creation/close did not complete")
		var zero T
		return zero
	}
}
