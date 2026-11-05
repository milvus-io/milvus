package transformlog

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestEventStreamSubscribeCloseCreationResult(t *testing.T) {
	for _, test := range []struct {
		name              string
		acknowledge       bool
		blockHandlerClose bool
	}{
		{name: "pending with ready and done closed"},
		{name: "pending while handler Close is running", blockHandlerClose: true},
		{name: "create acknowledged before Close", acknowledge: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			closeEntered, releaseClose := make(chan struct{}), make(chan struct{})
			closePatch := mockey.Mock((*closeTestHandler).Close).To(func(*closeTestHandler) {
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
			stream := newCloseTestStream(t, t.Context(), func(rpc streamingpb.StreamingNodeHandlerService_SubscribeTransformServer) error {
				for {
					req, err := rpc.Recv()
					if err != nil {
						return err
					}
					if create := req.GetCreate(); create != nil && test.acknowledge {
						if err := rpc.Send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_Create{
							Create: &streamingpb.CreateTransformSubscriptionResponse{SubscriptionId: create.GetSubscriptionId()},
						}}); err != nil {
							return err
						}
					}
					if req.GetCloseStream() != nil {
						return rpc.Send(&streamingpb.TransformResponse{Response: &streamingpb.TransformResponse_CloseStream{
							CloseStream: &streamingpb.CloseTransformStreamResponse{},
						}})
					}
				}
			})
			// Let the real Send release its lock, then delay Subscribe's select
			// until the real RPC close handshake has completed.
			sent := make(chan *eventSubscription, 1)
			resumeSubscribe := make(chan struct{})
			var original func(*EventStream, *streamingpb.TransformRequest) error
			patch := mockey.Mock((*EventStream).send).Origin(&original).To(func(s *EventStream, req *streamingpb.TransformRequest) error {
				err := original(s, req)
				if create := req.GetCreate(); create != nil && err == nil {
					sent <- s.getSubscription(create.GetSubscriptionId())
					<-resumeSubscribe
				}
				return err
			}).Build()
			defer patch.UnPatch()
			type result struct {
				sub wal.TransformLogSubscription
				err error
			}
			results := make(chan result, 1)
			go func() {
				sub, err := stream.Subscribe(t.Context(), wal.TransformLogSubscriptionOption{VChannel: "v", Handler: &closeTestHandler{newRecordingHandler()}})
				results <- result{sub, err}
			}()
			sub := recvEvent(t, sent)
			if test.acknowledge {
				recvEvent(t, sub.ready)
			}
			require.NoError(t, stream.Close())
			recvEvent(t, closeEntered)
			if !test.blockHandlerClose {
				close(releaseClose)
				recvEvent(t, sub.done)
			}
			close(resumeSubscribe)
			got := recvEvent(t, results)
			if test.acknowledge {
				require.NoError(t, got.err)
				require.Same(t, sub, got.sub)
			} else {
				require.Nil(t, got.sub)
				require.Error(t, got.err)
				require.True(t, status.AsStreamingError(got.err).IsOnShutdown())
			}
			require.NoError(t, sub.Error())
			if test.blockHandlerClose {
				close(releaseClose)
				recvEvent(t, sub.done)
			}
		})
	}
}
