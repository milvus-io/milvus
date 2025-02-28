package broadcast

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestBroadcastWatch(t *testing.T) {
	resource.InitForTest()
	w := mock_broadcaster.NewMockWatcher(t)
	input := make(chan *message.BroadcastEvent, 5)
	output := make(chan *message.BroadcastEvent, 5)
	w.EXPECT().ObserveResourceKeyEvent(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ev *messagespb.BroadcastEvent) error {
		output <- ev
		return nil
	})
	w.EXPECT().EventChan().Return(output)
	streamServer := mock_streamingpb.NewMockStreamingCoordBroadcastService_WatchServer(t)
	streamServer.EXPECT().Context().Return(context.Background())
	closed := false
	streamServer.EXPECT().Recv().RunAndReturn(func() (*streamingpb.BroadcastWatchRequest, error) {
		if closed {
			return nil, io.EOF
		}
		ev, ok := <-input
		if !ok {
			closed = true
			return &streamingpb.BroadcastWatchRequest{
				Command: &streamingpb.BroadcastWatchRequest_Close{
					Close: &streamingpb.CloseBroadcastWatchRequest{},
				},
			}, nil
		}
		return &streamingpb.BroadcastWatchRequest{
			Command: &streamingpb.BroadcastWatchRequest_CreateEventWatch{
				CreateEventWatch: &streamingpb.BroadcastCreateEventWatchRequest{
					Event: ev,
				},
			},
		}, nil
	})

	streamOutput := make(chan *message.BroadcastEvent, 5)
	streamServer.EXPECT().Send(mock.Anything).RunAndReturn(func(bwr *streamingpb.BroadcastWatchResponse) error {
		if bwr.GetEventDone() != nil {
			streamOutput <- bwr.GetEventDone().Event
		}
		return nil
	})
	s := NewBroadcastWatchServer(w, streamServer)

	input <- message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey("c1"))
	input <- message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey("c2"))
	input <- message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey("c3"))
	input <- message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey("c4"))
	done := make(chan struct{})
	go func() {
		s.Execute()
		close(done)
	}()
	for i := 0; i < 4; i++ {
		<-streamOutput
	}
	close(input)
	<-done
}
