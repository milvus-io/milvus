package broadcast

import (
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

type broadcastWatchGrpcServerHelper struct {
	streamingpb.StreamingCoordBroadcastService_WatchServer
}

// SendResourceKeyEvent sends the resource key event to client.
func (h *broadcastWatchGrpcServerHelper) SendResourceKeyEvent(ev *message.BroadcastEvent) error {
	return h.Send(&streamingpb.BroadcastWatchResponse{
		Response: &streamingpb.BroadcastWatchResponse_EventDone{
			EventDone: &streamingpb.BroadcastEventWatchResponse{
				Event: ev,
			},
		},
	})
}

// SendCloseResponse sends the close response to client.
func (h *broadcastWatchGrpcServerHelper) SendCloseResponse() error {
	return h.Send(&streamingpb.BroadcastWatchResponse{
		Response: &streamingpb.BroadcastWatchResponse_Close{
			Close: &streamingpb.CloseBroadcastWatchResponse{},
		},
	})
}
