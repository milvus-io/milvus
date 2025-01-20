package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service/broadcast"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// BroadcastService is the interface of the broadcast service.
type BroadcastService interface {
	streamingpb.StreamingCoordBroadcastServiceServer
}

// NewBroadcastService creates a new broadcast service.
func NewBroadcastService(bc *syncutil.Future[broadcaster.Broadcaster]) BroadcastService {
	return &broadcastServceImpl{
		broadcaster: bc,
	}
}

// broadcastServiceeeeImpl is the implementation of the broadcast service.
type broadcastServceImpl struct {
	broadcaster *syncutil.Future[broadcaster.Broadcaster]
}

// Broadcast broadcasts the message to all channels.
func (s *broadcastServceImpl) Broadcast(ctx context.Context, req *streamingpb.BroadcastRequest) (*streamingpb.BroadcastResponse, error) {
	broadcaster, err := s.broadcaster.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	results, err := broadcaster.Broadcast(ctx, message.NewBroadcastMutableMessageBeforeAppend(req.Message.Payload, req.Message.Properties))
	if err != nil {
		return nil, err
	}
	protoResult := make(map[string]*streamingpb.ProduceMessageResponseResult, len(results.AppendResults))
	for vchannel, result := range results.AppendResults {
		protoResult[vchannel] = result.IntoProto()
	}
	return &streamingpb.BroadcastResponse{
		BroadcastId: results.BroadcastID,
		Results:     protoResult,
	}, nil
}

// Ack acknowledges the message at the specified vchannel.
func (s *broadcastServceImpl) Ack(ctx context.Context, req *streamingpb.BroadcastAckRequest) (*streamingpb.BroadcastAckResponse, error) {
	broadcaster, err := s.broadcaster.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := broadcaster.Ack(ctx, types.BroadcastAckRequest{
		BroadcastID: req.BroadcastId,
		VChannel:    req.Vchannel,
	}); err != nil {
		return nil, err
	}
	return &streamingpb.BroadcastAckResponse{}, nil
}

func (s *broadcastServceImpl) Watch(svr streamingpb.StreamingCoordBroadcastService_WatchServer) error {
	broadcaster, err := s.broadcaster.GetWithContext(svr.Context())
	if err != nil {
		return err
	}
	watcher, err := broadcaster.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	server := broadcast.NewBroadcastWatchServer(watcher, svr)
	return server.Execute()
}
