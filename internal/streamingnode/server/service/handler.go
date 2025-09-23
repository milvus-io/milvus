package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/service/handler/consumer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/service/handler/producer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

var _ HandlerService = (*handlerServiceImpl)(nil)

// NewHandlerService creates a new handler service.
func NewHandlerService(walManager walmanager.Manager) HandlerService {
	return &handlerServiceImpl{
		walManager: walManager,
	}
}

type HandlerService = streamingpb.StreamingNodeHandlerServiceServer

// handlerServiceImpl implements HandlerService.
// handlerServiceImpl is just a rpc level to handle incoming grpc.
// It should not handle any wal related logic, just
// 1. recv request and transfer param into wal
// 2. wait wal handling result and transform it into grpc response (convert error into grpc error)
// 3. send response to client.
type handlerServiceImpl struct {
	streamingpb.UnimplementedStreamingNodeHandlerServiceServer

	walManager walmanager.Manager
}

// GetReplicateCheckpoint returns the replicate checkpoint of the wal.
func (hs *handlerServiceImpl) GetReplicateCheckpoint(ctx context.Context, req *streamingpb.GetReplicateCheckpointRequest) (*streamingpb.GetReplicateCheckpointResponse, error) {
	wal, err := hs.walManager.GetAvailableWAL(types.NewPChannelInfoFromProto(req.GetPchannel()))
	if err != nil {
		return nil, err
	}
	cp, err := wal.GetReplicateCheckpoint()
	if err != nil {
		return nil, err
	}
	return &streamingpb.GetReplicateCheckpointResponse{Checkpoint: cp.IntoProto()}, nil
}

// Produce creates a new producer for the channel on this log node.
func (hs *handlerServiceImpl) Produce(streamServer streamingpb.StreamingNodeHandlerService_ProduceServer) error {
	p, err := producer.CreateProduceServer(hs.walManager, streamServer)
	if err != nil {
		return err
	}
	return p.Execute()
}

// Consume creates a new consumer for the channel on this log node.
func (hs *handlerServiceImpl) Consume(streamServer streamingpb.StreamingNodeHandlerService_ConsumeServer) error {
	c, err := consumer.CreateConsumeServer(hs.walManager, streamServer)
	if err != nil {
		return err
	}
	return c.Execute()
}
