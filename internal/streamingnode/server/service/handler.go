package service

import (
	"context"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/service/handler/consumer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/service/handler/producer"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

var _ HandlerService = (*handlerServiceImpl)(nil)

// NewHandlerService creates a new handler service.
func NewHandlerService(walManager walmanager.Manager, writeBufferManager writebuffer.BufferManager) HandlerService {
	return &handlerServiceImpl{
		walManager:         walManager,
		writeBufferManager: writeBufferManager,
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

	walManager         walmanager.Manager
	writeBufferManager writebuffer.BufferManager
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

// GetSalvageCheckpoint returns all salvage checkpoints captured during force promote.
func (hs *handlerServiceImpl) GetSalvageCheckpoint(ctx context.Context, req *streamingpb.GetSalvageCheckpointRequest) (*streamingpb.GetSalvageCheckpointResponse, error) {
	wal, err := hs.walManager.GetAvailableWAL(types.NewPChannelInfoFromProto(req.GetPchannel()))
	if err != nil {
		return nil, err
	}
	cps := wal.GetSalvageCheckpoint()
	protoCps := make([]*commonpb.ReplicateCheckpoint, 0, len(cps))
	for _, cp := range cps {
		protoCps = append(protoCps, cp.IntoProto())
	}
	return &streamingpb.GetSalvageCheckpointResponse{Checkpoints: protoCps}, nil
}

// GetGrowingFlushProgress returns growing-source flush progress tracked by the write buffer.
func (hs *handlerServiceImpl) GetGrowingFlushProgress(ctx context.Context, req *streamingpb.GetGrowingFlushProgressRequest) (*streamingpb.GetGrowingFlushProgressResponse, error) {
	if hs.writeBufferManager == nil {
		return nil, status.NewInner("write buffer manager is not initialized")
	}
	if req.GetVchannel() == "" {
		return nil, status.NewInvaildArgument("vchannel is empty")
	}

	if _, err := hs.walManager.GetAvailableWAL(types.NewPChannelInfoFromProto(req.GetPchannel())); err != nil {
		return nil, err
	}
	progress, err := hs.writeBufferManager.GetGrowingFlushProgress(ctx, req.GetVchannel(), req.GetSegmentIds(), req.GetFenceTs())
	if err != nil {
		return nil, err
	}
	return &streamingpb.GetGrowingFlushProgressResponse{
		Progress: marshalGrowingFlushSegmentProgress(progress),
	}, nil
}

func marshalGrowingFlushSegmentProgress(progress []writebuffer.GrowingFlushSegmentProgress) []*streamingpb.GrowingFlushSegmentProgress {
	result := make([]*streamingpb.GrowingFlushSegmentProgress, 0, len(progress))
	for _, item := range progress {
		result = append(result, &streamingpb.GrowingFlushSegmentProgress{
			SegmentId:          item.SegmentID,
			TargetOffset:       item.TargetOffset,
			HasGrowingProgress: item.HasGrowingProgress,
			SourceMode:         marshalGrowingFlushSourceMode(item.SourceMode),
		})
	}
	return result
}

func marshalGrowingFlushSourceMode(mode metacache.FlushSourceMode) streamingpb.GrowingFlushSourceMode {
	switch mode {
	case metacache.FlushSourceWriteBuffer:
		return streamingpb.GrowingFlushSourceMode_GROWING_FLUSH_SOURCE_WRITE_BUFFER
	case metacache.FlushSourceGrowing:
		return streamingpb.GrowingFlushSourceMode_GROWING_FLUSH_SOURCE_GROWING
	default:
		return streamingpb.GrowingFlushSourceMode_GROWING_FLUSH_SOURCE_UNKNOWN
	}
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
