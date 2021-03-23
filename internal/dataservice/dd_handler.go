package dataservice

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type ddHandler struct {
	meta             *meta
	segmentAllocator segmentAllocatorInterface
}

func newDDHandler(meta *meta, allocator segmentAllocatorInterface) *ddHandler {
	return &ddHandler{
		meta:             meta,
		segmentAllocator: allocator,
	}
}

func (handler *ddHandler) HandleDDMsg(ctx context.Context, msg msgstream.TsMsg) error {
	switch msg.Type() {
	case commonpb.MsgType_CreateCollection:
		realMsg := msg.(*msgstream.CreateCollectionMsg)
		return handler.handleCreateCollection(realMsg)
	case commonpb.MsgType_DropCollection:
		realMsg := msg.(*msgstream.DropCollectionMsg)
		return handler.handleDropCollection(ctx, realMsg)
	case commonpb.MsgType_CreatePartition:
		realMsg := msg.(*msgstream.CreatePartitionMsg)
		return handler.handleCreatePartition(realMsg)
	case commonpb.MsgType_DropPartition:
		realMsg := msg.(*msgstream.DropPartitionMsg)
		return handler.handleDropPartition(ctx, realMsg)
	default:
		return nil
	}
}

func (handler *ddHandler) handleCreateCollection(msg *msgstream.CreateCollectionMsg) error {
	schema := &schemapb.CollectionSchema{}
	if err := proto.Unmarshal(msg.Schema, schema); err != nil {
		return err
	}
	err := handler.meta.AddCollection(&collectionInfo{
		ID:     msg.CollectionID,
		Schema: schema,
	})
	if err != nil {
		return err
	}
	return nil
}

func (handler *ddHandler) handleDropCollection(ctx context.Context, msg *msgstream.DropCollectionMsg) error {
	segmentsOfCollection := handler.meta.GetSegmentsOfCollection(msg.CollectionID)
	for _, id := range segmentsOfCollection {
		handler.segmentAllocator.DropSegment(ctx, id)
	}
	if err := handler.meta.DropCollection(msg.CollectionID); err != nil {
		return err
	}
	return nil
}

func (handler *ddHandler) handleDropPartition(ctx context.Context, msg *msgstream.DropPartitionMsg) error {
	segmentsOfPartition := handler.meta.GetSegmentsOfPartition(msg.CollectionID, msg.PartitionID)
	for _, id := range segmentsOfPartition {
		handler.segmentAllocator.DropSegment(ctx, id)
	}
	if err := handler.meta.DropPartition(msg.CollectionID, msg.PartitionID); err != nil {
		return err
	}
	return nil
}

func (handler *ddHandler) handleCreatePartition(msg *msgstream.CreatePartitionMsg) error {
	return handler.meta.AddPartition(msg.CollectionID, msg.PartitionID)
}
