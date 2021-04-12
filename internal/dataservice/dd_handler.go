package dataservice

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/types"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type ddHandler struct {
	meta             *meta
	segmentAllocator segmentAllocatorInterface
	masterClient     types.MasterService
}

func newDDHandler(meta *meta, allocator segmentAllocatorInterface, client types.MasterService) *ddHandler {
	return &ddHandler{
		meta:             meta,
		segmentAllocator: allocator,
		masterClient:     client,
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
	presp, err := handler.masterClient.ShowPartitions(context.TODO(), &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowPartitions,
			MsgID:     -1, // todo
			Timestamp: 0,  // todo
			SourceID:  Params.NodeID,
		},
		DbName:         "",
		CollectionName: schema.Name,
		CollectionID:   msg.CollectionID,
	})
	if err = VerifyResponse(presp, err); err != nil {
		return err
	}
	err = handler.meta.AddCollection(&datapb.CollectionInfo{
		ID:         msg.CollectionID,
		Schema:     schema,
		Partitions: presp.PartitionIDs,
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
