package dataservice

import (
	"fmt"

	"github.com/golang/protobuf/proto"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type ddHandler struct {
	meta             *meta
	segmentAllocator segmentAllocator
}

func newDDHandler(meta *meta, allocator segmentAllocator) *ddHandler {
	return &ddHandler{
		meta:             meta,
		segmentAllocator: allocator,
	}
}

func (handler *ddHandler) HandleDDMsg(msg msgstream.TsMsg) error {
	switch msg.Type() {
	case commonpb.MsgType_kCreateCollection:
		realMsg := msg.(*msgstream.CreateCollectionMsg)
		return handler.handleCreateCollection(realMsg)
	case commonpb.MsgType_kDropCollection:
		realMsg := msg.(*msgstream.DropCollectionMsg)
		return handler.handleDropCollection(realMsg)
	case commonpb.MsgType_kCreatePartition:
		realMsg := msg.(*msgstream.CreatePartitionMsg)
		return handler.handleCreatePartition(realMsg)
	case commonpb.MsgType_kDropPartition:
		realMsg := msg.(*msgstream.DropPartitionMsg)
		return handler.handleDropPartition(realMsg)
	default:
		return fmt.Errorf("unknown msg type: %v", msg.Type())
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

func (handler *ddHandler) handleDropCollection(msg *msgstream.DropCollectionMsg) error {
	ids := handler.meta.GetSegmentsByCollectionID(msg.CollectionID)
	for _, id := range ids {
		if err := handler.meta.DropSegment(id); err != nil {
			continue
		}
		handler.segmentAllocator.DropSegment(id)
	}
	if err := handler.meta.DropCollection(msg.CollectionID); err != nil {
		return err
	}
	return nil
}

func (handler *ddHandler) handleDropPartition(msg *msgstream.DropPartitionMsg) error {
	ids := handler.meta.GetSegmentsByCollectionAndPartitionID(msg.CollectionID, msg.PartitionID)
	for _, id := range ids {
		if err := handler.meta.DropSegment(id); err != nil {
			return err
		}
		handler.segmentAllocator.DropSegment(id)
	}
	if err := handler.meta.DropPartition(msg.CollectionID, msg.PartitionID); err != nil {
		return err
	}
	return nil
}

func (handler *ddHandler) handleCreatePartition(msg *msgstream.CreatePartitionMsg) error {
	return handler.meta.AddPartition(msg.CollectionID, msg.PartitionID)
}
