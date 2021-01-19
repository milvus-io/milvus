package dataservice

import (
	"context"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type ddHandler struct {
	meta         *meta
	segAllocator segmentAllocator
}

func newDDHandler(meta *meta, segAllocator segmentAllocator) *ddHandler {
	return &ddHandler{
		meta:         meta,
		segAllocator: segAllocator,
	}
}

func (handler *ddHandler) Start(ctx context.Context, inputStream ms.MsgStream) {
	for {
		select {
		case msgPack := <-inputStream.Chan():
			for _, msg := range msgPack.Msgs {
				switch msg.Type() {
				case commonpb.MsgType_kCreateCollection:
					createCollectionMsg, ok := msg.(*ms.CreateCollectionMsg)
					if !ok {
						log.Println("message with type MsgType_kCreateCollection can not be cast to CreateCollectionMsg")
						continue
					}

					if err := handler.handleCreateCollection(&createCollectionMsg.CreateCollectionRequest); err != nil {
						log.Printf("handle create collection error: %s", err.Error())
					}
				case commonpb.MsgType_kDropCollection:
					dropCollectionMsg, ok := msg.(*ms.DropCollectionMsg)
					if !ok {
						log.Println("message with type MsgType_kDropCollection can not be cast to DropCollectionMsg")
						continue
					}

					if err := handler.handleDropCollection(&dropCollectionMsg.DropCollectionRequest); err != nil {
						log.Printf("handle drop collection error: %s", err.Error())
					}
				case commonpb.MsgType_kCreatePartition:
					createPartitionMsg, ok := msg.(*ms.CreatePartitionMsg)
					if !ok {
						log.Println("message with type MsgType_kCreatePartition can not be cast to CreatePartitionMsg")
						continue
					}
					if err := handler.handleCreatePartition(&createPartitionMsg.CreatePartitionRequest); err != nil {
						log.Printf("handle create partition error: %s", err.Error())
					}
				case commonpb.MsgType_kDropPartition:
					dropPartitionMsg, ok := msg.(*ms.DropPartitionMsg)
					if !ok {
						log.Println("message with type MsgType_kDropPartition can not be cast to DropPartitionMsg")
						continue
					}
					if err := handler.handleDropPartition(&dropPartitionMsg.DropPartitionRequest); err != nil {
						log.Printf("handle drop partition error: %s", err.Error())
					}
				default:
					log.Printf("invalid message type %s", msg.Type())
				}
			}
		case <-ctx.Done():
			log.Println("dd handler is shut down.")
			break
		}

	}
}

func (handler *ddHandler) handleCreateCollection(req *internalpb2.CreateCollectionRequest) error {
	var schema schemapb.CollectionSchema
	if err := proto.UnmarshalMerge(req.Schema, &schema); err != nil {
		return err
	}
	info := &collectionInfo{
		ID:     req.CollectionID,
		Schema: &schema,
	}
	return handler.meta.AddCollection(info)
}

func (handler *ddHandler) handleDropCollection(req *internalpb2.DropCollectionRequest) error {
	if err := handler.meta.DropCollection(req.CollectionID); err != nil {
		return err
	}

	segmentIDs := handler.meta.GetSegmentsByCollectionID(req.CollectionID)
	for _, id := range segmentIDs {
		if err := handler.meta.DropSegment(id); err != nil {
			return err
		}
		handler.segAllocator.DropSegment(id)
	}

	return nil
}

func (handler *ddHandler) handleCreatePartition(req *internalpb2.CreatePartitionRequest) error {
	return handler.meta.AddPartition(req.CollectionID, req.PartitionID)
}

func (handler *ddHandler) handleDropPartition(req *internalpb2.DropPartitionRequest) error {
	if err := handler.meta.DropPartition(req.CollectionID, req.PartitionID); err != nil {
		return err
	}
	ids := handler.meta.GetSegmentsByPartitionID(req.PartitionID)
	for _, id := range ids {
		if err := handler.meta.DropSegment(id); err != nil {
			return err
		}
		handler.segAllocator.DropSegment(id)
	}
	return nil
}
