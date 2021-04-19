package datanode

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
)

type (
	allocator interface {
		allocID() (UniqueID, error)
	}

	allocatorImpl struct {
		masterService MasterServiceInterface
	}
)

func newAllocatorImpl(s MasterServiceInterface) *allocatorImpl {
	return &allocatorImpl{
		masterService: s,
	}
}

func (alloc *allocatorImpl) allocID() (UniqueID, error) {
	resp, err := alloc.masterService.AllocID(&masterpb.IDRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kShowCollections,
			MsgID:     1, // GOOSE TODO
			Timestamp: 0, // GOOSE TODO
			SourceID:  Params.NodeID,
		},
		Count: 1,
	})
	if err != nil {
		return 0, err
	}
	return resp.ID, nil
}
