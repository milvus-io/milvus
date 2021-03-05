package datanode

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
)

type (
	allocatorInterface interface {
		allocID() (UniqueID, error)
	}

	allocator struct {
		masterService MasterServiceInterface
	}
)

func newAllocator(s MasterServiceInterface) *allocator {
	return &allocator{
		masterService: s,
	}
}

func (alloc *allocator) allocID() (UniqueID, error) {
	ctx := context.TODO()
	resp, err := alloc.masterService.AllocID(ctx, &masterpb.IDRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kRequestID,
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
