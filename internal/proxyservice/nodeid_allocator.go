package proxyservice

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/allocator"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type NodeIDAllocator interface {
	AllocOne() UniqueID
}

type NaiveNodeIDAllocatorImpl struct {
	impl *allocator.IDAllocator
}

func (allocator *NaiveNodeIDAllocatorImpl) AllocOne() UniqueID {
	id, err := allocator.impl.AllocOne()
	if err != nil {
		panic(err)
	}
	return id
}

func NewNodeIDAllocator() NodeIDAllocator {
	impl, err := allocator.NewIDAllocator(context.Background(), Params.MasterAddress())
	if err != nil {
		panic(err)
	}
	return &NaiveNodeIDAllocatorImpl{
		impl: impl,
	}
}
