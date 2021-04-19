package proxyservice

import (
	"sync"

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
	now  UniqueID
	mtx  sync.Mutex
}

func (allocator *NaiveNodeIDAllocatorImpl) AllocOne() UniqueID {
	allocator.mtx.Lock()
	defer func() {
		allocator.now++
		allocator.mtx.Unlock()
	}()
	return allocator.now
}

func NewNodeIDAllocator() NodeIDAllocator {
	return &NaiveNodeIDAllocatorImpl{
		now: 0,
	}
}
