package proxyservice

import (
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type NodeIDAllocator interface {
	AllocOne() UniqueID
}

type NaiveNodeIDAllocatorImpl struct {
	mtx sync.Mutex
	now UniqueID
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
