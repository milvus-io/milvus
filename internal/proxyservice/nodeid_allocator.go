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

type NaiveNodeIDAllocator struct {
	allocator *allocator.IDAllocator
	now       UniqueID
	mtx       sync.Mutex
}

func (allocator *NaiveNodeIDAllocator) AllocOne() UniqueID {
	allocator.mtx.Lock()
	defer func() {
		// allocator.now++
		allocator.mtx.Unlock()
	}()
	return allocator.now
}

func NewNodeIDAllocator() NodeIDAllocator {
	return &NaiveNodeIDAllocator{
		now: 1,
	}
}
