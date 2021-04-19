package proxyservice

import (
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type nodeIDAllocator interface {
	AllocOne() UniqueID
}

type naiveNodeIDAllocator struct {
	allocator *allocator.IDAllocator
	now       UniqueID
	mtx       sync.Mutex
}

func (allocator *naiveNodeIDAllocator) AllocOne() UniqueID {
	allocator.mtx.Lock()
	defer func() {
		// allocator.now++
		allocator.mtx.Unlock()
	}()
	return allocator.now
}

func newNodeIDAllocator() *naiveNodeIDAllocator {
	return &naiveNodeIDAllocator{
		now: 1,
	}
}
