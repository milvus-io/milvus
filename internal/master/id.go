package master

import (
	"github.com/zilliztech/milvus-distributed/internal/kv"
)

type IDAllocator interface {
	Alloc(count uint32) (UniqueID, UniqueID, error)
	AllocOne() (UniqueID, error)
	UpdateID() error
}

// GlobalTSOAllocator is the global single point TSO allocator.
type GlobalIDAllocator struct {
	allocator Allocator
}

func NewGlobalIDAllocator(key string, base kv.TxnBase) *GlobalIDAllocator {
	return &GlobalIDAllocator{
		allocator: NewGlobalTSOAllocator(key, base),
	}
}

// Initialize will initialize the created global TSO allocator.
func (gia *GlobalIDAllocator) Initialize() error {
	return gia.allocator.Initialize()
}

// GenerateTSO is used to generate a given number of TSOs.
// Make sure you have initialized the TSO allocator before calling.
func (gia *GlobalIDAllocator) Alloc(count uint32) (UniqueID, UniqueID, error) {
	timestamp, err := gia.allocator.GenerateTSO(count)
	if err != nil {
		return 0, 0, err
	}
	idStart := UniqueID(timestamp)
	idEnd := idStart + int64(count)
	return idStart, idEnd, nil
}

func (gia *GlobalIDAllocator) AllocOne() (UniqueID, error) {
	timestamp, err := gia.allocator.GenerateTSO(1)
	if err != nil {
		return 0, err
	}
	idStart := UniqueID(timestamp)
	return idStart, nil
}

func (gia *GlobalIDAllocator) UpdateID() error {
	return gia.allocator.UpdateTSO()
}
