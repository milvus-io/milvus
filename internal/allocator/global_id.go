package allocator

import (
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/tso"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type GIDAllocator interface {
	Alloc(count uint32) (UniqueID, UniqueID, error)
	AllocOne() (UniqueID, error)
	UpdateID() error
}

// GlobalTSOAllocator is the global single point TSO allocator.
type GlobalIDAllocator struct {
	allocator tso.Allocator
}

func NewGlobalIDAllocator(key string, base kv.TxnBase) *GlobalIDAllocator {
	return &GlobalIDAllocator{
		allocator: tso.NewGlobalTSOAllocator(key, base),
	}
}

// Initialize will initialize the created global TSO allocator.
func (gia *GlobalIDAllocator) Initialize() error {
	return gia.allocator.Initialize()
}

// GenerateTSO is used to generate a given number of TSOs.
// Make sure you have initialized the TSO allocator before calling.
func (gia *GlobalIDAllocator) Alloc(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
	timestamp, err := gia.allocator.GenerateTSO(count)
	if err != nil {
		return 0, 0, err
	}
	idStart := typeutil.UniqueID(timestamp)
	idEnd := idStart + int64(count)
	return idStart, idEnd, nil
}

func (gia *GlobalIDAllocator) AllocOne() (typeutil.UniqueID, error) {
	timestamp, err := gia.allocator.GenerateTSO(1)
	if err != nil {
		return 0, err
	}
	idStart := typeutil.UniqueID(timestamp)
	return idStart, nil
}

func (gia *GlobalIDAllocator) UpdateID() error {
	return gia.allocator.UpdateTSO()
}
