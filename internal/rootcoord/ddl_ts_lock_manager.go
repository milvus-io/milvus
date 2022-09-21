package rootcoord

import (
	"sync"

	"github.com/milvus-io/milvus/internal/tso"

	"go.uber.org/atomic"
)

type DdlTsLockManagerV2 interface {
	GetMinDdlTs() Timestamp
	AddRefCnt(delta int32)
	Lock()
	Unlock()
	UpdateLastTs(ts Timestamp)
}

type ddlTsLockManagerV2 struct {
	lastTs        atomic.Uint64
	inProgressCnt atomic.Int32
	tsoAllocator  tso.Allocator
	mu            sync.Mutex
}

func (c *ddlTsLockManagerV2) GetMinDdlTs() Timestamp {
	// In fact, `TryLock` can replace the `inProgressCnt` but it's not recommended.
	if c.inProgressCnt.Load() > 0 {
		return c.lastTs.Load()
	}
	c.Lock()
	defer c.Unlock()
	ts, err := c.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return c.lastTs.Load()
	}
	c.UpdateLastTs(ts)
	return ts
}

func (c *ddlTsLockManagerV2) AddRefCnt(delta int32) {
	c.inProgressCnt.Add(delta)
}

func (c *ddlTsLockManagerV2) Lock() {
	c.mu.Lock()
}

func (c *ddlTsLockManagerV2) Unlock() {
	c.mu.Unlock()
}

func (c *ddlTsLockManagerV2) UpdateLastTs(ts Timestamp) {
	c.lastTs.Store(ts)
}

func newDdlTsLockManagerV2(tsoAllocator tso.Allocator) *ddlTsLockManagerV2 {
	return &ddlTsLockManagerV2{
		lastTs:        *atomic.NewUint64(0),
		inProgressCnt: *atomic.NewInt32(0),
		tsoAllocator:  tsoAllocator,
		mu:            sync.Mutex{},
	}
}
