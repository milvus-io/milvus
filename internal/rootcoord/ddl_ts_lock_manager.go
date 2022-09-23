package rootcoord

import (
	"sync"

	"github.com/milvus-io/milvus/internal/tso"

	"go.uber.org/atomic"
)

type DdlTsLockManager interface {
	GetMinDdlTs() Timestamp
	AddRefCnt(delta int32)
	Lock()
	Unlock()
	UpdateLastTs(ts Timestamp)
}

type ddlTsLockManager struct {
	lastTs        atomic.Uint64
	inProgressCnt atomic.Int32
	tsoAllocator  tso.Allocator
	mu            sync.Mutex
}

func (c *ddlTsLockManager) GetMinDdlTs() Timestamp {
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

func (c *ddlTsLockManager) AddRefCnt(delta int32) {
	c.inProgressCnt.Add(delta)
}

func (c *ddlTsLockManager) Lock() {
	c.mu.Lock()
}

func (c *ddlTsLockManager) Unlock() {
	c.mu.Unlock()
}

func (c *ddlTsLockManager) UpdateLastTs(ts Timestamp) {
	c.lastTs.Store(ts)
}

func newDdlTsLockManager(tsoAllocator tso.Allocator) *ddlTsLockManager {
	return &ddlTsLockManager{
		lastTs:        *atomic.NewUint64(0),
		inProgressCnt: *atomic.NewInt32(0),
		tsoAllocator:  tsoAllocator,
		mu:            sync.Mutex{},
	}
}
