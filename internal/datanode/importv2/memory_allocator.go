// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importv2

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var (
	globalMemoryAllocator     MemoryAllocator
	globalMemoryAllocatorOnce sync.Once
)

// GetMemoryAllocator returns the global memory allocator instance
func GetMemoryAllocator() MemoryAllocator {
	globalMemoryAllocatorOnce.Do(func() {
		globalMemoryAllocator = NewMemoryAllocator(int64(hardware.GetMemoryCount()))
	})
	return globalMemoryAllocator
}

// MemoryAllocator handles memory allocation and deallocation for import tasks
type MemoryAllocator interface {
	// BlockingAllocate blocks until memory is available and then allocates
	// This method will block until memory becomes available
	BlockingAllocate(taskID int64, size int64)
	// Allocate reserves snapshot row/delete budgets atomically and returns the
	// admitted row buffer, shrinking it if needed to retain the full delete budget.
	Allocate(ctx context.Context, taskID, preferredRowBuffer, deleteBudget int64) (int64, error)
	// AllocateSnapshotTask atomically admits a delete batch, a bounded number
	// of row buffers and a bitmap pool (one delete budget per admitted reader).
	// No task holds a partial lease; the existing slot estimate stays conservative.
	AllocateSnapshotTask(ctx context.Context, taskID, preferredRowBuffer, deleteBudget int64, readers int) (int64, int, error)

	// Release releases memory of the specified size
	Release(taskID int64, size int64)
}

type memoryAllocator struct {
	systemTotalMemory int64
	usedMemory        int64
	mutex             sync.RWMutex
	cond              *sync.Cond
}

// NewMemoryAllocator creates a new MemoryAllocator instance
func NewMemoryAllocator(systemTotalMemory int64) MemoryAllocator {
	mlog.Info(context.TODO(), "new import memory allocator", mlog.Int64("systemTotalMemory", systemTotalMemory))
	ma := &memoryAllocator{
		systemTotalMemory: systemTotalMemory,
		usedMemory:        0,
	}
	ma.cond = sync.NewCond(&ma.mutex)
	return ma
}

// BlockingAllocate blocks until memory is available and then allocates
func (ma *memoryAllocator) BlockingAllocate(taskID int64, size int64) {
	ma.mutex.Lock()
	defer ma.mutex.Unlock()

	percentage := paramtable.Get().DataNodeCfg.ImportMemoryLimitPercentage.GetAsFloat()
	memoryLimit := int64(float64(ma.systemTotalMemory) * percentage / 100.0)

	// Wait until enough memory is available
	for ma.usedMemory+size > memoryLimit {
		mlog.Warn(context.TODO(), "task waiting for memory allocation...",
			mlog.FieldTaskID(taskID),
			mlog.Int64("requestedSize", size),
			mlog.Int64("usedMemory", ma.usedMemory),
			mlog.Int64("availableMemory", memoryLimit-ma.usedMemory))

		ma.cond.Wait()
	}

	// Allocate memory
	ma.usedMemory += size
	mlog.Info(context.TODO(), "memory allocated successfully",
		mlog.FieldTaskID(taskID),
		mlog.Int64("allocatedSize", size),
		mlog.Int64("usedMemory", ma.usedMemory),
		mlog.Int64("availableMemory", memoryLimit-ma.usedMemory))
}

// Allocate reserves row and delete-map memory atomically for a typed snapshot
// source. Keep BlockingAllocate and all legacy callers' behavior unchanged.
func (ma *memoryAllocator) Allocate(ctx context.Context, taskID, preferredRowBuffer, deleteBudget int64) (int64, error) {
	ma.mutex.Lock()
	defer ma.mutex.Unlock()
	limit := int64(float64(ma.systemTotalMemory) * paramtable.Get().DataNodeCfg.ImportMemoryLimitPercentage.GetAsFloat() / 100)
	if preferredRowBuffer <= 0 || deleteBudget <= 0 || deleteBudget >= limit {
		return 0, merr.Wrapf(merr.ErrServiceResourceInsufficient,
			"snapshot import task %d cannot reserve row buffer %d and delete budget %d, allowance %d",
			taskID, preferredRowBuffer, deleteBudget, limit)
	}
	// GetBufferSize may already consume the whole allowance. Rows can be read
	// in smaller batches; the delete map must retain its full budget. Clamp
	// against the total allowance, not its currently free portion, so concurrent
	// readers wait without changing each other's batch sizes. Subtracting before
	// adding also avoids overflow for a very large preferred row buffer.
	rowBuffer := min(preferredRowBuffer, limit-deleteBudget)
	size := rowBuffer + deleteBudget
	if err := ma.waitForMemory(ctx, size, limit); err != nil {
		return 0, err
	}
	return rowBuffer, nil
}

func (ma *memoryAllocator) AllocateSnapshotTask(ctx context.Context, taskID, preferredRowBuffer, deleteBudget int64, readers int) (int64, int, error) {
	ma.mutex.Lock()
	defer ma.mutex.Unlock()
	limit := int64(float64(ma.systemTotalMemory) * paramtable.Get().DataNodeCfg.ImportMemoryLimitPercentage.GetAsFloat() / 100)
	if limit <= 0 || preferredRowBuffer <= 0 || readers <= 0 || deleteBudget <= 0 || deleteBudget > (limit-1)/2 {
		return 0, 0, merr.Wrapf(merr.ErrServiceResourceInsufficient,
			"snapshot import task %d cannot reserve delete batch/bitmaps %d and rows %d for %d readers, allowance %d",
			taskID, deleteBudget, preferredRowBuffer, readers, limit)
	}
	rowBuffer := min(preferredRowBuffer, limit-2*deleteBudget)
	admitted := min(int64(readers), (limit-deleteBudget)/(rowBuffer+deleteBudget))
	size := deleteBudget + admitted*(rowBuffer+deleteBudget)
	if err := ma.waitForMemory(ctx, size, limit); err != nil {
		return 0, 0, err
	}
	return rowBuffer, int(admitted), nil
}

// Caller holds ma.mutex. Waiting acquires no partial reservation, and releasing
// any existing reservation or canceling the context wakes the waiter.
func (ma *memoryAllocator) waitForMemory(ctx context.Context, size, limit int64) error {
	// Acquiring the same lock in the callback prevents a lost wakeup between
	// checking ctx.Err and Cond.Wait. Stop need not wait for the callback.
	stop := context.AfterFunc(ctx, func() {
		ma.mutex.Lock()
		defer ma.mutex.Unlock()
		ma.cond.Broadcast()
	})
	defer stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if size <= limit-ma.usedMemory {
			ma.usedMemory += size
			return nil
		}
		ma.cond.Wait()
	}
}

// Return the exact budgets to pass to the reader and capture them for release;
// neither the reader nor cleanup should recompute them after a config refresh.
func reserveSnapshotRead(ctx context.Context, taskID, preferredRowBuffer int64) (int64, int64, func(), error) {
	budget := paramtable.Get().DataNodeCfg.ImportDeleteBufferSize.GetAsInt64()
	allocator := GetMemoryAllocator()
	rowBuffer, err := allocator.Allocate(ctx, taskID, preferredRowBuffer, budget)
	if err != nil {
		return 0, 0, nil, err
	}
	return rowBuffer, budget, func() { allocator.Release(taskID, rowBuffer+budget) }, nil
}

// Release releases memory of the specified size
func (ma *memoryAllocator) Release(taskID int64, size int64) {
	ma.mutex.Lock()
	defer ma.mutex.Unlock()

	ma.usedMemory -= size
	if ma.usedMemory < 0 {
		ma.usedMemory = 0 // Prevent negative memory usage
		mlog.Warn(context.TODO(), "memory release resulted in negative usage, reset to 0",
			mlog.FieldTaskID(taskID),
			mlog.Int64("releaseSize", size))
	}

	mlog.Info(context.TODO(), "memory released successfully",
		mlog.FieldTaskID(taskID),
		mlog.Int64("releasedSize", size),
		mlog.Int64("usedMemory", ma.usedMemory))

	// Wake up waiting tasks after memory is released
	ma.cond.Broadcast()
}
