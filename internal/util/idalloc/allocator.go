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

package idalloc

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// batchAllocateSize is the size of batch allocate from remote allocator.
const batchAllocateSize = 1000

var _ Allocator = (*allocatorImpl)(nil)

// NewTSOAllocator creates a new allocator.
func NewTSOAllocator(mix *syncutil.Future[types.MixCoordClient]) Allocator {
	return &allocatorImpl{
		cond:            syncutil.NewContextCond(&sync.Mutex{}),
		remoteAllocator: newTSOAllocator(mix),
		localAllocator:  newLocalAllocator(),
	}
}

// NewIDAllocator creates a new allocator.
func NewIDAllocator(mix *syncutil.Future[types.MixCoordClient]) Allocator {
	return &allocatorImpl{
		cond:            syncutil.NewContextCond(&sync.Mutex{}),
		remoteAllocator: newIDAllocator(mix),
		localAllocator:  newLocalAllocator(),
	}
}

type remoteBatchAllocator interface {
	batchAllocate(ctx context.Context, count uint32) (uint64, int, error)
}

type Allocator interface {
	// Allocate allocates a timestamp.
	Allocate(ctx context.Context) (uint64, error)

	// BarrierUtil make a barrier, next allocate call will generate id greater than barrier.
	BarrierUntil(ctx context.Context, barrier uint64) error

	// Sync expire the local allocator messages,
	// syncs the local allocator and remote allocator.
	Sync()

	// SyncIfExpired syncs the local allocator and remote allocator if the duration since last sync operation is greater than expire.
	SyncIfExpired(expire time.Duration)
}

type allocatorImpl struct {
	cond            *syncutil.ContextCond
	remoteAllocator remoteBatchAllocator
	lastSyncTime    time.Time
	lastAllocated   uint64
	localAllocator  *localAllocator
}

func (ta *allocatorImpl) Allocate(ctx context.Context) (uint64, error) {
	ta.cond.L.Lock()
	defer ta.cond.L.Unlock()

	return ta.allocateOne(ctx)
}

func (ta *allocatorImpl) BarrierUntil(ctx context.Context, barrier uint64) error {
	err := ta.barrierFastPath(ctx, barrier)
	if err == nil {
		return nil
	}
	if !errors.Is(err, errFastPathFailed) {
		return err
	}

	// Fall back to the slow path to avoid block other id allocation opeartions.
	ta.cond.L.Lock()
	for ta.lastAllocated < barrier {
		if err := ta.cond.Wait(ctx); err != nil {
			return err
		}
	}
	ta.cond.L.Unlock()
	return nil
}

func (ta *allocatorImpl) barrierFastPath(ctx context.Context, barrier uint64) error {
	ta.cond.L.Lock()
	defer ta.cond.L.Unlock()

	for i := 0; i < 2; i++ {
		id, err := ta.allocateOne(ctx)
		if err != nil {
			return err
		}

		// check if the allocated id is greater than barrier.
		if id >= barrier {
			return nil
		}
		if i == 0 {
			// force to syncup the local allocator and remote allocator at first time.
			// It's the fast path if the barrier is allocated from same remote allocator.
			ta.localAllocator.exhausted()
		}
	}
	return errFastPathFailed
}

func (ta *allocatorImpl) allocateOne(ctx context.Context) (uint64, error) {
	// allocate one from local allocator first.
	if id, err := ta.localAllocator.allocateOne(); err == nil {
		ta.lastAllocated = id
		ta.cond.UnsafeBroadcast()
		return id, nil
	}
	// allocate from remote.
	id, err := ta.allocateRemote(ctx)
	if err != nil {
		return 0, err
	}
	ta.lastAllocated = id
	ta.cond.UnsafeBroadcast()
	return id, nil
}

// Sync expire the local allocator messages,
// syncs the local allocator and remote allocator.
func (ta *allocatorImpl) Sync() {
	ta.cond.L.Lock()
	defer ta.cond.L.Unlock()

	ta.localAllocator.exhausted()
}

func (ta *allocatorImpl) SyncIfExpired(expire time.Duration) {
	ta.cond.L.Lock()
	defer ta.cond.L.Unlock()

	if time.Since(ta.lastSyncTime) > expire {
		ta.localAllocator.exhausted()
	}
}

// allocateRemote allocates timestamp from remote root coordinator.
func (ta *allocatorImpl) allocateRemote(ctx context.Context) (uint64, error) {
	// Update local allocator from remote.
	start, count, err := ta.remoteAllocator.batchAllocate(ctx, batchAllocateSize)
	if err != nil {
		return 0, err
	}
	ta.localAllocator.update(start, count)
	ta.lastSyncTime = time.Now()

	// Get from local again.
	return ta.localAllocator.allocateOne()
}
