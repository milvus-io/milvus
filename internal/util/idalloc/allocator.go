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
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
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
		asyncRefresh:    true,
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

	// Sync invalidates the cache and any in-flight range. The next allocation
	// must use a remote request started after Sync.
	Sync()

	// SyncIfExpired refreshes an expired TSO cache in the background, retaining
	// usable timestamps until completion. ID allocators expire their cache.
	// Use Sync when the next allocation must fence earlier remote requests.
	SyncIfExpired(expire time.Duration)
}

type allocatorImpl struct {
	cond            *syncutil.ContextCond
	remoteAllocator remoteBatchAllocator
	lastSyncTime    time.Time
	lastAllocated   uint64
	localAllocator  *localAllocator
	asyncRefresh    bool
	generation      uint64
	refresh         *allocationRequest
	lastRefreshTime time.Time // Limits periodic retries, including failed refreshes.
}

type allocationRequest struct {
	generation uint64
	done       chan struct{}
	cancel     context.CancelFunc
	err        error
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
			ta.invalidate()
		}
	}
	return errFastPathFailed
}

func (ta *allocatorImpl) allocateOne(ctx context.Context) (uint64, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, context.Cause(ctx)
		}
		if id, err := ta.localAllocator.allocateOne(); err == nil {
			ta.lastAllocated = id
			ta.cond.UnsafeBroadcast()
			return id, nil
		}
		request := ta.refresh
		if request == nil {
			request = ta.startRefresh(ctx)
		}
		// Actual exhaustion joins the in-flight RPC without holding the mutex.
		// Each waiter can cancel independently of the request's owner.
		ta.cond.L.Unlock()
		select {
		case <-ctx.Done():
		case <-request.done:
		}
		ta.cond.L.Lock()
		if request.generation == ta.generation && request.err != nil {
			return 0, request.err
		}
		// Sync may have invalidated the response, or another caller may have
		// consumed the range. Recheck the cache before issuing another RPC.
	}
}

// Sync invalidates the cache and fences any in-flight remote request.
func (ta *allocatorImpl) Sync() {
	ta.cond.L.Lock()
	defer ta.cond.L.Unlock()

	ta.invalidate()
}

// invalidate fences both cached IDs and responses to earlier requests.
// The old request retains the single-flight slot until it exits, so repeated
// Sync calls cannot leave an unbounded number of canceled RPCs behind.
func (ta *allocatorImpl) invalidate() {
	ta.localAllocator.exhausted()
	ta.generation++
	if ta.refresh != nil {
		ta.refresh.cancel()
	}
}

func (ta *allocatorImpl) SyncIfExpired(expire time.Duration) {
	ta.cond.L.Lock()
	defer ta.cond.L.Unlock()

	if time.Since(ta.lastSyncTime) > expire {
		if !ta.asyncRefresh {
			ta.invalidate()
			return
		}
		if ta.refresh == nil && time.Since(ta.lastRefreshTime) > expire {
			ta.startRefresh(context.Background())
		}
	}
}

// startRefresh is called with the mutex held. There is at most one remote
// request, shared by periodic refreshes and allocations on an exhausted cache.
func (ta *allocatorImpl) startRefresh(ctx context.Context) *allocationRequest {
	// Allocator has process lifetime and no persistent worker. Bound each task
	// even when there are no subsequent allocations or expiration checks.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	request := &allocationRequest{
		generation: ta.generation,
		done:       make(chan struct{}),
		cancel:     cancel,
	}
	ta.refresh = request
	ta.lastRefreshTime = time.Now()
	go func() {
		defer cancel()
		start, count, err := ta.remoteAllocator.batchAllocate(ctx, batchAllocateSize)
		ta.cond.L.Lock()
		defer ta.cond.L.Unlock()
		request.err = err
		if err == nil && request.generation == ta.generation {
			ta.localAllocator.update(start, count)
			ta.lastSyncTime = time.Now()
		}
		ta.refresh = nil
		close(request.done)
	}()
	return request
}
