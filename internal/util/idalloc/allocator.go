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
	// next allocate call only uses a range requested from the remote allocator after Sync returns.
	Sync()

	// SyncIfExpired starts a background refresh of the local allocator if the duration
	// since last sync operation is greater than expire. It never waits for the remote allocator:
	// the cached ids stay usable until the refreshed range is published, and if the refresh fails.
	SyncIfExpired(expire time.Duration)

	// WaitRefresh waits until the remote request in flight, if any, has finished, so a following
	// allocate call can see its range. Allocations from other goroutines are not blocked meanwhile.
	// It returns the error of that request or the cause of ctx, the cached ids stay usable either way.
	WaitRefresh(ctx context.Context) error
}

// remoteFetch is a request for a new range from the remote allocator.
// All fields but seq and cancel are protected by the cond lock of the allocator.
type remoteFetch struct {
	seq    uint64 // order in which the fetches were started.
	cancel context.CancelFunc
	done   bool  // the fetch is finished or abandoned by Sync, waiters can make progress.
	err    error // the result of a finished fetch, an abandoned fetch has no error.
}

type allocatorImpl struct {
	cond            *syncutil.ContextCond
	remoteAllocator remoteBatchAllocator
	lastSyncTime    time.Time // when the last remote fetch finished, even if failed, it paces the background refresh.
	lastAllocated   uint64
	localAllocator  *localAllocator
	fetch           *remoteFetch // the only fetch in flight which is allowed to publish its range, nil if there's none.
	fetchSeq        uint64       // seq of the last started fetch.
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

	id, err := ta.allocateOne(ctx)
	if err != nil {
		return err
	}
	if id >= barrier {
		return nil
	}

	// force to syncup the local allocator and remote allocator.
	// It's the fast path if the barrier is allocated from same remote allocator.
	// The fetch in flight may be served before the barrier is allocated, so only a later one can be trusted.
	if err := ta.waitNewFetch(ctx); err != nil {
		return err
	}
	id, err = ta.allocateOne(ctx)
	if err != nil {
		return err
	}
	if id >= barrier {
		return nil
	}
	return errFastPathFailed
}

func (ta *allocatorImpl) allocateOne(ctx context.Context) (uint64, error) {
	for {
		// allocate one from local allocator first.
		if id, err := ta.localAllocator.allocateOne(); err == nil {
			ta.lastAllocated = id
			ta.cond.UnsafeBroadcast()
			return id, nil
		}
		// allocate from remote.
		f, err := ta.joinFetch(ctx)
		if err != nil {
			return 0, err
		}
		if f.err != nil {
			return 0, f.err
		}
	}
}

// Sync expire the local allocator messages,
// next allocate call only uses a range requested from the remote allocator after Sync returns.
func (ta *allocatorImpl) Sync() {
	ta.cond.L.Lock()
	defer ta.cond.L.Unlock()

	ta.localAllocator.exhausted()
	// The fetch in flight may be served before the Sync, so its range can not satisfy the Sync.
	// Don't wait for it, the next allocate call starts a new one.
	if f := ta.fetch; f != nil {
		f.cancel()
		f.done = true
		ta.fetch = nil
		ta.cond.UnsafeBroadcast()
	}
}

func (ta *allocatorImpl) SyncIfExpired(expire time.Duration) {
	ta.cond.L.Lock()
	defer ta.cond.L.Unlock()

	if ta.fetch == nil && time.Since(ta.lastSyncTime) > expire {
		ta.startFetch()
	}
}

func (ta *allocatorImpl) WaitRefresh(ctx context.Context) error {
	ta.cond.L.Lock()
	defer ta.cond.L.Unlock()

	f := ta.fetch
	if f == nil {
		return nil
	}
	if err := ta.waitFetch(ctx, f); err != nil {
		return err
	}
	return f.err
}

// joinFetch waits until the fetch in flight is finished, a new fetch is started if there's none.
// The cond lock is released while waiting and held when it returns.
func (ta *allocatorImpl) joinFetch(ctx context.Context) (*remoteFetch, error) {
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	f := ta.fetch
	if f == nil {
		f = ta.startFetch()
	}
	if err := ta.waitFetch(ctx, f); err != nil {
		return nil, err
	}
	return f, nil
}

// waitNewFetch waits until a fetch started after this call is finished, and returns its error.
// Concurrent callers share the same next fetch, and the cached ids stay usable while waiting.
func (ta *allocatorImpl) waitNewFetch(ctx context.Context) error {
	minSeq := ta.fetchSeq + 1
	for {
		f, err := ta.joinFetch(ctx)
		if err != nil {
			return err
		}
		if f.seq >= minSeq {
			return f.err
		}
	}
}

// waitFetch waits until the fetch is done, the only error is the cause of ctx.
// The cond lock is released while waiting and held when it returns.
func (ta *allocatorImpl) waitFetch(ctx context.Context, f *remoteFetch) error {
	for !f.done {
		if err := ta.cond.Wait(ctx); err != nil {
			// ContextCond.Wait returns without the lock on failure.
			ta.cond.L.Lock()
			return err
		}
	}
	return nil
}

// startFetch starts a remote fetch which doesn't hold any lock while waiting for the remote allocator.
// It tries only once, the duration is bounded by the timeout of the remote allocator.
// It's independent of the context of its waiters, so its range can be used after they leave.
func (ta *allocatorImpl) startFetch() *remoteFetch {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // Ownership transfers to remoteFetch; runFetch and Sync cancel it.
	ta.fetchSeq++
	f := &remoteFetch{seq: ta.fetchSeq, cancel: cancel}
	ta.fetch = f
	go ta.runFetch(ctx, f)
	return f
}

// runFetch allocates a range from the remote allocator and publishes it atomically.
func (ta *allocatorImpl) runFetch(ctx context.Context, f *remoteFetch) {
	defer f.cancel()
	start, count, err := ta.remoteAllocator.batchAllocate(ctx, batchAllocateSize)

	ta.cond.L.Lock()
	defer ta.cond.L.Unlock()

	if ta.fetch != f {
		// The fetch was abandoned by Sync, a late response must not be used.
		return
	}
	if err == nil && !ta.localAllocator.update(start, count) {
		// keep the cached range, and report it to the waiters which have nothing to allocate from.
		err = errExhausted
	}
	f.err = err
	f.done = true
	ta.fetch = nil
	ta.lastSyncTime = time.Now()
	ta.cond.UnsafeBroadcast()
}
