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

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// batchAllocateSize is the size of batch allocate from remote allocator.
const batchAllocateSize = 1000

var _ Allocator = (*allocatorImpl)(nil)

// NewTSOAllocator creates a new allocator.
func NewTSOAllocator(rc *syncutil.Future[types.RootCoordClient]) Allocator {
	return &allocatorImpl{
		mu:              sync.Mutex{},
		remoteAllocator: newTSOAllocator(rc),
		localAllocator:  newLocalAllocator(),
	}
}

// NewIDAllocator creates a new allocator.
func NewIDAllocator(rc *syncutil.Future[types.RootCoordClient]) Allocator {
	return &allocatorImpl{
		mu:              sync.Mutex{},
		remoteAllocator: newIDAllocator(rc),
		localAllocator:  newLocalAllocator(),
	}
}

type remoteBatchAllocator interface {
	batchAllocate(ctx context.Context, count uint32) (uint64, int, error)
}

type Allocator interface {
	// Allocate allocates a timestamp.
	Allocate(ctx context.Context) (uint64, error)

	// Sync expire the local allocator messages,
	// syncs the local allocator and remote allocator.
	Sync()

	// SyncIfExpired syncs the local allocator and remote allocator if the duration since last sync operation is greater than expire.
	SyncIfExpired(expire time.Duration)
}

type allocatorImpl struct {
	mu              sync.Mutex
	remoteAllocator remoteBatchAllocator
	lastSyncTime    time.Time
	localAllocator  *localAllocator
}

// AllocateOne allocates a timestamp.
func (ta *allocatorImpl) Allocate(ctx context.Context) (uint64, error) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// allocate one from local allocator first.
	if id, err := ta.localAllocator.allocateOne(); err == nil {
		return id, nil
	}
	// allocate from remote.
	return ta.allocateRemote(ctx)
}

// Sync expire the local allocator messages,
// syncs the local allocator and remote allocator.
func (ta *allocatorImpl) Sync() {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	ta.localAllocator.exhausted()
}

func (ta *allocatorImpl) SyncIfExpired(expire time.Duration) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

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
