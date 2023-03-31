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

package rootcoord

import (
	"sync"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/tso"
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
