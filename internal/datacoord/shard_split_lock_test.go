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

package datacoord

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// countingLock records whether the write switch released the exclusion.
type countingLock struct{ closed int }

func (c *countingLock) Close() { c.closed++ }

func TestWriteSwitchTakesAndReleasesTheCollectionLock(t *testing.T) {
	// Released when the write switch returns, whichever way it returns. Held
	// into the rewrite it would block every collection DDL for as long as the
	// data takes to move -- hours, for a rehash.
	m := newHashRewriteMeta([]int64{201})
	mgr, _ := newHashSplitTestManager(t, m)
	// fenced already, so the switch reaches the barrier allocation, which fails
	// here: an early return AFTER the lock was taken.
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(0), errors.New("no tso")).Once()
	mgr.allocator = alloc

	lock := &countingLock{}
	var lockedCollection string
	mgr.collectionLocker = func(dbName, collectionName string) (splitWriteSwitchLock, error) {
		lockedCollection = collectionName
		return lock, nil
	}
	task := fenceSources(newHashTask(nil), 100)
	task.State = datapb.SplitShardTaskState_SplitShardTaskFencing
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceTask(task)

	assert.Equal(t, "hash_split_test", lockedCollection,
		"the exclusion is taken on the collection being split")
	assert.Equal(t, 1, lock.closed,
		"the exclusion is released on every path out of the write switch")
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing,
		mgr.mustGetTask(task.GetTaskId()).GetState())
}

func TestWriteSwitchSkipsWhileACollectionDDLHoldsTheLock(t *testing.T) {
	// A DDL holding the key is an ordinary outcome: the task keeps its state and
	// comes back next tick. It must NOT fence, because a fence is irreversible
	// and would commit the task to a write switch it cannot finish safely.
	m := newHashRewriteMeta([]int64{201})
	mgr, _ := newHashSplitTestManager(t, m)
	mgr.collectionLocker = func(string, string) (splitWriteSwitchLock, error) {
		return nil, errors.Wrap(broadcaster.ErrResourceKeyBusy, "held by a DDL")
	}
	task := newHashTask(nil)
	task.State = datapb.SplitShardTaskState_SplitShardTaskFencing
	task.Fenced = false
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceTask(task)

	updated := mgr.mustGetTask(task.GetTaskId())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, updated.GetState())
	assert.False(t, updated.GetFenced(), "no source may be fenced without the exclusion")
}

func TestWriteSwitchRefusesWithoutALocker(t *testing.T) {
	// The locker is wired during server initialization. A task that ticks before
	// that must wait rather than run the write switch unprotected -- the whole
	// point of the lock is that the switch is not safe without it.
	m := newHashRewriteMeta([]int64{201})
	mgr, _ := newHashSplitTestManager(t, m)
	mgr.collectionLocker = nil
	task := newHashTask(nil)
	task.State = datapb.SplitShardTaskState_SplitShardTaskFencing
	task.Fenced = false
	mgr.tasks.Insert(task.GetTaskId(), task)

	mgr.advanceTask(task)

	updated := mgr.mustGetTask(task.GetTaskId())
	assert.False(t, updated.GetFenced())
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskFencing, updated.GetState())
}

func TestCollectionBusyIsDistinguishedFromARealFailure(t *testing.T) {
	// The two are treated differently -- one is logged as routine, the other as a
	// warning -- so they must actually be distinguishable.
	require.True(t, isCollectionBusy(errors.Wrap(broadcaster.ErrResourceKeyBusy, "wrapped")))
	require.False(t, isCollectionBusy(errors.New("streamingcoord unavailable")))
	require.False(t, isCollectionBusy(nil))
}
