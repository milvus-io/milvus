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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestShardSplitTaskStoreLoadsFromTheCatalog(t *testing.T) {
	// An in-flight split must survive a datacoord restart: the record is the
	// only thing that tells the restarted coordinator which shards are mid-way
	// through a handoff.
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSplitShardTask(mock.Anything).Return([]*datapb.SplitShardTask{
		{TaskId: 200, CollectionId: 100},
		{TaskId: 201, CollectionId: 100},
	}, nil).Once()

	store := newShardSplitTasks()
	require.NoError(t, store.load(context.Background(), catalog))

	task, ok := store.get(200)
	require.True(t, ok)
	assert.Equal(t, int64(100), task.GetCollectionId())
	_, ok = store.get(201)
	assert.True(t, ok)
	_, ok = store.get(202)
	assert.False(t, ok)
}

func TestShardSplitTaskStoreLoadFailurePropagates(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSplitShardTask(mock.Anything).Return(nil, errors.New("etcd down")).Once()

	store := newShardSplitTasks()
	assert.Error(t, store.load(context.Background(), catalog))
}

func TestShardSplitTaskStoreUpsertPersistsBeforeCaching(t *testing.T) {
	// A cached task the catalog never took would silently vanish on restart,
	// so a failed save must leave the store empty rather than half-committed.
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(errors.New("etcd down")).Once()

	store := newShardSplitTasks()
	assert.Error(t, store.upsert(context.Background(), catalog, &datapb.SplitShardTask{TaskId: 200}))
	_, ok := store.get(200)
	assert.False(t, ok)

	catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, store.upsert(context.Background(), catalog, &datapb.SplitShardTask{TaskId: 200}))
	_, ok = store.get(200)
	assert.True(t, ok)
}

func TestSplitSourceVChannels(t *testing.T) {
	assert.Equal(t, []string{"a", "b"}, splitSourceVChannels(&datapb.SplitShardTask{
		Sources: []*datapb.SplitShardTaskSource{{Vchannel: "a"}, {Vchannel: "b"}},
	}))
	assert.Empty(t, splitSourceVChannels(&datapb.SplitShardTask{}))
}

func TestShardSplitTaskStoreSourceSwitchTimeTick(t *testing.T) {
	t.Run("load indexes every recorded source", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().ListSplitShardTask(mock.Anything).Return([]*datapb.SplitShardTask{
			{TaskId: 200, Sources: []*datapb.SplitShardTaskSource{{Vchannel: "src-a", SwitchTimeTick: 2000}}},
			{TaskId: 201, Sources: []*datapb.SplitShardTaskSource{{Vchannel: "src-b"}}},
		}, nil).Once()

		store := newShardSplitTasks()
		require.NoError(t, store.load(context.Background(), catalog))

		tick, ok := store.sourceSwitchTimeTick("src-a")
		assert.True(t, ok)
		assert.Equal(t, uint64(2000), tick)
		// A fence not on record never counts.
		_, ok = store.sourceSwitchTimeTick("src-b")
		assert.False(t, ok)
		_, ok = store.sourceSwitchTimeTick("not-a-source")
		assert.False(t, ok)
	})

	t.Run("upsert reflects the latest record of the task", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil)

		store := newShardSplitTasks()
		require.NoError(t, store.upsert(context.Background(), catalog, &datapb.SplitShardTask{
			TaskId: 200, Sources: []*datapb.SplitShardTaskSource{{Vchannel: "src-a"}},
		}))
		_, ok := store.sourceSwitchTimeTick("src-a")
		assert.False(t, ok)

		// The ack callback records the fence tick on the same task id.
		require.NoError(t, store.upsert(context.Background(), catalog, &datapb.SplitShardTask{
			TaskId: 200, Sources: []*datapb.SplitShardTaskSource{{Vchannel: "src-a", SwitchTimeTick: 2000}},
		}))
		tick, ok := store.sourceSwitchTimeTick("src-a")
		assert.True(t, ok)
		assert.Equal(t, uint64(2000), tick)
		assert.Len(t, store.sourceIndex["src-a"], 1)
	})

	t.Run("a zero-tick task on the same source does not hide a recorded fence", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil)

		store := newShardSplitTasks()
		require.NoError(t, store.upsert(context.Background(), catalog, &datapb.SplitShardTask{
			TaskId: 200, Sources: []*datapb.SplitShardTaskSource{{Vchannel: "src-a", SwitchTimeTick: 2000}},
		}))
		require.NoError(t, store.upsert(context.Background(), catalog, &datapb.SplitShardTask{
			TaskId: 201, State: datapb.SplitShardTaskState_SplitShardTaskAborted,
			Sources: []*datapb.SplitShardTaskSource{{Vchannel: "src-a"}},
		}))
		tick, ok := store.sourceSwitchTimeTick("src-a")
		assert.True(t, ok)
		assert.Equal(t, uint64(2000), tick)
	})

	t.Run("an indexed id missing from the cache is skipped", func(t *testing.T) {
		store := newShardSplitTasks()
		store.sourceIndex["src-a"] = []int64{999}
		_, ok := store.sourceSwitchTimeTick("src-a")
		assert.False(t, ok)
	})
}

func TestShardSplitTaskStoreLocksOneTaskAtATime(t *testing.T) {
	store := newShardSplitTasks()
	store.lockTask(200)
	// The same id is held; another id is not.
	assert.False(t, store.taskLocks.TryLock(200))
	assert.True(t, store.taskLocks.TryLock(201))
	store.taskLocks.Unlock(201)
	store.unlockTask(200)
	assert.True(t, store.taskLocks.TryLock(200))
	store.taskLocks.Unlock(200)
}

func TestShardSplitTaskStoreModify(t *testing.T) {
	ctx := context.Background()
	catalog := mocks.NewDataCoordCatalog(t)
	store := newShardSplitTasks()

	// an unknown task is a System error: nothing acts on a task it never saw.
	_, err := store.modify(ctx, catalog, 200, func(*datapb.SplitShardTask) bool { return true })
	require.ErrorIs(t, err, merr.ErrServiceInternal)

	catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, store.create(ctx, catalog, &datapb.SplitShardTask{TaskId: 200, CollectionId: 100}))
	// an id is created once.
	require.ErrorIs(t, store.create(ctx, catalog, &datapb.SplitShardTask{TaskId: 200}), merr.ErrServiceInternal)
	assert.Len(t, store.list(), 1)

	// a mutation that declines writes nothing and returns the record as it is.
	got, err := store.modify(ctx, catalog, 200, func(*datapb.SplitShardTask) bool { return false })
	require.NoError(t, err)
	assert.Equal(t, int64(100), got.GetCollectionId())

	// a mutation works on a copy: a failed save leaves the cached record alone.
	catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(errors.New("etcd down")).Once()
	_, err = store.modify(ctx, catalog, 200, func(task *datapb.SplitShardTask) bool {
		task.FailReason = "lost"
		return true
	})
	require.Error(t, err)
	cached, _ := store.get(200)
	assert.Empty(t, cached.GetFailReason())

	catalog.EXPECT().SaveSplitShardTask(mock.Anything, mock.Anything).Return(nil).Once()
	got, err = store.modify(ctx, catalog, 200, func(task *datapb.SplitShardTask) bool {
		task.FailReason = "kept"
		return true
	})
	require.NoError(t, err)
	assert.Equal(t, "kept", got.GetFailReason())
	cached, _ = store.get(200)
	assert.Equal(t, "kept", cached.GetFailReason())
}

// The split manager and the SplitShard ack callback write one record. The
// manager's read-modify-write must take the same per-task lock the callback
// holds, or a manager write that read the record before the callback landed
// reverts the fence the callback recorded (audit 1.5): the task then waits
// forever for a fence that is already in the WAL.
func TestShardSplitManagerWriteCannotRevertTheCallbacksFence(t *testing.T) {
	svr := newShardSplitTestServer(t)
	ctx := context.Background()
	require.NoError(t, svr.shardSplitTasks.create(ctx, svr.meta.catalog, &datapb.SplitShardTask{
		TaskId:       200,
		CollectionId: 100,
		State:        datapb.SplitShardTaskState_SplitShardTaskPreparing,
		Sources:      []*datapb.SplitShardTaskSource{{Vchannel: splitTestSource}},
		Targets:      splitTestCommitRequest().GetTargets(),
	}))

	// The manager has read the Preparing record and is about to write Fencing;
	// the callback lands in between.
	committed := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	_, err := svr.shardSplitTasks.modify(ctx, svr.meta.catalog, 200, func(task *datapb.SplitShardTask) bool {
		go func() {
			defer wg.Done()
			status, err := svr.CommitShardSplit(ctx, splitTestCommitRequest())
			assert.NoError(t, merr.CheckRPCCall(status, err))
			close(committed)
		}()
		select {
		case <-committed:
			t.Error("the callback ran inside the manager's read-modify-write")
		case <-time.After(100 * time.Millisecond):
		}
		task.State = datapb.SplitShardTaskState_SplitShardTaskFencing
		return true
	})
	require.NoError(t, err)
	wg.Wait()

	task, _ := svr.shardSplitTasks.get(200)
	assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, task.GetState())
	assert.True(t, task.GetFenced())
	assert.Equal(t, uint64(2000), task.GetSources()[0].GetSwitchTimeTick())
}

// Fields 9-11 and the source's pending segments are the split manager's. A
// redelivered SplitShard callback carries none of them and must keep all of
// them.
func TestCommitShardSplitRedeliveryKeepsTheManagersFields(t *testing.T) {
	svr := newShardSplitTestServer(t)
	ctx := context.Background()
	status, err := svr.CommitShardSplit(ctx, splitTestCommitRequest())
	require.NoError(t, merr.CheckRPCCall(status, err))

	_, err = svr.shardSplitTasks.modify(ctx, svr.meta.catalog, 200, func(task *datapb.SplitShardTask) bool {
		task.GetSources()[0].PendingSegments = []int64{7, 8}
		task.DispatchedPlanIds = []int64{70}
		task.FailReason = "noted"
		task.EndTime = 12345
		return true
	})
	require.NoError(t, err)

	status, err = svr.CommitShardSplit(ctx, splitTestCommitRequest())
	require.NoError(t, merr.CheckRPCCall(status, err))

	task, _ := svr.shardSplitTasks.get(200)
	assert.Equal(t, []int64{7, 8}, task.GetSources()[0].GetPendingSegments())
	assert.Equal(t, []int64{70}, task.GetDispatchedPlanIds())
	assert.Equal(t, "noted", task.GetFailReason())
	assert.Equal(t, uint64(12345), task.GetEndTime())
}
