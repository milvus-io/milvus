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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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
