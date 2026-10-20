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

	"github.com/milvus-io/milvus/internal/metastore"
	mocks2 "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// TestPartitionStatsCleaner_CleanPartitionStatsInfo proves CleanPartitionStatsInfo persists
// the analyze task removal, the current-version rollback (when the dropped
// version is the current one), and the partition-stats info removal as a
// single composite catalog.Update - analyze task first, rollback second,
// partition-stats info last - and only mutates the analyzeMeta/
// partitionStatsMeta in-memory state after that write succeeds.
func TestPartitionStatsCleaner_CleanPartitionStatsInfo(t *testing.T) {
	ctx := context.Background()
	info := &datapb.PartitionStatsInfo{
		CollectionID:  1,
		PartitionID:   2,
		VChannel:      "ch-1",
		Version:       100,
		AnalyzeTaskID: 55,
		SegmentIDs:    []int64{1000},
	}

	newTestChunkManager := func(t *testing.T) *mocks.ChunkManager {
		cm := mocks.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root").Maybe()
		cm.EXPECT().MultiRemove(mock.Anything, mock.Anything).Return(nil).Once()
		return cm
	}

	t.Run("no rollback needed", func(t *testing.T) {
		catalog := mocks2.NewDataCoordCatalog(t)
		var gotActions []metastore.UpdateAction
		catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, actions ...metastore.UpdateAction) error {
				gotActions = actions
				return nil
			}).Once()

		am := &analyzeMeta{ctx: ctx, catalog: catalog, tasks: map[int64]*indexpb.AnalyzeTask{
			55: {TaskID: 55, CollectionID: 1},
		}}
		psm := &partitionStatsMeta{
			ctx:     ctx,
			catalog: catalog,
			partitionStatsInfos: map[string]map[int64]*partitionStatsInfo{
				"ch-1": {
					2: {
						currentVersion: emptyPartitionStatsVersion, // not current -> no rollback
						infos:          map[int64]*datapb.PartitionStatsInfo{100: info},
					},
				},
			},
		}

		cleaner := newPartitionStatsCleaner(newTestChunkManager(t), catalog, am, psm)

		err := cleaner.CleanPartitionStatsInfo(ctx, info)
		require.NoError(t, err)

		require.Len(t, gotActions, 2)
		assert.Equal(t, metastore.ActionDelete, gotActions[0].Type)
		assert.Equal(t, metastore.AnalyzeTaskEntry{TaskID: 55}, gotActions[0].Entry)
		assert.Equal(t, metastore.ActionDelete, gotActions[1].Type)
		psEntry, ok := gotActions[1].Entry.(metastore.PartitionStatsEntry)
		require.True(t, ok)
		assert.Same(t, info, psEntry.Info)

		// Memory updated after success.
		assert.Nil(t, am.GetTask(55))
		assert.Nil(t, psm.GetPartitionStats(1, 2, "ch-1", 100))
	})

	t.Run("rolls back current version", func(t *testing.T) {
		catalog := mocks2.NewDataCoordCatalog(t)
		var gotActions []metastore.UpdateAction
		catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, actions ...metastore.UpdateAction) error {
				gotActions = actions
				return nil
			}).Once()

		am := &analyzeMeta{ctx: ctx, catalog: catalog, tasks: map[int64]*indexpb.AnalyzeTask{
			55: {TaskID: 55, CollectionID: 1},
		}}
		otherInfo := &datapb.PartitionStatsInfo{CollectionID: 1, PartitionID: 2, VChannel: "ch-1", Version: 90}
		psm := &partitionStatsMeta{
			ctx:     ctx,
			catalog: catalog,
			partitionStatsInfos: map[string]map[int64]*partitionStatsInfo{
				"ch-1": {
					2: {
						currentVersion: 100, // info.Version is current -> rollback needed
						infos: map[int64]*datapb.PartitionStatsInfo{
							100: info,
							90:  otherInfo,
						},
					},
				},
			},
		}

		cleaner := newPartitionStatsCleaner(newTestChunkManager(t), catalog, am, psm)

		err := cleaner.CleanPartitionStatsInfo(ctx, info)
		require.NoError(t, err)

		require.Len(t, gotActions, 3)
		assert.Equal(t, metastore.ActionDelete, gotActions[0].Type)
		assert.Equal(t, metastore.AnalyzeTaskEntry{TaskID: 55}, gotActions[0].Entry)
		assert.Equal(t, metastore.ActionUpdate, gotActions[1].Type)
		assert.Equal(t, metastore.PartitionStatsVersionEntry{
			CollectionID: 1, PartitionID: 2, VChannel: "ch-1", Version: 90,
		}, gotActions[1].Entry)
		assert.Equal(t, metastore.ActionDelete, gotActions[2].Type)
		psEntry, ok := gotActions[2].Entry.(metastore.PartitionStatsEntry)
		require.True(t, ok)
		assert.Same(t, info, psEntry.Info)

		// Memory updated after success: rollback applied, dropped info gone.
		assert.Equal(t, int64(90), psm.GetCurrentPartitionStatsVersion(1, 2, "ch-1"))
		assert.Nil(t, psm.GetPartitionStats(1, 2, "ch-1", 100))
		assert.NotNil(t, psm.GetPartitionStats(1, 2, "ch-1", 90))
	})

	t.Run("holds both meta locks across catalog write", func(t *testing.T) {
		// The rollback compute, catalog write, and in-memory apply must run in
		// one critical section, so a concurrent SaveCurrentPartitionStatsVersion
		// cannot interleave between compute and apply and get clobbered. Assert
		// both meta locks are held while catalog.Update runs: TryLock returns
		// false only when the lock is already held.
		catalog := mocks2.NewDataCoordCatalog(t)

		var am *analyzeMeta
		var psm *partitionStatsMeta
		catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, _ ...metastore.UpdateAction) error {
				if psm.TryLock() {
					psm.Unlock()
					t.Error("partitionStatsMeta lock must be held during catalog.Update")
				}
				if am.TryLock() {
					am.Unlock()
					t.Error("analyzeMeta lock must be held during catalog.Update")
				}
				return nil
			}).Once()

		am = &analyzeMeta{ctx: ctx, catalog: catalog, tasks: map[int64]*indexpb.AnalyzeTask{
			55: {TaskID: 55, CollectionID: 1},
		}}
		otherInfo := &datapb.PartitionStatsInfo{CollectionID: 1, PartitionID: 2, VChannel: "ch-1", Version: 90}
		psm = &partitionStatsMeta{
			ctx:     ctx,
			catalog: catalog,
			partitionStatsInfos: map[string]map[int64]*partitionStatsInfo{
				"ch-1": {
					2: {
						currentVersion: 100,
						infos: map[int64]*datapb.PartitionStatsInfo{
							100: info,
							90:  otherInfo,
						},
					},
				},
			},
		}

		cleaner := newPartitionStatsCleaner(newTestChunkManager(t), catalog, am, psm)

		err := cleaner.CleanPartitionStatsInfo(ctx, info)
		require.NoError(t, err)
		// Locks released after the call: both TryLock succeed now.
		require.True(t, psm.TryLock())
		psm.Unlock()
		require.True(t, am.TryLock())
		am.Unlock()
	})

	t.Run("catalog update failure leaves memory untouched", func(t *testing.T) {
		catalog := mocks2.NewDataCoordCatalog(t)
		catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("update failed")).Once()

		am := &analyzeMeta{ctx: ctx, catalog: catalog, tasks: map[int64]*indexpb.AnalyzeTask{
			55: {TaskID: 55, CollectionID: 1},
		}}
		psm := &partitionStatsMeta{
			ctx:     ctx,
			catalog: catalog,
			partitionStatsInfos: map[string]map[int64]*partitionStatsInfo{
				"ch-1": {
					2: {
						currentVersion: emptyPartitionStatsVersion,
						infos:          map[int64]*datapb.PartitionStatsInfo{100: info},
					},
				},
			},
		}

		cleaner := newPartitionStatsCleaner(newTestChunkManager(t), catalog, am, psm)

		err := cleaner.CleanPartitionStatsInfo(ctx, info)
		require.Error(t, err)

		// A failed composite write must not desync memory from disk.
		assert.NotNil(t, am.GetTask(55))
		assert.NotNil(t, psm.GetPartitionStats(1, 2, "ch-1", 100))
	})
}
