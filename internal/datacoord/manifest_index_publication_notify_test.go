// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
)

func captureUnlockedManifestNotifications(t *testing.T, m *meta) *[]string {
	t.Helper()
	var paths []string
	var original func(*meta, context.Context, []*SegmentInfo)
	patch := mockey.Mock((*meta).notifyQueryViewSegmentUpdates).Origin(&original).To(func(current *meta, ctx context.Context, segments []*SegmentInfo) {
		if current == m && len(segments) > 0 {
			unlocked := m.segMu.TryLock()
			if unlocked {
				m.segMu.Unlock()
			}
			require.True(t, unlocked, "notification must run outside segMu")
			for _, segment := range segments {
				locks := m.getSegmentManifestLocks()
				unlocked = locks.TryLock(segment.GetID())
				if unlocked {
					locks.Unlock(segment.GetID())
				}
				require.True(t, unlocked, "notification must run outside the manifest lock")
				require.Equal(t, segment.GetManifestPath(), m.GetSegment(ctx, segment.GetID()).GetManifestPath())
				paths = append(paths, segment.GetManifestPath())
			}
		}
		original(current, ctx, segments)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return &paths
}

func TestManifestIndexMigrationNotifiesOnlyCommittedRevision(t *testing.T) {
	for _, name := range []string{"backfill", "rollback", "gc"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var m *meta
			var kv *failBackfillCatalogKV
			if name != "backfill" {
				m, _, _, kv = rollbackFixture(t)
			} else {
				withSegmentIndexManifestWrites(t, false)
				withManifestIndexRollback(t, false)
				newFakeManifestStore(t)
				kv = &failBackfillCatalogKV{metaMemoryKV: NewMetaMemoryKV()}
				m = bootMetaForRestart(t, metastorekv.NewCatalog(kv, "", ""), restartCollID)
				seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
			}
			paths := captureUnlockedManifestNotifications(t, m)
			migrate := func() error {
				if name == "rollback" {
					_, err := m.rollbackSegmentIndexes(ctx, newGCManifestReadIO(t), restartSegID, restartBuildID)
					return err
				}
				if name == "gc" {
					record, ok := m.indexMeta.GetIndexJob(restartBuildID)
					require.True(t, ok)
					gc := &garbageCollector{meta: m}
					_, err := gc.commitManifestIndexRetractions(ctx, []segmentIndexGCItem{{segIdx: record}})
					return err
				}
				return newManifestIndexBackfillInspector(ctx, m).backfillIndexes(ctx, restartSegID, restartBuildID)
			}
			before := m.GetSegment(ctx, restartSegID).GetManifestPath()
			kv.failAtomicUpdate = true
			require.Error(t, migrate())
			require.Empty(t, *paths, "failed catalog publication must not notify QueryView")
			require.Equal(t, before, m.GetSegment(ctx, restartSegID).GetManifestPath())
			kv.failAtomicUpdate = false
			require.NoError(t, migrate())
			after := m.GetSegment(ctx, restartSegID).GetManifestPath()
			require.NotEqual(t, before, after)
			require.Equal(t, []string{after}, *paths)
		})
	}
}

func TestManifestIndexGCNotifiesAfterBatchUnlock(t *testing.T) {
	m, _, _, _ := rollbackFixture(t)
	paths := captureUnlockedManifestNotifications(t, m)
	record, ok := m.indexMeta.GetIndexJob(restartBuildID)
	require.True(t, ok)
	gc := newGarbageCollector(m, nil, GcOption{cli: m.chunkManager})
	t.Cleanup(gc.close)
	gc.recycleManifestSegmentIndexes(context.Background(), []segmentIndexGCItem{{segIdx: record}})
	require.Equal(t, []string{m.GetSegment(context.Background(), restartSegID).GetManifestPath()}, *paths)
	_, ok = m.indexMeta.GetIndexJob(restartBuildID)
	require.False(t, ok)
}

func TestCopyResultNotifiesAfterUnlockDespiteTaskCompletionFailure(t *testing.T) {
	m, catalog, copies, task, result := copyPublicationFixture(t)
	paths := captureUnlockedManifestNotifications(t, m)
	catalog.failCompletion = true
	require.ErrorContains(t, SyncCopySegmentTask(task, result, copies, m), "injected task completion")
	require.Equal(t, []string{result.GetSegmentResults()[0].GetManifestPath()}, *paths)
	catalog.failCompletion = false
	require.NoError(t, SyncCopySegmentTask(task, result, copies, m))
	require.Len(t, *paths, 1, "finalized-result replay must not publish the target again")
}
