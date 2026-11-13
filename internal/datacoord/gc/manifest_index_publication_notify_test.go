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

package gc

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/copysegment"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
)

func captureUnlockedManifestNotifications(t *testing.T, m *meta.Meta) *[]string {
	t.Helper()
	var paths []string
	var original func(*meta.Meta, context.Context, []*meta.SegmentInfo)
	patch := mockey.Mock((*meta.Meta).NotifyQueryViewSegmentUpdates).Origin(&original).To(func(current *meta.Meta, ctx context.Context, segments []*meta.SegmentInfo) {
		if current == m && len(segments) > 0 {
			for _, segment := range segments {
				locks := m.GetSegmentLocks()
				unlocked := locks.TryLock(segment.GetID())
				if unlocked {
					locks.Unlock(segment.GetID())
				}
				require.True(t, unlocked, "notification must run outside the segment writer lock")
				locks = m.GetSegmentManifestLocks()
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
			var m *meta.Meta
			var kv *index.FailBackfillCatalogKV
			if name != "backfill" {
				m, _, _, kv = index.RollbackFixture(t)
			} else {
				meta.WithSegmentIndexManifestWrites(t, false)
				index.WithManifestIndexRollback(t, false)
				meta.NewFakeManifestStore(t)
				kv = &index.FailBackfillCatalogKV{MetaMemoryKV: meta.NewMetaMemoryKV()}
				m = index.BootMigrationMeta(t, metastorekv.NewCatalog(kv, "", ""), meta.RestartCollID)
				index.SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
			}
			paths := captureUnlockedManifestNotifications(t, m)
			migrate := func() error {
				if name == "rollback" {
					_, err := m.RollbackSegmentIndexes(ctx, NewGCManifestReadIO(t), meta.RestartSegID, meta.RestartBuildID)
					return err
				}
				if name == "gc" {
					record, ok := m.IndexCatalog.(*index.IndexMeta).GetIndexJob(meta.RestartBuildID)
					require.True(t, ok)
					gc := &GarbageCollector{Meta: m, Option: GcOption{IndexMeta: m.IndexCatalog.(*index.IndexMeta)}}
					_, err := gc.commitManifestIndexRetractions(ctx, []segmentIndexGCItem{{segIdx: record}})
					return err
				}
				return index.NewMigrationBackfillInspector(ctx, m).BackfillIndexesForTest(ctx, meta.RestartSegID, meta.RestartBuildID)
			}
			before := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
			kv.FailAtomicUpdate = true
			require.Error(t, migrate())
			require.Empty(t, *paths, "failed catalog publication must not notify QueryView")
			require.Equal(t, before, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
			kv.FailAtomicUpdate = false
			require.NoError(t, migrate())
			after := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
			require.NotEqual(t, before, after)
			require.Equal(t, []string{after}, *paths)
		})
	}
}

func TestManifestIndexGCNotifiesAfterBatchUnlock(t *testing.T) {
	m, _, _, _ := index.RollbackFixture(t)
	paths := captureUnlockedManifestNotifications(t, m)
	record, ok := m.IndexCatalog.(*index.IndexMeta).GetIndexJob(meta.RestartBuildID)
	require.True(t, ok)
	gc := NewGarbageCollector(m, nil, GcOption{Cli: m.ChunkManager, IndexMeta: m.IndexCatalog.(*index.IndexMeta)})
	t.Cleanup(gc.Close)
	gc.recycleManifestSegmentIndexes(context.Background(), []segmentIndexGCItem{{segIdx: record}})
	require.Equal(t, []string{m.GetSegment(context.Background(), meta.RestartSegID).GetManifestPath()}, *paths)
	_, ok = m.IndexCatalog.(*index.IndexMeta).GetIndexJob(meta.RestartBuildID)
	require.False(t, ok)
}

func TestCopyResultNotifiesAfterUnlockDespiteTaskCompletionFailure(t *testing.T) {
	m, catalog, copies, task, result, _ := copyPublicationFixture(t)
	paths := captureUnlockedManifestNotifications(t, m)
	catalog.failCompletion = true
	require.ErrorContains(t, copysegment.SyncCopySegmentTask(task, result, copies, m, m.IndexCatalog.(*index.IndexMeta)), "injected task completion")
	require.Equal(t, []string{result.GetSegmentResults()[0].GetManifestPath()}, *paths)
	catalog.failCompletion = false
	require.NoError(t, copysegment.SyncCopySegmentTask(task, result, copies, m, m.IndexCatalog.(*index.IndexMeta)))
	require.Len(t, *paths, 1, "finalized-result replay must not publish the target again")
}
