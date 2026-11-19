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

package index

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestManifestIndexRollbackBoundedShutdown(t *testing.T) {
	m, catalog, _, _ := RollbackFixture(t)
	ctx := context.TODO()
	for offset := int64(1); offset < 4; offset++ {
		SeedLegacyBackfillRecord(t, m, meta.RestartSegID+offset, meta.RestartBuildID+offset)
		require.NoError(t, NewMigrationBackfillInspector(ctx, m).backfillIndexes(ctx, meta.RestartSegID+offset, meta.RestartBuildID+offset))
	}
	SetRollbackTestParam(t, &Params.DataCoordCfg.ManifestIndexRollbackConcurrency, "2")
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	var once sync.Once
	patch := mockey.Mock(meta.CommitManifestMutationForTest).When(func(_ *meta.Meta, _ context.Context, _ *packed.ManifestIOContext, _ string, _ meta.SegmentManifestCommit) bool {
		entered <- struct{}{}
		<-release
		return false
	}).Build()
	inspector := NewMigrationRollbackInspector(ctx, m)
	t.Cleanup(func() {
		inspector.cancel()
		once.Do(func() { close(release) })
		inspector.Stop()
		patch.UnPatch()
	})
	inspector.Start()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("rollback did not fill its two worker slots")
		}
	}
	assert.Empty(t, entered)
	inspector.cancel()
	stopped := make(chan struct{})
	go func() { inspector.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned before in-flight manifest work drained")
	default:
	}
	once.Do(func() { close(release) })
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("rollback failed to drain")
	}
	assert.Empty(t, entered, "canceled queued groups must not start I/O")
	assert.Zero(t, testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackReady))
	rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(rows), 2)
}

func TestManifestIndexRollbackRecordsFirstAndIdleWake(t *testing.T) {
	m, catalog, _, _ := RollbackFixture(t)
	ctx := context.Background()
	inspector := NewMigrationRollbackInspector(ctx, m)
	var segmentScans atomic.Int32
	segmentPatch := mockey.Mock((*meta.Meta).SelectSegments).When(func(*meta.Meta, context.Context, ...meta.SegmentFilter) bool {
		segmentScans.Add(1)
		return false
	}).Build()
	t.Cleanup(func() { segmentPatch.UnPatch() })
	work, records := inspector.scan(ctx)
	require.Equal(t, 1, records)
	require.Len(t, work, 1)
	assert.Equal(t, []int64{meta.RestartBuildID}, work[0].buildIDs)
	assert.Zero(t, segmentScans.Load(), "ordinary rollback must discover work from index records")

	interval := mockey.Mock(manifestIndexRollbackInterval).Return(10 * time.Millisecond).Build()
	t.Cleanup(func() { interval.UnPatch() })
	var scans atomic.Int32
	scanPatch := mockey.Mock((*ManifestIndexRollbackInspector).scan).When(func(*ManifestIndexRollbackInspector, context.Context) bool {
		scans.Add(1)
		return false
	}).Build()
	t.Cleanup(func() { scanPatch.UnPatch() })
	inspector.Start()
	t.Cleanup(inspector.Stop)
	require.Eventually(t, func() bool {
		return scans.Load() >= 2 && testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackReady) == 1
	}, 5*time.Second, time.Millisecond)
	assert.Zero(t, segmentScans.Load(), "rollback never enumerates segments for residual cleanup")
	require.Never(t, func() bool { return scans.Load() != 2 }, 100*time.Millisecond, time.Millisecond)

	// Emulate a late manifest publication after idle. The publication path
	// must wake rollback; it cannot depend on a still-running periodic scan.
	SeedLegacyBackfillRecord(t, m, meta.RestartSegID+1, meta.RestartBuildID+1)
	require.NoError(t, NewMigrationBackfillInspector(ctx, m).backfillIndexes(ctx, meta.RestartSegID+1, meta.RestartBuildID+1))
	require.Eventually(t, func() bool {
		record, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID + 1)
		return ok && !record.ManifestPublished && scans.Load() >= 4 &&
			testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackReady) == 1
	}, 5*time.Second, time.Millisecond)
	require.Never(t, func() bool { return scans.Load() != 4 }, 100*time.Millisecond, time.Millisecond)
	inspector.Stop()
	rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
	assert.False(t, m.GetSegment(ctx, meta.RestartSegID+1).GetManifestHasIndex())
}

func TestManifestIndexRollbackMissingSegmentStaysPending(t *testing.T) {
	m, _, _, _ := RollbackFixture(t)
	unlock := m.LockSegments(meta.RestartSegID)
	m.Segments.DropSegment(meta.RestartSegID)
	unlock()
	inspector := NewMigrationRollbackInspector(context.Background(), m)
	assert.True(t, inspector.runOnce(context.Background()), "orphaned manifest-only records must block readiness and keep retries active")
	assert.False(t, inspector.ready)
	assert.EqualValues(t, 1, testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackPendingRecords))
}

func TestManifestIndexRollbackLeavesUninstalledCopyIndex(t *testing.T) {
	m, catalog, store, _ := RollbackFixture(t)
	ctx := context.Background()
	first, _ := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
	late := model.CloneSegmentIndex(first)
	late.BuildID++
	late.IndexID++
	definition := model.CloneIndex(m.IndexCatalog.(*IndexMeta).GetIndexesForCollection(meta.RestartCollID, "")[0])
	definition.IndexID = late.IndexID
	definition.IndexName = "late_copy_index"
	require.NoError(t, m.IndexCatalog.(*IndexMeta).CreateIndex(ctx, definition))
	entry, err := meta.BuildManifestIndexInfo(m, m.GetSegment(ctx, meta.RestartSegID), late)
	require.NoError(t, err)
	before := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
	// A copy result has published both entries, but has only installed the
	// first record. Rollback may process that record while copy continues.
	store.Revisions[before] = append(store.Revisions[before], entry)
	inspector := NewMigrationRollbackInspector(ctx, m)
	inspector.runOnce(ctx)
	assert.False(t, inspector.runOnce(ctx))
	assert.True(t, inspector.ready)
	remaining := store.BackfillEntriesAt(m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
	require.Len(t, remaining, 1)
	assert.Equal(t, late.BuildID, remaining[0].BuildID)
	assert.True(t, m.GetSegment(ctx, meta.RestartSegID).GetManifestHasIndex())
	rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, first.BuildID, rows[0].BuildID)

	// The remaining record arrives after the inspector has gone idle.
	interval := mockey.Mock(manifestIndexRollbackInterval).Return(10 * time.Millisecond).Build()
	t.Cleanup(func() { interval.UnPatch() })
	inspector.Start()
	t.Cleanup(inspector.Stop)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackReady) == 1
	}, 5*time.Second, time.Millisecond)
	require.NoError(t, m.IndexCatalog.(*IndexMeta).AddSegmentIndexFromManifest(ctx, late))
	require.Eventually(t, func() bool {
		record, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(late.BuildID)
		return ok && !record.ManifestPublished && testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackReady) == 1
	}, 5*time.Second, time.Millisecond)
	inspector.Stop()
	assert.Empty(t, store.BackfillEntriesAt(m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()))
	rows, err = catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

func TestManifestIndexRollbackRevalidatesSelectedRecord(t *testing.T) {
	for _, retire := range []bool{false, true} {
		t.Run(map[bool]string{false: "rewritten to catalog", true: "retired"}[retire], func(t *testing.T) {
			m, catalog, store, _ := RollbackFixture(t)
			ctx := context.Background()
			before := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
			commits := store.CommitCount
			record, _ := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
			// Change the selected record after selection but before staging
			// takes its BuildID lock. No manifest revision should be published.
			patch := mockey.Mock((*meta.Meta).ReadManifestIndexesWithIO).When(func(_ *meta.Meta, _ context.Context, _ *packed.ManifestIOContext, _ string, _ *indexpb.StorageConfig) bool {
				if retire {
					require.NoError(t, m.IndexCatalog.(*IndexMeta).RemoveSegmentIndex(ctx, meta.RestartBuildID))
				} else {
					require.NoError(t, m.IndexCatalog.(*IndexMeta).AddSegmentIndex(ctx, record))
				}
				return false
			}).Build()
			t.Cleanup(func() { patch.UnPatch() })
			n, err := m.RollbackSegmentIndexes(ctx, newMigrationManifestReadIO(t), meta.RestartSegID, meta.RestartBuildID)
			require.ErrorIs(t, err, meta.ErrSegmentIndexRollbackSkipped)
			assert.Zero(t, n)
			assert.Equal(t, commits, store.CommitCount)
			assert.Equal(t, before, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
			assert.Len(t, store.BackfillEntriesAt(before), 1)
			rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
			require.NoError(t, err)
			if retire {
				assert.Empty(t, rows)
			} else {
				require.Len(t, rows, 1)
				assert.Equal(t, record.BuildID, rows[0].BuildID)
			}
		})
	}
}
