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

package index

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManifestIndexBackfillRevalidatesSelectedRecord(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(context.Context, *meta.Meta) error
	}{
		{"deleted definition", func(ctx context.Context, m *meta.Meta) error {
			return m.IndexCatalog.(*IndexMeta).MarkIndexAsDeleted(ctx, meta.RestartCollID, []int64{meta.RestartIndexID})
		}},
		{"replaced build", func(ctx context.Context, m *meta.Meta) error {
			old, _ := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
			replacement := model.CloneSegmentIndex(old)
			replacement.BuildID++
			return m.IndexCatalog.(*IndexMeta).AddSegmentIndex(ctx, replacement)
		}},
		{"changed artifact version", func(_ context.Context, m *meta.Meta) error {
			return m.IndexCatalog.(*IndexMeta).UpdateVersion(meta.RestartBuildID, 10)
		}},
		{"import protection", func(ctx context.Context, m *meta.Meta) error {
			return m.UpdateSegmentsInfo(ctx, meta.UpdateIsImporting(meta.RestartSegID, true))
		}},
		{"import state", func(ctx context.Context, m *meta.Meta) error {
			return m.UpdateSegmentsInfo(ctx, meta.UpdateStatusOperator(meta.RestartSegID, commonpb.SegmentState_Importing))
		}},
		{"changed level", func(ctx context.Context, m *meta.Meta) error {
			return m.UpdateSegmentsInfo(ctx, meta.UpdateSegmentLevelOperator(meta.RestartSegID, datapb.SegmentLevel_L0))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			meta.WithSegmentIndexManifestWrites(t, false)
			store := meta.NewFakeManifestStore(t)
			ctx := context.TODO()
			catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
			m := BootMigrationMeta(t, catalog, meta.RestartCollID)
			SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
			base := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
			// The inspector has constructed its entry, but the framework has
			// not acquired the locks that authorize publishing it yet.
			var original func(*meta.Meta, context.Context, meta.SegmentManifestCommit) error
			var changeErr error
			changed := false
			patch := mockey.Mock((*meta.Meta).CommitSegmentManifest).Origin(&original).To(
				func(receiver *meta.Meta, ctx context.Context, commit meta.SegmentManifestCommit) error {
					changed = true
					changeErr = test.change(ctx, receiver)
					if changeErr != nil {
						return changeErr
					}
					return original(receiver, ctx, commit)
				}).Build()
			t.Cleanup(func() { patch.UnPatch() })
			err := NewMigrationBackfillInspector(ctx, m).backfillIndexes(ctx, meta.RestartSegID, meta.RestartBuildID)
			require.True(t, changed)
			require.NoError(t, changeErr, "the competing operation must succeed before checking publication")
			require.Error(t, err)
			assert.Zero(t, store.CommitCount, "stale candidate must fail before manifest I/O")
			assert.Equal(t, base, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
			rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
			require.NoError(t, err)
			assert.NotEmpty(t, rows)
			assert.False(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID))
		})
	}
}

func TestManifestIndexBackfillBatchRevalidatesEveryRecord(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	store := meta.NewFakeManifestStore(t)
	ctx := context.Background()
	catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m := BootMigrationMeta(t, catalog, meta.RestartCollID)
	records := SeedLegacyBackfillIndexes(t, m, 2)
	base := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
	meta.WithSegmentIndexManifestWrites(t, true)

	// Change the second record after projection but before the batch locks.
	// The first record must not be published on its own.
	var original func(*meta.Meta, context.Context, meta.SegmentManifestCommit) error
	patch := mockey.Mock((*meta.Meta).CommitSegmentManifest).Origin(&original).To(
		func(receiver *meta.Meta, ctx context.Context, commit meta.SegmentManifestCommit) error {
			if err := receiver.IndexCatalog.(*IndexMeta).UpdateVersion(records[1].BuildID, 10); err != nil {
				return err
			}
			return original(receiver, ctx, commit)
		}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	inspector := NewMigrationBackfillInspector(ctx, m)
	work, _ := inspector.scan(ctx)
	assert.Zero(t, inspector.execute(ctx, work))
	assert.Zero(t, store.CommitCount)
	assert.Equal(t, base, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
	rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	for _, record := range records {
		assert.False(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), record.BuildID))
	}
	patch.UnPatch()
	work, _ = inspector.scan(ctx)
	assert.Equal(t, 2, inspector.execute(ctx, work))
	assert.Equal(t, 1, store.CommitCount)
}

func TestManifestIndexBackfillPublicationPreservesReplacement(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	meta.NewFakeManifestStore(t)
	ctx := context.TODO()
	catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m := BootMigrationMeta(t, catalog, meta.RestartCollID)
	SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
	old, ok := m.IndexCatalog.(*IndexMeta).SegmentBuildInfo.Get(meta.RestartBuildID)
	require.True(t, ok)
	replacement := model.CloneSegmentIndex(old)
	replacement.BuildID++
	meta.WithSegmentIndexManifestWrites(t, true)

	// The old build is staged and locked. A new build can still take over its
	// (segment, index) slot under a different BuildID lock during manifest I/O.
	var original func(*meta.Meta, context.Context, *packed.ManifestIOContext, string, meta.SegmentManifestCommit) (packed.ManifestUpdateResult, error)
	patch := mockey.Mock(meta.CommitManifestMutationForTest).Origin(&original).To(
		func(m *meta.Meta, callCtx context.Context, io *packed.ManifestIOContext, base string, commit meta.SegmentManifestCommit) (packed.ManifestUpdateResult, error) {
			if err := m.IndexCatalog.(*IndexMeta).AddSegmentIndex(ctx, replacement); err != nil {
				return packed.ManifestUpdateResult{}, err
			}
			return original(m, callCtx, io, base, commit)
		}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	require.NoError(t, NewMigrationBackfillInspector(ctx, m).backfillIndexes(ctx, meta.RestartSegID, meta.RestartBuildID))

	assert.False(t, old.ManifestPublished, "a reader's existing snapshot must remain unchanged")
	assert.True(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), old.BuildID))
	assert.False(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), replacement.BuildID))
	assert.Equal(t, replacement, m.IndexCatalog.(*IndexMeta).GetSegmentIndexes(meta.RestartCollID, meta.RestartSegID)[meta.RestartIndexID])
	rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, replacement.BuildID, rows[0].BuildID)
}

func TestManifestIndexBackfillFailureDuringManifestIO(t *testing.T) {
	for _, scenario := range []string{"write fails", "pointer advances", "segment drops"} {
		t.Run(scenario, func(t *testing.T) {
			meta.WithSegmentIndexManifestWrites(t, false)
			WithManifestIndexBackfillEnabled(t, true)
			store := meta.NewFakeManifestStore(t)
			ctx := context.TODO()
			catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
			m := BootMigrationMeta(t, catalog, meta.RestartCollID)
			SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
			base := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
			meta.WithSegmentIndexManifestWrites(t, true)
			var original func(*meta.Meta, context.Context, *packed.ManifestIOContext, string, meta.SegmentManifestCommit) (packed.ManifestUpdateResult, error)
			inject := true
			patch := mockey.Mock(meta.CommitManifestMutationForTest).Origin(&original).To(
				func(m *meta.Meta, callCtx context.Context, io *packed.ManifestIOContext, base string, commit meta.SegmentManifestCommit) (packed.ManifestUpdateResult, error) {
					if inject && scenario == "write fails" {
						return packed.ManifestUpdateResult{}, merr.ErrIoTooManyRequests
					}
					result, err := original(m, callCtx, io, base, commit)
					if err != nil || !inject {
						return result, err
					}
					if scenario == "pointer advances" {
						err = m.UpdateSegmentsInfo(ctx, meta.UpdateManifestVersion(meta.RestartSegID, 3))
					} else {
						err = m.SetState(ctx, meta.RestartSegID, commonpb.SegmentState_Dropped)
					}
					return result, err
				}).Build()
			t.Cleanup(func() { patch.UnPatch() })
			inspector := NewMigrationBackfillInspector(ctx, m)
			err := inspector.backfillIndexes(ctx, meta.RestartSegID, meta.RestartBuildID)
			require.Error(t, err)
			switch scenario {
			case "write fails":
				assert.ErrorIs(t, err, merr.ErrIoTooManyRequests)
			case "pointer advances":
				assert.ErrorIs(t, err, meta.ErrSegmentManifestStale)
			case "segment drops":
				assert.ErrorIs(t, err, merr.ErrSegmentNotFound)
			}
			rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.False(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID))
			assert.False(t, m.GetSegment(ctx, meta.RestartSegID).GetManifestHasIndex())
			if scenario == "write fails" {
				assert.Equal(t, base, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
			}
			inject = false
			inspector.runOnce(ctx)
			rows, err = catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
			require.NoError(t, err)
			if scenario == "segment drops" {
				assert.Len(t, rows, 1, "dropped segment remains owned by GC")
				assert.Zero(t, inspector.lastPending)
			} else {
				assert.Empty(t, rows)
				assert.Len(t, store.BackfillEntriesAt(m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()), 1)
			}
		})
	}
}

func TestManifestIndexBackfillConcurrencyAndShutdown(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	WithManifestIndexBackfillEnabled(t, true)
	meta.NewFakeManifestStore(t)
	ctx := context.TODO()
	catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m := BootMigrationMeta(t, catalog, meta.RestartCollID)
	for offset := int64(0); offset < 4; offset++ {
		SeedLegacyBackfillRecord(t, m, meta.RestartSegID+offset, meta.RestartBuildID+offset)
	}
	key := Params.DataCoordCfg.ManifestIndexBackfillConcurrency.Key
	previous := Params.DataCoordCfg.ManifestIndexBackfillConcurrency.GetValue()
	Params.Save(key, "2")
	t.Cleanup(func() { Params.Save(key, previous) })
	meta.WithSegmentIndexManifestWrites(t, true)
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	var once sync.Once
	// A false condition forwards to the original function after the barrier.
	// Avoid Origin: mockey 1.4.6 writes shared call arguments in that adapter,
	// which races when multiple segment workers enter it concurrently.
	patch := mockey.Mock(meta.CommitManifestMutationForTest).When(func(_ *meta.Meta, _ context.Context, _ *packed.ManifestIOContext, _ string, _ meta.SegmentManifestCommit) bool {
		entered <- struct{}{}
		<-release
		return false
	}).Build()
	inspector := NewMigrationBackfillInspector(ctx, m)
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
			t.Fatal("backfill did not fill its two worker slots")
		}
	}
	select {
	case <-entered:
		t.Fatal("backfill exceeded configured concurrency")
	default:
	}
	inspector.cancel()
	stopped := make(chan struct{})
	go func() { inspector.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned before in-flight manifest I/O drained")
	default:
	}
	once.Do(func() { close(release) })
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("backfill did not stop after draining")
	}
	assert.Empty(t, entered, "queued segment groups must not enter manifest I/O after cancellation")
	rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(rows), 2, "unstarted work must remain durable for the next leader")
}

func TestManifestIndexBackfillIdleAndWake(t *testing.T) {
	for _, writer := range []string{"copy install", "finish task", "version update"} {
		t.Run(writer, func(t *testing.T) {
			meta.WithSegmentIndexManifestWrites(t, false)
			WithManifestIndexBackfillEnabled(t, true)
			meta.NewFakeManifestStore(t)
			ctx := context.Background()
			m := BootMigrationMeta(t, metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", ""), meta.RestartCollID)
			SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
			// An in-flight task remains in etcd and must not keep migration polling.
			require.NoError(t, m.IndexCatalog.(*IndexMeta).AddSegmentIndex(ctx, &model.SegmentIndex{
				CollectionID: meta.RestartCollID, PartitionID: RestartPartID, SegmentID: meta.RestartSegID,
				IndexID: meta.RestartIndexID + 10, BuildID: meta.RestartBuildID + 10,
				IndexState: commonpb.IndexState_InProgress,
			}))
			meta.WithSegmentIndexManifestWrites(t, true)
			inspector := NewMigrationBackfillInspector(ctx, m)
			interval := mockey.Mock(manifestIndexBackfillInterval).Return(10 * time.Millisecond).Build()
			t.Cleanup(func() { interval.UnPatch() })
			var scans atomic.Int32
			scanPatch := mockey.Mock((*ManifestIndexBackfillInspector).scan).When(func(_ *ManifestIndexBackfillInspector, _ context.Context) bool {
				scans.Add(1)
				return false
			}).Build()
			t.Cleanup(func() { scanPatch.UnPatch() })
			inspector.Start()
			t.Cleanup(inspector.Stop)
			require.Eventually(t, func() bool { return scans.Load() >= 2 }, 5*time.Second, time.Millisecond)
			assert.True(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID))
			require.Never(t, func() bool { return scans.Load() != 2 }, 100*time.Millisecond, time.Millisecond,
				"an empty backlog must stop periodic scans")

			buildID := meta.RestartBuildID
			switch writer {
			case "copy install":
				record, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(buildID)
				require.True(t, ok)
				record.BuildID++
				buildID = record.BuildID
				require.NoError(t, m.IndexCatalog.(*IndexMeta).AddSegmentIndex(ctx, record))
			case "finish task":
				require.NoError(t, m.IndexCatalog.(*IndexMeta).FinishTask(&workerpb.IndexTaskInfo{
					BuildID: buildID, State: commonpb.IndexState_Finished,
					IndexFileKeys:         []string{"0", "1"},
					IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
				}))
			case "version update":
				require.NoError(t, m.IndexCatalog.(*IndexMeta).UpdateVersion(buildID, 10))
			}
			require.Eventually(t, func() bool {
				record, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(buildID)
				return ok && record.ManifestPublished && scans.Load() >= 4
			}, 5*time.Second, time.Millisecond)
			require.Never(t, func() bool { return scans.Load() != 4 }, 100*time.Millisecond, time.Millisecond)
			inspector.Stop()
		})
	}
}

func TestManifestIndexBackfillDoesNotLoseWakeAtIdleBoundary(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	WithManifestIndexBackfillEnabled(t, true)
	meta.NewFakeManifestStore(t)
	ctx := context.Background()
	m := BootMigrationMeta(t, metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", ""), meta.RestartCollID)
	SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
	meta.WithSegmentIndexManifestWrites(t, true)
	interval := mockey.Mock(manifestIndexBackfillInterval).Return(20 * time.Millisecond).Build()
	t.Cleanup(func() { interval.UnPatch() })
	inspector := NewMigrationBackfillInspector(ctx, m)
	require.NoError(t, inspector.backfillIndexes(ctx, meta.RestartSegID, meta.RestartBuildID))

	// Freeze the first empty scan after it observed zero, but before it can
	// wait for notification. A record installed here must survive that handoff.
	emptyScan := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	var original func(*ManifestIndexBackfillInspector, context.Context) ([]segmentManifestIndexBackfill, int)
	patch := mockey.Mock((*ManifestIndexBackfillInspector).scan).Origin(&original).To(
		func(receiver *ManifestIndexBackfillInspector, ctx context.Context) ([]segmentManifestIndexBackfill, int) {
			work, pending := original(receiver, ctx)
			if calls.Add(1) == 1 && pending == 0 {
				close(emptyScan)
				<-release
			}
			return work, pending
		}).Build()
	t.Cleanup(func() {
		inspector.cancel()
		once.Do(func() { close(release) })
		inspector.Stop()
		patch.UnPatch()
	})
	inspector.Start()
	select {
	case <-emptyScan:
	case <-time.After(5 * time.Second):
		t.Fatal("initial empty scan did not finish")
	}
	record, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
	require.True(t, ok)
	record.BuildID++
	require.NoError(t, m.IndexCatalog.(*IndexMeta).AddSegmentIndex(ctx, record))
	once.Do(func() { close(release) })
	require.Eventually(t, func() bool {
		current, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(record.BuildID)
		return ok && current.ManifestPublished
	}, 5*time.Second, time.Millisecond, "late copy installation must survive the empty-scan cooldown")
}

func TestManifestIndexBackfillScansRecordsBeforeSegments(t *testing.T) {
	meta.WithSegmentIndexManifestWrites(t, false)
	meta.NewFakeManifestStore(t)
	ctx := context.Background()
	m := BootMigrationMeta(t, metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", ""), meta.RestartCollID)
	SeedLegacyBackfillRecord(t, m, meta.RestartSegID, meta.RestartBuildID)
	SeedLegacyBackfillRecord(t, m, meta.RestartSegID+1, meta.RestartBuildID+1)
	// Retain the old catalog row for GC but replace its current slot with a
	// manifest-resident build. Neither record should cause a segment lookup.
	replacement, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID + 1)
	require.True(t, ok)
	replacement.BuildID++
	require.NoError(t, m.IndexCatalog.(*IndexMeta).AddSegmentIndexFromManifest(ctx, replacement))
	var segmentLookups atomic.Int32
	patch := mockey.Mock((*meta.Meta).GetSegment).When(func(_ *meta.Meta, _ context.Context, _ int64) bool {
		segmentLookups.Add(1)
		return false
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	segmentScan := mockey.Mock((*meta.Meta).SelectSegments).To(func(*meta.Meta, context.Context, ...meta.SegmentFilter) []*meta.SegmentInfo {
		t.Error("backfill must not enumerate segments")
		return nil
	}).Build()
	t.Cleanup(func() { segmentScan.UnPatch() })
	work, pending := NewMigrationBackfillInspector(ctx, m).scan(ctx)
	require.Equal(t, 1, pending)
	require.Len(t, work, 1)
	require.Equal(t, meta.RestartBuildID, work[0].records[0].BuildID)
	assert.EqualValues(t, 1, segmentLookups.Load())
}

func TestManifestIndexMigrationLoopPacesEmptyNotifications(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates := make(chan struct{}, 1)
	scans := make(chan time.Time, 4)
	done := make(chan struct{})
	const interval = 50 * time.Millisecond
	go func() {
		defer close(done)
		runManifestIndexMigrationLoop(ctx, updates, func() time.Duration { return interval }, func(context.Context) bool {
			scans <- time.Now()
			// A finished legacy index can arrive during every empty scan.
			select {
			case updates <- struct{}{}:
			default:
			}
			return false
		})
	}()
	var previous time.Time
	for range 3 {
		select {
		case current := <-scans:
			if !previous.IsZero() {
				require.GreaterOrEqual(t, current.Sub(previous), interval, "empty notifications must not bypass the scan interval")
			}
			previous = current
		case <-time.After(5 * time.Second):
			t.Fatal("notification was lost")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cooldown did not stop on cancellation")
	}
}
