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
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManifestIndexRollbackRoundTrip(t *testing.T) {
	for _, scenario := range []string{"healthy", "dropped", "deleted definition"} {
		t.Run(scenario, func(t *testing.T) {
			m, catalog, store, _ := RollbackFixture(t)
			ctx := context.TODO()
			if scenario == "dropped" {
				require.NoError(t, m.SetState(ctx, meta.RestartSegID, commonpb.SegmentState_Dropped))
			}
			if scenario == "deleted definition" {
				require.NoError(t, m.IndexCatalog.(*IndexMeta).MarkIndexAsDeleted(ctx, meta.RestartCollID, []int64{meta.RestartIndexID}))
			}
			before := m.GetSegment(ctx, meta.RestartSegID)
			record, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
			require.True(t, ok)
			published, ok := m.IndexCatalog.(*IndexMeta).SegmentBuildInfo.Get(meta.RestartBuildID)
			require.True(t, ok)
			require.True(t, published.ManifestPublished)
			inspector := NewMigrationRollbackInspector(ctx, m)
			inspector.runOnce(ctx)
			assert.False(t, inspector.ready, "completion requires a subsequent full scan")
			after := m.GetSegment(ctx, meta.RestartSegID)
			assert.NotEqual(t, before.GetManifestPath(), after.GetManifestPath())
			assert.Equal(t, before.GetState(), after.GetState())
			assert.False(t, after.GetManifestHasIndex())
			assert.Empty(t, store.BackfillEntriesAt(after.GetManifestPath()))
			rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.True(t, published.ManifestPublished, "rollback must not mutate a reader's snapshot")
			record.ManifestPublished = false
			record.IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED
			assert.Equal(t, record, rows[0])
			assert.False(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID))
			inspector.runOnce(ctx)
			assert.True(t, inspector.ready)
			assert.Equal(t, float64(1), testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackReady))
			assert.Equal(t, after.GetManifestPath(), m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
			store.FailReadsFrom()
			restarted := BootMigrationMeta(t, catalog, meta.RestartCollID)
			recovered, found := restarted.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
			require.True(t, found, "catalog-only recovery must find the artifact")
			assert.Equal(t, record, recovered)
		})
	}
}

func TestManifestIndexRollbackIndependentSwitch(t *testing.T) {
	WithManifestIndexRollback(t, false)
	meta.WithSegmentIndexManifestWrites(t, true)
	WithManifestIndexBackfillEnabled(t, true)
	assert.True(t, WriteSegmentIndexToManifest())
	assert.True(t, manifestIndexBackfillActive())
	WithManifestIndexRollback(t, true)
	assert.False(t, WriteSegmentIndexToManifest())
	assert.False(t, manifestIndexBackfillActive())
	store := meta.NewFakeManifestStore(t)
	ctx := context.TODO()
	catalog := metastorekv.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	m := BootMigrationMeta(t, catalog, meta.RestartCollID)
	SeedRestartTask(t, m, m.IndexCatalog.(*IndexMeta))
	record, _ := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
	task := NewIndexBuildTask(model.CloneSegmentIndex(record), 1, m, m.IndexCatalog.(*IndexMeta), nil, nil, nil)
	require.NoError(t, task.SetJobInfo(&workerpb.IndexTaskInfo{
		BuildID: meta.RestartBuildID, State: commonpb.IndexState_Finished, IndexFileKeys: []string{"0"},
	}))
	assert.Zero(t, store.CommitCount)
	rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, commonpb.IndexState_Finished, rows[0].IndexState)
	// A restart with both forward switches off stays catalog-backed after
	// the rollback override itself is removed.
	meta.WithSegmentIndexManifestWrites(t, false)
	WithManifestIndexRollback(t, false)
	assert.False(t, WriteSegmentIndexToManifest())
}

func TestManifestIndexRollbackFailureAndRetry(t *testing.T) {
	for _, scenario := range []string{"read", "write", "no read back", "catalog", "pointer advances", "segment drops", "invalid entry"} {
		t.Run(scenario, func(t *testing.T) {
			m, catalog, store, kv := RollbackFixture(t)
			ctx := context.TODO()
			before := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
			originalRecord, _ := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
			var original func(*meta.Meta, context.Context, *packed.ManifestIOContext, string, meta.SegmentManifestCommit) (packed.ManifestUpdateResult, error)
			var patch *mockey.Mocker
			switch scenario {
			case "read":
				store.FailReadsFrom()
			case "catalog":
				kv.FailAtomicUpdate = true
			case "invalid entry":
				store.Revisions[before][0].Path = "/invalid/index/path"
			default:
				patch = mockey.Mock(meta.CommitManifestMutationForTest).Origin(&original).To(func(m *meta.Meta, callCtx context.Context, io *packed.ManifestIOContext, base string, commit meta.SegmentManifestCommit) (packed.ManifestUpdateResult, error) {
					if scenario == "write" {
						return packed.ManifestUpdateResult{}, merr.ErrIoTooManyRequests
					}
					next, err := original(m, callCtx, io, base, commit)
					if err != nil {
						return next, err
					}
					switch scenario {
					case "no read back":
						store.FailReadsFrom()
					case "pointer advances":
						// Model a new source revision with the same artifact.
						basePath, _, parseErr := packed.UnmarshalManifestPath(base)
						if parseErr != nil {
							return packed.ManifestUpdateResult{}, parseErr
						}
						pointer := packed.MarshalManifestPath(basePath, 50)
						store.Revisions[pointer] = store.BackfillEntriesAt(base)
						err = m.UpdateSegmentsInfo(ctx, meta.UpdateManifest(meta.RestartSegID, pointer))
					case "segment drops":
						err = m.SetState(ctx, meta.RestartSegID, commonpb.SegmentState_Dropped)
					}
					return next, err
				}).Build()
				t.Cleanup(func() { patch.UnPatch() })
			}
			n, err := m.RollbackSegmentIndexes(ctx, newMigrationManifestReadIO(t), meta.RestartSegID, meta.RestartBuildID)
			if scenario == "no read back" {
				require.NoError(t, err, "the async commit result supplies the marker without reopening the revision")
				assert.Equal(t, 1, n)
				assert.False(t, m.GetSegment(ctx, meta.RestartSegID).GetManifestHasIndex())
				rows, listErr := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
				require.NoError(t, listErr)
				assert.Len(t, rows, 1)
				return
			}
			if scenario == "segment drops" {
				require.NoError(t, err)
				assert.Equal(t, 1, n)
				assert.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, meta.RestartSegID).GetState())
				return
			}
			require.Error(t, err)
			assert.Zero(t, n)
			if scenario == "write" {
				assert.ErrorIs(t, err, merr.ErrIoTooManyRequests)
			}
			if scenario == "pointer advances" {
				assert.ErrorIs(t, err, meta.ErrSegmentManifestStale)
			} else {
				assert.Equal(t, before, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath())
			}
			rows, listErr := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
			require.NoError(t, listErr)
			assert.Empty(t, rows)
			assert.True(t, m.GetSegment(ctx, meta.RestartSegID).GetManifestHasIndex())
			assert.True(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID))
			if patch != nil {
				patch.UnPatch()
			}
			kv.FailAtomicUpdate = false
			store.ResumeReads()
			if scenario == "invalid entry" {
				entry, buildErr := meta.BuildManifestIndexInfo(m, m.GetSegment(ctx, meta.RestartSegID), originalRecord)
				require.NoError(t, buildErr)
				store.Revisions[before] = []packed.ManifestIndexInfo{entry}
			}
			n, err = m.RollbackSegmentIndexes(ctx, newMigrationManifestReadIO(t), meta.RestartSegID, meta.RestartBuildID)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
			rows, listErr = catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
			require.NoError(t, listErr)
			assert.Len(t, rows, 1)
		})
	}
}

func TestManifestIndexRollbackPreservesCatalogAndReplacement(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(strconv.FormatBool(replacement), func(t *testing.T) {
			m, catalog, _, _ := RollbackFixture(t)
			ctx := context.TODO()
			old, _ := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
			current := model.CloneSegmentIndex(old)
			current.IndexVersion++
			current.IndexState = commonpb.IndexState_InProgress
			if replacement {
				current.BuildID++
			}
			require.NoError(t, m.IndexCatalog.(*IndexMeta).AddSegmentIndex(ctx, current))
			current.ManifestPublished = false
			n, err := m.RollbackSegmentIndexes(ctx, newMigrationManifestReadIO(t), meta.RestartSegID, meta.RestartBuildID)
			require.NoError(t, err)
			if replacement {
				assert.Equal(t, 1, n)
			} else {
				assert.Zero(t, n, "catalog-backed records are no longer rollback work")
			}
			rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
			require.NoError(t, err)
			if replacement {
				assert.Len(t, rows, 2)
			} else {
				assert.Equal(t, []*model.SegmentIndex{current}, rows)
			}
			assert.Equal(t, current, m.IndexCatalog.(*IndexMeta).GetSegmentIndexes(meta.RestartCollID, meta.RestartSegID)[meta.RestartIndexID])
		})
	}
}

func TestManifestIndexRollbackRejectsPublicMutation(t *testing.T) {
	m, _, store, _ := RollbackFixture(t)
	count := store.CommitCount
	err := m.CommitSegmentManifest(context.TODO(), meta.SegmentManifestCommit{
		SegmentID: meta.RestartSegID,
		Mutation: meta.ManifestMutation{Type: meta.ManifestMutationCommitUpdates, Updates: &packed.ManifestUpdates{
			DropIndexes: []packed.DropIndexEntry{{IndexID: meta.RestartIndexID, ExpectedBuildID: meta.RestartBuildID}},
		}},
		CatalogMutation: meta.SegmentCatalogMutation{SegmentIndexes: []meta.SegmentIndexMutation{{Type: meta.SegmentIndexRollback, BuildID: meta.RestartBuildID}}},
	})
	assert.Error(t, err)
	assert.Equal(t, count, store.CommitCount)
}

func TestManifestIndexRollbackRestoresSupersededBuilds(t *testing.T) {
	m, catalog, store, _ := RollbackFixture(t)
	ctx := context.TODO()
	old, _ := m.IndexCatalog.(*IndexMeta).GetIndexJob(meta.RestartBuildID)
	replacement := model.CloneSegmentIndex(old)
	replacement.BuildID = 10000 // sorts before 8100 as an etcd key, but is newer
	require.NoError(t, m.IndexCatalog.(*IndexMeta).AddSegmentIndex(ctx, replacement))
	require.NoError(t, NewMigrationBackfillInspector(ctx, m).backfillIndexes(ctx, meta.RestartSegID, replacement.BuildID))
	replacement, ok := m.IndexCatalog.(*IndexMeta).GetIndexJob(replacement.BuildID)
	require.True(t, ok)
	require.True(t, replacement.ManifestPublished)
	// Model real index_id replacement (the shared fake appends instead).
	current := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
	entries := store.BackfillEntriesAt(current)
	require.Len(t, entries, 2)
	store.Revisions[current] = entries[1:]
	SetRollbackTestParam(t, &Params.MetaStoreCfg.MaxEtcdTxnNum, "2")
	inspector := NewMigrationRollbackInspector(ctx, m)
	inspector.runOnce(ctx)
	rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, old.BuildID, rows[0].BuildID)
	assert.Equal(t, current, m.GetSegment(ctx, meta.RestartSegID).GetManifestPath(), "absent old entry requires only an atomic catalog publication")
	assert.Equal(t, replacement, m.IndexCatalog.(*IndexMeta).GetSegmentIndexes(meta.RestartCollID, meta.RestartSegID)[meta.RestartIndexID])
	inspector.runOnce(ctx)
	inspector.runOnce(ctx)
	assert.True(t, inspector.ready)
	store.FailReadsFrom()
	restarted := BootMigrationMeta(t, catalog, meta.RestartCollID)
	assert.Equal(t, replacement.BuildID, restarted.IndexCatalog.(*IndexMeta).GetSegmentIndexes(meta.RestartCollID, meta.RestartSegID)[meta.RestartIndexID].BuildID)
	_, present := restarted.IndexCatalog.(*IndexMeta).GetIndexJob(old.BuildID)
	assert.True(t, present, "superseded artifact remains durably available to GC")
	require.NoError(t, restarted.IndexCatalog.(*IndexMeta).RemoveSegmentIndex(ctx, old.BuildID))
	assert.Equal(t, replacement.BuildID, restarted.IndexCatalog.(*IndexMeta).GetSegmentIndexes(meta.RestartCollID, meta.RestartSegID)[meta.RestartIndexID].BuildID,
		"retiring a restored superseded record must preserve the replacement slot")
}

func TestManifestIndexRollbackIgnoresMarkerOnlySegments(t *testing.T) {
	m, catalog, store, _ := RollbackFixture(t)
	ctx := context.TODO()
	first := m.GetSegment(ctx, meta.RestartSegID).GetManifestPath()
	store.Revisions[first] = nil
	require.NoError(t, m.UpdateSegmentsInfo(ctx, meta.ClearEmptyManifestIndexMarker(meta.RestartSegID, first)))
	// A marker-only segment must not be read or considered rollback work.
	SeedLegacyBackfillRecord(t, m, meta.RestartSegID-1, meta.RestartBuildID-1)
	require.NoError(t, m.UpdateSegmentsInfo(ctx, meta.UpdateManifestHasIndex(meta.RestartSegID-1), meta.UpdateManifest(meta.RestartSegID-1, "")))
	SetRollbackTestParam(t, &Params.DataCoordCfg.ManifestIndexRollbackBatchSize, "1")
	inspector := NewMigrationRollbackInspector(ctx, m)
	inspector.runOnce(ctx)
	assert.False(t, inspector.ready)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackPending))
	assert.False(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID))
	inspector.runOnce(ctx)
	rows, err := catalog.ListSegmentIndexes(ctx, meta.RestartCollID)
	require.NoError(t, err)
	assert.Len(t, rows, 2, "unmarked manifest-absent record also returns to etcd")
	assert.False(t, IndexManifestPublished(t, m.IndexCatalog.(*IndexMeta), meta.RestartBuildID))
	assert.True(t, inspector.ready, "marker-only segments belong to GC")
}
