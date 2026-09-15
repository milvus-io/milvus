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
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	metastorekv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func setRollbackTestParam(t *testing.T, item *paramtable.ParamItem, value string) {
	t.Helper()
	previous := item.GetValue()
	Params.Save(item.Key, value)
	t.Cleanup(func() { Params.Save(item.Key, previous) })
}

func withManifestIndexRollback(t *testing.T, enabled bool) {
	t.Helper()
	setRollbackTestParam(t, &Params.DataCoordCfg.ManifestIndexRollbackEnabled, strconv.FormatBool(enabled))
}

func rollbackFixture(t *testing.T) (*meta, *metastorekv.Catalog, *fakeManifestStore, *failBackfillCatalogKV) {
	t.Helper()
	withSegmentIndexManifestWrites(t, false)
	withManifestIndexRollback(t, false)
	store := newFakeManifestStore(t)
	kv := &failBackfillCatalogKV{metaMemoryKV: NewMetaMemoryKV()}
	catalog := metastorekv.NewCatalog(kv, "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedLegacyBackfillRecord(t, m, restartSegID, restartBuildID)
	require.NoError(t, newManifestIndexBackfillInspector(context.TODO(), m).backfillIndex(context.TODO(), restartSegID, restartBuildID))
	withManifestIndexRollback(t, true)
	return m, catalog, store, kv
}

func TestManifestIndexRollbackRoundTrip(t *testing.T) {
	for _, scenario := range []string{"healthy", "dropped", "deleted definition"} {
		t.Run(scenario, func(t *testing.T) {
			m, catalog, store, _ := rollbackFixture(t)
			ctx := context.TODO()
			if scenario == "dropped" {
				require.NoError(t, m.SetState(ctx, restartSegID, commonpb.SegmentState_Dropped))
			}
			if scenario == "deleted definition" {
				require.NoError(t, m.indexMeta.MarkIndexAsDeleted(ctx, restartCollID, []int64{restartIndexID}))
			}
			before := m.GetSegment(ctx, restartSegID)
			record, ok := m.indexMeta.GetIndexJob(restartBuildID)
			require.True(t, ok)
			inspector := newManifestIndexRollbackInspector(ctx, m, nil)
			inspector.runOnce(ctx)
			assert.False(t, inspector.ready, "completion requires a subsequent full scan")
			after := m.GetSegment(ctx, restartSegID)
			assert.NotEqual(t, before.GetManifestPath(), after.GetManifestPath())
			assert.Equal(t, before.GetState(), after.GetState())
			assert.False(t, after.GetManifestHasIndex())
			assert.Empty(t, store.backfillEntriesAt(after.GetManifestPath()))
			rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.Equal(t, record, rows[0])
			assert.False(t, m.indexMeta.isSegmentIndexCatalogAbsent(restartBuildID))
			inspector.runOnce(ctx)
			assert.True(t, inspector.ready)
			assert.Equal(t, float64(1), testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackReady))
			assert.Equal(t, after.GetManifestPath(), m.GetSegment(ctx, restartSegID).GetManifestPath())
			store.failReadsFrom()
			restarted := bootMetaForRestart(t, catalog, restartCollID)
			recovered, found := restarted.indexMeta.GetIndexJob(restartBuildID)
			require.True(t, found, "catalog-only recovery must find the artifact")
			assert.Equal(t, record, recovered)
		})
	}
}

func TestManifestIndexRollbackIndependentSwitch(t *testing.T) {
	withManifestIndexRollback(t, false)
	withSegmentIndexManifestWrites(t, true)
	withManifestIndexBackfillEnabled(t, true)
	assert.True(t, writeSegmentIndexToManifest())
	assert.True(t, manifestIndexBackfillActive())
	withManifestIndexRollback(t, true)
	assert.False(t, writeSegmentIndexToManifest())
	assert.False(t, manifestIndexBackfillActive())
	store := newFakeManifestStore(t)
	ctx := context.TODO()
	catalog := metastorekv.NewCatalog(NewMetaMemoryKV(), "", "")
	m := bootMetaForRestart(t, catalog, restartCollID)
	seedRestartTask(t, m)
	record, _ := m.indexMeta.GetIndexJob(restartBuildID)
	task := &indexBuildTask{SegmentIndex: model.CloneSegmentIndex(record), meta: m}
	require.NoError(t, task.setJobInfo(&workerpb.IndexTaskInfo{
		BuildID: restartBuildID, State: commonpb.IndexState_Finished, IndexFileKeys: []string{"0"},
	}))
	assert.Zero(t, store.commitCount)
	rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, commonpb.IndexState_Finished, rows[0].IndexState)
	// A restart with both forward switches off stays catalog-backed after
	// the rollback override itself is removed.
	withSegmentIndexManifestWrites(t, false)
	withManifestIndexRollback(t, false)
	assert.False(t, writeSegmentIndexToManifest())
}

func TestManifestIndexRollbackFailureAndRetry(t *testing.T) {
	for _, scenario := range []string{"read", "write", "read back", "catalog", "pointer advances", "segment drops", "invalid entry"} {
		t.Run(scenario, func(t *testing.T) {
			m, catalog, store, kv := rollbackFixture(t)
			ctx := context.TODO()
			before := m.GetSegment(ctx, restartSegID).GetManifestPath()
			originalRecord, _ := m.indexMeta.GetIndexJob(restartBuildID)
			var original func(string, SegmentManifestCommit) (string, error)
			var patch *mockey.Mocker
			switch scenario {
			case "read":
				store.failReadsFrom()
			case "catalog":
				kv.failAtomicUpdate = true
			case "invalid entry":
				store.revisions[before][0].Path = "/invalid/index/path"
			default:
				patch = mockey.Mock(commitManifestMutation).Origin(&original).To(func(base string, commit SegmentManifestCommit) (string, error) {
					if scenario == "write" {
						return "", merr.ErrIoTooManyRequests
					}
					next, err := original(base, commit)
					if err != nil {
						return next, err
					}
					switch scenario {
					case "read back":
						store.failReadsFrom()
					case "pointer advances":
						// Model a new source revision with the same artifact.
						basePath, _, parseErr := packed.UnmarshalManifestPath(base)
						if parseErr != nil {
							return "", parseErr
						}
						pointer := packed.MarshalManifestPath(basePath, 50)
						store.revisions[pointer] = store.backfillEntriesAt(base)
						err = m.UpdateSegmentsInfo(ctx, UpdateManifest(restartSegID, pointer))
					case "segment drops":
						err = m.SetState(ctx, restartSegID, commonpb.SegmentState_Dropped)
					}
					return next, err
				}).Build()
				t.Cleanup(func() { patch.UnPatch() })
			}
			n, err := m.rollbackSegmentIndexes(ctx, restartSegID)
			if scenario == "segment drops" {
				require.NoError(t, err)
				assert.Equal(t, 1, n)
				assert.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, restartSegID).GetState())
				return
			}
			require.Error(t, err)
			assert.Zero(t, n)
			if scenario == "write" {
				assert.ErrorIs(t, err, merr.ErrIoTooManyRequests)
			}
			if scenario == "pointer advances" {
				assert.ErrorIs(t, err, errSegmentManifestStale)
			} else {
				assert.Equal(t, before, m.GetSegment(ctx, restartSegID).GetManifestPath())
			}
			rows, listErr := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, listErr)
			assert.Empty(t, rows)
			assert.True(t, m.GetSegment(ctx, restartSegID).GetManifestHasIndex())
			assert.True(t, m.indexMeta.isSegmentIndexCatalogAbsent(restartBuildID))
			if patch != nil {
				patch.UnPatch()
			}
			kv.failAtomicUpdate = false
			store.failReads = false
			if scenario == "invalid entry" {
				entry, buildErr := buildManifestIndexInfo(m, m.GetSegment(ctx, restartSegID), originalRecord)
				require.NoError(t, buildErr)
				store.revisions[before] = []packed.ManifestIndexInfo{entry}
			}
			n, err = m.rollbackSegmentIndexes(ctx, restartSegID)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
			rows, listErr = catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, listErr)
			assert.Len(t, rows, 1)
		})
	}
}

func TestManifestIndexRollbackPreservesCatalogAndReplacement(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(strconv.FormatBool(replacement), func(t *testing.T) {
			m, catalog, _, _ := rollbackFixture(t)
			ctx := context.TODO()
			old, _ := m.indexMeta.GetIndexJob(restartBuildID)
			current := model.CloneSegmentIndex(old)
			current.IndexVersion++
			current.IndexState = commonpb.IndexState_InProgress
			if replacement {
				current.BuildID++
			}
			require.NoError(t, m.indexMeta.AddSegmentIndex(ctx, current))
			n, err := m.rollbackSegmentIndexes(ctx, restartSegID)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
			rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
			require.NoError(t, err)
			if replacement {
				assert.Len(t, rows, 2)
			} else {
				assert.Equal(t, []*model.SegmentIndex{current}, rows)
			}
			assert.Equal(t, current, m.indexMeta.GetSegmentIndexes(restartCollID, restartSegID)[restartIndexID])
		})
	}
}

func TestManifestIndexRollbackRejectsPublicMutation(t *testing.T) {
	m, _, store, _ := rollbackFixture(t)
	count := store.commitCount
	err := m.CommitSegmentManifest(context.TODO(), SegmentManifestCommit{
		SegmentID: restartSegID,
		Mutation: ManifestMutation{Type: ManifestMutationCommitUpdates, Updates: &packed.ManifestUpdates{
			DropIndexes: []packed.DropIndexEntry{{IndexID: restartIndexID, ExpectedBuildID: restartBuildID}},
		}},
		CatalogMutation: SegmentCatalogMutation{SegmentIndexes: []SegmentIndexMutation{{Type: SegmentIndexRollback, BuildID: restartBuildID}}},
	})
	assert.Error(t, err)
	assert.Equal(t, count, store.commitCount)
}

func TestManifestIndexRollbackRestoresSupersededBuilds(t *testing.T) {
	m, catalog, store, _ := rollbackFixture(t)
	ctx := context.TODO()
	old, _ := m.indexMeta.GetIndexJob(restartBuildID)
	replacement := model.CloneSegmentIndex(old)
	replacement.BuildID = 10000 // sorts before 8100 as an etcd key, but is newer
	require.NoError(t, m.indexMeta.AddSegmentIndex(ctx, replacement))
	require.NoError(t, newManifestIndexBackfillInspector(ctx, m).backfillIndex(ctx, restartSegID, replacement.BuildID))
	// Model real index_id replacement (the shared fake appends instead).
	current := m.GetSegment(ctx, restartSegID).GetManifestPath()
	entries := store.backfillEntriesAt(current)
	require.Len(t, entries, 2)
	store.revisions[current] = entries[1:]
	setRollbackTestParam(t, &Params.MetaStoreCfg.MaxEtcdTxnNum, "2")
	inspector := newManifestIndexRollbackInspector(ctx, m, nil)
	inspector.runOnce(ctx)
	rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, old.BuildID, rows[0].BuildID)
	assert.Equal(t, current, m.GetSegment(ctx, restartSegID).GetManifestPath(), "absent old entry requires only an atomic catalog publication")
	assert.Equal(t, replacement, m.indexMeta.GetSegmentIndexes(restartCollID, restartSegID)[restartIndexID])
	inspector.runOnce(ctx)
	inspector.runOnce(ctx)
	assert.True(t, inspector.ready)
	store.failReadsFrom()
	restarted := bootMetaForRestart(t, catalog, restartCollID)
	assert.Equal(t, replacement.BuildID, restarted.indexMeta.GetSegmentIndexes(restartCollID, restartSegID)[restartIndexID].BuildID)
	_, present := restarted.indexMeta.GetIndexJob(old.BuildID)
	assert.True(t, present, "superseded artifact remains durably available to GC")
	require.NoError(t, restarted.indexMeta.RemoveSegmentIndex(ctx, old.BuildID))
	assert.Equal(t, replacement.BuildID, restarted.indexMeta.GetSegmentIndexes(restartCollID, restartSegID)[restartIndexID].BuildID,
		"retiring a restored superseded record must preserve the replacement slot")
}

func TestManifestIndexRollbackUnmarkedRecordsAndFairness(t *testing.T) {
	m, catalog, store, _ := rollbackFixture(t)
	ctx := context.TODO()
	first := m.GetSegment(ctx, restartSegID).GetManifestPath()
	store.revisions[first] = nil
	require.NoError(t, m.UpdateSegmentsInfo(ctx, clearEmptyManifestIndexMarker(restartSegID, first)))
	// A broken marked segment sorted before the good one must not starve it.
	seedLegacyBackfillRecord(t, m, restartSegID-1, restartBuildID-1)
	require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateManifestHasIndex(restartSegID-1), UpdateManifest(restartSegID-1, "")))
	setRollbackTestParam(t, &Params.DataCoordCfg.ManifestIndexRollbackBatchSize, "1")
	inspector := newManifestIndexRollbackInspector(ctx, m, nil)
	inspector.runOnce(ctx)
	assert.False(t, inspector.ready)
	assert.Equal(t, float64(2), testutil.ToFloat64(metrics.DataCoordManifestIndexRollbackPending))
	inspector.runOnce(ctx)
	rows, err := catalog.ListSegmentIndexes(ctx, restartCollID)
	require.NoError(t, err)
	assert.Len(t, rows, 2, "unmarked manifest-absent record also returns to etcd")
	assert.False(t, m.indexMeta.isSegmentIndexCatalogAbsent(restartBuildID))
	assert.False(t, inspector.ready, "the broken marked segment remains a blocker")
}
