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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metacache"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func jsonFlatTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 101, Name: "json", DataType: schemapb.DataType_JSON}, {FieldID: 102, Name: "scalar", DataType: schemapb.DataType_Int64}}}
}

func jsonFlatDefinition() *model.Index {
	return &model.Index{CollectionID: 1, FieldID: 101, IndexID: 10, IndexName: "json_flat", IndexParams: []*commonpb.KeyValuePair{
		{Key: common.IndexTypeKey, Value: "INVERTED"},
		{Key: common.JSONCastTypeKey, Value: "JSON"},
		{Key: common.JSONPathKey, Value: ""},
	}}
}

func TestJSONFlatIndexRecoveryRemovesPendingBuildForDowngrade(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().DropSegmentIndex(mock.Anything, int64(1), int64(0), int64(100), int64(1000)).Return(nil).Once()
	m := NewSegmentIndexMeta(catalog)
	m.getCollectionSchema = func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() }
	m.updateCollectionIndex(jsonFlatDefinition())
	segIdx := &model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, NodeID: 20, IndexState: commonpb.IndexState_InProgress}
	m.updateSegmentIndex(segIdx)
	store := metacache.NewMetaStore(nil)
	mt := &meta.Meta{IndexCatalog: m, MetaStore: store, Segments: meta.NewSegmentsInfo(store)}
	mt.Segments.SetSegment(100, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, IsSorted: true}})
	scheduler := task.NewMockGlobalScheduler(t)
	var recovered *indexBuildTask
	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(value task.Task) { recovered = value.(*indexBuildTask) })
	inspector := &IndexInspector{meta: mt, indexMeta: m, scheduler: scheduler}
	inspector.reloadFromMeta()
	require.NotNil(t, recovered)
	require.NoError(t, inspector.createIndexesForSegment(context.Background(), mt.GetSegment(context.Background(), 100)))
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().DropIndex(int64(20), int64(1000)).Return(nil)
	recovered.QueryTaskOnWorker(cluster)
	_, ok := m.GetIndexJob(1000)
	require.False(t, ok)
	assert.Empty(t, m.GetAllSegmentIndexSummaries(100))

	// A late completion cannot publish files even after recovery retired the worker.
	require.NoError(t, recovered.SetJobInfo(&workerpb.IndexTaskInfo{BuildID: 1000, State: commonpb.IndexState_Finished, IndexFileKeys: []string{"late-file"}}))
	_, ok = m.GetIndexJob(1000)
	assert.False(t, ok, "late completion must not recreate the retired record")
}

func TestJSONFlatIndexRecoveryRemovesUndispatchedBuilds(t *testing.T) {
	for _, state := range []commonpb.IndexState{commonpb.IndexState_Unissued, commonpb.IndexState_Retry} {
		t.Run(state.String(), func(t *testing.T) {
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().DropSegmentIndex(mock.Anything, int64(1), int64(0), int64(100), int64(1000)).Return(nil).Once()
			m := NewSegmentIndexMeta(catalog)
			m.getCollectionSchema = func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() }
			m.updateCollectionIndex(jsonFlatDefinition())
			nodeID := int64(0)
			if state == commonpb.IndexState_Retry {
				nodeID = 20
			}
			m.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, NodeID: nodeID, IndexState: state})
			store := metacache.NewMetaStore(nil)
			mt := &meta.Meta{IndexCatalog: m, MetaStore: store, Segments: meta.NewSegmentsInfo(store)}
			mt.Segments.SetSegment(100, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, IsSorted: true}})
			scheduler := task.NewMockGlobalScheduler(t)
			var recovered *indexBuildTask
			scheduler.EXPECT().Enqueue(mock.Anything).Run(func(value task.Task) { recovered = value.(*indexBuildTask) })
			(&IndexInspector{meta: mt, indexMeta: m, scheduler: scheduler}).reloadFromMeta()
			require.NotNil(t, recovered)
			worker := session.NewMockCluster(t)
			if nodeID != 0 {
				worker.EXPECT().DropIndex(nodeID, int64(1000)).Return(nil).Once()
			}
			recovered.CreateTaskOnWorker(20, worker, recovered.GetTaskResource())
			_, ok := m.GetIndexJob(1000)
			require.False(t, ok)
			assert.Empty(t, m.GetAllSegmentIndexSummaries(100))
		})
	}
}

func TestJSONFlatIndexColdSchemaDefersBeforeCreatingBuild(t *testing.T) {
	m := NewSegmentIndexMeta(nil)
	m.updateCollectionIndex(jsonFlatDefinition())
	store := metacache.NewMetaStore(nil)
	mt := &meta.Meta{IndexCatalog: m, MetaStore: store, Segments: meta.NewSegmentsInfo(store)}
	handler := meta.NewNMockHandler(t)
	handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(&meta.CollectionInfo{ID: 1, Schema: jsonFlatTestSchema()}, nil)
	segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, IsSorted: true}}
	require.NoError(t, (&IndexInspector{meta: mt, indexMeta: m, Handler: handler}).createIndexesForSegment(context.Background(), segment))
	assert.Empty(t, m.GetAllSegmentIndexSummaries(100))
}

func TestJSONFlatIndexUsesBoundCollectionSchemaCache(t *testing.T) {
	mt, domains, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	im := domains.IndexMeta
	im.updateCollectionIndex(jsonFlatDefinition())
	// Params alone cannot prove a JSON field while the schema cache is cold.
	require.Empty(t, im.GetPhysicalIndexesForCollection(1))
	mt.AddCollection(&meta.CollectionInfo{ID: 1, Schema: jsonFlatTestSchema()})
	assert.Empty(t, im.GetPhysicalIndexesForCollection(1))
	assert.Len(t, im.GetIndexesForCollection(1, "json_flat"), 1)
}

func TestJSONFlatIndexManifestMigrationDirections(t *testing.T) {
	paramtable.Init()
	for _, published := range []bool{false, true} {
		name := "backfill"
		if published {
			name = "rollback"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			im := NewSegmentIndexMeta(nil)
			im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() })
			flat := jsonFlatDefinition()
			typed := model.CloneIndex(flat)
			typed.IndexID, typed.IndexName = 11, "typed"
			typed.IndexParams[1].Value = "DOUBLE"
			scalar := model.CloneIndex(flat)
			scalar.IndexID, scalar.FieldID, scalar.IndexName = 12, 102, "scalar"
			for _, definition := range []*model.Index{flat, typed, scalar} {
				im.updateCollectionIndex(definition)
				// Historical records must remain available to ordinary GC.
				im.updateSegmentIndex(&model.SegmentIndex{
					CollectionID: 1, SegmentID: 100, IndexID: definition.IndexID,
					BuildID: definition.IndexID + 1000, IndexState: commonpb.IndexState_Finished,
					IndexFileKeys: []string{"old-file"}, ManifestPublished: published,
				})
			}
			store := metacache.NewMetaStore(nil)
			mt := &meta.Meta{IndexCatalog: im, MetaStore: store, Segments: meta.NewSegmentsInfo(store)}
			mt.Segments.SetSegment(100, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed,
				StorageVersion: storage.StorageV3,
			}})
			mutation := meta.SegmentIndexMutation{Type: meta.SegmentIndexBackfill, BuildID: 1010}
			wantErr := meta.ErrSegmentIndexBackfillSkipped
			if published {
				work, pending := NewManifestIndexRollbackInspector(ctx, mt, im).scan(ctx)
				require.Len(t, work, 1)
				assert.Equal(t, 3, pending)
				assert.ElementsMatch(t, []int64{1010, 1011, 1012}, work[0].buildIDs)
				mutation.Type = meta.SegmentIndexRollback
				wantErr = nil
			} else {
				work, pending := NewManifestIndexBackfillInspector(ctx, mt, im).scan(ctx)
				require.Len(t, work, 1)
				assert.Equal(t, 2, pending)
				require.Len(t, work[0].records, 2)
				assert.EqualValues(t, 11, work[0].records[0].IndexID)
				assert.EqualValues(t, 12, work[0].records[1].IndexID)
			}
			// Backfill rejects flat; rollback restores its ownership record.
			im.LockBuildIDs([]int64{1010})
			_, err := im.StageSegmentIndexMutation(mutation)
			im.UnlockBuildIDs([]int64{1010})
			assert.ErrorIs(t, err, wantErr)
			assert.Len(t, im.GetAllSegmentIndexSummaries(100), 3)
			assert.Len(t, im.GetIndexesForCollection(1, ""), 3)
			// Ordinary typed and scalar indexes still have a migratable artifact.
			for _, buildID := range []int64{1011, 1012} {
				mutation.BuildID = buildID
				im.LockBuildIDs([]int64{buildID})
				_, err = im.StageSegmentIndexMutation(mutation)
				im.UnlockBuildIDs([]int64{buildID})
				assert.NoError(t, err)
			}
		})
	}
}

func TestJSONFlatIndexManifestMigrationRetriesColdSchema(t *testing.T) {
	paramtable.Init()
	for _, published := range []bool{false, true} {
		name := "backfill"
		if published {
			name = "rollback"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().DropSegmentIndex(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			im := NewSegmentIndexMeta(catalog)
			var schema *schemapb.CollectionSchema
			im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return schema })
			scalar := jsonFlatDefinition()
			scalar.IndexID, scalar.FieldID, scalar.IndexName = 11, 102, "scalar"
			for _, definition := range []*model.Index{jsonFlatDefinition(), scalar} {
				im.updateCollectionIndex(definition)
				im.updateSegmentIndex(&model.SegmentIndex{
					CollectionID: 1, SegmentID: 100, IndexID: definition.IndexID,
					BuildID: definition.IndexID + 1000, IndexState: commonpb.IndexState_Finished,
					IndexFileKeys: []string{"old-file"}, ManifestPublished: published,
				})
			}
			store := metacache.NewMetaStore(nil)
			mt := &meta.Meta{IndexCatalog: im, MetaStore: store, Segments: meta.NewSegmentsInfo(store)}
			mt.Segments.SetSegment(100, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
			}})
			WithManifestIndexRollback(t, published)
			meta.WithSegmentIndexManifestWrites(t, !published)
			WithManifestIndexBackfillEnabled(t, !published)
			backfill := NewManifestIndexBackfillInspector(ctx, mt, im)
			rollback := NewManifestIndexRollbackInspector(ctx, mt, im)
			if published {
				work, pending := rollback.scan(ctx)
				require.Len(t, work, 1)
				assert.ElementsMatch(t, []int64{1010, 1011}, work[0].buildIDs)
				assert.Equal(t, 2, pending)
			} else {
				work, pending := backfill.scan(ctx)
				assert.Empty(t, work)
				assert.Equal(t, 2, pending)
				assert.True(t, backfill.runOnce(ctx), "unresolved records must stay on the retry loop")
			}
			assert.Len(t, im.GetAllSegmentIndexSummaries(100), 2)
			schema = jsonFlatTestSchema()
			if published {
				work, pending := rollback.scan(ctx)
				require.Len(t, work, 1)
				assert.ElementsMatch(t, []int64{1010, 1011}, work[0].buildIDs)
				assert.Equal(t, 2, pending)
			} else {
				work, pending := backfill.scan(ctx)
				require.Len(t, work, 1)
				require.Len(t, work[0].records, 1)
				assert.EqualValues(t, 1011, work[0].records[0].BuildID)
				assert.Equal(t, 1, pending)
			}
			mutation := meta.SegmentIndexMutation{Type: meta.SegmentIndexBackfill, BuildID: 1011}
			if published {
				mutation.Type = meta.SegmentIndexRollback
			}
			im.LockBuildIDs([]int64{1011})
			_, err := im.StageSegmentIndexMutation(mutation)
			im.UnlockBuildIDs([]int64{1011})
			assert.NoError(t, err, "an ordinary scalar with JSON extras resumes migration")
			// Explicit GC retirement also removes a schema-less record from the backlog.
			schema = nil
			require.NoError(t, im.RemoveSegmentIndex(ctx, 1010))
			require.NoError(t, im.RemoveSegmentIndex(ctx, 1011))
			if published {
				work, pending := rollback.scan(ctx)
				assert.Empty(t, work)
				assert.Zero(t, pending)
				assert.False(t, rollback.runOnce(ctx))
			} else {
				work, pending := backfill.scan(ctx)
				assert.Empty(t, work)
				assert.Zero(t, pending)
				assert.False(t, backfill.runOnce(ctx))
			}
		})
	}
}

func TestJSONFlatHistoricalRollbackPreservesOwnership(t *testing.T) {
	for _, scenario := range []string{"flat", "cold schema", "missing field", "deleted definition", "missing definition"} {
		t.Run(scenario, func(t *testing.T) {
			mt, catalog, manifests, kv := RollbackFixture(t)
			ctx := context.Background()
			im := mt.IndexCatalog.(*IndexMeta)
			record, ok := im.GetIndexJob(meta.RestartBuildID)
			require.True(t, ok)
			flat := jsonFlatDefinition()
			flat.CollectionID, flat.FieldID, flat.IndexID = meta.RestartCollID, meta.RestartFieldID, meta.RestartIndexID
			im.updateCollectionIndex(flat)
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: flat.FieldID, DataType: schemapb.DataType_JSON}}}
			switch scenario {
			case "cold schema":
				schema = nil
			case "missing field":
				schema = &schemapb.CollectionSchema{}
			case "deleted definition":
				require.NoError(t, im.MarkIndexAsDeleted(ctx, flat.CollectionID, []int64{flat.IndexID}))
			case "missing definition":
				delete(im.Indexes[flat.CollectionID], flat.IndexID)
			}
			im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return schema })
			before := mt.GetSegment(ctx, record.SegmentID).GetManifestPath()
			// Model the retired physical artifact, including its original file keys.
			record.IndexType = "INVERTED"
			im.updateSegmentIndex(record)
			manifests.Revisions[before][0].IndexType = record.IndexType
			inspector := NewMigrationRollbackInspector(ctx, mt)
			kv.FailAtomicUpdate = true
			require.True(t, inspector.runOnce(ctx))
			assert.False(t, inspector.ready)
			assert.Equal(t, before, mt.GetSegment(ctx, record.SegmentID).GetManifestPath())
			rows, err := catalog.ListSegmentIndexes(ctx, record.CollectionID)
			require.NoError(t, err)
			assert.Empty(t, rows)
			retained, _ := im.GetIndexJob(record.BuildID)
			assert.Equal(t, record, retained)
			assert.Len(t, manifests.BackfillEntriesAt(before), 1)

			kv.FailAtomicUpdate = false
			require.True(t, inspector.runOnce(ctx))
			assert.False(t, inspector.ready, "a successful batch is not a complete scan")
			require.False(t, inspector.runOnce(ctx))
			assert.True(t, inspector.ready)
			rows, err = catalog.ListSegmentIndexes(ctx, record.CollectionID)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			record.ManifestPublished = false
			record.IndexStorePathVersion = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED
			assert.Equal(t, record, rows[0])
			assert.Empty(t, manifests.BackfillEntriesAt(mt.GetSegment(ctx, record.SegmentID).GetManifestPath()))
			manifests.FailReadsFrom()
			restarted := BootMigrationMeta(t, catalog, record.CollectionID)
			recovered, found := restarted.IndexCatalog.GetIndexJob(record.BuildID)
			require.True(t, found)
			assert.Equal(t, record, recovered)
		})
	}
}

func TestJSONFlatBackfillFieldLifecycle(t *testing.T) {
	mt, catalog, _, _ := RollbackFixture(t)
	ctx := context.Background()
	im := mt.IndexCatalog.(*IndexMeta)
	// Restore the historical artifact to etcd before testing forward migration.
	_, err := mt.RollbackSegmentIndexes(ctx, newMigrationManifestReadIO(t), meta.RestartSegID, meta.RestartBuildID)
	require.NoError(t, err)
	flat := jsonFlatDefinition()
	flat.CollectionID, flat.FieldID, flat.IndexID = meta.RestartCollID, meta.RestartFieldID, meta.RestartIndexID
	im.updateCollectionIndex(flat)
	var schema *schemapb.CollectionSchema
	im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return schema })
	inspector := NewMigrationBackfillInspector(ctx, mt)
	for _, view := range []*schemapb.CollectionSchema{nil, {Version: 1}} {
		schema = view
		work, pending := inspector.scan(ctx)
		assert.Empty(t, work)
		assert.Equal(t, 1, pending, "a missing field is not authoritative deletion")
	}
	// A recovered ordinary scalar with ignored JSON extras can migrate.
	schema = &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: flat.FieldID, DataType: schemapb.DataType_Int64}}}
	work, pending := inspector.scan(ctx)
	require.Len(t, work, 1)
	assert.Equal(t, 1, pending)
	// A confirmed historical flat stays in etcd, retaining file ownership.
	schema.Fields[0].DataType = schemapb.DataType_JSON
	work, pending = inspector.scan(ctx)
	assert.Empty(t, work)
	assert.Zero(t, pending)
	rows, err := catalog.ListSegmentIndexes(ctx, flat.CollectionID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.NotEmpty(t, rows[0].IndexFileKeys)
	original := rows[0]
	// Real field-drop completion cascades MarkIndexAsDeleted. The classifier
	// needs no tombstone inferred from an incomplete or stale schema snapshot.
	schema = &schemapb.CollectionSchema{Version: 2}
	require.NoError(t, im.MarkIndexAsDeleted(ctx, flat.CollectionID, []int64{flat.IndexID}))
	work, pending = inspector.scan(ctx)
	assert.Empty(t, work)
	assert.Zero(t, pending)
	im.LockBuildIDs([]int64{original.BuildID})
	_, err = im.StageSegmentIndexMutation(meta.SegmentIndexMutation{Type: meta.SegmentIndexBackfill, BuildID: original.BuildID})
	im.UnlockBuildIDs([]int64{original.BuildID})
	assert.ErrorIs(t, err, meta.ErrSegmentIndexBackfillSkipped)
	rows, err = catalog.ListSegmentIndexes(ctx, flat.CollectionID)
	require.NoError(t, err)
	assert.Equal(t, []*model.SegmentIndex{original}, rows)
}

func TestJSONFlatReadinessCompatibility(t *testing.T) {
	for _, scenario := range []string{"flat", "scalar", "typed same field", "scalar and flat", "vector FLAT"} {
		t.Run(scenario, func(t *testing.T) {
			im := NewSegmentIndexMeta(nil)
			im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() })
			definition := jsonFlatDefinition()
			var fields []int64
			switch scenario {
			case "scalar":
				definition.FieldID = 102 // Ignored JSON extras do not make this logical.
			case "typed same field":
				im.updateCollectionIndex(definition)
				definition = model.CloneIndex(definition)
				definition.IndexID = 11
				definition.IndexParams[1].Value = "DOUBLE"
				fields = []int64{101}
			case "scalar and flat":
				im.updateCollectionIndex(definition)
				definition = model.CloneIndex(definition)
				definition.IndexID, definition.FieldID = 11, 102
			case "vector FLAT":
				definition.FieldID = 103
				definition.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}}
				fields = []int64{103}
			}
			im.updateCollectionIndex(definition)
			segments := []int64{100}
			if scenario == "flat" {
				assert.Equal(t, segments, im.GetIndexedSegments(1, segments, nil))
				assert.Equal(t, segments, im.GetIndexedSegments(1, segments, []int64{101}))
				assert.Empty(t, im.GetUnindexedSegments(1, segments))
			} else {
				assert.Empty(t, im.GetIndexedSegments(1, segments, fields), "no segment map")
				assert.Equal(t, segments, im.GetUnindexedSegments(1, segments))
				for _, state := range []commonpb.IndexState{commonpb.IndexState_Unissued, commonpb.IndexState_Failed, commonpb.IndexState_Finished} {
					for _, deleted := range []bool{false, true} {
						im.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: definition.IndexID, BuildID: 1000, IndexState: state, IsDeleted: deleted})
						ready := state == commonpb.IndexState_Finished && !deleted
						assert.Equal(t, ready || len(fields) == 0, len(im.GetIndexedSegments(1, segments, fields)) == 1, "legacy empty targets only require a map")
						assert.Equal(t, !ready, len(im.GetUnindexedSegments(1, segments)) == 1)
					}
				}
			}
			assert.Empty(t, im.GetIndexedSegments(1, segments, []int64{999}), "required vector without an index is not ready")
		})
	}
}

func TestJSONFlatClassificationVisibility(t *testing.T) {
	for _, scenario := range []string{"flat", "nested flat", "scalar extras", "typed path", "vector FLAT"} {
		t.Run(scenario, func(t *testing.T) {
			im := NewSegmentIndexMeta(nil)
			definition := jsonFlatDefinition()
			warm := jsonFlatTestSchema()
			logical := scenario == "flat" || scenario == "nested flat"
			candidate := true
			switch scenario {
			case "nested flat":
				warm.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 200, Fields: warm.Fields}}
				warm.Fields = nil
			case "scalar extras":
				definition.FieldID = 102
			case "typed path":
				definition.IndexParams[1].Value = "DOUBLE"
				candidate = false
			case "vector FLAT":
				definition.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}}
				warm.Fields[0].DataType = schemapb.DataType_FloatVector
				candidate = false
			}
			var schema *schemapb.CollectionSchema
			im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return schema })
			im.updateCollectionIndex(definition)
			im.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: commonpb.IndexState_Finished, IndexFileKeys: []string{"historical"}})
			for _, visible := range []bool{false, true} {
				if visible {
					schema = warm
				}
				handler := meta.NewNMockHandler(t)
				handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(&meta.CollectionInfo{ID: 1, Schema: schema}, nil).Once()
				segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1}}
				_, buildable := (&IndexInspector{indexMeta: im, Handler: handler}).CanCreateIndexForSegment(context.Background(), segment, definition)
				assert.Equal(t, visible && !logical, buildable)
				physical := !candidate || (visible && !logical)
				assert.Equal(t, physical, len(im.GetPhysicalIndexesForCollection(1)) == 1)
				assert.Equal(t, physical, len(im.GetSegmentIndexes(1, 100)) == 1)
				assert.Equal(t, physical, len(im.GetSegmentsIndexes(1, []int64{100})[100]) == 1)
				defs, records := im.GetQueryViewIndexSnapshot(1, []int64{100})
				assert.Equal(t, physical, len(defs) == 1)
				assert.Equal(t, physical, len(records[100]) == 1)
				assert.Equal(t, physical, len(im.GetSegmentsIndexStates(1, []int64{100})[100]) == 1)
				assert.Equal(t, visible || !candidate, len(im.GetUnindexedSegments(1, []int64{100})) == 0)
				assert.Equal(t, visible || !candidate, len(im.GetIndexedSegments(1, []int64{100}, []int64{definition.FieldID})) == 1)
				if candidate && !visible {
					assert.Equal(t, commonpb.IndexState_Unissued, im.GetSegmentIndexState(1, 100, 10).GetState())
					assert.Error(t, im.AddSegmentIndex(context.Background(), &model.SegmentIndex{CollectionID: 1, SegmentID: 101, IndexID: 10, BuildID: 1001}))
				}
				assert.Len(t, im.GetAllSegmentIndexSummaries(100), 1, "raw ownership is retained")
			}
		})
	}
}

func TestJSONFlatFilterInIndexedSegments(t *testing.T) {
	paramtable.Init()
	for _, forceAll := range []bool{false, true} {
		SetRollbackTestParam(t, &Params.DataCoordCfg.DVForceAllIndexReady, strconv.FormatBool(forceAll))
		for _, withScalar := range []bool{false, true} {
			im := NewSegmentIndexMeta(nil)
			var schema *schemapb.CollectionSchema
			im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return schema })
			im.updateCollectionIndex(jsonFlatDefinition())
			scalar := &model.Index{CollectionID: 1, FieldID: 102, IndexID: 11}
			if withScalar {
				im.updateCollectionIndex(scalar)
			}
			handler := meta.NewNMockHandler(t)
			handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(&meta.CollectionInfo{ID: 1, Schema: jsonFlatTestSchema()}, nil).Maybe()
			segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, IsSorted: true}}
			assert.Empty(t, FilterInIndexedSegments(context.Background(), handler, im, false, segment))
			schema = jsonFlatTestSchema()
			assert.Equal(t, !withScalar, len(FilterInIndexedSegments(context.Background(), handler, im, false, segment)) == 1)
			if !withScalar {
				// No allocator or scheduler: any new placeholder/build is a failure.
				inspector := &IndexInspector{indexMeta: im, Handler: handler}
				require.NoError(t, inspector.createIndexesForSegment(context.Background(), segment))
				assert.Empty(t, im.GetAllSegmentIndexSummaries(segment.ID))
				continue
			}
			im.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 11, BuildID: 1001, IndexState: commonpb.IndexState_Failed})
			assert.Equal(t, !forceAll, len(FilterInIndexedSegments(context.Background(), handler, im, false, segment)) == 1)
			assert.Equal(t, []int64{100}, im.GetUnindexedSegments(1, []int64{100}))
			im.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 11, BuildID: 1001, IndexState: commonpb.IndexState_Finished})
			assert.Len(t, FilterInIndexedSegments(context.Background(), handler, im, false, segment), 1)
			assert.Empty(t, im.GetUnindexedSegments(1, []int64{100}))
		}
	}
}

func TestJSONFlatColdTaskCannotBuildOrPublish(t *testing.T) {
	im := NewSegmentIndexMeta(nil)
	im.updateCollectionIndex(jsonFlatDefinition())
	var schema *schemapb.CollectionSchema
	im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return schema })
	record := &model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: commonpb.IndexState_Unissued}
	im.updateSegmentIndex(record)
	record, _ = im.GetIndexJob(record.BuildID)
	store := metacache.NewMetaStore(nil)
	mt := &meta.Meta{IndexCatalog: im, MetaStore: store, Segments: meta.NewSegmentsInfo(store)}
	mt.Segments.SetSegment(100, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed}})
	build := NewIndexBuildTask(model.CloneSegmentIndex(record), 1, mt, im, nil, nil, nil)
	// No handler, catalog or worker calls are allowed while the schema is cold,
	// even with zero rows (which would otherwise take the fake-finish path).
	build.CreateTaskOnWorker(20, session.NewMockCluster(t), build.GetTaskResource())
	result := &workerpb.IndexTaskInfo{BuildID: 1000, State: commonpb.IndexState_Finished, IndexFileKeys: []string{"late-file"}}
	require.ErrorIs(t, im.FinishTask(result), merr.ErrServiceUnavailable)
	require.ErrorIs(t, build.SetJobInfo(result), merr.ErrServiceUnavailable)
	unchanged, _ := im.GetIndexJob(1000)
	assert.Equal(t, record, unchanged)

	schema = jsonFlatTestSchema()
	require.NoError(t, build.SetJobInfo(result))
	require.NoError(t, im.AddSegmentIndex(context.Background(), &model.SegmentIndex{CollectionID: 1, SegmentID: 101, IndexID: 10, BuildID: 1001}))
	assert.Empty(t, im.GetAllSegmentIndexSummaries(101))
	unchanged, _ = im.GetIndexJob(1000)
	assert.Equal(t, record, unchanged, "late flat completion cannot publish files")
}

func TestJSONFlatInspectorDeduplicatesAcrossSchemaVisibility(t *testing.T) {
	paramtable.Init()
	for _, scenario := range []struct {
		name              string
		state             commonpb.IndexState
		physical          bool
		flat              bool
		noRecord          bool
		deletedRecord     bool
		deletedDefinition bool
		unrelatedRecord   bool
		supersededRecord  bool
		wantNew           bool
	}{
		{name: "scalar extras finished", state: commonpb.IndexState_Finished},
		{name: "scalar extras unissued", state: commonpb.IndexState_Unissued},
		{name: "scalar extras in progress", state: commonpb.IndexState_InProgress},
		{name: "scalar extras failed", state: commonpb.IndexState_Failed},
		{name: "physical finished", physical: true, state: commonpb.IndexState_Finished},
		{name: "physical unissued", physical: true, state: commonpb.IndexState_Unissued},
		{name: "deleted record permits build", deletedRecord: true, wantNew: true},
		{name: "superseded record does not block replacement", deletedRecord: true, supersededRecord: true, wantNew: true},
		{name: "unrelated record permits build", unrelatedRecord: true, wantNew: true},
		{name: "missing record permits build", noRecord: true, wantNew: true},
		{name: "deleted definition stays retired", deletedDefinition: true},
		{name: "flat preserves historical record", flat: true, state: commonpb.IndexState_Finished},
		{name: "flat creates no record", flat: true, noRecord: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			catalog := catalogmocks.NewDataCoordCatalog(t)
			im := NewSegmentIndexMeta(catalog)
			definition := jsonFlatDefinition()
			if !scenario.flat {
				definition.FieldID = 102
			}
			if scenario.physical {
				definition.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "INVERTED"}}
			}
			definition.IsDeleted = scenario.deletedDefinition
			im.updateCollectionIndex(definition)
			// A second missing requirement makes the production segment-level
			// prefilter enter reconciliation even when index 10 already exists.
			// It resolves to logical-only at admission, so needs no new build.
			missing := jsonFlatDefinition()
			missing.IndexID = 11
			im.updateCollectionIndex(missing)
			var schema *schemapb.CollectionSchema
			im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return schema })
			var original *model.SegmentIndex
			if scenario.supersededRecord {
				im.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 999, IndexState: commonpb.IndexState_Unissued})
			}
			if !scenario.noRecord {
				original = &model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: scenario.state, IsDeleted: scenario.deletedRecord, IndexFileKeys: []string{"retained-file"}}
				if scenario.unrelatedRecord {
					original.IndexID = 99
				}
				im.updateSegmentIndex(original)
				original, _ = im.GetIndexJob(1000)
			}
			alloc := allocator.NewMockAllocator(t)
			scheduler := task.NewMockGlobalScheduler(t)
			handler := meta.NewNMockHandler(t)
			// The projection sees a cold cache. Admission sees the same local
			// collection after it becomes visible, without sleeps or goroutines.
			handler.EXPECT().GetCollection(mock.Anything, int64(1)).RunAndReturn(func(context.Context, int64) (*meta.CollectionInfo, error) {
				schema = jsonFlatTestSchema()
				return &meta.CollectionInfo{ID: 1, Schema: schema}, nil
			}).Maybe()
			alloc.EXPECT().AllocID(mock.Anything).Return(int64(1001), nil).Maybe()
			catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Maybe()
			scheduler.EXPECT().Enqueue(mock.Anything).Return().Maybe()
			inspector := NewIndexInspector(context.Background(), nil, nil, im, scheduler, alloc, handler, nil, NewIndexEngineVersionManager())
			segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, IsSorted: true}}
			require.True(t, im.GetUnIndexedSegmentIDsForIndexTask(1, []int64{100}).Contain(100))
			inspector.processIndexSegmentsForCollection(context.Background(), 1, []*meta.SegmentInfo{segment})
			require.NotNil(t, schema, "admission must have made the cache visible")
			// Also inspect again with a warm cache: neither round may duplicate
			// an existing build, including the one legitimately created above.
			require.NoError(t, inspector.createIndexesForSegment(context.Background(), segment))
			wantCalls := 0
			if scenario.wantNew {
				wantCalls = 1
			}
			alloc.AssertNumberOfCalls(t, "AllocID", wantCalls)
			catalog.AssertNumberOfCalls(t, "CreateSegmentIndex", wantCalls)
			scheduler.AssertNumberOfCalls(t, "Enqueue", wantCalls)
			if original != nil {
				retained, ok := im.GetIndexJob(original.BuildID)
				require.True(t, ok)
				assert.Equal(t, original, retained, "preserve historical task and file ownership")
			}
			expectedRecords := 0
			if original != nil {
				expectedRecords = 1
			}
			if scenario.wantNew && (original == nil || scenario.unrelatedRecord) {
				expectedRecords++
			}
			records := im.getCurrentSegmentIndexTasks(100)
			require.Len(t, records, expectedRecords)
			for _, record := range records {
				if record.IndexID == definition.IndexID {
					if scenario.wantNew {
						assert.EqualValues(t, 1001, record.BuildID)
						assert.Equal(t, commonpb.IndexState_Unissued, record.IndexState)
					} else {
						assert.Equal(t, original, record, "do not overwrite the current state")
					}
				}
			}
		})
	}
}

func TestJSONFlatRecoveryDoesNotReviveSupersededBuild(t *testing.T) {
	im := NewSegmentIndexMeta(nil)
	im.updateCollectionIndex(jsonFlatDefinition())
	im.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 999, IndexState: commonpb.IndexState_Unissued})
	im.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: commonpb.IndexState_Finished, IndexFileKeys: []string{"flat.bin"}})
	store := metacache.NewMetaStore(nil)
	mt := &meta.Meta{IndexCatalog: im, MetaStore: store, Segments: meta.NewSegmentsInfo(store)}
	mt.Segments.SetSegment(100, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed}})
	scheduler := task.NewMockGlobalScheduler(t)
	(&IndexInspector{meta: mt, indexMeta: im, scheduler: scheduler}).reloadFromMeta()
	scheduler.AssertNotCalled(t, "Enqueue", mock.Anything)
	require.Len(t, im.GetAllSegmentIndexSummaries(100), 2, "retain superseded ownership for garbage collection")
}

func TestJSONFlatManifestOwnershipDoesNotAdmitNewBuilds(t *testing.T) {
	for _, schemaVisible := range []bool{false, true} {
		t.Run(strconv.FormatBool(schemaVisible), func(t *testing.T) {
			im := NewSegmentIndexMeta(nil)
			im.updateCollectionIndex(jsonFlatDefinition())
			if schemaVisible {
				im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() })
			}
			record := &model.SegmentIndex{
				CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000,
				IndexState: commonpb.IndexState_Finished, IndexFileKeys: []string{"flat.bin"},
			}
			require.NoError(t, im.AddSegmentIndexFromManifest(context.Background(), record))
			owned, ok := im.GetIndexJob(1000)
			require.True(t, ok)
			require.True(t, owned.ManifestPublished)
			require.Equal(t, record.IndexFileKeys, owned.IndexFileKeys)
			require.Empty(t, im.GetPhysicalIndexesForCollection(1))
			pending := &model.SegmentIndex{CollectionID: 1, SegmentID: 101, IndexID: 10, BuildID: 1001}
			err := im.AddSegmentIndex(context.Background(), pending)
			if schemaVisible {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			}
			_, exists := im.GetIndexJob(1001)
			require.False(t, exists, "ownership recovery must not enable new flat builds")
			require.Error(t, im.AddSegmentIndexFromManifest(context.Background(), pending))
			pending.IndexState = commonpb.IndexState_Finished
			require.Error(t, im.AddSegmentIndexFromManifest(context.Background(), pending), "no fake-finished manifest records")
		})
	}
}

func TestJSONFlatCoordinatorRetirementAfterSchedulerRebase(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().DropSegmentIndex(mock.Anything, int64(1), int64(0), int64(100), int64(1000)).Return(merr.ErrServiceUnavailable).Once()
	catalog.EXPECT().DropSegmentIndex(mock.Anything, int64(1), int64(0), int64(100), int64(1000)).Return(nil).Once()
	im := NewSegmentIndexMeta(catalog)
	im.updateCollectionIndex(jsonFlatDefinition())
	var schema *schemapb.CollectionSchema
	im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return schema })
	record := &model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: commonpb.IndexState_Unissued}
	im.updateSegmentIndex(record)
	mt := meta.NewEmptyTestMeta()
	mt.IndexCatalog = im
	mt.Segments.SetSegment(100, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed}))
	build := NewIndexBuildTask(model.CloneSegmentIndex(record), 1, mt, im, nil, nil, nil)
	require.False(t, build.CanCompleteOnCoordinator(), "cold schema must not fake-finish even a zero-row task")
	require.Equal(t, task.LocalRetry, build.CompleteOnCoordinator(context.Background()))
	schema = jsonFlatTestSchema()
	build.NodeID = 20
	require.False(t, build.CanCompleteOnCoordinator())
	require.Equal(t, task.LocalNeedsWorker, build.CompleteOnCoordinator(context.Background()), "assigned work must be canceled first")
	build.NodeID = 0
	require.True(t, build.CanCompleteOnCoordinator())
	require.Equal(t, task.LocalRetry, build.CompleteOnCoordinator(context.Background()))
	_, exists := im.GetIndexJob(1000)
	require.True(t, exists, "catalog failure preserves the pending record")
	require.Equal(t, task.LocalCompleted, build.CompleteOnCoordinator(context.Background()))
	_, exists = im.GetIndexJob(1000)
	require.False(t, exists, "unassigned retirement needs no worker resources")
}
