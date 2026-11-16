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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metacache"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
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

func TestJSONFlatIndexRecoveryCancelsAndTerminalizesBuild(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
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
	persisted, ok := m.GetIndexJob(1000)
	require.True(t, ok)
	assert.Equal(t, commonpb.IndexState_Failed, persisted.IndexState)
	assert.Contains(t, persisted.FailReason, "deprecated")
	// A late completion cannot publish files even after recovery retired the worker.
	require.NoError(t, recovered.SetJobInfo(&workerpb.IndexTaskInfo{BuildID: 1000, State: commonpb.IndexState_Finished, IndexFileKeys: []string{"late-file"}}))
	persisted, _ = m.GetIndexJob(1000)
	assert.Empty(t, persisted.IndexFileKeys)
	assert.Equal(t, commonpb.IndexState_Failed, persisted.IndexState)
}

func TestJSONFlatIndexRecoveryRetiresUndispatchedBuilds(t *testing.T) {
	for _, state := range []commonpb.IndexState{commonpb.IndexState_Unissued, commonpb.IndexState_Retry} {
		t.Run(state.String(), func(t *testing.T) {
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
			m := NewSegmentIndexMeta(catalog)
			m.getCollectionSchema = func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() }
			m.updateCollectionIndex(jsonFlatDefinition())
			m.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: state})
			store := metacache.NewMetaStore(nil)
			mt := &meta.Meta{IndexCatalog: m, MetaStore: store, Segments: meta.NewSegmentsInfo(store)}
			mt.Segments.SetSegment(100, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, IsSorted: true}})
			scheduler := task.NewMockGlobalScheduler(t)
			var recovered *indexBuildTask
			scheduler.EXPECT().Enqueue(mock.Anything).Run(func(value task.Task) { recovered = value.(*indexBuildTask) })
			(&IndexInspector{meta: mt, indexMeta: m, scheduler: scheduler}).reloadFromMeta()
			require.NotNil(t, recovered)
			recovered.CreateTaskOnWorker(20, session.NewMockCluster(t))
			persisted, ok := m.GetIndexJob(1000)
			require.True(t, ok)
			assert.Equal(t, commonpb.IndexState_Failed, persisted.IndexState)
			assert.Contains(t, persisted.FailReason, "deprecated")
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
	assert.Empty(t, m.GetAllSegmentIndexes(100))
}

func TestJSONFlatIndexUsesBoundCollectionSchemaCache(t *testing.T) {
	mt, domains, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	im := domains.IndexMeta
	im.updateCollectionIndex(jsonFlatDefinition())
	// Params alone cannot prove a JSON field while the schema cache is cold.
	require.Len(t, im.GetPhysicalIndexesForCollection(1), 1)
	mt.AddCollection(&meta.CollectionInfo{ID: 1, Schema: jsonFlatTestSchema()})
	assert.Empty(t, im.GetPhysicalIndexesForCollection(1))
	assert.Len(t, im.GetIndexesForCollection(1, "json_flat"), 1)
}

func TestJSONFlatIndexExcludedFromManifestMigration(t *testing.T) {
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
				assert.Equal(t, 2, pending)
				assert.ElementsMatch(t, []int64{1011, 1012}, work[0].buildIDs)
				mutation.Type = meta.SegmentIndexRollback
				wantErr = meta.ErrSegmentIndexRollbackSkipped
			} else {
				work, pending := NewManifestIndexBackfillInspector(ctx, mt, im).scan(ctx)
				require.Len(t, work, 1)
				assert.Equal(t, 2, pending)
				require.Len(t, work[0].records, 2)
				assert.EqualValues(t, 11, work[0].records[0].IndexID)
				assert.EqualValues(t, 12, work[0].records[1].IndexID)
			}
			// A stale selection must be rejected under the publication lock too.
			im.LockBuildIDs([]int64{1010})
			_, err := im.StageSegmentIndexMutation(mutation)
			im.UnlockBuildIDs([]int64{1010})
			assert.ErrorIs(t, err, wantErr)
			assert.Len(t, im.GetAllSegmentIndexes(100), 3)
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
				assert.Empty(t, work)
				assert.Equal(t, 2, pending)
				assert.True(t, rollback.runOnce(ctx), "unresolved records must stay on the retry loop")
			} else {
				work, pending := backfill.scan(ctx)
				assert.Empty(t, work)
				assert.Equal(t, 2, pending)
				assert.True(t, backfill.runOnce(ctx), "unresolved records must stay on the retry loop")
			}
			assert.Len(t, im.GetAllSegmentIndexes(100), 2)
			schema = jsonFlatTestSchema()
			if published {
				work, pending := rollback.scan(ctx)
				require.Len(t, work, 1)
				assert.Equal(t, []int64{1011}, work[0].buildIDs)
				assert.Equal(t, 1, pending)
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
