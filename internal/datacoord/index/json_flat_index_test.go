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
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func jsonFlatTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 101, Name: "json", DataType: schemapb.DataType_JSON}, {FieldID: 102, Name: "scalar", DataType: schemapb.DataType_Int64}}}
}

func putJSONFlatTestIndex(m *IndexMeta, index *model.Index) {
	if m.Indexes[index.CollectionID] == nil {
		m.Indexes[index.CollectionID] = make(map[int64]*model.Index)
	}
	m.Indexes[index.CollectionID][index.IndexID] = index
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
	putJSONFlatTestIndex(m, jsonFlatDefinition())
	segIdx := &model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, NodeID: 20, IndexState: commonpb.IndexState_InProgress}
	m.updateSegmentIndex(segIdx)
	mt := &meta.Meta{IndexCatalog: m, Collections: typeutil.NewConcurrentMap[UniqueID, *meta.CollectionInfo](), Segments: meta.NewSegmentsInfo()}
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
	require.NoError(t, recovered.setJobInfo(&workerpb.IndexTaskInfo{BuildID: 1000, State: commonpb.IndexState_Finished, IndexFileKeys: []string{"late-file"}}))
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
			putJSONFlatTestIndex(m, jsonFlatDefinition())
			m.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: state})
			mt := &meta.Meta{IndexCatalog: m, Collections: typeutil.NewConcurrentMap[UniqueID, *meta.CollectionInfo](), Segments: meta.NewSegmentsInfo()}
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
	putJSONFlatTestIndex(m, jsonFlatDefinition())
	mt := &meta.Meta{IndexCatalog: m, Collections: typeutil.NewConcurrentMap[UniqueID, *meta.CollectionInfo](), Segments: meta.NewSegmentsInfo()}
	handler := meta.NewNMockHandler(t)
	handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(&meta.CollectionInfo{ID: 1, Schema: jsonFlatTestSchema()}, nil)
	segment := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, IsSorted: true}}
	require.NoError(t, (&IndexInspector{meta: mt, indexMeta: m, Handler: handler}).createIndexesForSegment(context.Background(), segment))
	assert.Empty(t, m.GetAllSegmentIndexes(100))
}

func TestJSONFlatIndexColdTaskResolution(t *testing.T) {
	for _, inProgress := range []bool{false, true} {
		t.Run(map[bool]string{false: "dispatch", true: "poll"}[inProgress], func(t *testing.T) {
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
			m := NewSegmentIndexMeta(catalog)
			putJSONFlatTestIndex(m, jsonFlatDefinition())
			job := &model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, NodeID: 20, IndexState: commonpb.IndexState_Unissued}
			if inProgress {
				job.IndexState = commonpb.IndexState_InProgress
			}
			m.updateSegmentIndex(job)
			handler := meta.NewNMockHandler(t)
			handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(&meta.CollectionInfo{ID: 1, Schema: jsonFlatTestSchema()}, nil)
			worker := session.NewMockCluster(t)
			build := newIndexBuildTask(model.CloneSegmentIndex(job), 1, &meta.Meta{}, m, handler, nil, nil)
			if inProgress {
				worker.EXPECT().DropIndex(int64(20), int64(1000)).Return(nil)
				build.QueryTaskOnWorker(worker)
			} else {
				build.CreateTaskOnWorker(20, worker)
			}
			persisted, ok := m.GetIndexJob(1000)
			require.True(t, ok)
			assert.Equal(t, commonpb.IndexState_Failed, persisted.IndexState)
		})
	}
}

func TestJSONFlatIndexColdTaskDefersWhenSchemaUnavailable(t *testing.T) {
	m := NewSegmentIndexMeta(nil)
	putJSONFlatTestIndex(m, jsonFlatDefinition())
	job := &model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, NodeID: 20, IndexState: commonpb.IndexState_InProgress}
	m.updateSegmentIndex(job)
	handler := meta.NewNMockHandler(t)
	handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(nil, merr.ErrServiceNotReady).Times(3)
	worker := session.NewMockCluster(t)
	build := newIndexBuildTask(model.CloneSegmentIndex(job), 1, &meta.Meta{}, m, handler, nil, nil)
	build.CreateTaskOnWorker(20, worker)
	build.QueryTaskOnWorker(worker)
	require.ErrorIs(t, build.setJobInfo(&workerpb.IndexTaskInfo{BuildID: 1000, State: commonpb.IndexState_Finished, IndexFileKeys: []string{"late-file"}}), merr.ErrServiceNotReady)
	persisted, ok := m.GetIndexJob(1000)
	require.True(t, ok)
	assert.Equal(t, commonpb.IndexState_InProgress, persisted.IndexState)
	assert.Empty(t, persisted.IndexFileKeys)
}

func TestJSONFlatIndexUsesBoundCollectionSchemaCache(t *testing.T) {
	mt, domains, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	im := domains.IndexMeta
	putJSONFlatTestIndex(im, jsonFlatDefinition())
	assert.False(t, im.JSONFlatSchemaReady(1))
	mt.AddCollection(&meta.CollectionInfo{ID: 1, Schema: jsonFlatTestSchema()})
	assert.True(t, im.JSONFlatSchemaReady(1))
	assert.Empty(t, im.GetPhysicalIndexesForCollection(1))
	assert.Len(t, im.GetIndexesForCollection(1, "json_flat"), 1)
}

func TestJSONFlatIndexColdMixedCollectionRecoveredTaskCompletion(t *testing.T) {
	paramtable.Init()
	for _, indexType := range []string{"HNSW", "INVERTED"} {
		for _, state := range []commonpb.IndexState{commonpb.IndexState_Finished, commonpb.IndexState_Failed} {
			t.Run(indexType+"/"+state.String(), func(t *testing.T) {
				ctx := context.Background()
				catalog := catalogmocks.NewDataCoordCatalog(t)
				catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.MatchedBy(func(records []*model.SegmentIndex) bool {
					return len(records) == 1 && records[0].BuildID == 1001 && records[0].IndexState == state
				})).Return(nil).Once()
				catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.MatchedBy(func(records []*model.SegmentIndex) bool {
					return len(records) == 1 && records[0].BuildID == 1000 && records[0].IndexState == commonpb.IndexState_Failed
				})).Return(nil).Once()
				im := NewSegmentIndexMeta(catalog)
				putJSONFlatTestIndex(im, jsonFlatDefinition())
				putJSONFlatTestIndex(im, &model.Index{CollectionID: 1, FieldID: 102, IndexID: 11, IndexName: "ordinary", IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexType}}})
				// No startup reload is running; only an explicit later cache recovery can resolve the flat task.
				var cached *schemapb.CollectionSchema
				im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return cached })
				for indexID, buildID := range map[int64]int64{10: 1000, 11: 1001} {
					im.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: indexID, BuildID: buildID, NodeID: 20, IndexState: commonpb.IndexState_InProgress})
				}
				mt := &meta.Meta{Ctx: ctx, IndexCatalog: im, Collections: typeutil.NewConcurrentMap[UniqueID, *meta.CollectionInfo](), Segments: meta.NewSegmentsInfo()}
				mt.Segments.SetSegment(100, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, IsSorted: true}})
				handler := meta.NewNMockHandler(t)
				handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(nil, merr.ErrServiceNotReady).Once()
				scheduler := task.NewMockGlobalScheduler(t)
				recovered := make(map[int64]*indexBuildTask)
				scheduler.EXPECT().Enqueue(mock.Anything).Run(func(value task.Task) {
					build := value.(*indexBuildTask)
					recovered[build.BuildID] = build
				}).Twice()
				(&IndexInspector{ctx: ctx, meta: mt, indexMeta: im, scheduler: scheduler, Handler: handler}).reloadFromMeta()
				require.Len(t, recovered, 2)
				result := &workerpb.IndexTaskInfo{BuildID: 1001, State: state}
				if state == commonpb.IndexState_Finished {
					result.IndexFileKeys = []string{"ordinary-file"}
				} else {
					result.FailReason = "worker failure"
				}
				worker := session.NewMockCluster(t)
				worker.EXPECT().QueryIndex(int64(20), mock.Anything).Return(&workerpb.IndexJobResults{Results: []*workerpb.IndexTaskInfo{result}}, nil).Once()
				recovered[1001].QueryTaskOnWorker(worker)
				ordinary, ok := im.GetIndexJob(1001)
				require.True(t, ok)
				assert.Equal(t, state, ordinary.IndexState)
				assert.Equal(t, state, recovered[1001].IndexState)
				assert.ElementsMatch(t, result.GetIndexFileKeys(), ordinary.IndexFileKeys)
				assert.Equal(t, result.GetFailReason(), ordinary.FailReason)
				assert.Nil(t, cached, "ordinary completion must not depend on schema recovery")

				recovered[1000].QueryTaskOnWorker(worker)
				flat, ok := im.GetIndexJob(1000)
				require.True(t, ok)
				assert.Equal(t, commonpb.IndexState_InProgress, flat.IndexState)
				assert.Empty(t, flat.IndexFileKeys)
				require.ErrorIs(t, im.FinishTask(&workerpb.IndexTaskInfo{BuildID: 1000, State: commonpb.IndexState_Finished, IndexFileKeys: []string{"late-flat-file"}}), merr.ErrServiceNotReady)

				cached = jsonFlatTestSchema()
				if indexType == "HNSW" {
					cached.Fields[1].DataType = schemapb.DataType_FloatVector
				}
				worker.EXPECT().DropIndex(int64(20), int64(1000)).Return(nil).Once()
				recovered[1000].QueryTaskOnWorker(worker)
				flat, _ = im.GetIndexJob(1000)
				assert.Equal(t, commonpb.IndexState_Failed, flat.IndexState)
				assert.Contains(t, flat.FailReason, "deprecated")
				assert.Empty(t, flat.IndexFileKeys)
			})
		}
	}
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
				putJSONFlatTestIndex(im, definition)
				definition = model.CloneIndex(definition)
				definition.IndexID = 11
				definition.IndexParams[1].Value = "DOUBLE"
				fields = []int64{101}
			case "scalar and flat":
				putJSONFlatTestIndex(im, definition)
				definition = model.CloneIndex(definition)
				definition.IndexID, definition.FieldID = 11, 102
			case "vector FLAT":
				definition.FieldID = 103
				definition.IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}}
				fields = []int64{103}
			}
			putJSONFlatTestIndex(im, definition)
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
			putJSONFlatTestIndex(im, definition)
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
				assert.Equal(t, physical, len(im.GetSegmentsIndexStates(1, []int64{100})[100]) == 1)
				assert.Equal(t, visible || !candidate, len(im.GetUnindexedSegments(1, []int64{100})) == 0)
				assert.Equal(t, visible || !candidate, len(im.GetIndexedSegments(1, []int64{100}, []int64{definition.FieldID})) == 1)
				if candidate && !visible {
					assert.Equal(t, commonpb.IndexState_Unissued, im.GetSegmentIndexState(1, 100, 10).GetState())
					assert.Error(t, im.AddSegmentIndex(context.Background(), &model.SegmentIndex{CollectionID: 1, SegmentID: 101, IndexID: 10, BuildID: 1001}))
				}
				assert.Len(t, im.GetAllSegmentIndexes(100), 1, "raw ownership is retained")
			}
		})
	}
}

func TestJSONFlatFilterInIndexedSegments(t *testing.T) {
	paramtable.Init()
	for _, forceAll := range []bool{false, true} {
		require.NoError(t, Params.Save(Params.DataCoordCfg.DVForceAllIndexReady.Key, strconv.FormatBool(forceAll)))
		t.Cleanup(func() { Params.Reset(Params.DataCoordCfg.DVForceAllIndexReady.Key) })
		for _, withScalar := range []bool{false, true} {
			im := NewSegmentIndexMeta(nil)
			var schema *schemapb.CollectionSchema
			im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return schema })
			putJSONFlatTestIndex(im, jsonFlatDefinition())
			scalar := &model.Index{CollectionID: 1, FieldID: 102, IndexID: 11}
			if withScalar {
				putJSONFlatTestIndex(im, scalar)
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
				assert.Empty(t, im.GetAllSegmentIndexes(segment.ID))
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
		wantNew           bool
	}{
		{name: "scalar extras finished", state: commonpb.IndexState_Finished},
		{name: "scalar extras unissued", state: commonpb.IndexState_Unissued},
		{name: "scalar extras in progress", state: commonpb.IndexState_InProgress},
		{name: "scalar extras failed", state: commonpb.IndexState_Failed},
		{name: "physical finished", physical: true, state: commonpb.IndexState_Finished},
		{name: "physical unissued", physical: true, state: commonpb.IndexState_Unissued},
		{name: "deleted record permits build", deletedRecord: true, wantNew: true},
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
			putJSONFlatTestIndex(im, definition)
			// A second missing requirement keeps periodic reconciliation eligible
			// even when index 10 already exists.
			// It resolves to logical-only at admission, so needs no new build.
			missing := jsonFlatDefinition()
			missing.IndexID = 11
			putJSONFlatTestIndex(im, missing)
			var schema *schemapb.CollectionSchema
			im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return schema })
			var original *model.SegmentIndex
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
			require.True(t, im.IsUnIndexedSegment(1, 100))
			require.NoError(t, inspector.createIndexesForSegment(context.Background(), segment))
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
			records := im.GetAllSegmentIndexes(100)
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

func TestJSONFlatTaskMissingFieldDefers(t *testing.T) {
	im := NewSegmentIndexMeta(nil)
	putJSONFlatTestIndex(im, jsonFlatDefinition())
	schema := &schemapb.CollectionSchema{}
	im.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return schema })
	im.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: commonpb.IndexState_Unissued})
	original, _ := im.GetIndexJob(1000)
	build := newIndexBuildTask(model.CloneSegmentIndex(original), 1, &meta.Meta{}, im, nil, nil, nil)
	result := &workerpb.IndexTaskInfo{BuildID: 1000, State: commonpb.IndexState_Finished, IndexFileKeys: []string{"late-file"}}
	assert.False(t, im.JSONFlatSchemaReady(1))
	require.ErrorIs(t, im.FinishTask(result), merr.ErrServiceNotReady)
	require.ErrorIs(t, build.setJobInfo(result), merr.ErrServiceNotReady)
	require.ErrorIs(t, im.AddSegmentIndex(context.Background(), &model.SegmentIndex{CollectionID: 1, SegmentID: 101, IndexID: 10, BuildID: 1001}), merr.ErrServiceNotReady)
	assert.Equal(t, []int64{100}, im.GetUnindexedSegments(1, []int64{100}))
	assert.Empty(t, im.GetSegmentIndexes(1, 100))
	schema = jsonFlatTestSchema()
	require.NoError(t, build.setJobInfo(result))
	require.NoError(t, im.AddSegmentIndex(context.Background(), &model.SegmentIndex{CollectionID: 1, SegmentID: 101, IndexID: 10, BuildID: 1001}))
	assert.Empty(t, im.GetAllSegmentIndexes(101))
	retained, _ := im.GetIndexJob(1000)
	assert.Equal(t, original, retained)
}
