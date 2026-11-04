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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func jsonFlatTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 101, Name: "json", DataType: schemapb.DataType_JSON}, {FieldID: 102, Name: "scalar", DataType: schemapb.DataType_Int64}}}
}

func putJSONFlatTestIndex(m *indexMeta, index *model.Index) {
	if m.indexes[index.CollectionID] == nil {
		m.indexes[index.CollectionID] = make(map[int64]*model.Index)
	}
	m.indexes[index.CollectionID][index.IndexID] = index
}

func jsonFlatDefinition() *model.Index {
	return &model.Index{CollectionID: 1, FieldID: 101, IndexID: 10, IndexName: "json_flat", IndexParams: []*commonpb.KeyValuePair{
		{Key: common.IndexTypeKey, Value: "INVERTED"},
		{Key: common.JSONCastTypeKey, Value: "JSON"},
		{Key: common.JSONPathKey, Value: ""},
	}}
}

func TestJSONFlatIndexLogicalAndPhysicalViews(t *testing.T) {
	m := newSegmentIndexMeta(nil)
	m.getCollectionSchema = func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() }
	flat := jsonFlatDefinition()
	putJSONFlatTestIndex(m, flat)
	m.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: commonpb.IndexState_Failed, FailReason: "old failure", IndexFileKeys: []string{"old-file"}})
	require.Len(t, m.GetIndexesForCollection(1, "json_flat"), 1)
	assert.True(t, m.IsIndexExist(1, 10))
	assert.Empty(t, m.GetIndexesForCollection(1, "missing"))
	assert.False(t, m.HasIndex(1))
	assert.Empty(t, m.GetSegmentIndexes(1, 100))
	assert.Len(t, m.GetAllSegmentIndexes(100), 1)
	assert.False(t, m.IsUnIndexedSegment(1, 100))
	assert.False(t, m.IsUnIndexedSegment(1, 101))
	assert.Empty(t, m.GetUnindexedSegments(1, []int64{100, 101}))
	assert.Equal(t, commonpb.IndexState_Finished, m.GetSegmentIndexState(1, 100, 10).GetState())

	serverMeta := &meta{indexMeta: m, collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](), segments: NewSegmentsInfo()}
	serverMeta.segments.SetSegment(100, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, NumOfRows: 100}})
	s := &Server{meta: serverMeta}
	s.stateCode.Store(commonpb.StateCode_Healthy)
	files, err := s.GetIndexInfos(context.Background(), &indexpb.GetIndexInfoRequest{CollectionID: 1, SegmentIDs: []int64{100}})
	require.NoError(t, merr.CheckRPCCall(files, err))
	assert.Empty(t, files.GetSegmentInfo()[100].GetIndexInfos())
	assert.False(t, files.GetSegmentInfo()[100].GetEnableIndex())
	listed, err := s.ListIndexes(context.Background(), &indexpb.ListIndexesRequest{CollectionID: 1})
	require.NoError(t, merr.CheckRPCCall(listed, err))
	assert.Empty(t, listed.GetIndexInfos())

	described, err := s.DescribeIndex(context.Background(), &indexpb.DescribeIndexRequest{CollectionID: 1, IndexName: "json_flat"})
	require.NoError(t, merr.CheckRPCCall(described, err))
	require.Len(t, described.GetIndexInfos(), 1)
	assert.Equal(t, commonpb.IndexState_Finished, described.GetIndexInfos()[0].GetState())
	assert.EqualValues(t, 100, described.GetIndexInfos()[0].GetIndexedRows())
	progress, err := s.GetIndexBuildProgress(context.Background(), &indexpb.GetIndexBuildProgressRequest{CollectionID: 1, IndexName: "json_flat"})
	require.NoError(t, merr.CheckRPCCall(progress, err))
	assert.EqualValues(t, 100, progress.GetTotalRows())
	assert.Equal(t, progress.GetTotalRows(), progress.GetIndexedRows())
	assert.Zero(t, progress.GetPendingIndexRows())
	state, err := s.GetIndexState(context.Background(), &indexpb.GetIndexStateRequest{CollectionID: 1, IndexName: "json_flat"})
	require.NoError(t, merr.CheckRPCCall(state, err))
	assert.Equal(t, commonpb.IndexState_Finished, state.GetState())
	statistics, err := s.GetIndexStatistics(context.Background(), &indexpb.GetIndexStatisticsRequest{CollectionID: 1, IndexName: "json_flat"})
	require.NoError(t, merr.CheckRPCCall(statistics, err))
	require.Len(t, statistics.GetIndexInfos(), 1)
	assert.EqualValues(t, 100, statistics.GetIndexInfos()[0].GetIndexedRows())
	assert.Zero(t, statistics.GetIndexInfos()[0].GetPendingIndexRows())
	for _, missing := range []string{"missing", "json_flat"} {
		if missing == "json_flat" {
			flat.IsDeleted = true
		}
		described, err := s.DescribeIndex(context.Background(), &indexpb.DescribeIndexRequest{CollectionID: 1, IndexName: missing})
		require.Error(t, merr.CheckRPCCall(described, err))
	}
	flat.IsDeleted = false
	// Even a direct late producer cannot add a new physical record.
	require.NoError(t, m.AddSegmentIndex(context.Background(), &model.SegmentIndex{CollectionID: 1, SegmentID: 101, IndexID: 10, BuildID: 1002}))
	assert.Empty(t, m.GetAllSegmentIndexes(101))

	typed := model.CloneIndex(flat)
	typed.IndexID, typed.IndexName = 11, "typed"
	typed.IndexParams[1].Value = "DOUBLE"
	putJSONFlatTestIndex(m, typed)
	assert.True(t, m.HasIndex(1))
	assert.True(t, m.IsUnIndexedSegment(1, 100))
	m.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 11, BuildID: 1001, IndexState: commonpb.IndexState_Finished})
	assert.Equal(t, []int64{100}, m.GetIndexedSegments(1, []int64{100}, []int64{101}))
	assert.Len(t, m.GetSegmentIndexes(1, 100), 1)
	flat.IsDeleted = true
	assert.Empty(t, m.GetIndexesForCollection(1, "json_flat"))
	assert.False(t, m.IsIndexExist(1, 10))
}

func TestJSONFlatIndexPublicProjection(t *testing.T) {
	flat := jsonFlatDefinition()
	for _, realtime := range []bool{false, true} {
		info := &indexpb.IndexInfo{IndexID: flat.IndexID}
		(&Server{meta: &meta{indexMeta: func() *indexMeta {
			m := newSegmentIndexMeta(nil)
			m.getCollectionSchema = func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() }
			return m
		}()}}).completeIndexInfo(info, flat, map[int64]*indexStats{
			100: {numRows: 100, state: commonpb.SegmentState_Flushed, indexStates: map[int64]*indexpb.SegmentIndexState{10: {State: commonpb.IndexState_Failed, FailReason: "old failure"}}},
			101: {numRows: 200, state: commonpb.SegmentState_Flushing},
			102: {numRows: 400, state: commonpb.SegmentState_Dropped},
		}, realtime, 0)
		assert.Equal(t, commonpb.IndexState_Finished, info.GetState())
		assert.EqualValues(t, 300, info.GetTotalRows())
		assert.Equal(t, info.GetTotalRows(), info.GetIndexedRows())
		assert.Zero(t, info.GetPendingIndexRows())
		assert.Empty(t, info.GetIndexStateFailReason())
	}
}

func TestJSONFlatIndexRecoveryCancelsAndTerminalizesBuild(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
	m := newSegmentIndexMeta(catalog)
	m.getCollectionSchema = func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() }
	putJSONFlatTestIndex(m, jsonFlatDefinition())
	segIdx := &model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, NodeID: 20, IndexState: commonpb.IndexState_InProgress}
	m.updateSegmentIndex(segIdx)
	meta := &meta{indexMeta: m, collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](), segments: NewSegmentsInfo()}
	meta.segments.SetSegment(100, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, IsSorted: true}})
	scheduler := task.NewMockGlobalScheduler(t)
	var recovered *indexBuildTask
	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(value task.Task) { recovered = value.(*indexBuildTask) })
	inspector := &indexInspector{meta: meta, scheduler: scheduler}
	inspector.reloadFromMeta()
	require.NotNil(t, recovered)
	require.NoError(t, inspector.createIndexesForSegment(context.Background(), meta.GetSegment(context.Background(), 100)))
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

func TestJSONFlatIndexPreservesScalarWithIgnoredJSONExtras(t *testing.T) {
	m := newSegmentIndexMeta(nil)
	m.getCollectionSchema = func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() }
	scalar := jsonFlatDefinition()
	scalar.FieldID = 102
	scalar.IndexName = "scalar"
	putJSONFlatTestIndex(m, scalar)
	m.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: commonpb.IndexState_Failed, FailReason: "real scalar failure"})
	assert.True(t, m.HasIndex(1))
	assert.Len(t, m.GetPhysicalIndexesForCollection(1), 1)
	assert.Len(t, m.GetSegmentIndexes(1, 100), 1)
	info := &indexpb.IndexInfo{IndexID: scalar.IndexID}
	(&Server{meta: &meta{indexMeta: m}}).completeIndexInfo(info, scalar, map[int64]*indexStats{100: {numRows: 100, state: commonpb.SegmentState_Flushed, indexStates: map[int64]*indexpb.SegmentIndexState{10: {State: commonpb.IndexState_Failed, FailReason: "real scalar failure"}}}}, true, 0)
	assert.Equal(t, commonpb.IndexState_Failed, info.GetState())
	assert.Zero(t, info.GetIndexedRows())
	assert.EqualValues(t, 100, info.GetPendingIndexRows())
}

func TestJSONFlatIndexRecoveryRetiresUndispatchedBuilds(t *testing.T) {
	for _, state := range []commonpb.IndexState{commonpb.IndexState_Unissued, commonpb.IndexState_Retry} {
		t.Run(state.String(), func(t *testing.T) {
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
			m := newSegmentIndexMeta(catalog)
			m.getCollectionSchema = func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() }
			putJSONFlatTestIndex(m, jsonFlatDefinition())
			m.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: state})
			meta := &meta{indexMeta: m, collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](), segments: NewSegmentsInfo()}
			meta.segments.SetSegment(100, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, IsSorted: true}})
			scheduler := task.NewMockGlobalScheduler(t)
			var recovered *indexBuildTask
			scheduler.EXPECT().Enqueue(mock.Anything).Run(func(value task.Task) { recovered = value.(*indexBuildTask) })
			(&indexInspector{meta: meta, scheduler: scheduler}).reloadFromMeta()
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
	m := newSegmentIndexMeta(nil)
	putJSONFlatTestIndex(m, jsonFlatDefinition())
	meta := &meta{indexMeta: m, collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](), segments: NewSegmentsInfo()}
	handler := NewNMockHandler(t)
	handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(&collectionInfo{ID: 1, Schema: jsonFlatTestSchema()}, nil)
	segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, IsSorted: true}}
	require.NoError(t, (&indexInspector{meta: meta, handler: handler}).createIndexesForSegment(context.Background(), segment))
	assert.Empty(t, m.GetAllSegmentIndexes(100))
}

func TestJSONFlatIndexColdTaskResolution(t *testing.T) {
	for _, inProgress := range []bool{false, true} {
		t.Run(map[bool]string{false: "dispatch", true: "poll"}[inProgress], func(t *testing.T) {
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
			m := newSegmentIndexMeta(catalog)
			putJSONFlatTestIndex(m, jsonFlatDefinition())
			job := &model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, NodeID: 20, IndexState: commonpb.IndexState_Unissued}
			if inProgress {
				job.IndexState = commonpb.IndexState_InProgress
			}
			m.updateSegmentIndex(job)
			handler := NewNMockHandler(t)
			handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(&collectionInfo{ID: 1, Schema: jsonFlatTestSchema()}, nil)
			worker := session.NewMockCluster(t)
			build := newIndexBuildTask(model.CloneSegmentIndex(job), 1, &meta{indexMeta: m}, handler, nil, nil)
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
	m := newSegmentIndexMeta(nil)
	putJSONFlatTestIndex(m, jsonFlatDefinition())
	job := &model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, NodeID: 20, IndexState: commonpb.IndexState_InProgress}
	m.updateSegmentIndex(job)
	handler := NewNMockHandler(t)
	handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(nil, merr.ErrServiceNotReady).Times(3)
	worker := session.NewMockCluster(t)
	build := newIndexBuildTask(model.CloneSegmentIndex(job), 1, &meta{indexMeta: m}, handler, nil, nil)
	build.CreateTaskOnWorker(20, worker)
	build.QueryTaskOnWorker(worker)
	require.ErrorIs(t, build.setJobInfo(&workerpb.IndexTaskInfo{BuildID: 1000, State: commonpb.IndexState_Finished, IndexFileKeys: []string{"late-file"}}), merr.ErrServiceNotReady)
	persisted, ok := m.GetIndexJob(1000)
	require.True(t, ok)
	assert.Equal(t, commonpb.IndexState_InProgress, persisted.IndexState)
	assert.Empty(t, persisted.IndexFileKeys)
}

func TestJSONFlatIndexColdPublicProjection(t *testing.T) {
	m := newSegmentIndexMeta(nil)
	putJSONFlatTestIndex(m, jsonFlatDefinition())
	var cached *schemapb.CollectionSchema
	m.getCollectionSchema = func(int64) *schemapb.CollectionSchema { return cached }
	handler := NewNMockHandler(t)
	handler.EXPECT().GetCollection(mock.Anything, int64(1)).Run(func(context.Context, int64) { cached = jsonFlatTestSchema() }).Return(&collectionInfo{ID: 1, Schema: jsonFlatTestSchema()}, nil).Once()
	s := &Server{meta: &meta{indexMeta: m, collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](), segments: NewSegmentsInfo()}, handler: handler}
	s.stateCode.Store(commonpb.StateCode_Healthy)
	described, err := s.DescribeIndex(context.Background(), &indexpb.DescribeIndexRequest{CollectionID: 1, IndexName: "json_flat"})
	require.NoError(t, merr.CheckRPCCall(described, err))
	require.Len(t, described.GetIndexInfos(), 1)
	assert.Equal(t, commonpb.IndexState_Finished, described.GetIndexInfos()[0].GetState())
	// The resolved cache serves subsequent polling without another handler call.
	described, err = s.DescribeIndex(context.Background(), &indexpb.DescribeIndexRequest{CollectionID: 1, IndexName: "json_flat"})
	require.NoError(t, merr.CheckRPCCall(described, err))
}

func TestJSONFlatIndexCopySyncSkipsHistoricalArtifacts(t *testing.T) {
	flat := jsonFlatDefinition()
	scalar := model.CloneIndex(flat)
	scalar.FieldID, scalar.IndexID, scalar.IndexName = 102, 11, "scalar"
	m := createTestIndexMeta(t, 1, map[int64]*model.Index{10: flat, 11: scalar})
	m.getCollectionSchema = func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() }
	task := createTestCopyTask(1, 100)
	result := &datapb.CopySegmentResult{SegmentId: 100, IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
		1000: {FieldId: 101, IndexId: 10, BuildId: 1000, IndexName: "json_flat", IndexFilePaths: []string{"flat"}},
		1001: {FieldId: 102, IndexId: 11, BuildId: 1001, IndexName: "scalar", IndexFilePaths: []string{"scalar"}},
	}}
	require.NoError(t, syncVectorScalarIndexes(context.Background(), result, task, &meta{indexMeta: m, segments: NewSegmentsInfo()}, nil))
	_, found := m.GetIndexJob(1000)
	assert.False(t, found)
	_, found = m.GetIndexJob(1001)
	assert.True(t, found)
}

func TestJSONFlatIndexCopyResultRetriesColdSchema(t *testing.T) {
	task := createTestCopyTask(1, 100).(*copySegmentTask)
	copies, meta := newCopySegmentTaskTestMeta(t, task)
	meta.collections = typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	meta.segments.SetSegment(100, NewSegmentInfo(&datapb.SegmentInfo{ID: 100, CollectionID: 1, PartitionID: 10, State: commonpb.SegmentState_Importing, NumOfRows: 20}))
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.MatchedBy(func(index *model.SegmentIndex) bool { return index.IndexID == 11 && index.BuildID == 1001 })).Return(nil).Once()
	indexes := newSegmentIndexMeta(catalog)
	flat := jsonFlatDefinition()
	scalar := model.CloneIndex(flat)
	scalar.FieldID, scalar.IndexID, scalar.IndexName = 102, 11, "scalar"
	putJSONFlatTestIndex(indexes, flat)
	putJSONFlatTestIndex(indexes, scalar)
	indexes.getCollectionSchema = func(collectionID int64) *schemapb.CollectionSchema {
		if collection := meta.GetCollection(collectionID); collection != nil {
			return collection.Schema
		}
		return nil
	}
	meta.indexMeta = indexes
	source := broker.NewMockBroker(t)
	source.EXPECT().DescribeCollectionInternal(mock.Anything, int64(1)).Return(nil, merr.ErrServiceNotReady).Once()
	source.EXPECT().DescribeCollectionInternal(mock.Anything, int64(1)).Return(&milvuspb.DescribeCollectionResponse{CollectionID: 1, Schema: jsonFlatTestSchema()}, nil).Once()
	source.EXPECT().ShowPartitionsInternal(mock.Anything, int64(1)).Return([]int64{10}, nil).Once()
	meta.broker = source
	task.meta, task.copyMeta = meta, copies
	response := &datapb.QueryCopySegmentResponse{State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted, SegmentResults: []*datapb.CopySegmentResult{{SegmentId: 100, IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
		1000: {FieldId: 101, IndexId: 10, BuildId: 1000, IndexName: "json_flat", IndexFilePaths: []string{"flat"}},
		1001: {FieldId: 102, IndexId: 11, BuildId: 1001, IndexName: "scalar", IndexFilePaths: []string{"scalar"}},
	}}}}
	worker := session.NewMockCluster(t)
	worker.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(response, nil).Twice()
	task.QueryTaskOnWorker(worker)
	assert.Nil(t, meta.GetCollection(1))
	assert.Empty(t, indexes.GetAllSegmentIndexes(100))
	assert.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskInProgress, copies.GetTask(context.Background(), task.GetTaskId()).GetState())
	// A later worker poll retries this collection directly after the startup
	// schema reload has stopped retrying; it does not depend on that goroutine.
	task.QueryTaskOnWorker(worker)
	require.NotNil(t, meta.GetCollection(1))
	assert.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, copies.GetTask(context.Background(), task.GetTaskId()).GetState())
	_, found := indexes.GetIndexJob(1000)
	assert.False(t, found)
	_, found = indexes.GetIndexJob(1001)
	assert.True(t, found)
	assert.Len(t, indexes.GetAllSegmentIndexes(100), 1)
}

func TestJSONFlatIndexSnapshotFilesUsePhysicalView(t *testing.T) {
	indexes := newSegmentIndexMeta(nil)
	indexes.getCollectionSchema = func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() }
	flat := jsonFlatDefinition()
	scalar := model.CloneIndex(flat)
	scalar.FieldID, scalar.IndexID, scalar.IndexName = 102, 11, "scalar"
	putJSONFlatTestIndex(indexes, flat)
	putJSONFlatTestIndex(indexes, scalar)
	indexes.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: commonpb.IndexState_Finished, IndexFileKeys: []string{"flat"}})
	indexes.updateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 11, BuildID: 1001, IndexState: commonpb.IndexState_Finished, IndexFileKeys: []string{"scalar"}})
	chunks := mocks.NewChunkManager(t)
	chunks.EXPECT().RootPath().Return("files").Once()
	handler := &ServerHandler{s: &Server{meta: &meta{indexMeta: indexes, chunkManager: chunks}}}
	files := uncompressIndexFiles(handler, 1, 100)
	require.Len(t, files, 1)
	assert.EqualValues(t, 11, files[0].GetIndexID())
}
