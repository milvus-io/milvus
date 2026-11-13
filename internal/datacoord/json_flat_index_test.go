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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

func TestJSONFlatIndexLogicalAndPhysicalViews(t *testing.T) {
	m := index.NewSegmentIndexMeta(nil)
	m.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() })
	flat := jsonFlatDefinition()
	m.Indexes[flat.CollectionID] = map[int64]*model.Index{flat.IndexID: flat}
	m.UpdateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: commonpb.IndexState_Failed, FailReason: "old failure", IndexFileKeys: []string{"old-file"}})
	require.Len(t, m.GetIndexesForCollection(1, "json_flat"), 1)
	assert.True(t, m.IsIndexExist(1, 10))
	assert.Empty(t, m.GetIndexesForCollection(1, "missing"))
	assert.False(t, m.HasIndex(1))
	assert.Empty(t, m.GetSegmentIndexes(1, 100))
	assert.Len(t, m.GetAllSegmentIndexes(100), 1)
	assert.Empty(t, m.GetUnIndexedSegmentIDsForIndexTask(1, []int64{100, 101}))
	assert.Empty(t, m.GetUnindexedSegments(1, []int64{100, 101}))
	assert.Equal(t, commonpb.IndexState_Finished, m.GetSegmentIndexState(1, 100, 10).GetState())

	store := metacache.NewMetaStore(nil)
	serverMeta := &meta.Meta{IndexCatalog: m, MetaStore: store, Segments: meta.NewSegmentsInfo(store)}
	serverMeta.Segments.SetSegment(100, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed, NumOfRows: 100}})
	s := &Server{meta: serverMeta, domainMetas: domainMetas{indexMeta: m}}
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
	m.Indexes[typed.CollectionID][typed.IndexID] = typed
	assert.True(t, m.HasIndex(1))
	assert.Equal(t, []int64{100}, m.GetUnIndexedSegmentIDsForIndexTask(1, []int64{100}).Collect())
	m.UpdateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 11, BuildID: 1001, IndexState: commonpb.IndexState_Finished})
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
		(&Server{meta: &meta.Meta{}, domainMetas: domainMetas{indexMeta: func() *index.IndexMeta {
			m := index.NewSegmentIndexMeta(nil)
			m.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() })
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

func TestJSONFlatIndexPreservesScalarWithIgnoredJSONExtras(t *testing.T) {
	m := index.NewSegmentIndexMeta(nil)
	m.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() })
	scalar := jsonFlatDefinition()
	scalar.FieldID = 102
	scalar.IndexName = "scalar"
	m.Indexes[scalar.CollectionID] = map[int64]*model.Index{scalar.IndexID: scalar}
	m.UpdateSegmentIndex(&model.SegmentIndex{CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, IndexState: commonpb.IndexState_Failed, FailReason: "real scalar failure"})
	assert.True(t, m.HasIndex(1))
	assert.Len(t, m.GetPhysicalIndexesForCollection(1), 1)
	assert.Len(t, m.GetSegmentIndexes(1, 100), 1)
	info := &indexpb.IndexInfo{IndexID: scalar.IndexID}
	(&Server{meta: &meta.Meta{}, domainMetas: domainMetas{indexMeta: m}}).completeIndexInfo(info, scalar, map[int64]*indexStats{100: {numRows: 100, state: commonpb.SegmentState_Flushed, indexStates: map[int64]*indexpb.SegmentIndexState{10: {State: commonpb.IndexState_Failed, FailReason: "real scalar failure"}}}}, true, 0)
	assert.Equal(t, commonpb.IndexState_Failed, info.GetState())
	assert.Zero(t, info.GetIndexedRows())
	assert.EqualValues(t, 100, info.GetPendingIndexRows())
}
