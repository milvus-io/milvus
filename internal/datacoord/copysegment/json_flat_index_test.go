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

package copysegment

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
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func jsonFlatTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 101, Name: "json", DataType: schemapb.DataType_JSON}, {FieldID: 102, Name: "scalar", DataType: schemapb.DataType_Int64}}}
}

func putJSONFlatTestIndex(m *index.IndexMeta, index *model.Index) {
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

func TestJSONFlatIndexCopySyncSkipsHistoricalArtifacts(t *testing.T) {
	flat := jsonFlatDefinition()
	scalar := model.CloneIndex(flat)
	scalar.FieldID, scalar.IndexID, scalar.IndexName = 102, 11, "scalar"
	m := index.CreateTestIndexMeta(t, 1, map[int64]*model.Index{10: flat, 11: scalar})
	m.SetCollectionSchemaGetter(func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() })
	task := CreateTestCopyTask(1, 100)
	result := &datapb.CopySegmentResult{SegmentId: 100, IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
		1000: {FieldId: 101, IndexId: 10, BuildId: 1000, IndexName: "json_flat", IndexFilePaths: []string{"flat"}},
		1001: {FieldId: 102, IndexId: 11, BuildId: 1001, IndexName: "scalar", IndexFilePaths: []string{"scalar"}},
	}}
	require.NoError(t, syncVectorScalarIndexes(context.Background(), result, task, &meta.Meta{IndexCatalog: m, Segments: meta.NewSegmentsInfo()}, nil, m))
	_, found := m.GetIndexJob(1000)
	assert.False(t, found)
	_, found = m.GetIndexJob(1001)
	assert.True(t, found)
}

func TestJSONFlatIndexCopyResultRetriesColdSchema(t *testing.T) {
	task := CreateTestCopyTask(1, 100).(*Task)
	copies, mt := newCopySegmentTaskTestMeta(t, task)
	mt.Collections = typeutil.NewConcurrentMap[meta.UniqueID, *meta.CollectionInfo]()
	mt.Segments.SetSegment(100, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 100, CollectionID: 1, PartitionID: 10, State: commonpb.SegmentState_Importing, NumOfRows: 20}))
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.MatchedBy(func(index *model.SegmentIndex) bool { return index.IndexID == 11 && index.BuildID == 1001 })).Return(nil).Once()
	indexes := index.NewSegmentIndexMeta(catalog)
	flat := jsonFlatDefinition()
	scalar := model.CloneIndex(flat)
	scalar.FieldID, scalar.IndexID, scalar.IndexName = 102, 11, "scalar"
	putJSONFlatTestIndex(indexes, flat)
	putJSONFlatTestIndex(indexes, scalar)
	indexes.SetCollectionSchemaGetter(func(collectionID int64) *schemapb.CollectionSchema {
		if collection := mt.GetCollection(collectionID); collection != nil {
			return collection.Schema
		}
		return nil
	})
	mt.IndexCatalog = indexes
	source := broker.NewMockBroker(t)
	source.EXPECT().DescribeCollectionInternal(mock.Anything, int64(1)).Return(nil, merr.ErrServiceNotReady).Once()
	source.EXPECT().DescribeCollectionInternal(mock.Anything, int64(1)).Return(&milvuspb.DescribeCollectionResponse{CollectionID: 1, Schema: jsonFlatTestSchema()}, nil).Once()
	source.EXPECT().ShowPartitionsInternal(mock.Anything, int64(1)).Return([]int64{10}, nil).Once()
	mt.Broker = source
	task.Meta, task.CopyMeta, task.IndexMeta = mt, copies, indexes
	response := &datapb.QueryCopySegmentResponse{State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted, SegmentResults: []*datapb.CopySegmentResult{{SegmentId: 100, IndexInfos: map[int64]*datapb.VectorScalarIndexInfo{
		1000: {FieldId: 101, IndexId: 10, BuildId: 1000, IndexName: "json_flat", IndexFilePaths: []string{"flat"}},
		1001: {FieldId: 102, IndexId: 11, BuildId: 1001, IndexName: "scalar", IndexFilePaths: []string{"scalar"}},
	}}}}
	worker := session.NewMockCluster(t)
	worker.EXPECT().QueryCopySegment(mock.Anything, mock.Anything).Return(response, nil).Twice()
	task.QueryTaskOnWorker(worker)
	assert.Nil(t, mt.GetCollection(1))
	assert.Empty(t, indexes.GetAllSegmentIndexes(100))
	assert.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskInProgress, copies.GetTask(context.Background(), task.GetTaskId()).GetState())
	// A later worker poll retries this collection directly after the startup
	// schema reload has stopped retrying; it does not depend on that goroutine.
	task.QueryTaskOnWorker(worker)
	require.NotNil(t, mt.GetCollection(1))
	assert.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, copies.GetTask(context.Background(), task.GetTaskId()).GetState())
	_, found := indexes.GetIndexJob(1000)
	assert.False(t, found)
	_, found = indexes.GetIndexJob(1001)
	assert.True(t, found)
	assert.Len(t, indexes.GetAllSegmentIndexes(100), 1)
}
