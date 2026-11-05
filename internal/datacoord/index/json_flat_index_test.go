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
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
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
