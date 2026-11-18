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
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestJSONFlatRetirementRetriesAndRestarts(t *testing.T) {
	ctx := context.Background()
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().DropSegmentIndex(mock.Anything, int64(1), int64(0), int64(100), int64(1000)).Return(merr.ErrServiceUnavailable).Once()
	catalog.EXPECT().DropSegmentIndex(mock.Anything, int64(1), int64(0), int64(100), int64(1000)).Return(nil).Once()
	m := NewSegmentIndexMeta(catalog)
	m.getCollectionSchema = func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() }
	putJSONFlatTestIndex(m, jsonFlatDefinition())
	original := &model.SegmentIndex{
		CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000, NodeID: 20,
		IndexState: commonpb.IndexState_InProgress, IndexVersion: 7, IndexStorePathVersion: 1, IndexFileKeys: []string{},
	}
	m.updateSegmentIndex(model.CloneSegmentIndex(original))
	mt := &meta.Meta{IndexCatalog: m, Collections: typeutil.NewConcurrentMap[UniqueID, *meta.CollectionInfo](), Segments: meta.NewSegmentsInfo()}
	mt.Segments.SetSegment(100, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed}})
	running := newIndexBuildTask(model.CloneSegmentIndex(original), 1, mt, m, nil, nil, nil)
	worker := session.NewMockCluster(t)
	worker.EXPECT().DropIndex(int64(20), int64(1000)).Return(merr.ErrServiceUnavailable).Once()
	worker.EXPECT().DropIndex(int64(20), int64(1000)).Return(nil).Once()
	worker.EXPECT().DropIndex(int64(20), int64(1000)).Return(merr.ErrNodeNotFound).Once()
	// Failed cancellation must not write metadata; failed persistence must not
	// retire the scheduler entry. Both leave the original durable identity intact.
	for range 2 {
		running.QueryTaskOnWorker(worker)
		got, _ := m.GetIndexJob(1000)
		assert.Equal(t, original, got)
		assert.Equal(t, commonpb.IndexState_InProgress, running.IndexState)
	}
	catalog.AssertNumberOfCalls(t, "DropSegmentIndex", 1)
	// Recover from the unchanged durable row after a coordinator restart.
	scheduler := task.NewMockGlobalScheduler(t)
	var recovered *indexBuildTask
	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(value task.Task) { recovered = value.(*indexBuildTask) })
	inspector := &IndexInspector{ctx: ctx, meta: mt, indexMeta: m, scheduler: scheduler}
	inspector.reloadFromMeta()
	require.NotNil(t, recovered)
	recovered.QueryTaskOnWorker(worker)
	_, ok := m.GetIndexJob(1000)
	require.False(t, ok)
	assert.Empty(t, m.GetAllSegmentIndexes(100))
	assert.Equal(t, commonpb.IndexState(indexpb.JobState_JobStateNone), recovered.IndexState)
	// Restart has nothing to recover; new-version inspection must not recreate
	// the deleted task. Downgrade uses the retained collection definition.
	inspector.reloadFromMeta()
	scheduler.AssertNumberOfCalls(t, "Enqueue", 1)
	require.NoError(t, inspector.createIndexesForSegment(ctx, mt.GetSegment(ctx, 100)))
	assert.Empty(t, m.GetAllSegmentIndexes(100))
	assert.Len(t, m.GetIndexesForCollection(1, ""), 1)
}

func TestJSONFlatRetirementPreservesArtifactOwnership(t *testing.T) {
	for _, state := range []commonpb.IndexState{commonpb.IndexState_Unissued, commonpb.IndexState_InProgress, commonpb.IndexState_Finished, commonpb.IndexState_Failed} {
		t.Run(state.String(), func(t *testing.T) {
			m := NewSegmentIndexMeta(catalogmocks.NewDataCoordCatalog(t))
			original := &model.SegmentIndex{
				CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000,
				IndexState: state, IndexFileKeys: []string{"historical-file"}, FailReason: "original", IndexVersion: 7,
			}
			m.updateSegmentIndex(model.CloneSegmentIndex(original))
			require.NoError(t, m.retireJSONFlatTask(1000))
			got, _ := m.GetIndexJob(1000)
			assert.Equal(t, original, got)
		})
	}
}

func TestJSONFlatRecoversOnlyDeprecationFailure(t *testing.T) {
	for _, reason := range []string{indexparams.JSONFlatIndexDeprecationReason, "real build failure"} {
		t.Run(reason, func(t *testing.T) {
			catalog := catalogmocks.NewDataCoordCatalog(t)
			m := NewSegmentIndexMeta(catalog)
			m.getCollectionSchema = func(int64) *schemapb.CollectionSchema { return jsonFlatTestSchema() }
			putJSONFlatTestIndex(m, jsonFlatDefinition())
			m.updateSegmentIndex(&model.SegmentIndex{
				CollectionID: 1, SegmentID: 100, IndexID: 10, BuildID: 1000,
				IndexState: commonpb.IndexState_Failed, FailReason: reason,
			})
			mt := &meta.Meta{IndexCatalog: m, Collections: typeutil.NewConcurrentMap[UniqueID, *meta.CollectionInfo](), Segments: meta.NewSegmentsInfo()}
			mt.Segments.SetSegment(100, &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 100, CollectionID: 1, State: commonpb.SegmentState_Flushed}})
			scheduler := task.NewMockGlobalScheduler(t)
			var recovered *indexBuildTask
			if reason == indexparams.JSONFlatIndexDeprecationReason {
				scheduler.EXPECT().Enqueue(mock.Anything).Run(func(value task.Task) { recovered = value.(*indexBuildTask) }).Once()
				catalog.EXPECT().DropSegmentIndex(mock.Anything, int64(1), int64(0), int64(100), int64(1000)).Return(nil).Once()
			}
			(&IndexInspector{meta: mt, indexMeta: m, scheduler: scheduler}).reloadFromMeta()
			if reason != indexparams.JSONFlatIndexDeprecationReason {
				assert.Nil(t, recovered)
				return
			}
			require.NotNil(t, recovered)
			recovered.QueryTaskOnWorker(session.NewMockCluster(t))
			_, ok := m.GetIndexJob(1000)
			assert.False(t, ok)
			assert.Equal(t, commonpb.IndexState(indexpb.JobState_JobStateNone), recovered.IndexState)
		})
	}
}
