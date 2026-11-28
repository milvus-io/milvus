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
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/datacoord/taskresource"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Publish a revision after classification/pricing and before worker dispatch.
// All preparation, dispatch and scheduling behavior remains the real code.
type manifestChangingIndexTask struct {
	*indexBuildTask
	once    sync.Once
	publish func()
}

func (t *manifestChangingIndexTask) GetTaskResource() taskcommon.Resource {
	resource := t.indexBuildTask.GetTaskResource()
	t.once.Do(t.publish)
	return resource
}

func TestNgramManifestRevisionRequeuesWithoutWorkerFailure(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	for _, setting := range []struct {
		param *paramtable.ParamItem
		value string
	}{
		{&params.DataCoordCfg.TaskRetryBackoffInterval, "3600"},
		{&params.DataCoordCfg.TaskRetryBackoffMaxInterval, "3600"},
		{&params.DataCoordCfg.TaskScheduleInterval, "10"},
		{&params.DataCoordCfg.MinSegmentNumRowsToEnableIndex, "1"},
	} {
		original := setting.param.GetValue()
		require.NoError(t, params.Save(setting.param.Key, setting.value))
		t.Cleanup(func() { require.NoError(t, params.Save(setting.param.Key, original)) })
	}
	fixture := meta.NewLocalPackedFixture(t, false)
	mt, im := fixtureIndexMeta(t, taskresource.NewFixtureMeta(false))
	im.Indexes[1][4].IndexParams = []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexparamcheck.IndexNGRAM}}
	schema := taskresource.FixtureSchema()
	schema.Fields = schema.Fields[:2]
	schema.Fields[1].DataType = schemapb.DataType_Text
	schema.Fields[1].TypeParams = nil
	taskresource.PutFixtureSchema(mt, schema)
	taskresource.UpdateFixtureSegment(mt, func(seg *datapb.SegmentInfo) {
		seg.ManifestPath = fixture.PhysicalManifest
		seg.NumOfRows = fixture.Segment.NumOfRows
	})
	base, version, err := packed.UnmarshalManifestPath(fixture.PhysicalManifest)
	require.NoError(t, err)
	updated, err := packed.CommitManifestUpdates(base, version, meta.CreateStorageConfig(), &packed.ManifestUpdates{
		Stats: []packed.StatEntry{{
			Key: "bloom_filter.100", Files: []string{filepath.Join(base, "_stats/bloom_filter.100/42")},
			Metadata: map[string]string{"memory_size": "25"},
		}},
	})
	require.NoError(t, err)
	require.NotEqual(t, fixture.PhysicalManifest, updated)
	job, ok := im.GetIndexJob(5)
	require.True(t, ok)
	handler := meta.NewNMockHandler(t)
	handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(mt.GetCollection(1), nil)
	it := newIndexBuildTask(job, 1, mt, im, handler, fixture.Cm, NewIndexEngineVersionManager())
	require.Equal(t, globalTask.LocalNeedsWorker, it.CompleteOnCoordinator(context.Background()))
	price := it.GetTaskResource()
	task := &manifestChangingIndexTask{indexBuildTask: it, publish: func() {
		taskresource.UpdateFixtureSegment(mt, func(seg *datapb.SegmentInfo) { seg.ManifestPath = updated })
	}}
	type dispatch struct {
		request  *workerpb.CreateJobRequest
		resource taskcommon.Resource
	}
	dispatched := make(chan dispatch, 1)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QuerySlot().Return(map[int64]*session.WorkerSlots{
		1: {NodeID: 1, TotalCPU: 4, AvailableCPU: 4, TotalMemory: 1 << 30, AvailableMemory: 1 << 30, Idle: true},
	}).Maybe()
	cluster.EXPECT().CreateIndex(int64(1), mock.Anything, mock.Anything).RunAndReturn(func(_ int64, req *workerpb.CreateJobRequest, resource taskcommon.Resource) error {
		dispatched <- dispatch{request: req, resource: resource}
		return nil
	}).Once()
	cluster.EXPECT().QueryIndex(int64(1), mock.Anything).Return(&workerpb.IndexJobResults{Results: []*workerpb.IndexTaskInfo{
		{BuildID: job.BuildID, State: commonpb.IndexState_InProgress},
	}}, nil).Maybe()
	scheduler := globalTask.NewGlobalTaskScheduler(context.Background(), cluster)
	scheduler.Enqueue(task)
	scheduler.Start()
	defer scheduler.Stop()
	select {
	case got := <-dispatched:
		require.Equal(t, updated, got.request.Manifest)
		require.Equal(t, price, got.resource, "stats-only publication must preserve the input price")
		require.EqualValues(t, 1, got.request.IndexVersion, "the stale attempt must not allocate a worker version")
	case <-time.After(3 * time.Second):
		t.Fatal("manifest repricing incorrectly entered the one-hour worker-failure backoff")
	}
}
