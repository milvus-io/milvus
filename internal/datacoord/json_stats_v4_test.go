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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func jsonStatsNode(id int64, version int32) *sessionutil.Session {
	return &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{
		ServerID: id,
		ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{
			CurrentIndexVersion: version, MaximumIndexVersion: version,
		},
	}}
}

func TestJSONStatsV4NodeEligibility(t *testing.T) {
	require.NoError(t, Params.Save(Params.DataCoordCfg.JSONStatsFormatVersion.Key, "4"))
	t.Cleanup(func() { Params.Reset(Params.DataCoordCfg.JSONStatsFormatVersion.Key) })
	m := newIndexEngineVersionManager().(*versionManagerImpl)
	task := &statsTask{StatsTask: &indexpb.StatsTask{
		SubJobType:          indexpb.StatsSubJob_JsonKeyIndexJob,
		JsonStatsDataFormat: common.JSONStatsDataFormatV4,
	}, ievm: m}
	require.False(t, task.CanRunOnNode(10))
	m.AddNode(jsonStatsNode(1, 6))
	m.AddDataNode(jsonStatsNode(10, 5))
	m.AddDataNode(jsonStatsNode(11, 6))
	require.False(t, task.CanRunOnNode(10))
	require.True(t, task.CanRunOnNode(11), "an old DN must not block a compatible DN")
	m.AddNode(jsonStatsNode(2, 5))
	require.False(t, task.CanRunOnNode(11), "old reader blocks V4")
	m.RemoveNode(jsonStatsNode(2, 5))
	require.True(t, task.CanRunOnNode(11))
	m.RejectJSONStatsWriter(11)
	require.False(t, task.CanRunOnNode(11))
	m.UpdateDataNode(jsonStatsNode(11, 6))
	require.False(t, task.CanRunOnNode(11), "a session update must not erase rejected output")
	m.AddDataNode(jsonStatsNode(12, 6))
	require.True(t, task.CanRunOnNode(12))
	m.StartupDataNodes(map[string]*sessionutil.Session{})
	require.False(t, task.CanRunOnNode(12), "rewatch must remove offline writer capabilities")
	task.JsonStatsDataFormat = 0
	require.True(t, task.CanRunOnNode(10), "legacy V3 tasks retain their behavior")
	require.Equal(t, common.JSONStatsDataFormatV3, task.getJSONStatsDataFormat())
}

func TestJSONStatsV4BoundNodeEligibility(t *testing.T) {
	params := paramtable.Get()
	oldBindMode := params.DataCoordCfg.BindIndexNodeMode.GetValue()
	oldNodeID := params.DataCoordCfg.IndexNodeID.GetValue()
	require.NoError(t, params.Save(params.DataCoordCfg.BindIndexNodeMode.Key, "true"))
	require.NoError(t, params.Save(params.DataCoordCfg.IndexNodeID.Key, "10"))
	t.Cleanup(func() {
		params.Save(params.DataCoordCfg.BindIndexNodeMode.Key, oldBindMode)
		params.Save(params.DataCoordCfg.IndexNodeID.Key, oldNodeID)
	})

	task, versions := newV4StatsTestTask(t)
	versions.StartupDataNodes(nil) // Bind mode has no writer session capabilities.
	require.True(t, task.CanRunOnNode(10))
	require.False(t, task.CanRunOnNode(11), "only the configured bound worker is trusted")
	require.False(t, versions.SupportsScalarIndexVersion(6), "the stats exception must not enable whole-segment migration")

	versions.Startup(nil)
	require.False(t, task.CanRunOnNode(10), "unknown readers still block V4")
	versions.AddNode(jsonStatsNode(1, 5))
	require.False(t, task.CanRunOnNode(10), "old readers still block V4")
	versions.Update(jsonStatsNode(1, 6))
	require.True(t, task.CanRunOnNode(10))

	require.NoError(t, params.Save(params.DataCoordCfg.BindIndexNodeMode.Key, "false"))
	require.False(t, task.CanRunOnNode(10), "ordinary mode still requires writer capabilities")
	require.NoError(t, params.Save(params.DataCoordCfg.BindIndexNodeMode.Key, "true"))

	// Exercise dispatch and result validation, not just the capability helper.
	task.State = indexpb.JobState_JobStateInit
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).Return(int64(100), int64(200), nil)
	task.allocator = alloc
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().CreateStats(int64(10), mock.MatchedBy(func(req *workerpb.CreateStatsRequest) bool {
		return req.GetTaskID() == task.GetTaskID() && req.GetNumRows() == 10 &&
			req.GetJsonKeyStatsDataFormat() == common.JSONStatsDataFormatV4
	})).Return(nil).Once()
	task.CreateTaskOnWorker(10, cluster)
	require.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())

	result := validV4StatsResult()
	result.JsonKeyStatsLogs[100].JsonKeyStatsDataFormat = common.JSONStatsDataFormatV3
	cluster.EXPECT().QueryStats(int64(10), mock.Anything).Return(&workerpb.StatsResults{Results: []*workerpb.StatsResult{result}}, nil).Once()
	cluster.EXPECT().DropStats(int64(10), int64(30)).Return(nil).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, indexpb.JobState_JobStateInit, task.GetState())
	require.Empty(t, task.meta.GetHealthySegment(context.Background(), 20).GetJsonKeyStats())
	require.False(t, task.CanRunOnNode(10), "known-invalid output overrides the default trust")
}

func TestJSONStatsV4RejectsWorkerOutputBeforePublication(t *testing.T) {
	for _, test := range []struct {
		name  string
		alter func(map[int64]*datapb.JsonKeyStats)
	}{
		{"old format", func(s map[int64]*datapb.JsonKeyStats) { s[100].JsonKeyStatsDataFormat = 3 }},
		{"old DN empty success", func(s map[int64]*datapb.JsonKeyStats) { delete(s, 100) }},
		{"empty files", func(s map[int64]*datapb.JsonKeyStats) { s[100].Files = nil }},
		{"wrong build", func(s map[int64]*datapb.JsonKeyStats) { s[100].BuildID = 999 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			task, versions := newV4StatsTestTask(t)
			result := validV4StatsResult()
			test.alter(result.JsonKeyStatsLogs)
			require.ErrorIs(t, task.SetJobInfo(context.Background(), result), errJSONStatsResultInvalid)
			require.Empty(t, task.meta.GetHealthySegment(context.Background(), 20).GetJsonKeyStats())

			// Disabling new work cannot excuse invalid output from an admitted attempt.
			key := Params.CommonCfg.EnabledJSONKeyStats.Key
			original := Params.CommonCfg.EnabledJSONKeyStats.GetValue()
			t.Cleanup(func() { Params.Save(key, original) })
			require.NoError(t, Params.Save(key, "false"))

			cluster := session.NewMockCluster(t)
			cluster.EXPECT().QueryStats(int64(10), mock.Anything).Return(&workerpb.StatsResults{Results: []*workerpb.StatsResult{result}}, nil)
			cluster.EXPECT().DropStats(int64(10), int64(30)).Return(nil)
			task.QueryTaskOnWorker(cluster)
			require.Equal(t, indexpb.JobState_JobStateInit, task.GetState())
			require.Equal(t, common.JSONStatsDataFormatV4, task.getJSONStatsDataFormat())
			require.False(t, versions.SupportsJSONStatsWriter(10))
			require.Empty(t, task.meta.GetHealthySegment(context.Background(), 20).GetJsonKeyStats())
		})
	}
}

func TestJSONStatsV4HoldsCompletedOutputForReaders(t *testing.T) {
	task, versions := newV4StatsTestTask(t)
	result := validV4StatsResult()
	require.NoError(t, task.validateJSONStatsResult(context.Background(), result))
	versions.AddNode(jsonStatsNode(2, 5))
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QueryStats(int64(10), mock.Anything).Return(&workerpb.StatsResults{Results: []*workerpb.StatsResult{result}}, nil)
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
	require.True(t, versions.SupportsJSONStatsWriter(10))
	require.Empty(t, task.meta.GetHealthySegment(context.Background(), 20).GetJsonKeyStats())
	versions.RemoveNode(jsonStatsNode(2, 5))
	require.NoError(t, task.validateJSONStatsResult(context.Background(), result))
}

func TestJSONStatsV4EmptySegmentNeedsNoArtifact(t *testing.T) {
	task, _ := newV4StatsTestTask(t)
	require.NoError(t, Params.Save(Params.DataCoordCfg.JSONStatsFormatVersion.Key, "auto"))
	task.meta.GetHealthySegment(context.Background(), 20).NumOfRows = 0
	require.True(t, task.CanRunOnNode(10))
	require.NoError(t, task.validateJSONStatsResult(context.Background(), &workerpb.StatsResult{}))
}

func TestJSONStatsV4WaitsForVersionGate(t *testing.T) {
	task, versions := newV4StatsTestTask(t)
	// Readers and writers are capable, but that says nothing about standby GC.
	require.NoError(t, Params.Save(Params.DataCoordCfg.JSONStatsFormatVersion.Key, "auto"))
	require.True(t, versions.SupportsJSONStatsReaders())
	require.True(t, versions.SupportsJSONStatsWriter(10))
	require.False(t, task.CanRunOnNode(10))
	require.ErrorIs(t, task.SetJobInfo(context.Background(), validV4StatsResult()), merr.ErrServiceNotReady)

	// A persisted pending task must not dispatch, even after coordinator restart.
	task.State = indexpb.JobState_JobStateInit
	cluster := session.NewMockCluster(t) // No CreateStats/DropStats before the gate.
	task.CreateTaskOnWorker(10, cluster)
	require.Equal(t, indexpb.JobState_JobStateInit, task.GetState())

	// Nor may an already completed worker result be published or reject its writer.
	task.State = indexpb.JobState_JobStateInProgress
	cluster.EXPECT().QueryStats(int64(10), mock.Anything).Return(
		&workerpb.StatsResults{Results: []*workerpb.StatsResult{validV4StatsResult()}}, nil).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
	require.True(t, versions.SupportsJSONStatsWriter(10))
	require.Empty(t, task.meta.GetHealthySegment(context.Background(), 20).GetJsonKeyStats())

	// The persisted target set by the gate releases this same task, without a rebuild.
	require.NoError(t, Params.Save(Params.DataCoordCfg.JSONStatsFormatVersion.Key, "4"))
	require.True(t, task.CanRunOnNode(10))
	require.NoError(t, task.validateJSONStatsResult(context.Background(), validV4StatsResult()))
	task.meta.catalog.(*catalogmocks.DataCoordCatalog).EXPECT().AlterSegments(mock.Anything,
		mock.MatchedBy(func(segments []*datapb.SegmentInfo) bool {
			return len(segments) == 1 && segments[0].GetJsonKeyStats()[100].GetJsonKeyStatsDataFormat() == common.JSONStatsDataFormatV4
		})).Return(nil).Once()
	cluster.EXPECT().QueryStats(int64(10), mock.Anything).Return(
		&workerpb.StatsResults{Results: []*workerpb.StatsResult{validV4StatsResult()}}, nil).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, indexpb.JobState_JobStateFinished, task.GetState())
	require.Equal(t, common.JSONStatsDataFormatV4, task.meta.GetHealthySegment(context.Background(), 20).GetJsonKeyStats()[100].GetJsonKeyStatsDataFormat())
}

func TestJSONStatsV4RejectsIncompatibleDispatch(t *testing.T) {
	task, versions := newV4StatsTestTask(t)
	versions.AddDataNode(jsonStatsNode(11, 5))
	task.State = indexpb.JobState_JobStateInit
	version := task.Version
	cluster := session.NewMockCluster(t) // No CreateStats or DropStats is allowed.
	task.CreateTaskOnWorker(11, cluster)
	require.Equal(t, indexpb.JobState_JobStateInit, task.GetState())
	require.Equal(t, version, task.Version)
	require.Contains(t, task.FailReason, "compatible DataNode")
}

func TestJSONStatsV4ShreddingPauseResume(t *testing.T) {
	for _, test := range []struct {
		name     string
		param    *paramtable.ParamItem
		disabled string
	}{
		{"shredding switch", &Params.CommonCfg.EnabledJSONKeyStats, "false"},
		{"deprecated kill switch", &Params.DataCoordCfg.JSONStatsTriggerCount, "0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			task, versions := newV4StatsTestTask(t)
			enabled := test.param.GetValue()
			t.Cleanup(func() { Params.Save(test.param.Key, enabled) })
			require.NoError(t, Params.Save(test.param.Key, test.disabled))
			cluster := session.NewMockCluster(t) // Paused tasks must make no worker RPC.
			for _, state := range []indexpb.JobState{indexpb.JobState_JobStateInit, indexpb.JobState_JobStateRetry} {
				// Recreate from persisted metadata, including a previous attempt.
				record := task.meta.statsTaskMeta.GetStatsTask(task.GetTaskID())
				record.State, record.Version = state, 1
				task.StatsTask = proto.Clone(record).(*indexpb.StatsTask)
				require.False(t, task.CanRunOnNode(10))
				task.CreateTaskOnWorker(10, cluster)
				require.Equal(t, indexpb.JobState_JobStateInit, task.GetState())
				require.Equal(t, int64(1), task.GetVersion())
				require.NotNil(t, task.meta.statsTaskMeta.GetStatsTask(task.GetTaskID()))
				require.Equal(t, int64(1), task.meta.statsTaskMeta.GetStatsTask(task.GetTaskID()).GetVersion())
				require.True(t, versions.SupportsJSONStatsWriter(10))
			}
			req, err := task.prepareJobRequest(context.Background(), task.meta.GetHealthySegment(context.Background(), 20))
			require.Nil(t, req)
			require.ErrorIs(t, err, merr.ErrServiceNotReady)

			// The same task and writer resume; no blacklist reset or new task ID.
			require.NoError(t, Params.Save(test.param.Key, enabled))
			require.True(t, task.CanRunOnNode(10))
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().AllocN(mock.Anything).Return(int64(100), int64(200), nil).Once()
			task.allocator = alloc
			cluster.EXPECT().CreateStats(int64(10), mock.MatchedBy(func(req *workerpb.CreateStatsRequest) bool {
				return req.GetTaskID() == 30 && req.GetEnableJsonKeyStats() &&
					req.GetJsonKeyStatsDataFormat() == common.JSONStatsDataFormatV4
			})).Return(nil).Once()
			task.CreateTaskOnWorker(10, cluster)
			require.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
			task.meta.catalog.(*catalogmocks.DataCoordCatalog).EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()
			cluster.EXPECT().QueryStats(int64(10), mock.Anything).Return(
				&workerpb.StatsResults{Results: []*workerpb.StatsResult{validV4StatsResult()}}, nil).Once()
			task.QueryTaskOnWorker(cluster)
			require.Equal(t, indexpb.JobState_JobStateFinished, task.GetState())
			require.True(t, versions.SupportsJSONStatsWriter(10))
			require.Equal(t, common.JSONStatsDataFormatV4, task.meta.GetHealthySegment(context.Background(), 20).GetJsonKeyStats()[100].GetJsonKeyStatsDataFormat())
		})
	}
}

func TestJSONStatsV4ShreddingDispatchRace(t *testing.T) {
	for _, beforePreparation := range []bool{true, false} {
		name := "disabled during request preparation"
		if beforePreparation {
			name = "disabled after node selection"
		}
		t.Run(name, func(t *testing.T) {
			task, versions := newV4StatsTestTask(t)
			task.State = indexpb.JobState_JobStateInit
			key := Params.CommonCfg.EnabledJSONKeyStats.Key
			original := Params.CommonCfg.EnabledJSONKeyStats.GetValue()
			t.Cleanup(func() { Params.Save(key, original) })
			require.NoError(t, Params.Save(key, "true"))
			require.True(t, task.CanRunOnNode(10))
			cluster := session.NewMockCluster(t)
			if beforePreparation {
				// UpdateTaskVersion runs after the last eligibility check.
				catalog := catalogmocks.NewDataCoordCatalog(t)
				catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Run(func(context.Context, *indexpb.StatsTask) {
					require.NoError(t, Params.Save(key, "false"))
				}).Return(nil)
				task.meta.catalog, task.meta.statsTaskMeta.catalog = catalog, catalog
				task.CreateTaskOnWorker(10, cluster)
				require.Equal(t, indexpb.JobState_JobStateInit, task.GetState())
				require.Contains(t, task.GetFailReason(), "shredding is disabled")
				// No schema lookup, allocation, or worker RPC is needed to pause.
				req, err := task.prepareJobRequest(context.Background(), task.meta.GetHealthySegment(context.Background(), 20))
				require.Nil(t, req)
				require.ErrorIs(t, err, merr.ErrServiceNotReady)
			} else {
				// An admitted attempt retains its request contract after a toggle.
				alloc := allocator.NewMockAllocator(t)
				alloc.EXPECT().AllocN(mock.Anything).Run(func(int64) {
					require.NoError(t, Params.Save(key, "false"))
				}).Return(int64(100), int64(200), nil).Once()
				task.allocator = alloc
				cluster.EXPECT().CreateStats(int64(10), mock.MatchedBy(func(req *workerpb.CreateStatsRequest) bool {
					return req.GetEnableJsonKeyStats() && req.GetJsonKeyStatsDataFormat() == common.JSONStatsDataFormatV4
				})).Return(nil).Once()
				task.CreateTaskOnWorker(10, cluster)
				require.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
				task.meta.catalog.(*catalogmocks.DataCoordCatalog).EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()
				cluster.EXPECT().QueryStats(int64(10), mock.Anything).Return(
					&workerpb.StatsResults{Results: []*workerpb.StatsResult{validV4StatsResult()}}, nil).Once()
				task.QueryTaskOnWorker(cluster)
				require.Equal(t, indexpb.JobState_JobStateFinished, task.GetState())
				require.Equal(t, common.JSONStatsDataFormatV4, task.meta.GetHealthySegment(context.Background(), 20).GetJsonKeyStats()[100].GetJsonKeyStatsDataFormat())
			}
			require.False(t, task.CanRunOnNode(10), "new attempts remain paused")
			require.True(t, versions.SupportsJSONStatsWriter(10), "a toggle must not blacklist the writer")
		})
	}
}

func TestJSONStatsV4ShreddingDisabledScope(t *testing.T) {
	key := Params.CommonCfg.EnabledJSONKeyStats.Key
	original := Params.CommonCfg.EnabledJSONKeyStats.GetValue()
	t.Cleanup(func() { Params.Save(key, original) })
	require.NoError(t, Params.Save(key, "false"))

	t.Run("legacy JSON tasks also pause", func(t *testing.T) {
		task, _ := newV4StatsTestTask(t)
		task.JsonStatsDataFormat = 0
		require.False(t, task.CanRunOnNode(10))
	})
	for _, subJob := range []indexpb.StatsSubJob{indexpb.StatsSubJob_Sort, indexpb.StatsSubJob_TextIndexJob} {
		t.Run(subJob.String(), func(t *testing.T) {
			task, _ := newV4StatsTestTask(t)
			task.SubJobType = subJob
			require.True(t, task.CanRunOnNode(10))
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().AllocN(mock.Anything).Return(int64(100), int64(200), nil).Once()
			task.allocator = alloc
			req, err := task.prepareJobRequest(context.Background(), task.meta.GetHealthySegment(context.Background(), 20))
			require.NoError(t, err)
			require.False(t, req.GetEnableJsonKeyStats())
			require.Equal(t, subJob, req.GetSubJobType())
		})
	}
	t.Run("empty segment completes locally", func(t *testing.T) {
		task, _ := newV4StatsTestTask(t)
		task.meta.GetHealthySegment(context.Background(), 20).NumOfRows = 0
		require.True(t, task.CanRunOnNode(10))
		task.CreateTaskOnWorker(10, session.NewMockCluster(t))
		require.Equal(t, indexpb.JobState_JobStateFinished, task.GetState())
	})
	t.Run("dropped segment cleans up locally", func(t *testing.T) {
		task, _ := newV4StatsTestTask(t)
		task.meta.segments.DropSegment(20)
		task.meta.catalog.(*catalogmocks.DataCoordCatalog).EXPECT().DropStatsTask(mock.Anything, int64(30)).Return(nil).Once()
		require.True(t, task.CanRunOnNode(10))
		task.CreateTaskOnWorker(10, session.NewMockCluster(t))
		require.Equal(t, indexpb.JobState_JobStateNone, task.GetState())
		require.Nil(t, task.meta.statsTaskMeta.GetStatsTask(30))
	})
}

func newV4StatsTestTask(t *testing.T) (*statsTask, *versionManagerImpl) {
	t.Helper()
	// These tests exercise post-gate worker/output contracts.
	require.NoError(t, Params.Save(Params.DataCoordCfg.JSONStatsFormatVersion.Key, "4"))
	t.Cleanup(func() { Params.Reset(Params.DataCoordCfg.JSONStatsFormatVersion.Key) })
	versions := newIndexEngineVersionManager().(*versionManagerImpl)
	reader := jsonStatsNode(1, common.MaximumScalarIndexEngineVersion)
	reader.ScalarIndexEngineVersion.CurrentIndexVersion = common.CurrentScalarIndexEngineVersion
	writer := jsonStatsNode(10, common.MaximumScalarIndexEngineVersion)
	writer.ScalarIndexEngineVersion.CurrentIndexVersion = common.CurrentScalarIndexEngineVersion
	versions.AddNode(reader)
	versions.AddDataNode(writer)
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	record := &indexpb.StatsTask{
		TaskID: 30, SegmentID: 20, TargetSegmentID: 20, CollectionID: 1, NodeID: 10,
		SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob, JsonStatsDataFormat: common.JSONStatsDataFormatV4,
		State: indexpb.JobState_JobStateInProgress, JsonStatsFieldIds: []int64{100},
	}
	tasks := typeutil.NewConcurrentMap[int64, *indexpb.StatsTask]()
	tasks.Insert(30, record)
	collection := &collectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "json", DataType: schemapb.DataType_JSON},
	}}}
	handler := NewNMockHandler(t)
	handler.EXPECT().GetCollection(mock.Anything, int64(1)).Return(collection, nil).Maybe()
	segments := NewSegmentsInfo()
	segments.SetSegment(20, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 20, CollectionID: 1, NumOfRows: 10, State: commonpb.SegmentState_Flushed,
	}))
	mt := &meta{
		ctx: context.Background(), catalog: catalog, segments: segments,
		collections: typeutil.NewConcurrentMap[int64, *collectionInfo](),
		statsTaskMeta: &statsTaskMeta{
			ctx: context.Background(), keyLock: lock.NewKeyLock[int64](), catalog: catalog, tasks: tasks,
			segmentID2Tasks: typeutil.NewConcurrentMap[string, *indexpb.StatsTask](),
		},
	}
	mt.collections.Insert(collection.ID, collection)
	mt.statsTaskMeta.segmentID2Tasks.Insert(createSecondaryIndexKey(record.SegmentID, record.SubJobType.String()), record)
	return newStatsTask(record, 1, mt, handler, nil, versions), versions
}

func validV4StatsResult() *workerpb.StatsResult {
	return &workerpb.StatsResult{
		TaskID: 30, State: indexpb.JobState_JobStateFinished, SegmentID: 20,
		JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{100: {
			FieldID: 100, BuildID: 30, JsonKeyStatsDataFormat: common.JSONStatsDataFormatV4,
			Files: []string{"meta.json"},
		}},
	}
}

func TestJSONStatsV4PublishesCompatibleResultAfterReaderUpgrade(t *testing.T) {
	task, versions := newV4StatsTestTask(t)
	result := validV4StatsResult()
	versions.AddNode(jsonStatsNode(2, 5))
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QueryStats(int64(10), mock.Anything).Return(&workerpb.StatsResults{Results: []*workerpb.StatsResult{result}}, nil)
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
	catalog := task.meta.catalog.(*catalogmocks.DataCoordCatalog)
	catalog.EXPECT().AlterSegments(mock.Anything, mock.MatchedBy(func(segments []*datapb.SegmentInfo) bool {
		return len(segments) == 1 && segments[0].GetJsonKeyStats()[100].GetJsonKeyStatsDataFormat() == common.JSONStatsDataFormatV4
	})).Return(nil).Once()
	versions.RemoveNode(jsonStatsNode(2, 5))
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, indexpb.JobState_JobStateFinished, task.GetState())
	require.Equal(t, common.JSONStatsDataFormatV4, task.meta.GetHealthySegment(context.Background(), 20).GetJsonKeyStats()[100].GetJsonKeyStatsDataFormat())
}

func TestJSONStatsV4SchemaAdditionDoesNotInvalidateRunningTask(t *testing.T) {
	task, _ := newV4StatsTestTask(t)
	collection, err := task.handler.GetCollection(context.Background(), 1)
	require.NoError(t, err)
	collection.Schema.Fields = append(collection.Schema.Fields, &schemapb.FieldSchema{
		FieldID: 101, Name: "later_json", DataType: schemapb.DataType_JSON,
	})
	// Recreate the wrapper from serialized metadata, as after a coordinator restart.
	encoded, err := proto.Marshal(task.StatsTask)
	require.NoError(t, err)
	persisted := &indexpb.StatsTask{}
	require.NoError(t, proto.Unmarshal(encoded, persisted))
	task.StatsTask = persisted
	require.Equal(t, []int64{100}, task.GetJsonStatsFieldIds())
	require.NoError(t, task.validateJSONStatsResult(context.Background(), validV4StatsResult()))

	// A task that actually requested both fields must receive both outputs.
	task.JsonStatsFieldIds = []int64{100, 101}
	require.ErrorIs(t, task.validateJSONStatsResult(context.Background(), validV4StatsResult()), errJSONStatsResultInvalid)
}

func TestJSONStatsV4LegacyTaskRetirementGuards(t *testing.T) {
	for _, test := range []struct {
		name  string
		alter func(*statsTask, *versionManagerImpl)
		param *paramtable.ParamItem
		value string
	}{
		{name: "old reader", alter: func(_ *statsTask, v *versionManagerImpl) { v.AddNode(jsonStatsNode(2, 5)) }},
		{name: "unknown readers", alter: func(_ *statsTask, v *versionManagerImpl) { v.Startup(nil) }},
		{name: "old writer", alter: func(_ *statsTask, v *versionManagerImpl) { v.UpdateDataNode(jsonStatsNode(10, 5)) }},
		{name: "rejected writer", alter: func(_ *statsTask, v *versionManagerImpl) { v.RejectJSONStatsWriter(10) }},
		{name: "running V3", alter: func(task *statsTask, _ *versionManagerImpl) { task.State = indexpb.JobState_JobStateInProgress }},
		{name: "completed V3", alter: func(task *statsTask, _ *versionManagerImpl) { task.State = indexpb.JobState_JobStateFinished }},
		{name: "owned by another workflow", alter: func(task *statsTask, _ *versionManagerImpl) { task.CanRecycle = false }},
		{name: "text stats", alter: func(task *statsTask, _ *versionManagerImpl) { task.SubJobType = indexpb.StatsSubJob_TextIndexJob }},
		{name: "already V4", alter: func(task *statsTask, _ *versionManagerImpl) { task.JsonStatsDataFormat = common.JSONStatsDataFormatV4 }},
		{name: "explicit V3 target", param: &Params.DataCoordCfg.JSONStatsFormatVersion, value: "3"},
		{name: "version gate pending", param: &Params.DataCoordCfg.JSONStatsFormatVersion, value: "auto"},
		{name: "shredding disabled", param: &Params.CommonCfg.EnabledJSONKeyStats, value: "false"},
		{name: "legacy shredding disable", param: &Params.DataCoordCfg.JSONStatsTriggerCount, value: "0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			task, versions := newV4StatsTestTask(t)
			task.JsonStatsDataFormat = 0
			task.State = indexpb.JobState_JobStateInit
			task.CanRecycle = true
			if test.alter != nil {
				test.alter(task, versions)
			}
			if test.param != nil {
				previous := test.param.GetValue()
				require.NoError(t, Params.Save(test.param.Key, test.value))
				t.Cleanup(func() { Params.Save(test.param.Key, previous) })
			}
			before := proto.Clone(task.StatsTask)
			retired, err := task.retireLegacyJSONStatsTask(context.Background(), 10, session.NewMockCluster(t))
			require.NoError(t, err)
			require.False(t, retired)
			require.True(t, proto.Equal(before, task.StatsTask))
			require.NotNil(t, task.meta.statsTaskMeta.GetStatsTask(30))
		})
	}
}

func TestJSONStatsV4LetsRunningV3TaskFinish(t *testing.T) {
	task, _ := newV4StatsTestTask(t)
	task.JsonStatsDataFormat = 0
	task.CanRecycle = true
	result := validV4StatsResult()
	result.JsonKeyStatsLogs[100].JsonKeyStatsDataFormat = common.JSONStatsDataFormatV3
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().QueryStats(int64(10), mock.Anything).Return(&workerpb.StatsResults{Results: []*workerpb.StatsResult{result}}, nil).Once()
	task.meta.catalog.(*catalogmocks.DataCoordCatalog).EXPECT().AlterSegments(mock.Anything, mock.MatchedBy(func(segments []*datapb.SegmentInfo) bool {
		return len(segments) == 1 && segments[0].GetJsonKeyStats()[100].GetJsonKeyStatsDataFormat() == common.JSONStatsDataFormatV3
	})).Return(nil).Once()
	task.QueryTaskOnWorker(cluster)
	require.Equal(t, indexpb.JobState_JobStateFinished, task.GetState())
	require.Equal(t, common.JSONStatsDataFormatV3, task.meta.GetHealthySegment(context.Background(), 20).GetJsonKeyStats()[100].GetJsonKeyStatsDataFormat())
}

func TestJSONStatsV4AutomaticallyReplacesLegacyTask(t *testing.T) {
	for _, test := range []struct {
		name       string
		format     int64
		servingV3  bool
		previousDN int64
	}{
		{name: "queued task without persisted format"},
		{name: "retry with existing V3 artifacts", format: common.JSONStatsDataFormatV3, servingV3: true, previousDN: 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			task, versions := newV4StatsTestTask(t)
			// After the cluster version gate has persisted the V4 target.
			require.Equal(t, common.JSONStatsDataFormatV4, Params.DataCoordCfg.JSONStatsFormatVersion.GetAsInt64())
			task.JsonStatsDataFormat = test.format
			task.State = indexpb.JobState_JobStateInit
			task.CanRecycle = true
			task.NodeID = test.previousDN
			encoded, err := proto.Marshal(task.StatsTask)
			require.NoError(t, err)
			persisted := &indexpb.StatsTask{}
			require.NoError(t, proto.Unmarshal(encoded, persisted))
			task.StatsTask = persisted // Coordinator restart preserves the old task contract.

			var statsFormats map[int64]int64
			if test.servingV3 {
				statsFormats = map[int64]int64{100: common.JSONStatsDataFormatV3}
			}
			segment := newJSONStatsMigrationTestSegment(1, 20, datapb.SegmentLevel_L1, statsFormats)
			task.meta.segments.SetSegment(20, segment)
			before := proto.Clone(segment.SegmentInfo)
			versions.Update(newJSONPathMigrationQueryNode(1, 6, 1))
			versions.UpdateDataNode(newJSONPathMigrationNode(10, 6))
			alloc := allocator.NewMockAllocator(t)
			scheduler := globalTask.NewMockGlobalScheduler(t)
			scheduler.EXPECT().GetPendingTaskCount(taskcommon.Stats, mock.Anything).Return(0)
			inspector := newStatsInspector(context.Background(), task.meta, scheduler, alloc, task.handler, nil, versions)
			t.Cleanup(inspector.cancel)
			checker := newMigrationCompactionChecker(task.meta, alloc, task.handler, versions)
			inspector.triggerJSONKeyIndexStatsTask()
			views, err := checker.Trigger(context.Background())
			require.NoError(t, err)
			require.Empty(t, views[TriggerTypeSingle], "the old task initially blocks migration")

			cluster := session.NewMockCluster(t)
			if test.previousDN != 0 {
				cluster.EXPECT().DropStats(test.previousDN, int64(30)).Return(nil).Once()
			}
			task.meta.catalog.(*catalogmocks.DataCoordCatalog).EXPECT().DropStatsTask(mock.Anything, int64(30)).Return(nil).Once()
			task.CreateTaskOnWorker(10, cluster) // No CreateStats RPC with the legacy task ID.
			require.Equal(t, indexpb.JobState_JobStateNone, task.GetState())
			require.Equal(t, common.JSONStatsDataFormatV3, task.getJSONStatsDataFormat())
			require.Nil(t, task.meta.statsTaskMeta.GetStatsTask(30))
			require.False(t, hasVersionedStatsTask(task.meta, 20))
			alloc.EXPECT().AllocID(mock.Anything).Return(int64(31), nil).Once()
			if !test.servingV3 {
				scheduler.EXPECT().Enqueue(mock.Anything).Once()
			}
			inspector.triggerJSONKeyIndexStatsTask()
			if test.servingV3 {
				views, err = checker.Trigger(context.Background())
				require.NoError(t, err)
				require.Equal(t, []int64{20}, viewSegmentIDs(views[TriggerTypeSingle]))
				require.False(t, hasVersionedStatsTask(task.meta, 20), "existing V3 is migrated by segment replacement")
			} else {
				replacement := task.meta.statsTaskMeta.GetStatsTaskBySegmentID(20, indexpb.StatsSubJob_JsonKeyIndexJob)
				require.NotNil(t, replacement)
				require.Equal(t, int64(31), replacement.GetTaskID())
				require.Equal(t, common.JSONStatsDataFormatV4, replacement.GetJsonStatsDataFormat())
				require.Equal(t, []int64{100}, replacement.GetJsonStatsFieldIds())
			}
			require.True(t, proto.Equal(before, task.meta.GetHealthySegment(context.Background(), 20).SegmentInfo),
				"retiring a task must not change the serving segment or its V3 artifacts")
		})
	}
}

func TestJSONStatsV4LegacyTaskRetirementRetriesCleanup(t *testing.T) {
	for _, failWorkerDrop := range []bool{true, false} {
		t.Run(map[bool]string{true: "worker drop failed", false: "catalog drop failed"}[failWorkerDrop], func(t *testing.T) {
			task, _ := newV4StatsTestTask(t)
			task.State = indexpb.JobState_JobStateInit
			task.CanRecycle = true
			task.JsonStatsDataFormat = 0
			cluster := session.NewMockCluster(t)
			catalog := task.meta.catalog.(*catalogmocks.DataCoordCatalog)
			failure := errors.New("temporary cleanup failure")
			if failWorkerDrop {
				cluster.EXPECT().DropStats(int64(10), int64(30)).Return(failure).Once()
			} else {
				cluster.EXPECT().DropStats(int64(10), int64(30)).Return(nil).Once()
				catalog.EXPECT().DropStatsTask(mock.Anything, int64(30)).Return(failure).Once()
			}
			task.CreateTaskOnWorker(10, cluster)
			require.Equal(t, indexpb.JobState_JobStateInit, task.GetState())
			require.Contains(t, task.GetFailReason(), failure.Error())
			require.True(t, hasVersionedStatsTask(task.meta, 20))
			require.Equal(t, common.JSONStatsDataFormatV3, task.getJSONStatsDataFormat())

			cluster.EXPECT().DropStats(int64(10), int64(30)).Return(nil).Once()
			catalog.EXPECT().DropStatsTask(mock.Anything, int64(30)).Return(nil).Once()
			task.CreateTaskOnWorker(10, cluster)
			require.Equal(t, indexpb.JobState_JobStateNone, task.GetState())
			require.False(t, hasVersionedStatsTask(task.meta, 20))
		})
	}
}
