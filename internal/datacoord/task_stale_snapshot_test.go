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
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globalTask "github.com/milvus-io/milvus/internal/datacoord/task"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Model the inspector retaining an active snapshot while the previous wrapper
// finishes (or enters Retry) and leaves the scheduler. No worker or catalog
// call is expected when the stale wrapper is subsequently dispatched/polled.
func TestTaskCallbacksRejectStaleSnapshot(t *testing.T) {
	factories := map[string]func(*testing.T, indexpb.JobState, indexpb.JobState) (globalTask.Task, func()){
		"index": func(t *testing.T, snapshotState, currentState indexpb.JobState) (globalTask.Task, func()) {
			catalog := catalogmocks.NewDataCoordCatalog(t)
			mt := &meta{indexMeta: createIndexMetaWithSegment(catalog, 1, 2, 3, 4, 5, 6)}
			current, _ := mt.indexMeta.GetIndexJob(6)
			snapshot := model.CloneSegmentIndex(current)
			snapshot.IndexState = commonpb.IndexState(snapshotState)
			current.IndexState = commonpb.IndexState(currentState)
			current.IndexFileKeys = []string{"published-index"}
			current.IndexVersion = 1
			current.NodeID = 10
			mt.indexMeta.segmentBuildInfo.Add(current)
			expected := model.CloneSegmentIndex(current)
			return newIndexBuildTask(snapshot, 1, mt, nil, nil, nil), func() {
				actual, exists := mt.indexMeta.GetIndexJob(6)
				require.True(t, exists)
				require.Equal(t, expected, actual)
			}
		},
		"stats": func(t *testing.T, snapshotState, currentState indexpb.JobState) (globalTask.Task, func()) {
			current := &indexpb.StatsTask{TaskID: 6, SegmentID: 3, State: snapshotState, NodeID: 10}
			snapshot := proto.Clone(current).(*indexpb.StatsTask)
			current.State = currentState
			mt := &meta{statsTaskMeta: &statsTaskMeta{
				catalog: catalogmocks.NewDataCoordCatalog(t),
				tasks:   typeutil.NewConcurrentMap[int64, *indexpb.StatsTask](),
			}}
			mt.statsTaskMeta.tasks.Insert(6, current)
			expected := proto.Clone(current)
			return newStatsTask(snapshot, 1, mt, nil, nil, nil), func() {
				require.True(t, proto.Equal(expected, mt.statsTaskMeta.GetStatsTask(6)))
			}
		},
		"external_refresh": func(t *testing.T, snapshotState, currentState indexpb.JobState) (globalTask.Task, func()) {
			current := &datapb.ExternalCollectionRefreshTask{TaskId: 6, JobId: 7, State: snapshotState, NodeId: 10}
			snapshot := proto.Clone(current).(*datapb.ExternalCollectionRefreshTask)
			current.State = currentState
			current.ResultPath = "published-result"
			mt := &externalCollectionRefreshMeta{
				catalog: catalogmocks.NewDataCoordCatalog(t),
				jobs:    typeutil.NewConcurrentMap[int64, *datapb.ExternalCollectionRefreshJob](),
				tasks:   typeutil.NewConcurrentMap[int64, *datapb.ExternalCollectionRefreshTask](),
			}
			mt.jobs.Insert(7, &datapb.ExternalCollectionRefreshJob{JobId: 7, State: indexpb.JobState_JobStateInProgress})
			mt.tasks.Insert(6, current)
			expected := proto.Clone(current)
			return newRefreshExternalCollectionTask(snapshot, mt, nil, nil), func() {
				require.True(t, proto.Equal(expected, mt.GetTask(6)))
			}
		},

		"analyze": func(t *testing.T, snapshotState, currentState indexpb.JobState) (globalTask.Task, func()) {
			current := &indexpb.AnalyzeTask{TaskID: 6, State: snapshotState, NodeID: 10}
			snapshot := proto.Clone(current).(*indexpb.AnalyzeTask)
			current.State = currentState
			current.CentroidsFile = "published-centroids"
			mt := &meta{analyzeMeta: &analyzeMeta{
				catalog: catalogmocks.NewDataCoordCatalog(t),
				tasks:   map[int64]*indexpb.AnalyzeTask{6: current},
			}}
			expected := proto.Clone(current)
			return newAnalyzeTask(snapshot, mt, nil), func() {
				require.True(t, proto.Equal(expected, mt.analyzeMeta.GetTask(6)))
			}
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			for _, snapshotState := range []indexpb.JobState{indexpb.JobState_JobStateInit, indexpb.JobState_JobStateInProgress} {
				t.Run(snapshotState.String(), func(t *testing.T) {
					for _, currentState := range []indexpb.JobState{
						indexpb.JobState_JobStateInit, indexpb.JobState_JobStateInProgress,
						indexpb.JobState_JobStateFinished, indexpb.JobState_JobStateFailed, indexpb.JobState_JobStateRetry,
					} {
						if currentState == snapshotState {
							continue
						}
						t.Run(currentState.String(), func(t *testing.T) {
							task, checkMeta := factory(t, snapshotState, currentState)
							cluster := session.NewMockCluster(t)
							if snapshotState == indexpb.JobState_JobStateInit {
								task.CreateTaskOnWorker(20, cluster)
							} else {
								task.QueryTaskOnWorker(cluster)
							}
							require.Equal(t, taskcommon.None, task.GetTaskState())
							checkMeta()
						})
					}
				})
			}
		})
	}
}

func TestRefreshExternalCollectionTask_TimeoutCancelsWorker(t *testing.T) {
	paramtable.Init()
	for _, test := range []struct {
		name    string
		dropErr error
	}{
		{name: "drop succeeds"},
		{name: "drop fails", dropErr: context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			refreshMeta, err := newExternalCollectionRefreshMeta(ctx, &stubCatalog{})
			require.NoError(t, err)
			timeout := Params.DataCoordCfg.ExternalCollectionJobTimeout.GetAsDuration(time.Second)
			job := &datapb.ExternalCollectionRefreshJob{
				JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress,
				StartTime: time.Now().Add(-timeout - time.Minute).UnixMilli(), TaskIds: []int64{1001},
			}
			require.NoError(t, refreshMeta.AddJob(job))
			taskProto := &datapb.ExternalCollectionRefreshTask{
				TaskId: 1001, JobId: 1, CollectionId: 100, NodeId: 7,
				State: indexpb.JobState_JobStateInProgress,
			}
			require.NoError(t, refreshMeta.AddTask(taskProto))
			wrapper := newRefreshExternalCollectionTask(proto.Clone(taskProto).(*datapb.ExternalCollectionRefreshTask), refreshMeta, nil, nil)
			checker := newRefreshChecker(ctx, nil, refreshMeta, make(chan struct{}), nil, nil, nil, nil, nil)

			// The real timeout path fails both metadata records, leaving the
			// scheduler wrapper InProgress until its next poll.
			checker.tryTimeoutJob(refreshMeta.GetJob(1))
			require.Equal(t, indexpb.JobState_JobStateFailed, refreshMeta.GetJob(1).GetState())
			require.Equal(t, indexpb.JobState_JobStateFailed, refreshMeta.GetTask(1001).GetState())
			require.Equal(t, taskcommon.InProgress, wrapper.GetTaskState())
			expectedJob := proto.Clone(refreshMeta.GetJob(1))
			expectedTask := proto.Clone(refreshMeta.GetTask(1001))
			cluster := &stubCluster{dropErr: test.dropErr}
			wrapper.QueryTaskOnWorker(cluster)

			require.Equal(t, 1, cluster.dropCalls)
			require.Equal(t, int64(7), cluster.droppedNodeID)
			require.Equal(t, int64(1001), cluster.droppedTaskID)
			require.Equal(t, taskcommon.None, wrapper.GetTaskState())
			// Even when Drop fails, GC must retain the terminal records and
			// worker assignment so it can retry cleanup later.
			require.True(t, proto.Equal(expectedJob, refreshMeta.GetJob(1)))
			require.True(t, proto.Equal(expectedTask, refreshMeta.GetTask(1001)))
		})
	}
}

func TestRefreshExternalCollectionTask_StaleSnapshotCancelsTerminalJobWorker(t *testing.T) {
	for _, test := range []struct {
		name        string
		jobState    indexpb.JobState
		taskRemoved bool
	}{
		{name: "finished job", jobState: indexpb.JobState_JobStateFinished},
		{name: "failed job", jobState: indexpb.JobState_JobStateFailed},
		{name: "missing job"},
		{name: "missing job and task", taskRemoved: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			var jobs []*datapb.ExternalCollectionRefreshJob
			if test.jobState != indexpb.JobState_JobStateNone {
				jobs = append(jobs, &datapb.ExternalCollectionRefreshJob{
					JobId: 1, CollectionId: 100, State: test.jobState,
				})
			}
			finished := &datapb.ExternalCollectionRefreshTask{
				TaskId: 1001, JobId: 1, CollectionId: 100, NodeId: 7,
				State: indexpb.JobState_JobStateFinished, ResultPath: "published-result",
			}
			var tasks []*datapb.ExternalCollectionRefreshTask
			if !test.taskRemoved {
				tasks = append(tasks, finished)
			}
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().ListExternalCollectionRefreshJobs(ctx).Return(jobs, nil).Once()
			catalog.EXPECT().ListExternalCollectionRefreshTasks(ctx).Return(tasks, nil).Once()
			refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
			require.NoError(t, err)
			snapshot := proto.Clone(finished).(*datapb.ExternalCollectionRefreshTask)
			snapshot.State = indexpb.JobState_JobStateInProgress
			wrapper := newRefreshExternalCollectionTask(snapshot, refreshMeta, nil, nil)
			cluster := session.NewMockCluster(t)
			cluster.EXPECT().DropRefreshExternalCollectionTask(int64(7), int64(1001)).Return(nil).Once()
			// No worker query or catalog write is allowed: cancellation must
			// preserve a Finished task's state and result instead of rewriting it.
			expected := proto.Clone(finished)
			wrapper.QueryTaskOnWorker(cluster)

			require.Equal(t, taskcommon.None, wrapper.GetTaskState())
			if test.taskRemoved {
				require.Nil(t, refreshMeta.GetTask(1001))
			} else {
				require.True(t, proto.Equal(expected, refreshMeta.GetTask(1001)))
			}
		})
	}
}
