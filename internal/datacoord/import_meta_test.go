// Licensed to the LF AI & Data foundation under one
// or more contributor license agreementassert. See the NOTICE file
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
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

func TestImportMeta_Restore(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return([]*datapb.ImportJob{{JobID: 0}}, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return([]*datapb.PreImportTask{{TaskID: 1, TaskVersion: 4}}, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return([]*datapb.ImportTaskV2{{TaskID: 2, TaskVersion: 7}}, nil)
	ctx := context.TODO()

	im, err := NewImportMeta(ctx, catalog, nil, nil)
	assert.NoError(t, err)

	jobs := im.GetJobBy(ctx)
	assert.Equal(t, 1, len(jobs))
	assert.Equal(t, int64(0), jobs[0].GetJobID())
	tasks := im.GetTaskBy(ctx)
	assert.Equal(t, 2, len(tasks))
	tasks = im.GetTaskBy(ctx, WithType(PreImportTaskType))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, int64(1), tasks[0].GetTaskID())
	assert.Equal(t, int64(4), tasks[0].GetTaskVersion(), "pre-import attempts must survive coordinator restart")
	tasks = im.GetTaskBy(ctx, WithType(ImportTaskType))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, int64(2), tasks[0].GetTaskID())
	tasks = im.GetTaskByJob(ctx, 0)
	assert.Equal(t, 2, len(tasks))
	tasks = im.GetTaskBy(ctx, WithType(ImportTaskType))
	assert.Equal(t, int64(7), tasks[0].GetTaskVersion(), "import lineage attempts must survive coordinator restart")
	assert.Equal(t, ctx, tasks[0].(*importTask).ctx,
		"restored tasks need the component context for crash-recovery retries")

	// new meta failed
	mockErr := errors.New("mock error")
	catalog = mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return([]*datapb.PreImportTask{{TaskID: 1}}, mockErr)
	_, err = NewImportMeta(ctx, catalog, nil, nil)
	assert.Error(t, err)

	catalog = mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return([]*datapb.ImportTaskV2{{TaskID: 2}}, mockErr)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return([]*datapb.PreImportTask{{TaskID: 1}}, nil)
	_, err = NewImportMeta(ctx, catalog, nil, nil)
	assert.Error(t, err)

	catalog = mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return([]*datapb.ImportJob{{JobID: 0}}, mockErr)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return([]*datapb.PreImportTask{{TaskID: 1}}, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return([]*datapb.ImportTaskV2{{TaskID: 2}}, nil)
	_, err = NewImportMeta(ctx, catalog, nil, nil)
	assert.Error(t, err)
}

func TestImportMeta_ReplaceRetryTask(t *testing.T) {
	oldIDs := []int64{10, 20}
	newSegments := []*SegmentInfo{
		NewSegmentInfo(&datapb.SegmentInfo{ID: 30, CollectionID: 100, State: commonpb.SegmentState_Importing, IsImporting: true}),
		NewSegmentInfo(&datapb.SegmentInfo{ID: 40, CollectionID: 100, State: commonpb.SegmentState_Importing, IsImporting: true}),
	}

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().Update(mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, actions ...metastore.UpdateAction) {
			assert.Len(t, actions, 6)
			drop, ok := actions[0].Entry.(metastore.ImportTaskEntry)
			assert.True(t, ok)
			assert.Equal(t, int64(1), drop.TaskID)
			_, ok = actions[len(actions)-1].Entry.(metastore.ImportTaskEntry)
			assert.True(t, ok, "the replacement task is part of the same catalog update")
		}).Return(nil).Once()

	im := &importMeta{jobs: make(map[int64]ImportJob), tasks: newImportTasks(), catalog: catalog}
	im.jobs[7] = &importJob{ImportJob: &datapb.ImportJob{JobID: 7, CollectionID: 100, State: internalpb.ImportJobState_Importing}}
	oldTask := &importTask{importMeta: im}
	oldTask.task.Store(&datapb.ImportTaskV2{
		JobID: 7, TaskID: 1, CollectionID: 100, SegmentIDs: oldIDs,
		NodeID: 9, State: datapb.ImportTaskStateV2_InProgress,
	})
	im.tasks.add(oldTask)
	replacement := oldTask.Clone().(*importTask)
	replacement.task.Load().TaskID = 2
	replacement.task.Load().NodeID = NullNodeID
	replacement.task.Load().State = datapb.ImportTaskStateV2_Pending

	segmentMeta := &meta{catalog: catalog, segments: NewSegmentsInfo()}
	for _, segmentID := range oldIDs {
		segmentMeta.segments.SetSegment(segmentID, NewSegmentInfo(&datapb.SegmentInfo{
			ID: segmentID, CollectionID: 100, State: commonpb.SegmentState_Flushed, IsImporting: true,
		}))
	}

	replaced, err := im.replaceRetryTask(context.Background(), segmentMeta, oldTask, replacement, newSegments)
	assert.NoError(t, err)
	assert.False(t, replaced, "a stale inspector snapshot cannot replace a non-Retry task")
	oldTask.task.Load().State = datapb.ImportTaskStateV2_Retry
	im.jobs[7].(*importJob).State = internalpb.ImportJobState_Failed

	replaced, err = im.replaceRetryTask(context.Background(), segmentMeta, oldTask, replacement, newSegments)
	assert.NoError(t, err)
	assert.False(t, replaced, "a retry snapshot cannot replace a task after its job failed")
	assert.Same(t, oldTask, im.GetTask(context.Background(), oldTask.GetTaskID()))
	assert.Nil(t, im.GetTask(context.Background(), replacement.GetTaskID()))
	im.jobs[7].(*importJob).State = internalpb.ImportJobState_Importing

	replaced, err = im.replaceRetryTask(context.Background(), segmentMeta, oldTask, replacement, newSegments)
	assert.NoError(t, err)
	assert.True(t, replaced)
	assert.Nil(t, im.GetTask(context.Background(), oldTask.GetTaskID()))
	assert.Same(t, replacement, im.GetTask(context.Background(), replacement.GetTaskID()))
	assert.Equal(t, []int64{30, 40}, replacement.GetSegmentIDs())
	assert.Equal(t, datapb.ImportTaskStateV2_Failed, oldTask.GetState())
	for _, segmentID := range oldIDs {
		segment := segmentMeta.GetSegment(context.Background(), segmentID)
		assert.Equal(t, commonpb.SegmentState_Dropped, segment.GetState())
		assert.False(t, segment.GetIsImporting())
	}
	for _, segment := range newSegments {
		assert.Same(t, segment, segmentMeta.GetSegment(context.Background(), segment.GetID()))
	}
}

func TestImportMeta_ReplacePreImportRetryTask(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, actions ...metastore.UpdateAction) {
			assert.Len(t, actions, 2)
			drop, ok := actions[0].Entry.(metastore.PreImportTaskEntry)
			assert.True(t, ok)
			assert.Equal(t, int64(1), drop.TaskID)
			save, ok := actions[1].Entry.(metastore.PreImportTaskEntry)
			assert.True(t, ok)
			assert.Equal(t, int64(2), save.Task.GetTaskID())
		}).Return(nil).Once()

	im := &importMeta{jobs: make(map[int64]ImportJob), tasks: newImportTasks(), catalog: catalog}
	im.jobs[7] = &importJob{ImportJob: &datapb.ImportJob{
		JobID: 7, CollectionID: 100, State: internalpb.ImportJobState_PreImporting,
	}}
	oldTask := &preImportTask{importMeta: im}
	oldTask.task.Store(&datapb.PreImportTask{
		JobID: 7, TaskID: 1, CollectionID: 100,
		NodeID: 9, State: datapb.ImportTaskStateV2_InProgress,
	})
	im.tasks.add(oldTask)
	replacement := oldTask.Clone().(*preImportTask)
	replacement.task.Load().TaskID = 2
	replacement.task.Load().NodeID = NullNodeID
	replacement.task.Load().State = datapb.ImportTaskStateV2_Pending

	replaced, err := im.replacePreImportRetryTask(context.Background(), oldTask, replacement)
	assert.NoError(t, err)
	assert.False(t, replaced, "a stale inspector snapshot cannot replace a non-Retry task")

	oldTask.task.Load().State = datapb.ImportTaskStateV2_Retry
	im.jobs[7].(*importJob).State = internalpb.ImportJobState_Failed
	replaced, err = im.replacePreImportRetryTask(context.Background(), oldTask, replacement)
	assert.NoError(t, err)
	assert.False(t, replaced, "a retry snapshot cannot replace a task after its job failed")

	im.jobs[7].(*importJob).State = internalpb.ImportJobState_PreImporting
	replaced, err = im.replacePreImportRetryTask(context.Background(), oldTask, replacement)
	assert.NoError(t, err)
	assert.True(t, replaced)
	assert.Nil(t, im.GetTask(context.Background(), oldTask.GetTaskID()))
	assert.Same(t, replacement, im.GetTask(context.Background(), replacement.GetTaskID()))
	assert.Equal(t, datapb.ImportTaskStateV2_Retry, oldTask.GetState())
}

func TestImportMeta_ReplacePreImportRetryTaskCatalogFailureKeepsOldTask(t *testing.T) {
	mockErr := errors.New("mock catalog failure")
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(mockErr).Once()
	im := &importMeta{jobs: make(map[int64]ImportJob), tasks: newImportTasks(), catalog: catalog}
	im.jobs[7] = &importJob{ImportJob: &datapb.ImportJob{
		JobID: 7, CollectionID: 100, State: internalpb.ImportJobState_PreImporting,
	}}
	oldTask := &preImportTask{importMeta: im}
	oldTask.task.Store(&datapb.PreImportTask{
		JobID: 7, TaskID: 1, CollectionID: 100,
		NodeID: 9, State: datapb.ImportTaskStateV2_Retry,
	})
	im.tasks.add(oldTask)
	replacement := oldTask.Clone().(*preImportTask)
	replacement.task.Load().TaskID = 2
	replacement.task.Load().NodeID = NullNodeID
	replacement.task.Load().State = datapb.ImportTaskStateV2_Pending

	replaced, err := im.replacePreImportRetryTask(context.Background(), oldTask, replacement)
	assert.ErrorIs(t, err, mockErr)
	assert.False(t, replaced)
	assert.Same(t, oldTask, im.GetTask(context.Background(), oldTask.GetTaskID()))
	assert.Nil(t, im.GetTask(context.Background(), replacement.GetTaskID()))
}

func TestImportMeta_AddImportTasks(t *testing.T) {
	task := &importTask{}
	task.task.Store(&datapb.ImportTaskV2{
		JobID: 1, TaskID: 2, CollectionID: 3, SegmentIDs: []int64{10, 20},
		State: datapb.ImportTaskStateV2_Pending,
	})
	segments := []*SegmentInfo{
		NewSegmentInfo(&datapb.SegmentInfo{ID: 10, CollectionID: 3, State: commonpb.SegmentState_Importing, IsImporting: true}),
		NewSegmentInfo(&datapb.SegmentInfo{ID: 20, CollectionID: 3, State: commonpb.SegmentState_Importing, IsImporting: true}),
	}

	t.Run("publish catalog before memory", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Run(func(_ context.Context, actions ...metastore.UpdateAction) {
				assert.Len(t, actions, 3)
				_, ok := actions[0].Entry.(metastore.SegmentEntry)
				assert.True(t, ok)
				_, ok = actions[1].Entry.(metastore.SegmentEntry)
				assert.True(t, ok)
				_, ok = actions[2].Entry.(metastore.ImportTaskEntry)
				assert.True(t, ok)
			}).
			Return(nil).
			Once()

		im := &importMeta{tasks: newImportTasks(), jobs: make(map[int64]ImportJob), catalog: catalog}
		segmentMeta := &meta{catalog: catalog, segments: NewSegmentsInfo()}
		assert.NoError(t, im.addImportTasks(context.Background(), segmentMeta, []ImportTask{task}, segments))
		assert.Same(t, task, im.GetTask(context.Background(), task.GetTaskID()))
		for _, segment := range segments {
			assert.Same(t, segment, segmentMeta.GetSegment(context.Background(), segment.GetID()))
		}
	})

	t.Run("catalog failure leaves memory unchanged", func(t *testing.T) {
		catalog := mocks.NewDataCoordCatalog(t)
		catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("catalog update failed")).
			Once()

		im := &importMeta{tasks: newImportTasks(), jobs: make(map[int64]ImportJob), catalog: catalog}
		segmentMeta := &meta{catalog: catalog, segments: NewSegmentsInfo()}
		assert.Error(t, im.addImportTasks(context.Background(), segmentMeta, []ImportTask{task}, segments))
		assert.Nil(t, im.GetTask(context.Background(), task.GetTaskID()))
		for _, segment := range segments {
			assert.Nil(t, segmentMeta.GetSegment(context.Background(), segment.GetID()))
		}
	})
}

func TestImportMeta_CompositeUpdateUsesCommitLockOrder(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	im := &importMeta{tasks: newImportTasks(), jobs: make(map[int64]ImportJob), catalog: catalog}
	segmentMeta := &meta{catalog: catalog, segments: NewSegmentsInfo()}
	task := &importTask{}
	task.task.Store(&datapb.ImportTaskV2{
		JobID: 1, TaskID: 2, CollectionID: 3, SegmentIDs: []int64{10},
		State: datapb.ImportTaskStateV2_Pending,
	})
	segment := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 10, CollectionID: 3, State: commonpb.SegmentState_Importing, IsImporting: true,
	})

	// Hold the import lock just as HandleCommitVchannel does before running its
	// segment callback. A composite update must wait here without holding segMu;
	// otherwise the callback and update can deadlock in opposite lock order.
	im.mu.Lock()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- im.addImportTasks(context.Background(), segmentMeta, []ImportTask{task}, []*SegmentInfo{segment})
	}()
	<-started

	inverted := false
	for i := 0; i < 1000; i++ {
		if !segmentMeta.segMu.TryLock() {
			inverted = true
			break
		}
		segmentMeta.segMu.Unlock()
		runtime.Gosched()
	}
	im.mu.Unlock()

	assert.False(t, inverted, "composite update must not hold segment meta while waiting for import meta")
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("composite update remained blocked after import meta was released")
	}
}

func TestImportMeta_Job(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportJob(mock.Anything, mock.Anything).Return(nil)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	jobIDs := []int64{1000, 2000, 3000}

	for i, jobID := range jobIDs {
		channel := fmt.Sprintf("ch-%d", rand.Int63())
		var job ImportJob = &importJob{
			ImportJob: &datapb.ImportJob{
				JobID:          jobID,
				CollectionID:   rand.Int63(),
				PartitionIDs:   []int64{rand.Int63()},
				Vchannels:      []string{channel},
				ReadyVchannels: []string{channel},
				State:          internalpb.ImportJobState_Pending,
			},
		}
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)
		ret := im.GetJob(context.TODO(), jobID)
		assert.Equal(t, job, ret)
		jobs := im.GetJobBy(context.TODO())
		assert.Equal(t, i+1, len(jobs))

		// Add again, test idempotency
		err = im.AddJob(context.TODO(), job)
		assert.NoError(t, err)
		ret = im.GetJob(context.TODO(), jobID)
		assert.EqualValues(t, job, ret)
		jobs = im.GetJobBy(context.TODO())
		assert.Equal(t, i+1, len(jobs))
	}

	jobs := im.GetJobBy(context.TODO())
	assert.Equal(t, 3, len(jobs))

	err = im.UpdateJob(context.TODO(), jobIDs[0], UpdateJobState(internalpb.ImportJobState_Completed))
	assert.NoError(t, err)
	job0 := im.GetJob(context.TODO(), jobIDs[0])
	assert.NotNil(t, job0)
	assert.Equal(t, internalpb.ImportJobState_Completed, job0.GetState())

	err = im.UpdateJob(context.TODO(), jobIDs[1], UpdateJobState(internalpb.ImportJobState_Importing))
	assert.NoError(t, err)
	job1 := im.GetJob(context.TODO(), jobIDs[1])
	assert.NotNil(t, job1)
	assert.Equal(t, internalpb.ImportJobState_Importing, job1.GetState())

	jobs = im.GetJobBy(context.TODO(), WithJobStates(internalpb.ImportJobState_Pending))
	assert.Equal(t, 1, len(jobs))
	jobs = im.GetJobBy(context.TODO(), WithoutJobStates(internalpb.ImportJobState_Pending))
	assert.Equal(t, 2, len(jobs))
	count := im.CountJobBy(context.TODO())
	assert.Equal(t, 3, count)
	count = im.CountJobBy(context.TODO(), WithJobStates(internalpb.ImportJobState_Pending))
	assert.Equal(t, 1, count)
	count = im.CountJobBy(context.TODO(), WithoutJobStates(internalpb.ImportJobState_Pending))
	assert.Equal(t, 2, count)

	err = im.RemoveJob(context.TODO(), jobIDs[0])
	assert.NoError(t, err)
	jobs = im.GetJobBy(context.TODO())
	assert.Equal(t, 2, len(jobs))
	count = im.CountJobBy(context.TODO())
	assert.Equal(t, 2, count)
}

func TestImportMeta_TerminalJobCatalogFailureFailsStopUnderLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	catalogErr := errors.New("ambiguous catalog response")
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.MatchedBy(func(job *datapb.ImportJob) bool {
		return job.GetState() == internalpb.ImportJobState_Completed
	})).Return(catalogErr).Once()

	const jobID = int64(1000)
	im := &importMeta{
		ctx:     ctx,
		jobs:    map[int64]ImportJob{jobID: &importJob{ImportJob: &datapb.ImportJob{JobID: jobID, State: internalpb.ImportJobState_PreImporting}}},
		tasks:   newImportTasks(),
		catalog: catalog,
	}

	fatalCalled := false
	lockHeld := false
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(context.Context, string, ...mlog.Field) {
			fatalCalled = true
			lockHeld = !im.mu.TryLock()
			if !lockHeld {
				im.mu.Unlock()
			}
		}).
		Build()
	defer mockFatal.UnPatch()

	err := im.UpdateJob(context.Background(), jobID, UpdateJobState(internalpb.ImportJobState_Completed))
	assert.ErrorIs(t, err, catalogErr)
	assert.True(t, fatalCalled)
	assert.True(t, lockHeld)
	assert.Equal(t, internalpb.ImportJobState_PreImporting, im.GetJob(context.Background(), jobID).GetState())
}

func TestImportMeta_CompletedTaskCatalogFailureFailsStopUnderLock(t *testing.T) {
	tests := []struct {
		name     string
		taskType TaskType
	}{
		{name: "preimport", taskType: PreImportTaskType},
		{name: "import", taskType: ImportTaskType},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			catalogErr := errors.New("ambiguous catalog response")
			catalog := mocks.NewDataCoordCatalog(t)
			const taskID = int64(1001)
			var task ImportTask
			switch test.taskType {
			case PreImportTaskType:
				catalog.EXPECT().SavePreImportTask(mock.Anything, mock.MatchedBy(func(task *datapb.PreImportTask) bool {
					return task.GetTaskID() == taskID && task.GetState() == datapb.ImportTaskStateV2_Completed
				})).Return(catalogErr).Once()
				preImport := &preImportTask{}
				preImport.task.Store(&datapb.PreImportTask{TaskID: taskID, State: datapb.ImportTaskStateV2_InProgress})
				task = preImport
			case ImportTaskType:
				catalog.EXPECT().SaveImportTask(mock.Anything, mock.MatchedBy(func(task *datapb.ImportTaskV2) bool {
					return task.GetTaskID() == taskID && task.GetState() == datapb.ImportTaskStateV2_Completed
				})).Return(catalogErr).Once()
				importTask := &importTask{}
				importTask.task.Store(&datapb.ImportTaskV2{TaskID: taskID, State: datapb.ImportTaskStateV2_InProgress})
				task = importTask
			}

			im := &importMeta{
				ctx:     ctx,
				jobs:    make(map[int64]ImportJob),
				tasks:   newImportTasks(),
				catalog: catalog,
			}
			im.tasks.add(task)

			fatalCalled := false
			lockHeld := false
			mockFatal := mockey.Mock(mlog.Fatal).
				To(func(context.Context, string, ...mlog.Field) {
					fatalCalled = true
					lockHeld = !im.mu.TryLock()
					if !lockHeld {
						im.mu.Unlock()
					}
				}).
				Build()
			defer mockFatal.UnPatch()

			operationCtx, cancelOperation := context.WithCancel(context.Background())
			cancelOperation()
			err := im.UpdateTask(operationCtx, taskID, UpdateState(datapb.ImportTaskStateV2_Completed))
			assert.ErrorIs(t, err, catalogErr)
			assert.True(t, fatalCalled)
			assert.True(t, lockHeld)
			assert.Equal(t, datapb.ImportTaskStateV2_InProgress, im.GetTask(context.Background(), taskID).GetState())
		})
	}
}

func TestImportMeta_TaskCatalogFailureDoesNotFailStopOutsideLiveCompletion(t *testing.T) {
	tests := []struct {
		name          string
		componentLive bool
		oldState      datapb.ImportTaskStateV2
		newState      datapb.ImportTaskStateV2
	}{
		{name: "retry write", componentLive: true, oldState: datapb.ImportTaskStateV2_InProgress, newState: datapb.ImportTaskStateV2_Retry},
		{name: "failed write", componentLive: true, oldState: datapb.ImportTaskStateV2_InProgress, newState: datapb.ImportTaskStateV2_Failed},
		{name: "completion during shutdown", componentLive: false, oldState: datapb.ImportTaskStateV2_InProgress, newState: datapb.ImportTaskStateV2_Completed},
		{name: "idempotent completion", componentLive: true, oldState: datapb.ImportTaskStateV2_Completed, newState: datapb.ImportTaskStateV2_Completed},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if !test.componentLive {
				cancel()
			}

			catalogErr := errors.New("catalog error")
			catalog := mocks.NewDataCoordCatalog(t)
			catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(catalogErr).Once()
			const taskID = int64(1002)
			task := &importTask{}
			task.task.Store(&datapb.ImportTaskV2{TaskID: taskID, State: test.oldState})
			im := &importMeta{
				ctx:     ctx,
				jobs:    make(map[int64]ImportJob),
				tasks:   newImportTasks(),
				catalog: catalog,
			}
			im.tasks.add(task)

			fatalCalled := false
			mockFatal := mockey.Mock(mlog.Fatal).
				To(func(context.Context, string, ...mlog.Field) {
					fatalCalled = true
				}).
				Build()
			defer mockFatal.UnPatch()

			err := im.UpdateTask(context.Background(), taskID, UpdateState(test.newState))
			assert.ErrorIs(t, err, catalogErr)
			assert.False(t, fatalCalled)
			assert.Equal(t, test.oldState, im.GetTask(context.Background(), taskID).GetState())
		})
	}
}

func TestImportMetaAddJob(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:          10000,
			CollectionID:   rand.Int63(),
			PartitionIDs:   []int64{rand.Int63()},
			Vchannels:      []string{"ch-1", "ch-2"},
			ReadyVchannels: []string{"ch-1"},
			State:          internalpb.ImportJobState_Pending,
		},
	}
	err = im.AddJob(context.TODO(), job)
	assert.NoError(t, err)

	job = &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:          10000,
			CollectionID:   rand.Int63(),
			PartitionIDs:   []int64{rand.Int63()},
			Vchannels:      []string{"ch-1", "ch-2"},
			ReadyVchannels: []string{"ch-2"},
			State:          internalpb.ImportJobState_Pending,
		},
	}
	err = im.AddJob(context.TODO(), job)
	assert.NoError(t, err)

	job = im.GetJob(context.TODO(), 10000)
	assert.NotNil(t, job)
	assert.Equal(t, []string{"ch-1", "ch-2"}, job.GetVchannels())
	assert.Equal(t, []string{"ch-1", "ch-2"}, job.GetReadyVchannels())
}

func TestImportMeta_ImportTask(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(nil)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	taskProto := &datapb.ImportTaskV2{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		SegmentIDs:   []int64{5, 6},
		NodeID:       7,
		State:        datapb.ImportTaskStateV2_Pending,
	}
	task1 := &importTask{}
	task1.task.Store(taskProto)
	err = im.AddTask(context.TODO(), task1)
	assert.NoError(t, err)
	err = im.AddTask(context.TODO(), task1)
	assert.NoError(t, err)
	res := im.GetTask(context.TODO(), task1.GetTaskID())
	assert.Equal(t, task1, res)

	task2 := task1.Clone()
	task2.(*importTask).task.Load().TaskID = 8
	task2.(*importTask).task.Load().State = datapb.ImportTaskStateV2_Completed
	err = im.AddTask(context.TODO(), task2)
	assert.NoError(t, err)

	tasks := im.GetTaskByJob(context.TODO(), task1.GetJobID())
	assert.Equal(t, 2, len(tasks))
	tasks = im.GetTaskByJob(context.TODO(), task1.GetJobID(), WithStates(datapb.ImportTaskStateV2_Completed))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, task2.GetTaskID(), tasks[0].GetTaskID())
	assert.Empty(t, im.GetTaskByJob(context.TODO(), 100))
	tasks = im.GetTaskBy(context.TODO(), WithType(ImportTaskType), WithStates(datapb.ImportTaskStateV2_Completed))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, task2.GetTaskID(), tasks[0].GetTaskID())

	err = im.UpdateTask(context.TODO(), task1.GetTaskID(), UpdateNodeID(9),
		UpdateState(datapb.ImportTaskStateV2_InProgress),
		UpdateFileStats([]*datapb.ImportFileStats{1: {
			FileSize: 100,
		}}))
	assert.NoError(t, err)
	task := im.GetTask(context.TODO(), task1.GetTaskID())
	assert.Equal(t, int64(9), task.GetNodeID())
	assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
	assert.Equal(t, int64(9), task1.GetNodeID())
	assert.Equal(t, datapb.ImportTaskStateV2_InProgress, task1.GetState())

	err = im.UpdateTask(context.TODO(), task1.GetTaskID(), UpdateNodeID(10),
		UpdateState(datapb.ImportTaskStateV2_Completed))
	assert.NoError(t, err)
	assert.Equal(t, int64(10), task1.GetNodeID())
	assert.Equal(t, datapb.ImportTaskStateV2_Completed, task1.GetState())

	err = im.RemoveTask(context.TODO(), task1.GetTaskID())
	assert.NoError(t, err)
	tasks = im.GetTaskBy(context.TODO())
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, 1, len(im.GetTaskByJob(context.TODO(), task1.GetJobID())))
	err = im.RemoveTask(context.TODO(), 10)
	assert.NoError(t, err)
	tasks = im.GetTaskBy(context.TODO())
	assert.Equal(t, 1, len(tasks))
}

func TestImportTasksByJobIndex(t *testing.T) {
	tasks := newImportTasks()
	newTask := func(jobID, taskID int64) ImportTask {
		task := &importTask{}
		task.task.Store(&datapb.ImportTaskV2{JobID: jobID, TaskID: taskID})
		return task
	}

	tasks.add(newTask(10, 1))
	tasks.add(newTask(10, 2))
	tasks.add(newTask(20, 3))
	assert.ElementsMatch(t, []int64{1, 2}, lo.Map(tasks.listTasksByJob(10), func(task ImportTask, _ int) int64 {
		return task.GetTaskID()
	}))
	assert.ElementsMatch(t, []int64{3}, lo.Map(tasks.listTasksByJob(20), func(task ImportTask, _ int) int64 {
		return task.GetTaskID()
	}))

	// Re-adding an existing task ID under a different job keeps both indexes consistent.
	tasks.add(newTask(20, 1))
	assert.ElementsMatch(t, []int64{2}, lo.Map(tasks.listTasksByJob(10), func(task ImportTask, _ int) int64 {
		return task.GetTaskID()
	}))
	assert.ElementsMatch(t, []int64{1, 3}, lo.Map(tasks.listTasksByJob(20), func(task ImportTask, _ int) int64 {
		return task.GetTaskID()
	}))

	tasks.remove(1)
	assert.ElementsMatch(t, []int64{3}, lo.Map(tasks.listTasksByJob(20), func(task ImportTask, _ int) int64 {
		return task.GetTaskID()
	}))
	tasks.remove(3)
	assert.Empty(t, tasks.listTasksByJob(20))
	assert.NotContains(t, tasks.taskIDsByJobID, int64(20))

	// Moving the only task of a job removes the old empty index bucket.
	tasks.add(newTask(30, 4))
	tasks.add(newTask(40, 4))
	assert.Empty(t, tasks.listTasksByJob(30))
	assert.NotContains(t, tasks.taskIDsByJobID, int64(30))
	assert.ElementsMatch(t, []int64{4}, lo.Map(tasks.listTasksByJob(40), func(task ImportTask, _ int) int64 {
		return task.GetTaskID()
	}))
}

func BenchmarkImportTaskLookupByJob(b *testing.B) {
	for _, jobCount := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("jobs_%d", jobCount), func(b *testing.B) {
			tasks := newImportTasks()
			for jobID := range jobCount {
				for taskOffset := range 2 {
					task := &importTask{}
					task.task.Store(&datapb.ImportTaskV2{
						JobID:  int64(jobID),
						TaskID: int64(jobID*2 + taskOffset),
					})
					tasks.add(task)
				}
			}
			targetJobID := int64(jobCount - 1)

			b.Run("full_scan", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					result := filterImportTasks(tasks.listTasks(), func(task ImportTask) bool {
						return task.GetJobID() == targetJobID
					})
					if len(result) != 2 {
						b.Fatalf("expected 2 tasks, got %d", len(result))
					}
				}
			})
			b.Run("job_index", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					result := filterImportTasks(tasks.listTasksByJob(targetJobID))
					if len(result) != 2 {
						b.Fatalf("expected 2 tasks, got %d", len(result))
					}
				}
			})
		})
	}
}

func TestImportMeta_Task_Failed(t *testing.T) {
	mockErr := errors.New("mock err")
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(mockErr)
	catalog.EXPECT().DropImportTask(mock.Anything, mock.Anything).Return(mockErr)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)
	im.(*importMeta).catalog = catalog

	taskProto := &datapb.ImportTaskV2{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		SegmentIDs:   []int64{5, 6},
		NodeID:       7,
		State:        datapb.ImportTaskStateV2_Pending,
	}
	task := &importTask{}
	task.task.Store(taskProto)

	err = im.AddTask(context.TODO(), task)
	assert.Error(t, err)
	im.(*importMeta).tasks.add(task)
	err = im.UpdateTask(context.TODO(), task.GetTaskID(), UpdateNodeID(9))
	assert.Error(t, err)
	err = im.RemoveTask(context.TODO(), task.GetTaskID())
	assert.Error(t, err)
}

func TestTaskStatsJSON(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	statsJSON := im.TaskStatsJSON(context.TODO())
	assert.Equal(t, "[]", statsJSON)

	taskProto := &datapb.ImportTaskV2{
		TaskID: 1,
	}
	task1 := &importTask{}
	task1.task.Store(taskProto)
	err = im.AddTask(context.TODO(), task1)
	assert.NoError(t, err)

	taskProto.TaskID = 2
	task2 := &importTask{}
	task2.task.Store(taskProto)
	err = im.AddTask(context.TODO(), task2)
	assert.NoError(t, err)

	err = im.UpdateTask(context.TODO(), 1, UpdateState(datapb.ImportTaskStateV2_Completed))
	assert.NoError(t, err)

	statsJSON = im.TaskStatsJSON(context.TODO())
	var tasks []*metricsinfo.ImportTask
	err = json.Unmarshal([]byte(statsJSON), &tasks)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tasks))

	taskMeta := im.(*importMeta).tasks
	taskMeta.remove(1)
	assert.Nil(t, taskMeta.get(1))
	assert.NotNil(t, taskMeta.get(2))
	assert.Equal(t, 2, len(taskMeta.listTaskStats()))
}

func TestHandleCommitVchannel(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	jobID := int64(100)
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:     jobID,
			State:     internalpb.ImportJobState_Committing,
			Vchannels: []string{"ch1", "ch2"},
		},
	}
	err = im.AddJob(context.TODO(), job)
	assert.NoError(t, err)

	callCount := 0
	cb := func() error { callCount++; return nil }

	// First commit of ch1 — should succeed and persist
	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch1", cb)
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)
	assert.Contains(t, im.GetJob(context.TODO(), jobID).GetCommittedVchannels(), "ch1")

	// Idempotent second commit of ch1 — callback should NOT fire again
	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch1", cb)
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount) // still 1, not 2

	// A callback for a channel outside the job must not make unrelated segments
	// visible or occupy one of the job's expected commit acknowledgements.
	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch3", cb)
	assert.ErrorIs(t, err, merr.ErrImportSysFailed)
	assert.Equal(t, 1, callCount)
	assert.NotContains(t, im.GetJob(context.TODO(), jobID).GetCommittedVchannels(), "ch3")

	// Terminal jobs no-op even for a stale foreign callback, so the flusher can
	// retire an already-settled broadcast instead of retrying forever.
	err = im.UpdateJob(context.TODO(), jobID, UpdateJobState(internalpb.ImportJobState_Completed))
	assert.NoError(t, err)
	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch3", cb)
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)

	// Unknown job returns error
	err = im.HandleCommitVchannel(context.TODO(), int64(9999), "ch1", cb)
	assert.Error(t, err)
	assert.Equal(t, 1, callCount) // callback not called for missing job
}

func TestHandleCommitVchannel_BeforeUncommitted_RetryWithoutMutation(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	const jobID int64 = 102
	err = im.AddJob(context.TODO(), &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:     jobID,
			State:     internalpb.ImportJobState_Importing,
			Vchannels: []string{"ch1"},
		},
	})
	assert.NoError(t, err)

	callCount := 0
	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch1", func() error {
		callCount++
		return nil
	})

	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrImportSysFailed))
	assert.Equal(t, 0, callCount)
	updated := im.GetJob(context.TODO(), jobID)
	assert.Equal(t, internalpb.ImportJobState_Importing, updated.GetState())
	assert.NotContains(t, updated.GetCommittedVchannels(), "ch1")
}

func TestHandleCommitVchannel_RetryAfterUncommitted(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	const jobID int64 = 103
	err = im.AddJob(context.TODO(), &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:     jobID,
			State:     internalpb.ImportJobState_Importing,
			Vchannels: []string{"ch1"},
		},
	})
	assert.NoError(t, err)

	callCount := 0
	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch1", func() error {
		callCount++
		return nil
	})
	assert.Error(t, err)
	assert.Equal(t, 0, callCount)

	err = im.UpdateJob(context.TODO(), jobID, UpdateJobState(internalpb.ImportJobState_Uncommitted))
	assert.NoError(t, err)

	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch1", func() error {
		callCount++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)
	updated := im.GetJob(context.TODO(), jobID)
	assert.Equal(t, internalpb.ImportJobState_Committing, updated.GetState())
	assert.Contains(t, updated.GetCommittedVchannels(), "ch1")
}

func TestHandleCommitVchannelTransitionsUncommittedToCommittingBeforeCallback(t *testing.T) {
	jobID := int64(101)
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)

	type savedJob struct {
		state          internalpb.ImportJobState
		committed      []string
		callbackCalled bool
	}
	var (
		recordSaves    bool
		callbackCalled bool
		saves          []savedJob
	)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Run(func(ctx context.Context, job *datapb.ImportJob) {
		if recordSaves && job.GetJobID() == jobID {
			saves = append(saves, savedJob{
				state:          job.GetState(),
				committed:      append([]string(nil), job.GetCommittedVchannels()...),
				callbackCalled: callbackCalled,
			})
		}
	}).Return(nil).Maybe()

	im, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:     jobID,
			State:     internalpb.ImportJobState_Uncommitted,
			Vchannels: []string{"ch1"},
		},
	}
	err = im.AddJob(context.TODO(), job)
	assert.NoError(t, err)

	recordSaves = true
	err = im.HandleCommitVchannel(context.TODO(), jobID, "ch1", func() error {
		callbackCalled = true
		return nil
	})
	assert.NoError(t, err)
	if assert.Len(t, saves, 2) {
		assert.Equal(t, internalpb.ImportJobState_Committing, saves[0].state)
		assert.Empty(t, saves[0].committed)
		assert.False(t, saves[0].callbackCalled)
		assert.Equal(t, internalpb.ImportJobState_Committing, saves[1].state)
		assert.Contains(t, saves[1].committed, "ch1")
		assert.True(t, saves[1].callbackCalled)
	}
	updated := im.GetJob(context.TODO(), jobID)
	assert.Equal(t, internalpb.ImportJobState_Committing, updated.GetState())
	assert.Contains(t, updated.GetCommittedVchannels(), "ch1")
}
