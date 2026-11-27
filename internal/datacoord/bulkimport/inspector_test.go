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

package bulkimport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	task2 "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestImportInspectorSkipsPendingTasksOfTerminalJobs(t *testing.T) {
	for _, state := range []internalpb.ImportJobState{
		internalpb.ImportJobState_Failed, internalpb.ImportJobState_Completed,
	} {
		t.Run(state.String(), func(t *testing.T) {
			importMeta := NewMockImportMeta(t)
			scheduler := task2.NewMockGlobalScheduler(t)
			inspector := NewImportInspector(context.Background(), nil, importMeta, scheduler, nil).(*Inspector)
			job := &Job{ImportJob: &datapb.ImportJob{JobID: 1, State: state}}
			preImport := &PreImportTask{}
			preImport.Task.Store(&datapb.PreImportTask{JobID: 1, TaskID: 10, State: datapb.ImportTaskStateV2_Pending})
			importing := &Task{}
			importing.Task.Store(&datapb.ImportTaskV2{JobID: 1, TaskID: 20, State: datapb.ImportTaskStateV2_Pending})
			importMeta.EXPECT().GetJobBy(mock.Anything).Return([]ImportJob{job}).Once()
			importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1)).Return([]ImportTask{preImport, importing}).Once()
			inspector.Inspect()
		})
	}
}

func TestImportInspectorSchedulesV3PendingTasksWithoutJobGate(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	scheduler := task2.NewMockGlobalScheduler(t)
	inspector := NewImportInspector(ctx, nil, importMeta, scheduler, nil).(*Inspector)
	tasks := []ImportTask{
		newReshardTask(&datapb.ReshardTask{JobId: 1, TaskId: 10, State: datapb.ImportTaskStateV2_Pending, NodeId: meta.NullNodeID}, importMeta, nil, nil),
		NewImportTaskV3(&datapb.ImportTaskV3{JobId: 2, TaskId: 20, State: datapb.ImportTaskStateV2_Pending, NodeId: meta.NullNodeID}, importMeta, nil, nil),
	}
	for _, task := range tasks {
		scheduler.EXPECT().Enqueue(task).Once()
		inspector.processPendingTask(task)
	}
}

// TestImportInspectorReshardDispatchGate pins that a Pending ReshardTask is
// enqueued only while the job can accept the dispatch (Pending/Resharding); in
// the early pre-reshard states the inspector skips it, because the task is
// persisted a moment before the job advances to Resharding.
func TestImportInspectorReshardDispatchGate(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		state    internalpb.ImportJobState
		dispatch bool
	}{
		{internalpb.ImportJobState_Resharding, true},
		{internalpb.ImportJobState_Pending, true},
		{internalpb.ImportJobState_PreImporting, false},
		{internalpb.ImportJobState_AssigningIDRange, false},
		{internalpb.ImportJobState_Planning, false},
	} {
		t.Run(tc.state.String(), func(t *testing.T) {
			importMeta := NewMockImportMeta(t)
			scheduler := task2.NewMockGlobalScheduler(t)
			inspector := NewImportInspector(ctx, nil, importMeta, scheduler, nil).(*Inspector)
			job := &Job{ImportJob: &datapb.ImportJob{JobID: 1, State: tc.state}}
			task := newReshardTask(&datapb.ReshardTask{
				JobId: 1, TaskId: 10, State: datapb.ImportTaskStateV2_Pending, NodeId: meta.NullNodeID,
			}, importMeta, nil, nil)
			importMeta.EXPECT().GetJobBy(mock.Anything).Return([]ImportJob{job}).Once()
			importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1)).Return([]ImportTask{task}).Once()
			if tc.dispatch {
				scheduler.EXPECT().Enqueue(task).Once()
			}

			inspector.Inspect()
		})
	}
}
