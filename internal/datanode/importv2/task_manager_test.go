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

package importv2

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestTaskManager_RemoveExpiredTasks(t *testing.T) {
	manager := NewTaskManager()
	now := time.Now()
	cutoff := now.Add(-24 * time.Hour)

	newTask := func(id int64, state datapb.ImportTaskStateV2) (*ImportTask, context.Context) {
		ctx, cancel := context.WithCancel(context.Background())
		return &ImportTask{
			ImportTaskV2: &datapb.ImportTaskV2{TaskID: id, State: state},
			ctx:          ctx,
			cancel:       cancel,
		}, ctx
	}
	expired, expiredCtx := newTask(1, datapb.ImportTaskStateV2_Completed)
	fresh, _ := newTask(2, datapb.ImportTaskStateV2_Failed)
	running, runningCtx := newTask(3, datapb.ImportTaskStateV2_InProgress)
	manager.Add(expired)
	manager.Add(fresh)
	manager.Add(running)

	impl := manager.(*taskManager)
	impl.startedAt[1] = cutoff
	impl.startedAt[2] = cutoff.Add(time.Second)
	impl.startedAt[3] = cutoff.Add(-time.Hour)

	assert.Equal(t, 2, manager.RemoveExpiredTasks(context.Background(), cutoff))
	assert.Nil(t, manager.Get(1))
	assert.NotNil(t, manager.Get(2))
	assert.Nil(t, manager.Get(3))
	for _, taskCtx := range []context.Context{expiredCtx, runningCtx} {
		select {
		case <-taskCtx.Done():
		default:
			t.Fatal("reclaiming an expired task must cancel its context")
		}
	}
}

func TestTaskManager_ExplicitRemoveReclaimsTerminalTask(t *testing.T) {
	manager := NewTaskManager()
	states := []datapb.ImportTaskStateV2{
		datapb.ImportTaskStateV2_Completed,
		datapb.ImportTaskStateV2_Failed,
		datapb.ImportTaskStateV2_Retry,
	}
	tasks := make([]*ImportTask, 0, len(states))
	for i, state := range states {
		ctx, cancel := context.WithCancel(context.Background())
		task := &ImportTask{
			ImportTaskV2: &datapb.ImportTaskV2{TaskID: int64(i + 1), State: state},
			ctx:          ctx,
			cancel:       cancel,
		}
		tasks = append(tasks, task)
		manager.Add(task)
	}

	// Terminal results remain available until the coordinator acknowledges them
	// through DropImport/DropCopySegment.
	for _, task := range tasks {
		assert.NotNil(t, manager.Get(task.GetTaskID()))
	}

	manager.Remove(tasks[0].GetTaskID())
	assert.Nil(t, manager.Get(tasks[0].GetTaskID()))
	assert.NotNil(t, manager.Get(tasks[1].GetTaskID()))
	assert.NotNil(t, manager.Get(tasks[2].GetTaskID()))
}

func TestTaskManager_DuplicateAddCancelsLosingTask(t *testing.T) {
	manager := NewTaskManager()
	winnerCtx, winnerCancel := context.WithCancel(context.Background())
	defer winnerCancel()
	loserCtx, loserCancel := context.WithCancel(context.Background())

	winner := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{TaskID: 1, State: datapb.ImportTaskStateV2_Pending},
		ctx:          winnerCtx,
		cancel:       winnerCancel,
	}
	loser := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{TaskID: 1, State: datapb.ImportTaskStateV2_Pending},
		ctx:          loserCtx,
		cancel:       loserCancel,
	}
	manager.Add(winner)
	manager.Add(loser)

	select {
	case <-loserCtx.Done():
	default:
		t.Fatal("duplicate Add must cancel the losing task instance")
	}
	select {
	case <-winnerCtx.Done():
		t.Fatal("duplicate Add must not cancel the registered owner")
	default:
	}
	assert.Same(t, winner, manager.Get(winner.GetTaskID()))
}

func TestTaskManager_StaleAttemptCannotUpdateReusedTaskID(t *testing.T) {
	manager := NewTaskManager()
	oldCtx, oldCancel := context.WithCancel(context.Background())
	oldTask := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			TaskID: 1,
			State:  datapb.ImportTaskStateV2_Pending,
		},
		ctx:    oldCtx,
		cancel: oldCancel,
	}
	manager.Add(oldTask)
	manager.Update(oldTask, UpdateState(datapb.ImportTaskStateV2_InProgress))

	// Model a worker that has passed its final cancellation check but has not
	// published its terminal result yet.
	releaseOldWorker := make(chan struct{})
	oldWorkerDone := make(chan struct{})
	go func() {
		defer close(oldWorkerDone)
		<-releaseOldWorker
		manager.Update(oldTask,
			UpdateState(datapb.ImportTaskStateV2_Failed),
			UpdateReason("stale attempt failed"),
		)
	}()

	manager.Remove(oldTask.GetTaskID())
	select {
	case <-oldCtx.Done():
	default:
		t.Fatal("removing a task must cancel its executing owner")
	}

	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()
	newTask := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			TaskID: 1,
			State:  datapb.ImportTaskStateV2_Pending,
		},
		ctx:    newCtx,
		cancel: newCancel,
	}
	manager.Add(newTask)

	close(releaseOldWorker)
	<-oldWorkerDone

	got := manager.Get(newTask.GetTaskID())
	assert.Equal(t, datapb.ImportTaskStateV2_Pending, got.GetState())
	assert.Empty(t, got.GetReason())

	manager.Update(newTask, UpdateState(datapb.ImportTaskStateV2_InProgress))
	assert.Equal(t, datapb.ImportTaskStateV2_InProgress, manager.Get(newTask.GetTaskID()).GetState())
}

func TestTaskManager_UpdateIfStateIsAtomicWithOwnerAndState(t *testing.T) {
	manager := NewTaskManager()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			TaskID: 1,
			State:  datapb.ImportTaskStateV2_Pending,
		},
		ctx:    ctx,
		cancel: cancel,
	}
	manager.Add(task)
	manager.Update(task, UpdateState(datapb.ImportTaskStateV2_Failed))

	updated := manager.UpdateIfState(task, datapb.ImportTaskStateV2_InProgress,
		UpdateState(datapb.ImportTaskStateV2_Completed))
	assert.False(t, updated)
	assert.Equal(t, datapb.ImportTaskStateV2_Failed, manager.Get(task.GetTaskID()).GetState())

	manager.Remove(task.GetTaskID())
	replacementCtx, replacementCancel := context.WithCancel(context.Background())
	defer replacementCancel()
	replacement := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			TaskID: 1,
			State:  datapb.ImportTaskStateV2_InProgress,
		},
		ctx:    replacementCtx,
		cancel: replacementCancel,
	}
	manager.Add(replacement)

	updated = manager.UpdateIfState(task, datapb.ImportTaskStateV2_InProgress,
		UpdateState(datapb.ImportTaskStateV2_Completed))
	assert.False(t, updated)
	assert.Equal(t, datapb.ImportTaskStateV2_InProgress, manager.Get(replacement.GetTaskID()).GetState())
	assert.True(t, manager.UpdateIfState(replacement, datapb.ImportTaskStateV2_InProgress,
		UpdateState(datapb.ImportTaskStateV2_Completed)))
	assert.Equal(t, datapb.ImportTaskStateV2_Completed, manager.Get(replacement.GetTaskID()).GetState())
}

func TestImportManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	manager := NewTaskManager()
	task1 := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_Pending,
		},
		ctx:    ctx,
		cancel: cancel,
	}
	manager.Add(task1)
	manager.Add(task1)
	res := manager.Get(task1.GetTaskID())
	assert.Equal(t, task1, res)

	task2 := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       8,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_Completed,
		},
		ctx:    ctx,
		cancel: cancel,
	}
	manager.Add(task2)

	tasks := manager.GetBy()
	assert.Equal(t, 2, len(tasks))
	tasks = manager.GetBy(WithStates(datapb.ImportTaskStateV2_Completed))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, task2.GetTaskID(), tasks[0].GetTaskID())

	// check idempotency
	manager.Add(task2)
	tasks = manager.GetBy(WithStates(datapb.ImportTaskStateV2_Completed))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, task2.GetTaskID(), tasks[0].GetTaskID())
	assert.True(t, task2 == tasks[0])

	manager.Update(task1, UpdateState(datapb.ImportTaskStateV2_Failed))
	task := manager.Get(task1.GetTaskID())
	assert.Equal(t, datapb.ImportTaskStateV2_Failed, task.GetState())

	manager.Remove(task1.GetTaskID())
	tasks = manager.GetBy()
	assert.Equal(t, 1, len(tasks))
	manager.Remove(10)
	tasks = manager.GetBy()
	assert.Equal(t, 1, len(tasks))
}

func TestImportManager_L0(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	t.Run("l0 preimport", func(t *testing.T) {
		manager := NewTaskManager()
		task := &L0PreImportTask{
			PreImportTask: &datapb.PreImportTask{
				JobID:        1,
				TaskID:       2,
				CollectionID: 3,
				NodeID:       7,
				State:        datapb.ImportTaskStateV2_Pending,
				FileStats: []*datapb.ImportFileStats{{
					TotalRows: 50,
				}},
			},
			ctx:    ctx,
			cancel: cancel,
		}
		manager.Add(task)
		res := manager.Get(task.GetTaskID())
		assert.Equal(t, task, res)

		reason := "mock reason"
		manager.Update(task, UpdateState(datapb.ImportTaskStateV2_Failed),
			UpdateReason(reason), UpdateFileStat(0, &datapb.ImportFileStats{
				TotalRows: 100,
			}))

		res = manager.Get(task.GetTaskID())
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, res.GetState())
		assert.Equal(t, reason, res.GetReason())
		assert.Equal(t, int64(100), res.(*L0PreImportTask).GetFileStats()[0].GetTotalRows())
	})

	t.Run("l0 import", func(t *testing.T) {
		manager := NewTaskManager()
		task := &L0ImportTask{
			ImportTaskV2: &datapb.ImportTaskV2{
				JobID:        1,
				TaskID:       2,
				CollectionID: 3,
				SegmentIDs:   []int64{5, 6},
				NodeID:       7,
				State:        datapb.ImportTaskStateV2_Pending,
			},
			segmentsInfo: map[int64]*datapb.ImportSegmentInfo{
				10: {ImportedRows: 50},
			},
			ctx:    ctx,
			cancel: cancel,
		}
		manager.Add(task)
		res := manager.Get(task.GetTaskID())
		assert.Equal(t, task, res)

		reason := "mock reason"
		manager.Update(task, UpdateState(datapb.ImportTaskStateV2_Failed),
			UpdateReason(reason), UpdateSegmentInfo(&datapb.ImportSegmentInfo{
				SegmentID:    10,
				ImportedRows: 100,
			}))

		res = manager.Get(task.GetTaskID())
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, res.GetState())
		assert.Equal(t, reason, res.GetReason())
		assert.Equal(t, int64(100), res.(*L0ImportTask).GetSegmentsInfo()[0].GetImportedRows())
	})
}

func TestUpdateSegmentInfoRefreshesStatsOnMerge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	task := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{TaskID: 1},
		segmentsInfo: map[int64]*datapb.ImportSegmentInfo{},
		ctx:          ctx,
		cancel:       cancel,
	}

	// A segment fed by multiple import files: each NewImportSegmentInfo ships the
	// segment's cumulative Statistics (Publish() is cumulative per segment), so a
	// later file's snapshot supersedes the earlier one. The merge must refresh
	// Stats to the latest info, like ImportedRows — not freeze the first.
	UpdateSegmentInfo(&datapb.ImportSegmentInfo{
		SegmentID:    10,
		ImportedRows: 50,
		Stats:        &datapb.Statistics{InsertBinlogSize: 100, InsertBinlogCount: 1},
	})(task)
	UpdateSegmentInfo(&datapb.ImportSegmentInfo{
		SegmentID:    10,
		ImportedRows: 120,
		Stats:        &datapb.Statistics{InsertBinlogSize: 250, InsertBinlogCount: 3},
	})(task)

	got := task.segmentsInfo[10]
	assert.EqualValues(t, 120, got.GetImportedRows())
	assert.EqualValues(t, 250, got.GetStats().GetInsertBinlogSize(), "stats must reflect the latest cumulative snapshot")
	assert.EqualValues(t, 3, got.GetStats().GetInsertBinlogCount())
}
