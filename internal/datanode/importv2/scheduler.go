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
	"sort"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/datanode/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
)

type Scheduler interface {
	Start()
	Slots() int64
	Close()
}

type scheduler struct {
	manager TaskManager

	// ctx ends when the scheduler is closed. It is what bounds a task's wait
	// for the node's budget, so a shutdown does not leave one queued forever.
	ctx    context.Context
	cancel context.CancelFunc

	closeOnce sync.Once
}

func NewScheduler(manager TaskManager) Scheduler {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel is stored in the scheduler and called on Close()
	return &scheduler{
		manager: manager,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (s *scheduler) Start() {
	mlog.Info(context.TODO(), "start import scheduler")

	var (
		exeTicker = time.NewTicker(1 * time.Second)
		logTicker = time.NewTicker(10 * time.Minute)
	)
	defer exeTicker.Stop()
	defer logTicker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			mlog.Info(context.TODO(), "import scheduler exited")
			return
		case <-exeTicker.C:
			s.scheduleTasks()
		case <-logTicker.C:
			LogStats(s.manager)
		}
	}
}

// ledgerTaskType maps this package's task kinds onto the families the shared
// resource ledger knows about. The L0 variants are the same families as their
// ordinary counterparts as far as admission is concerned; only their sizing
// differs, and that is settled in each task's GetResourceRequirement.
func ledgerTaskType(t TaskType) taskcommon.Type {
	switch t {
	case PreImportTaskType, L0PreImportTaskType:
		return taskcommon.PreImport
	case ImportTaskType, L0ImportTaskType:
		return taskcommon.Import
	case CopySegmentTaskType:
		return taskcommon.CopySegment
	default:
		return taskcommon.TypeNone
	}
}

func (s *scheduler) scheduleTasks() {
	tasks := s.manager.GetBy(WithStates(datapb.ImportTaskStateV2_Pending))
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].GetTaskID() < tasks[j].GetTaskID()
	})

	if len(tasks) == 0 {
		return
	}

	taskIDs := lo.Map(tasks, func(t Task, _ int) int64 {
		return t.GetTaskID()
	})
	mlog.Info(context.TODO(), "processing tasks...", mlog.Int64s("taskIDs", taskIDs))

	var wg sync.WaitGroup
	for _, task := range tasks {
		taskID := task.GetTaskID()
		req := task.GetResourceRequirement()

		// Reserve the node's budget before the task starts reading anything.
		// Tasks are admitted in the ID order they were sorted into above, and
		// the wait sits here rather than in the RPC that queued the task, so a
		// task that does not fit yet shows up as queued work on this node.
		//
		// s.ctx is the only bound on that wait, deliberately: the guard never
		// refuses permanently -- a task larger than the whole node waits for it
		// to drain and then runs alone -- so a timeout would only put the same
		// task back at the end of the same queue.
		if err := resource.GetGuard().Acquire(s.ctx, taskID, ledgerTaskType(task.GetType()), req); err != nil {
			mlog.Warn(context.TODO(), "import task gave up waiting for the node's budget",
				mlog.FieldTaskID(taskID), mlog.String("requirement", req.String()), mlog.Err(err))
			// Nothing was reserved, so nothing is released. The task stays
			// Pending and the next tick picks it up again.
			continue
		}

		wg.Add(1)
		go func(task Task, taskID int64) {
			defer wg.Done()
			// The reservation covers the whole task, not just its dispatch:
			// Execute only submits the task's files to the pool, and each of
			// them holds a read buffer until it is done. Waiting per task
			// rather than for the batch also keeps one slow task from holding
			// everyone else's budget.
			//
			// Execute runs under this defer rather than in the loop above so
			// that a panic inside it cannot unwind the scheduler with the
			// reservation still on the ledger -- that would shrink the node
			// permanently, and take the tick goroutine that would have retried
			// with it. Admission stays serialized regardless: Acquire is still
			// what the loop waits on.
			defer resource.GetGuard().Release(taskID)

			if err := conc.AwaitAll(task.Execute()...); err != nil {
				return
			}
			s.manager.Update(taskID, UpdateState(datapb.ImportTaskStateV2_Completed))
			mlog.Info(context.TODO(), "preimport/import done", mlog.FieldTaskID(taskID))
		}(task, taskID)
	}
	wg.Wait()

	mlog.Info(context.TODO(), "all tasks completed", mlog.Int64s("taskIDs", taskIDs))
}

// Slots returns the used slots for import
func (s *scheduler) Slots() int64 {
	tasks := s.manager.GetBy(WithStates(datapb.ImportTaskStateV2_Pending, datapb.ImportTaskStateV2_InProgress))
	used := lo.SumBy(tasks, func(t Task) int64 {
		return t.GetSlots()
	})
	return used
}

func (s *scheduler) Close() {
	s.closeOnce.Do(func() {
		s.cancel()
	})
}
