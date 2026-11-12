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

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
)

type Scheduler interface {
	Start()
	Slots() int64
	Resource() taskcommon.Resource
	Close()
}

type scheduler struct {
	manager TaskManager

	// futuresMu protects futures during submission and collection.
	futuresMu sync.Mutex
	// futures tracks every submitted task by ID: its file futures and the
	// booking they hold. An entry stays until all its futures have returned,
	// whatever happens to the task meanwhile (see Resource).
	futures map[int64]*submittedTask

	// submitMu allows only one goroutine to submit tasks at a time.
	submitMu sync.Mutex

	closeOnce sync.Once
	closeChan chan struct{}
}

// submittedTask is a task whose files have been handed to the exec pool.
type submittedTask struct {
	// resource is the task's booking, kept with the futures so it outlives
	// the task: a failed or dropped task leaves the manager's
	// Pending/InProgress states (or the manager) at once, but nothing cancels
	// its readers, which hold their buffers until they return.
	resource taskcommon.Resource
	futures  []*conc.Future[any]
	// detached marks a task that failed or was removed. Its entry is kept
	// only so its booking counts until its futures return.
	detached bool
}

func allDone(fs []*conc.Future[any]) bool {
	return lo.EveryBy(fs, func(f *conc.Future[any]) bool { return f.Done() })
}

func NewScheduler(manager TaskManager) Scheduler {
	return &scheduler{
		manager:   manager,
		futures:   make(map[int64]*submittedTask),
		closeChan: make(chan struct{}),
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
		case <-s.closeChan:
			mlog.Info(context.TODO(), "import scheduler exited")
			return
		case <-exeTicker.C:
			s.scheduleTasks()
			s.collectCompletedTasks()
		case <-logTicker.C:
			LogStats(s.manager)
		}
	}
}

func (s *scheduler) scheduleTasks() {
	if !s.submitMu.TryLock() {
		return
	}

	tasks := s.manager.GetBy(WithStates(datapb.ImportTaskStateV2_Pending))
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].GetTaskID() < tasks[j].GetTaskID()
	})

	if len(tasks) == 0 {
		s.submitMu.Unlock()
		return
	}

	taskIDs := lo.Map(tasks, func(t Task, _ int) int64 {
		return t.GetTaskID()
	})
	mlog.Info(context.TODO(), "processing tasks...", mlog.Int64s("taskIDs", taskIDs))

	go func() {
		defer s.submitMu.Unlock()

		for _, task := range tasks {
			// Execute may block, so do not hold futuresMu while submitting.
			fs := task.Execute()

			s.futuresMu.Lock()
			s.futures[task.GetTaskID()] = &submittedTask{resource: task.GetResource(), futures: fs}
			s.futuresMu.Unlock()
		}
	}()
}

// collectCompletedTasks marks successful tasks Completed. A failed or removed
// task is not waited for: it is detached at once, and its entry is dropped
// when its last future returns.
func (s *scheduler) collectCompletedTasks() {
	s.futuresMu.Lock()
	defer s.futuresMu.Unlock()

	for taskID, st := range s.futures {
		done := allDone(st.futures)
		if st.detached {
			if done {
				delete(s.futures, taskID)
			}
			continue
		}
		task := s.manager.Get(taskID)
		if task == nil || task.GetState() == datapb.ImportTaskStateV2_Failed {
			if task != nil {
				mlog.Warn(context.TODO(), "preimport/import failed", WrapLogFields(task, mlog.String("reason", task.GetReason()))...)
			}
			if done {
				delete(s.futures, taskID)
			} else {
				st.detached = true
			}
			continue
		}
		if !done {
			continue
		}
		delete(s.futures, taskID)
		if err := conc.AwaitAll(st.futures...); err != nil {
			mlog.Warn(context.TODO(), "preimport/import failed", WrapLogFields(task, mlog.Err(err))...)
			continue
		}
		s.manager.Update(taskID, UpdateState(datapb.ImportTaskStateV2_Completed))
		mlog.Info(context.TODO(), "preimport/import done", mlog.FieldTaskID(taskID))
	}
}

// Slots returns the used slots for import
func (s *scheduler) Slots() int64 {
	tasks := s.manager.GetBy(WithStates(datapb.ImportTaskStateV2_Pending, datapb.ImportTaskStateV2_InProgress))
	used := lo.SumBy(tasks, func(t Task) int64 {
		return t.GetSlots()
	})
	return used
}

// Resource returns the cpu/memory booked by every submitted task whose file
// futures have not all returned, whatever the task's state -- a failed or
// dropped task keeps counting until its readers exit, as in importv3 -- plus
// the pending and in-progress tasks not submitted yet.
//
// Memory is never reported below what the import memory allocator has
// reserved. DataCoord prices a file by min(buffer, file size), but every
// reader reserves its whole buffer from the allocator and a reader such as
// Parquet preallocates its batches by buffer capacity, not by the file. The
// allocator bounds that reservation to dataNode.import.memoryLimitPercentage of
// the machine, so this keeps the reported free memory honest without pricing
// every small file at a whole buffer.
func (s *scheduler) Resource() taskcommon.Resource {
	s.futuresMu.Lock()
	defer s.futuresMu.Unlock()
	var used taskcommon.Resource
	for _, st := range s.futures {
		if !allDone(st.futures) {
			used = used.Add(st.resource)
		}
	}
	for _, t := range s.manager.GetBy(WithStates(datapb.ImportTaskStateV2_Pending, datapb.ImportTaskStateV2_InProgress)) {
		if _, submitted := s.futures[t.GetTaskID()]; !submitted {
			used = used.Add(t.GetResource())
		}
	}
	used.Memory = max(used.Memory, GetMemoryAllocator().Used())
	return used
}

func (s *scheduler) Close() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
	})
}
