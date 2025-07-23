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
	"sort"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
)

type Scheduler interface {
	Start()
	Slots() int64
	Close()
}

type scheduler struct {
	manager         TaskManager
	memoryAllocator MemoryAllocator

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewScheduler(manager TaskManager) Scheduler {
	memoryAllocator := NewMemoryAllocator(int64(hardware.GetMemoryCount()))
	return &scheduler{
		manager:         manager,
		memoryAllocator: memoryAllocator,
		closeChan:       make(chan struct{}),
	}
}

func (s *scheduler) Start() {
	log.Info("start import scheduler")

	var (
		exeTicker = time.NewTicker(1 * time.Second)
		logTicker = time.NewTicker(10 * time.Minute)
	)
	defer exeTicker.Stop()
	defer logTicker.Stop()

	for {
		select {
		case <-s.closeChan:
			log.Info("import scheduler exited")
			return
		case <-exeTicker.C:
			s.scheduleTasks()
		case <-logTicker.C:
			LogStats(s.manager)
		}
	}
}

// scheduleTasks implements memory-based task scheduling using MemoryAllocator
// It selects tasks that can fit within the available memory.
func (s *scheduler) scheduleTasks() {
	pendingTasks := s.manager.GetBy(WithStates(datapb.ImportTaskStateV2_Pending))
	sort.Slice(pendingTasks, func(i, j int) bool {
		return pendingTasks[i].GetTaskID() < pendingTasks[j].GetTaskID()
	})

	selectedTasks := make([]Task, 0)
	tasksBufferSize := make(map[int64]int64)
	for _, task := range pendingTasks {
		taskBufferSize := task.GetBufferSize()
		if s.memoryAllocator.TryAllocate(task.GetTaskID(), taskBufferSize) {
			selectedTasks = append(selectedTasks, task)
			tasksBufferSize[task.GetTaskID()] = taskBufferSize
		}
	}

	if len(selectedTasks) == 0 {
		return
	}

	log.Info("processing selected tasks",
		zap.Int("pending", len(pendingTasks)),
		zap.Int("selected", len(selectedTasks)))

	futures := make(map[int64][]*conc.Future[any])
	for _, task := range selectedTasks {
		fs := task.Execute()
		futures[task.GetTaskID()] = fs
	}

	for taskID, fs := range futures {
		err := conc.AwaitAll(fs...)
		if err != nil {
			s.memoryAllocator.Release(taskID, tasksBufferSize[taskID])
			continue
		}
		s.manager.Update(taskID, UpdateState(datapb.ImportTaskStateV2_Completed))
		s.memoryAllocator.Release(taskID, tasksBufferSize[taskID])
		log.Info("preimport/import done", zap.Int64("taskID", taskID))
	}
}

// Slots returns the available slots for import
func (s *scheduler) Slots() int64 {
	tasks := s.manager.GetBy(WithStates(datapb.ImportTaskStateV2_Pending, datapb.ImportTaskStateV2_InProgress))
	used := lo.SumBy(tasks, func(t Task) int64 {
		return t.GetSlots()
	})
	return used
}

func (s *scheduler) Close() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
	})
}

func tryFreeFutures(futures map[int64][]*conc.Future[any]) {
	for k, fs := range futures {
		fs = lo.Filter(fs, func(f *conc.Future[any], _ int) bool {
			if f.Done() {
				_, err := f.Await()
				return err != nil
			}
			return true
		})
		futures[k] = fs
	}
}
