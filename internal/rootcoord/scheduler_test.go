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

package rootcoord

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type mockFailTask struct {
	baseTask
	prepareErr error
	executeErr error
}

func newMockFailTask() *mockFailTask {
	task := &mockFailTask{
		baseTask: newBaseTask(context.Background(), nil),
	}
	task.SetCtx(context.Background())
	return task
}

func newMockPrepareFailTask() *mockFailTask {
	task := newMockFailTask()
	task.prepareErr = errors.New("error mock Prepare")
	return task
}

func newMockExecuteFailTask() *mockFailTask {
	task := newMockFailTask()
	task.prepareErr = errors.New("error mock Execute")
	return task
}

func (m mockFailTask) Prepare(context.Context) error {
	return m.prepareErr
}

func (m mockFailTask) Execute(context.Context) error {
	return m.executeErr
}

type mockNormalTask struct {
	baseTask
}

func newMockNormalTask() *mockNormalTask {
	task := &mockNormalTask{
		baseTask: newBaseTask(context.Background(), nil),
	}
	task.SetCtx(context.Background())
	return task
}

func Test_scheduler_Start_Stop(t *testing.T) {
	idAlloc := newMockIDAllocator()
	tsoAlloc := newMockTsoAllocator()
	ctx := context.Background()
	s := newScheduler(ctx, idAlloc, tsoAlloc)
	s.Start()
	s.Stop()
}

func Test_scheduler_failed_to_set_id(t *testing.T) {
	idAlloc := newMockIDAllocator()
	tsoAlloc := newMockTsoAllocator()
	idAlloc.AllocOneF = func() (UniqueID, error) {
		return 0, errors.New("error mock AllocOne")
	}
	ctx := context.Background()
	s := newScheduler(ctx, idAlloc, tsoAlloc)
	s.Start()
	time.Sleep(time.Second)
	defer s.Stop()
	task := newMockNormalTask()
	err := s.AddTask(task)
	assert.Error(t, err)
}

func Test_scheduler_failed_to_set_ts(t *testing.T) {
	idAlloc := newMockIDAllocator()
	tsoAlloc := newMockTsoAllocator()
	idAlloc.AllocOneF = func() (UniqueID, error) {
		return 100, nil
	}
	tsoAlloc.GenerateTSOF = func(count uint32) (uint64, error) {
		return 0, errors.New("error mock GenerateTSO")
	}
	ctx := context.Background()
	s := newScheduler(ctx, idAlloc, tsoAlloc)
	s.Start()
	time.Sleep(time.Second)
	defer s.Stop()
	task := newMockNormalTask()
	err := s.AddTask(task)
	assert.Error(t, err)
}

func Test_scheduler_enqueue_normal_case(t *testing.T) {
	idAlloc := newMockIDAllocator()
	tsoAlloc := newMockTsoAllocator()
	idAlloc.AllocOneF = func() (UniqueID, error) {
		return 100, nil
	}
	tsoAlloc.GenerateTSOF = func(count uint32) (uint64, error) {
		return 101, nil
	}
	ctx := context.Background()
	s := newScheduler(ctx, idAlloc, tsoAlloc)
	s.Start()
	defer s.Stop()
	task := newMockNormalTask()
	err := s.AddTask(task)
	assert.NoError(t, err)
	assert.Equal(t, UniqueID(100), task.GetID())
	assert.Equal(t, Timestamp(101), task.GetTs())
}

func Test_scheduler_bg(t *testing.T) {
	idAlloc := newMockIDAllocator()
	tsoAlloc := newMockTsoAllocator()
	idAlloc.AllocOneF = func() (UniqueID, error) {
		return 100, nil
	}
	tsoAlloc.GenerateTSOF = func(count uint32) (uint64, error) {
		return 101, nil
	}
	ctx := context.Background()
	s := newScheduler(ctx, idAlloc, tsoAlloc)
	s.Start()

	n := 10
	tasks := make([]task, 0, n)
	for i := 0; i < n; i++ {
		which := rand.Int() % 3
		switch which {
		case 0:
			tasks = append(tasks, newMockPrepareFailTask())
		case 1:
			tasks = append(tasks, newMockExecuteFailTask())
		default:
			tasks = append(tasks, newMockNormalTask())
		}
	}

	for _, task := range tasks {
		s.AddTask(task)
	}

	for _, task := range tasks {
		err := task.WaitToFinish()
		switch task.(type) {
		case *mockFailTask:
			assert.Error(t, err)
		case *mockNormalTask:
			assert.NoError(t, err)
		}
	}

	s.Stop()
}

func Test_scheduler_updateDdlMinTsLoop(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		idAlloc := newMockIDAllocator()
		tsoAlloc := newMockTsoAllocator()
		tso := atomic.NewUint64(100)
		idAlloc.AllocOneF = func() (UniqueID, error) {
			return 100, nil
		}
		tsoAlloc.GenerateTSOF = func(count uint32) (uint64, error) {
			got := tso.Inc()
			return got, nil
		}
		ctx := context.Background()
		s := newScheduler(ctx, idAlloc, tsoAlloc)
		Params.Init()
		paramtable.Get().Save(Params.ProxyCfg.TimeTickInterval.Key, "1")
		s.Start()

		for i := 0; i < 100; i++ {
			if s.GetMinDdlTs() > Timestamp(100) {
				break
			}
			assert.True(t, i < 100)
			time.Sleep(time.Millisecond)
		}

		// add task to queue.
		n := 10
		for i := 0; i < n; i++ {
			task := newMockNormalTask()
			err := s.AddTask(task)
			assert.NoError(t, err)
		}

		time.Sleep(time.Millisecond * 4)
		s.Stop()
	})

	t.Run("invalid tso", func(t *testing.T) {
		idAlloc := newMockIDAllocator()
		tsoAlloc := newMockTsoAllocator()
		idAlloc.AllocOneF = func() (UniqueID, error) {
			return 100, nil
		}
		tsoAlloc.GenerateTSOF = func(count uint32) (uint64, error) {
			return 0, fmt.Errorf("error mock GenerateTSO")
		}
		ctx := context.Background()
		s := newScheduler(ctx, idAlloc, tsoAlloc)
		Params.Init()
		paramtable.Get().Save(Params.ProxyCfg.TimeTickInterval.Key, "1")
		s.Start()

		time.Sleep(time.Millisecond * 4)
		assert.Zero(t, s.GetMinDdlTs())
		s.Stop()
	})
}
