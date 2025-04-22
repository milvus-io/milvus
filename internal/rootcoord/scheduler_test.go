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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	mocktso "github.com/milvus-io/milvus/internal/tso/mocks"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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

type mockLockerKeyTask struct {
	baseTask
	lockerKey string
	rw        bool
}

func (m *mockLockerKeyTask) GetLockerKey() LockerKey {
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(m.lockerKey, m.rw),
	)
}

func newMockLockerKeyTask(lockerKey string, rw bool) *mockLockerKeyTask {
	task := &mockLockerKeyTask{
		baseTask:  newBaseTask(context.Background(), nil),
		lockerKey: lockerKey,
		rw:        rw,
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
		paramtable.Init()
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
			return 0, errors.New("error mock GenerateTSO")
		}
		ctx := context.Background()
		s := newScheduler(ctx, idAlloc, tsoAlloc)
		paramtable.Init()
		paramtable.Get().Save(Params.ProxyCfg.TimeTickInterval.Key, "1")
		s.Start()

		time.Sleep(time.Millisecond * 4)
		assert.Zero(t, s.GetMinDdlTs())
		s.Stop()
	})

	t.Run("concurrent task schedule", func(t *testing.T) {
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
		paramtable.Init()
		paramtable.Get().Save(Params.ProxyCfg.TimeTickInterval.Key, "1")
		s.Start()

		for i := 0; i < 100; i++ {
			if s.GetMinDdlTs() > Timestamp(100) {
				break
			}
			assert.True(t, i < 100)
			time.Sleep(time.Millisecond)
		}

		w := &sync.WaitGroup{}
		w.Add(5)
		// locker key rw true
		lockerKey := "hello"
		go func() {
			defer w.Done()
			n := 200
			for i := 0; i < n; i++ {
				task := newMockLockerKeyTask(lockerKey, true)
				err := s.AddTask(task)
				assert.NoError(t, err)
			}
		}()

		// locker key rw false
		go func() {
			defer w.Done()
			n := 200
			for i := 0; i < n; i++ {
				task := newMockLockerKeyTask(lockerKey, false)
				err := s.AddTask(task)
				assert.NoError(t, err)
			}
		}()

		go func() {
			defer w.Done()
			n := 200
			for i := 0; i < n; i++ {
				task := newMockLockerKeyTask(lockerKey, false)
				err := s.AddTask(task)
				assert.NoError(t, err)
			}
		}()

		go func() {
			defer w.Done()
			n := 200
			for i := 0; i < n; i++ {
				task := newMockNormalTask()
				err := s.AddTask(task)
				assert.NoError(t, err)
			}
		}()

		lastMin := s.GetMinDdlTs()
		go func() {
			defer w.Done()
			current := s.GetMinDdlTs()
			assert.True(t, current >= lastMin)
			lastMin = current
			time.Sleep(time.Millisecond * 100)
		}()
		w.Wait()
	})
}

type WithLockKeyTask struct {
	baseTask
	lockKey      LockerKey
	workDuration time.Duration
	newTime      time.Time
	name         string
}

func NewWithLockKeyTask(lockKey LockerKey, duration time.Duration, name string) *WithLockKeyTask {
	task := &WithLockKeyTask{
		baseTask:     newBaseTask(context.Background(), nil),
		lockKey:      lockKey,
		workDuration: duration,
		newTime:      time.Now(),
		name:         name,
	}
	return task
}

func (t *WithLockKeyTask) GetLockerKey() LockerKey {
	return t.lockKey
}

func (t *WithLockKeyTask) Execute(ctx context.Context) error {
	log.Info("execute task", zap.String("name", t.name), zap.Duration("duration", time.Since(t.newTime)))
	time.Sleep(t.workDuration)
	return nil
}

func TestExecuteTaskWithLock(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.RootCoordCfg.UseLockScheduler.Key, "true")
	defer Params.Reset(Params.RootCoordCfg.UseLockScheduler.Key)
	idMock := allocator.NewMockAllocator(t)
	tsMock := mocktso.NewAllocator(t)
	idMock.EXPECT().AllocOne().Return(1000, nil)
	tsMock.EXPECT().GenerateTSO(mock.Anything).Return(10000, nil)
	s := newScheduler(context.Background(), idMock, tsMock)
	w := &sync.WaitGroup{}
	w.Add(4)
	{
		go func() {
			defer w.Done()
			time.Sleep(1500 * time.Millisecond)
			lockKey := NewLockerKeyChain(NewClusterLockerKey(false), NewDatabaseLockerKey("test", false))
			t1 := NewWithLockKeyTask(lockKey, time.Second*2, "t1-1")
			err := s.AddTask(t1)
			assert.NoError(t, err)
		}()
	}
	{
		go func() {
			defer w.Done()
			time.Sleep(1500 * time.Millisecond)
			lockKey := NewLockerKeyChain(NewClusterLockerKey(false), NewDatabaseLockerKey("test", false))
			t1 := NewWithLockKeyTask(lockKey, time.Second*3, "t1-2")
			err := s.AddTask(t1)
			assert.NoError(t, err)
		}()
	}
	{
		go func() {
			defer w.Done()
			time.Sleep(500 * time.Millisecond)
			lockKey := NewLockerKeyChain(NewClusterLockerKey(false), NewDatabaseLockerKey("test", true))
			t2 := NewWithLockKeyTask(lockKey, time.Second*2, "t2")
			err := s.AddTask(t2)
			assert.NoError(t, err)
		}()
	}
	{
		go func() {
			defer w.Done()
			lockKey := NewLockerKeyChain(NewClusterLockerKey(true))
			t3 := NewWithLockKeyTask(lockKey, time.Second, "t3")
			err := s.AddTask(t3)
			assert.NoError(t, err)
		}()
	}

	startTime := time.Now()
	w.Wait()
	delta := time.Since(startTime)
	assert.True(t, delta > 6*time.Second)
	assert.True(t, delta < 8*time.Second)
}
