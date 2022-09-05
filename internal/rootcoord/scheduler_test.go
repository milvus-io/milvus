package rootcoord

import (
	"context"
	"errors"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockFailTask struct {
	baseTaskV2
	prepareErr error
	executeErr error
}

func newMockFailTask() *mockFailTask {
	task := &mockFailTask{
		baseTaskV2: baseTaskV2{
			ctx:  context.Background(),
			done: make(chan error, 1),
		},
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
	baseTaskV2
}

func newMockNormalTask() *mockNormalTask {
	task := &mockNormalTask{
		baseTaskV2: baseTaskV2{
			ctx:  context.Background(),
			done: make(chan error, 1),
		},
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
	defer s.Stop()
	task := newMockNormalTask()
	err := s.AddTask(task)
	assert.Error(t, err)
}

func Test_scheduler_enqueu_normal_case(t *testing.T) {
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
	tasks := make([]taskV2, 0, n)
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
