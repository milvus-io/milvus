package task

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFIFOQueue_Push(t *testing.T) {
	queue := NewPriorityQueuePolicy()

	// Test adding tasks
	task1 := NewMockTask(t)
	task1.EXPECT().GetTaskID().Return(int64(1))
	task2 := NewMockTask(t)
	task2.EXPECT().GetTaskID().Return(int64(2))

	queue.Push(task1)
	queue.Push(task2)
	assert.Equal(t, 2, queue.TaskCount())

	// Verify task ID list
	taskIDs := queue.TaskIDs()
	assert.Equal(t, 2, len(taskIDs))
	assert.Equal(t, int64(1), taskIDs[0])
	assert.Equal(t, int64(2), taskIDs[1])

	// Test adding task with duplicate ID
	queue.Push(task1)
	taskIDs = queue.TaskIDs()
	assert.Equal(t, 2, len(taskIDs))
	assert.Equal(t, 2, queue.TaskCount())
	assert.Equal(t, 1, queue.TaskCountBy(func(task Task) bool {
		return task.GetTaskID() == 2
	}))
}

func TestFIFOQueue_Pop(t *testing.T) {
	queue := NewPriorityQueuePolicy()

	// Test empty queue
	assert.Nil(t, queue.Pop())

	// Test normal pop operation
	task1 := NewMockTask(t)
	task1.EXPECT().GetTaskID().Return(int64(1))
	task2 := NewMockTask(t)
	task2.EXPECT().GetTaskID().Return(int64(2))
	queue.Push(task1)
	queue.Push(task2)

	poppedTask := queue.Pop()
	assert.Equal(t, int64(1), poppedTask.GetTaskID())
	assert.Equal(t, 1, len(queue.TaskIDs()))
	assert.Equal(t, 1, queue.TaskCount())

	poppedTask = queue.Pop()
	assert.Equal(t, int64(2), poppedTask.GetTaskID())
	assert.Equal(t, 0, len(queue.TaskIDs()))
	assert.Equal(t, 0, queue.TaskCount())
}

func TestFIFOQueue_Get(t *testing.T) {
	queue := NewPriorityQueuePolicy()

	// Test getting non-existent task
	assert.Nil(t, queue.Get(1))

	// Test getting existing task
	task := NewMockTask(t)
	task.EXPECT().GetTaskID().Return(int64(1))
	queue.Push(task)

	retrievedTask := queue.Get(1)
	assert.Equal(t, int64(1), retrievedTask.GetTaskID())
}

func TestFIFOQueue_Remove(t *testing.T) {
	queue := NewPriorityQueuePolicy()

	// Test removing non-existent task
	queue.Remove(1)
	assert.Equal(t, 0, len(queue.TaskIDs()))

	// Test removing existing task
	task1 := NewMockTask(t)
	task1.EXPECT().GetTaskID().Return(int64(1))
	task2 := NewMockTask(t)
	task2.EXPECT().GetTaskID().Return(int64(2))
	task3 := NewMockTask(t)
	task3.EXPECT().GetTaskID().Return(int64(3))

	queue.Push(task1)
	queue.Push(task2)
	queue.Push(task3)

	queue.Remove(2)
	taskIDs := queue.TaskIDs()
	assert.Equal(t, 2, len(taskIDs))
	assert.Equal(t, 2, queue.TaskCount())
	assert.Equal(t, int64(1), taskIDs[0])
	assert.Equal(t, int64(3), taskIDs[1])

	// Verify task is actually removed
	assert.Nil(t, queue.Get(2))
}

func TestFIFOQueue_TaskIDs(t *testing.T) {
	queue := NewPriorityQueuePolicy()

	// Test empty queue
	assert.Equal(t, 0, len(queue.TaskIDs()))

	// Test queue with tasks
	task1 := NewMockTask(t)
	task1.EXPECT().GetTaskID().Return(int64(1))
	task2 := NewMockTask(t)
	task2.EXPECT().GetTaskID().Return(int64(2))

	queue.Push(task1)
	queue.Push(task2)

	taskIDs := queue.TaskIDs()
	assert.Equal(t, 2, len(taskIDs))
	assert.Equal(t, int64(1), taskIDs[0])
	assert.Equal(t, int64(2), taskIDs[1])
}

func TestPriorityQueue_TaskIDsByPriorityPreservesQueue(t *testing.T) {
	for _, test := range []struct {
		name string
		ids  []int64
	}{
		{name: "empty"},
		{name: "single", ids: []int64{1}},
		{name: "heap traversal differs from priority", ids: []int64{1, 4, 2, 8, 5, 3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue := NewPriorityQueuePolicy()
			for _, id := range test.ids {
				task := NewMockTask(t)
				task.EXPECT().GetTaskID().Return(id)
				queue.Push(task)
			}
			before := queue.TaskIDs()

			snapshot := queue.TaskIDsByPriority()

			assert.Equal(t, before, queue.TaskIDs(), "snapshot must not rearrange the original heap")
			assert.Equal(t, len(test.ids), queue.TaskCount())
			for _, id := range test.ids {
				assert.NotNil(t, queue.Get(id), "task must remain available for cancellation")
			}
			popped := make([]int64, 0, len(test.ids))
			for range test.ids {
				task := queue.Pop()
				require.NotNil(t, task)
				popped = append(popped, task.GetTaskID())
			}
			assert.Equal(t, popped, snapshot, "snapshot must follow the same priority as Pop")
		})
	}
}
