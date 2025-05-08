package task

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFIFOQueue_Push(t *testing.T) {
	queue := NewFIFOQueue()

	// Test adding tasks
	task1 := NewMockTask(t)
	task1.EXPECT().GetTaskID().Return(int64(1))
	task2 := NewMockTask(t)
	task2.EXPECT().GetTaskID().Return(int64(2))

	queue.Push(task1)
	queue.Push(task2)

	// Verify task ID list
	taskIDs := queue.TaskIDs()
	assert.Equal(t, 2, len(taskIDs))
	assert.Equal(t, int64(1), taskIDs[0])
	assert.Equal(t, int64(2), taskIDs[1])

	// Test adding task with duplicate ID
	queue.Push(task1)
	taskIDs = queue.TaskIDs()
	assert.Equal(t, 2, len(taskIDs))
}

func TestFIFOQueue_Pop(t *testing.T) {
	queue := NewFIFOQueue()

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

	poppedTask = queue.Pop()
	assert.Equal(t, int64(2), poppedTask.GetTaskID())
	assert.Equal(t, 0, len(queue.TaskIDs()))
}

func TestFIFOQueue_Get(t *testing.T) {
	queue := NewFIFOQueue()

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
	queue := NewFIFOQueue()

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
	assert.Equal(t, int64(1), taskIDs[0])
	assert.Equal(t, int64(3), taskIDs[1])

	// Verify task is actually removed
	assert.Nil(t, queue.Get(2))
}

func TestFIFOQueue_TaskIDs(t *testing.T) {
	queue := NewFIFOQueue()

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
