package task

import (
	"fmt"
	"slices"
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

type priorityQueueTestTask struct {
	Task
	id int64
}

func (t *priorityQueueTestTask) GetTaskID() int64 { return t.id }

func TestPriorityQueue_MixedOperationsPreserveTasksAndPriority(t *testing.T) {
	queue := NewPriorityQueuePolicy()
	remaining := make(map[int64]Task)
	check := func() {
		t.Helper()
		ids := make([]int64, 0, len(remaining))
		for id, task := range remaining {
			ids = append(ids, id)
			assert.Same(t, task, queue.Get(id))
		}
		slices.Sort(ids)
		assert.Equal(t, len(ids), queue.TaskCount())
		before := queue.TaskIDs()
		assert.Equal(t, ids, queue.TaskIDsByPriority())
		assert.Equal(t, before, queue.TaskIDs(), "snapshot must leave the live queue untouched")
	}
	push := func(id int64) {
		t.Helper()
		task := &priorityQueueTestTask{id: id}
		queue.Push(task)
		if _, exists := remaining[id]; !exists {
			remaining[id] = task
		}
		check()
	}
	remove := func(id int64) {
		t.Helper()
		queue.Remove(id)
		delete(remaining, id)
		assert.Nil(t, queue.Get(id))
		check()
	}

	for _, id := range []int64{7, 2, 9, 1, 6, 8, 4, 3, 5} {
		push(id)
	}
	push(2) // A different object with the same ID must not replace the queued task.
	for _, id := range []int64{4, 1, 9, 99} {
		remove(id)
	}
	push(4) // Reusing a removed ID must use its new heap position.
	popped := queue.Pop()
	require.NotNil(t, popped)
	assert.Equal(t, int64(2), popped.GetTaskID())
	delete(remaining, popped.GetTaskID())
	check()
	remove(8)
	push(0)
	for queue.TaskCount() > 0 {
		ids := queue.TaskIDsByPriority()
		popped := queue.Pop()
		require.NotNil(t, popped)
		assert.Equal(t, ids[0], popped.GetTaskID())
		delete(remaining, popped.GetTaskID())
		check()
	}
	assert.Nil(t, queue.Pop())
}

func TestPriorityQueue_ConcurrentSnapshotsAndUpdates(t *testing.T) {
	queue := NewPriorityQueuePolicy()
	const taskCount = 64
	expected := make([]int64, taskCount)
	for id := range taskCount {
		expected[id] = int64(id)
		queue.Push(&priorityQueueTestTask{id: int64(id)})
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			ids := queue.TaskIDsByPriority()
			for i := 1; i < len(ids); i++ {
				assert.Less(t, ids[i-1], ids[i], "a snapshot must retain unique tasks in priority order")
			}
		}
	}()
	for i := range 100 {
		id := int64(i % taskCount)
		queue.Remove(id)
		queue.Push(&priorityQueueTestTask{id: id})
	}
	<-done

	assert.Equal(t, taskCount, queue.TaskCount())
	assert.Equal(t, expected, queue.TaskIDsByPriority())
	for _, id := range expected {
		assert.NotNil(t, queue.Get(id))
	}
}

func benchmarkPriorityQueueRemove(b *testing.B, size int, removeFirst bool) {
	queue := NewPriorityQueuePolicy()
	for id := range size {
		queue.Push(&priorityQueueTestTask{id: int64(id)})
	}
	id := int64(size - 1)
	if removeFirst {
		id = 0
	}
	task := queue.Get(id)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		queue.Remove(id)
		queue.Push(task)
	}
	b.StopTimer()
	require.Equal(b, size, queue.TaskCount())
	require.Same(b, task, queue.Get(id))
}

func BenchmarkPriorityQueueRemove(b *testing.B) {
	for _, size := range []int{1000, 10000, 100000} {
		for _, removeFirst := range []bool{true, false} {
			b.Run(fmt.Sprintf("tasks=%d/first=%t", size, removeFirst), func(b *testing.B) {
				benchmarkPriorityQueueRemove(b, size, removeFirst)
			})
		}
	}
}
