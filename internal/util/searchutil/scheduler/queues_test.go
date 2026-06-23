package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMergeTaskQueue(t *testing.T) {
	q := newMergeTaskQueue("test_user")
	assert.Equal(t, 0, q.len())
	assert.False(t, q.front().valid())
	q.pop()
	assert.False(t, q.front().valid())
	assert.Equal(t, 0, q.len())
	assert.False(t, q.expire(5*time.Second))
	time.Sleep(1 * time.Second)
	assert.True(t, q.expire(1*time.Second))

	// Test push.
	n := 50
	for i := 1; i <= n; i++ {
		task := newMockTask(mockTaskConfig{
			username: "test_user",
		})
		q.push(newQueuedTask(task, time.Now()))
		assert.Equal(t, i, q.len())
		assert.False(t, q.expire(time.Second))
	}

	// Test Pop.
	for i := 0; i < n; i++ {
		q.pop()
		assert.Equal(t, n-(i+1), q.len())
		assert.False(t, q.expire(time.Second))
	}
	time.Sleep(time.Second)
	assert.Equal(t, q.expire(time.Second), true)

	// Test Merge.
	task := newMockTask(mockTaskConfig{
		username:  "test_user",
		mergeAble: true,
		nq:        1,
	})
	q.push(newQueuedTask(task, time.Now()))
	for i := 1; i <= 20; i++ {
		task := newMockTask(mockTaskConfig{
			username:  "test_user",
			mergeAble: true,
			nq:        int64(i),
		})
		assert.Equal(t, i <= 10, q.tryMerge(newQueuedTask(task, time.Now()), 56, 0, 0))
	}
	for i := 1; i <= 2; i++ {
		task := newMockTask(mockTaskConfig{
			username:  "test_user",
			mergeAble: true,
			nq:        int64(1),
		})
		q.push(newQueuedTask(task, time.Now()))
	}
	for i := 1; i <= 20; i++ {
		task := newMockTask(mockTaskConfig{
			username:  "test_user",
			mergeAble: true,
			nq:        int64(i),
		})
		// 2 + 1 + ... + 9 < 55
		// 1 + 10 + 11 + 12 + 13 < 55
		assert.Equal(t, i <= 13, q.tryMerge(newQueuedTask(task, time.Now()), 55, 0, 0))
	}
}

func TestMergeTaskQueueNQMergeRatio(t *testing.T) {
	q := newMergeTaskQueue("test_user")
	q.push(newQueuedTask(newMockTask(mockTaskConfig{
		username:  "test_user",
		mergeAble: true,
		nq:        2,
	}), time.Now()))

	assert.True(t, q.tryMerge(newQueuedTask(newMockTask(mockTaskConfig{
		username:  "test_user",
		mergeAble: true,
		nq:        4,
	}), time.Now()), 16, 3, 0))

	assert.False(t, q.tryMerge(newQueuedTask(newMockTask(mockTaskConfig{
		username:  "test_user",
		mergeAble: true,
		nq:        4,
	}), time.Now()), 16, 3, 0))
	assert.Equal(t, int64(6), q.front().NQ())
}

func TestMergeTaskQueueDeadlineMergeGap(t *testing.T) {
	now := time.Now()
	baseDeadline := now.Add(time.Second)
	baseCtx, baseCancel := context.WithDeadline(context.Background(), baseDeadline)
	defer baseCancel()

	newTask := func(ctx context.Context, nq int64) *queuedTask {
		return newQueuedTask(newMockTask(mockTaskConfig{
			ctx:       ctx,
			username:  "test_user",
			mergeAble: true,
			nq:        nq,
		}), now)
	}

	t.Run("deadline gap too large", func(t *testing.T) {
		q := newMergeTaskQueue("test_user")
		q.push(newTask(baseCtx, 2))
		shortCtx, shortCancel := context.WithDeadline(context.Background(), baseDeadline.Add(-100*time.Millisecond))
		defer shortCancel()

		assert.False(t, q.tryMerge(newTask(shortCtx, 2), 16, 0, 50*time.Millisecond))
		assert.Equal(t, int64(2), q.front().NQ())
	})

	t.Run("deadline gap within threshold", func(t *testing.T) {
		q := newMergeTaskQueue("test_user")
		q.push(newTask(baseCtx, 2))
		closeCtx, closeCancel := context.WithDeadline(context.Background(), baseDeadline.Add(-40*time.Millisecond))
		defer closeCancel()

		assert.True(t, q.tryMerge(newTask(closeCtx, 2), 16, 0, 50*time.Millisecond))
		assert.Equal(t, int64(4), q.front().NQ())
	})

	t.Run("one side missing deadline", func(t *testing.T) {
		q := newMergeTaskQueue("test_user")
		q.push(newTask(baseCtx, 2))

		assert.False(t, q.tryMerge(newTask(context.Background(), 2), 16, 0, 50*time.Millisecond))
		assert.Equal(t, int64(2), q.front().NQ())
	})

	t.Run("both sides missing deadline", func(t *testing.T) {
		q := newMergeTaskQueue("test_user")
		q.push(newTask(context.Background(), 2))

		assert.True(t, q.tryMerge(newTask(context.Background(), 2), 16, 0, 50*time.Millisecond))
		assert.Equal(t, int64(4), q.front().NQ())
	})
}

func TestMergeTaskQueueCleanupSkipsRewriteWithoutExpiredTask(t *testing.T) {
	q := newMergeTaskQueue("test_user")
	now := time.Now()
	for i := 0; i < 3; i++ {
		task := newMockTask(mockTaskConfig{username: "test_user"})
		q.push(newQueuedTask(task, now))
	}

	firstTask := q.tasks[0].Task
	removed := q.cleanup(now)

	assert.Empty(t, removed)
	assert.Equal(t, 3, q.len())
	assert.Same(t, firstTask, q.tasks[0].Task)
}

func TestMergeTaskQueueCleanupMarksExpiredTasksInPlace(t *testing.T) {
	q := newMergeTaskQueue("test_user")
	now := time.Now()
	later := now.Add(time.Minute)

	ctx1, cancel1 := context.WithDeadline(context.Background(), later)
	defer cancel1()
	ctx2, cancel2 := context.WithDeadline(context.Background(), now.Add(-time.Millisecond))
	defer cancel2()
	ctx3, cancel3 := context.WithDeadline(context.Background(), later)
	defer cancel3()

	q.push(newQueuedTask(newMockTask(mockTaskConfig{ctx: ctx1, username: "test_user"}), now))
	expiredTask := newMockTask(mockTaskConfig{ctx: ctx2, username: "test_user"})
	q.push(newQueuedTask(expiredTask, now))
	q.push(newQueuedTask(newMockTask(mockTaskConfig{ctx: ctx3, username: "test_user"}), now))

	originalQueueLen := len(q.tasks)
	removed := q.cleanup(now)

	assert.Len(t, removed, 1)
	assert.Same(t, expiredTask, removed[0].Task)
	assert.Equal(t, 2, q.len())
	assert.Equal(t, originalQueueLen, len(q.tasks))
	assert.True(t, q.pop().valid())
	assert.True(t, q.pop().valid())
	assert.False(t, q.pop().valid())
	assert.Equal(t, 0, q.len())
}

func TestMergeTaskQueuePopClearsPoppedSlot(t *testing.T) {
	q := newMergeTaskQueue("test_user")
	task := newMockTask(mockTaskConfig{username: "test_user"})
	q.push(newQueuedTask(task, time.Now()))

	tasks := q.tasks
	popped := q.pop()

	assert.True(t, popped.valid())
	assert.False(t, tasks[0].valid())
	assert.Equal(t, 0, q.len())
}

func TestFairPollingTaskQueue(t *testing.T) {
	q := newFairPollingTaskQueue()
	assert.Equal(t, 0, q.len())
	assert.Equal(t, 0, q.groupLen(""))
	assert.False(t, q.pop(time.Second).valid())
	assert.Equal(t, 0, q.len())
	assert.Equal(t, 0, q.groupLen(""))

	n := 50
	userN := 10
	// Test Push
	for i := 1; i <= n; i++ {
		username := fmt.Sprintf("user_%d", (i-1)%userN)
		task := newMockTask(mockTaskConfig{
			username: username,
		})
		q.push(username, newQueuedTask(task, time.Now()))
		assert.Equal(t, i, q.len())
		assert.Equal(t, (i-1)/10+1, q.groupLen(username))
	}
	assert.Equal(t, userN, len(q.route))

	// Test Pop
	for i := 1; i <= n; i++ {
		task := q.pop(time.Minute)
		assert.True(t, task.valid())
		username := task.Username()
		expectedUserName := fmt.Sprintf("user_%d", (i-1)%userN)
		assert.Equal(t, n-i, q.len())
		assert.Equal(t, expectedUserName, username)
	}

	// Test Expire inner queue.
	assert.Equal(t, userN, len(q.route))
	time.Sleep(time.Second)
	username := "test_user"
	task := newMockTask(mockTaskConfig{
		username: username,
	})
	q.push(username, newQueuedTask(task, time.Now()))
	assert.Equal(t, userN+1, len(q.route))
	assert.True(t, q.pop(time.Second).valid())
	assert.Equal(t, 1, len(q.route))

	// Test Merge.
	// test on empty queue.
	task = newMockTask(mockTaskConfig{
		username:  username,
		mergeAble: true,
	})
	assert.False(t, q.tryMergeWithSameGroup(username, newQueuedTask(task, time.Now()), 1, 0, 0))
	assert.False(t, q.tryMergeWithOtherGroup(username, newQueuedTask(task, time.Now()), 1, 0, 0))

	// Add basic user info first.
	for i := 1; i <= userN; i++ {
		username := fmt.Sprintf("user_%d", (i-1)%userN)
		task := newMockTask(mockTaskConfig{
			username:  username,
			mergeAble: true,
		})
		q.push(username, newQueuedTask(task, time.Now()))
		assert.Equal(t, i, q.len())
		assert.Equal(t, 1, q.groupLen(username))
	}

	// Try to merge with same group.
	for i := 1; i <= n; i++ {
		username := fmt.Sprintf("user_%d", (i-1)%userN)
		task := newMockTask(mockTaskConfig{
			username:  username,
			mergeAble: true,
		})
		success := q.tryMergeWithSameGroup(username, newQueuedTask(task, time.Now()), int64(n/userN), 0, 0)
		assert.Equal(t, i+userN <= n, success)
		assert.Equal(t, userN, q.len())
		assert.Equal(t, 1, q.groupLen(username))
	}

	// Try to merge with other group.
	task = newMockTask(mockTaskConfig{
		username:  username,
		mergeAble: true,
	})
	assert.False(t, q.tryMergeWithOtherGroup(username, newQueuedTask(task, time.Now()), int64(n/userN), 0, 0))
	assert.True(t, q.tryMergeWithOtherGroup(username, newQueuedTask(task, time.Now()), int64(n/userN)+1, 0, 0))
	assert.Equal(t, 0, q.groupLen(username))
}
