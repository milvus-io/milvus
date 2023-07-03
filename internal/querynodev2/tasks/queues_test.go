package tasks

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMergeTaskQueue(t *testing.T) {
	q := newMergeTaskQueue("test_user")
	assert.Equal(t, 0, q.len())
	assert.Nil(t, q.front())
	q.pop()
	assert.Nil(t, q.front())
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
		q.push(task)
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
	q.push(task)
	for i := 1; i <= 20; i++ {
		task := newMockTask(mockTaskConfig{
			username:  "test_user",
			mergeAble: true,
			nq:        int64(i),
		})
		assert.Equal(t, i <= 10, q.tryMerge(tryIntoMergeTask(task), 56))
	}
	for i := 1; i <= 2; i++ {
		task := newMockTask(mockTaskConfig{
			username:  "test_user",
			mergeAble: true,
			nq:        int64(1),
		})
		q.push(task)
	}
	for i := 1; i <= 20; i++ {
		task := newMockTask(mockTaskConfig{
			username:  "test_user",
			mergeAble: true,
			nq:        int64(i),
		})
		// 2 + 1 + ... + 9 < 55
		// 1 + 10 + 11 + 12 + 13 < 55
		assert.Equal(t, i <= 13, q.tryMerge(tryIntoMergeTask(task), 55))
	}
}

func TestFairPollingTaskQueue(t *testing.T) {
	q := newFairPollingTaskQueue()
	assert.Equal(t, 0, q.len())
	assert.Equal(t, 0, q.groupLen(""))
	assert.Nil(t, q.pop(time.Second))
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
		q.push(username, task)
		assert.Equal(t, i, q.len())
		assert.Equal(t, (i-1)/10+1, q.groupLen(username))
	}
	assert.Equal(t, userN, len(q.route))

	// Test Pop
	for i := 1; i <= n; i++ {
		task := q.pop(time.Minute)
		assert.NotNil(t, task)
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
	q.push(username, task)
	assert.Equal(t, userN+1, len(q.route))
	assert.NotNil(t, q.pop(time.Second))
	assert.Equal(t, 1, len(q.route))

	// Test Merge.
	// test on empty queue.
	task = newMockTask(mockTaskConfig{
		username:  username,
		mergeAble: true,
	})
	assert.False(t, q.tryMergeWithSameGroup(username, tryIntoMergeTask(task), 1))
	assert.False(t, q.tryMergeWithOtherGroup(username, tryIntoMergeTask(task), 1))

	// Add basic user info first.
	for i := 1; i <= userN; i++ {
		username := fmt.Sprintf("user_%d", (i-1)%userN)
		task := newMockTask(mockTaskConfig{
			username:  username,
			mergeAble: true,
		})
		q.push(username, task)
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
		success := q.tryMergeWithSameGroup(username, tryIntoMergeTask(task), int64(n/userN))
		assert.Equal(t, i+userN <= n, success)
		assert.Equal(t, userN, q.len())
		assert.Equal(t, 1, q.groupLen(username))
	}

	// Try to merge with other group.
	task = newMockTask(mockTaskConfig{
		username:  username,
		mergeAble: true,
	})
	assert.False(t, q.tryMergeWithOtherGroup(username, tryIntoMergeTask(task), int64(n/userN)))
	assert.True(t, q.tryMergeWithOtherGroup(username, tryIntoMergeTask(task), int64(n/userN)+1))
	assert.Equal(t, 0, q.groupLen(username))
}
