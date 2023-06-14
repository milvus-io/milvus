package tasks

import (
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
)

func TestUserTaskPollingPolicy(t *testing.T) {
	paramtable.Init()
	testCommonPolicyOperation(t, newUserTaskPollingPolicy())
	testCrossUserMerge(t, newUserTaskPollingPolicy())
}

func TestFIFOPolicy(t *testing.T) {
	paramtable.Init()
	testCommonPolicyOperation(t, newFIFOPolicy())
}

func testCrossUserMerge(t *testing.T, policy schedulePolicy) {
	userN := 10
	maxNQ := paramtable.Get().QueryNodeCfg.MaxGroupNQ.GetAsInt64()
	// Do not open cross user merge.
	n := userN * 4
	for i := 1; i <= n; i++ {
		username := fmt.Sprintf("user_%d", (i-1)%userN)
		task := newMockTask(mockTaskConfig{
			username:  username,
			nq:        maxNQ / 2,
			mergeAble: true,
		})
		policy.Push(task)
	}
	nAfterMerge := n / 2
	assert.Equal(t, nAfterMerge, policy.Len())
	for i := 1; i <= nAfterMerge; i++ {
		assert.NotNil(t, policy.Pop())
		assert.Equal(t, nAfterMerge-i, policy.Len())
	}

	// Open cross user grouping
	paramtable.Get().QueryNodeCfg.SchedulePolicyEnableCrossUserGrouping.SwapTempValue("true")
	for i := 1; i <= n; i++ {
		username := fmt.Sprintf("user_%d", (i-1)%userN)
		task := newMockTask(mockTaskConfig{
			username:  username,
			nq:        maxNQ / 4,
			mergeAble: true,
		})
		policy.Push(task)
	}
	nAfterMerge = n / 4
	assert.Equal(t, nAfterMerge, policy.Len())
	for i := 1; i <= nAfterMerge; i++ {
		assert.NotNil(t, policy.Pop())
		assert.Equal(t, nAfterMerge-i, policy.Len())
	}
}

// testCommonPolicyOperation
func testCommonPolicyOperation(t *testing.T, policy schedulePolicy) {
	// Empty policy assertion.
	assert.Equal(t, 0, policy.Len())
	assert.Nil(t, policy.Pop())
	assert.Equal(t, 0, policy.Len())

	// Test no merge push pop.
	n := 50
	userN := 10
	// Test Push
	for i := 1; i <= n; i++ {
		username := fmt.Sprintf("user_%d", (i-1)%userN)
		task := newMockTask(mockTaskConfig{
			username: username,
		})
		policy.Push(task)
		assert.Equal(t, i, policy.Len())
	}
	// Test Pop
	for i := 1; i <= n; i++ {
		assert.NotNil(t, policy.Pop())
		assert.Equal(t, n-i, policy.Len())
	}

	// Test with merge
	maxNQ := paramtable.Get().QueryNodeCfg.MaxGroupNQ.GetAsInt64()
	// cannot merge if the nq is gte than maxNQ
	for i := 1; i <= n; i++ {
		username := fmt.Sprintf("user_%d", (i-1)%userN)
		task := newMockTask(mockTaskConfig{
			username:  username,
			nq:        maxNQ,
			mergeAble: true,
		})
		policy.Push(task)
	}
	assert.Equal(t, n, policy.Len())
	for i := 1; i <= n; i++ {
		assert.NotNil(t, policy.Pop())
		assert.Equal(t, n-i, policy.Len())
	}

	// Merge half MaxNQ
	n = userN * 2
	for i := 1; i <= n; i++ {
		username := fmt.Sprintf("user_%d", (i-1)%userN)
		task := newMockTask(mockTaskConfig{
			username:  username,
			nq:        maxNQ / 2,
			mergeAble: true,
		})
		policy.Push(task)
	}
	nAfterMerge := n / 2
	assert.Equal(t, nAfterMerge, policy.Len())
	for i := 1; i <= nAfterMerge; i++ {
		assert.NotNil(t, policy.Pop())
		assert.Equal(t, nAfterMerge-i, policy.Len())
	}
}
