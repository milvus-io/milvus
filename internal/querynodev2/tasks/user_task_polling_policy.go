package tasks

import (
	"fmt"
	"time"

	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var _ schedulePolicy = &userTaskPollingPolicy{}

// newUserTaskPollingPolicy create a new user task polling schedule policy.
func newUserTaskPollingPolicy() *userTaskPollingPolicy {
	return &userTaskPollingPolicy{
		queue: newFairPollingTaskQueue(),
	}
}

// userTaskPollingPolicy is a user based polling schedule policy.
type userTaskPollingPolicy struct {
	queue *fairPollingTaskQueue
}

// Push add a new task into scheduler, an error will be returned if scheduler reaches some limit.
func (p *userTaskPollingPolicy) Push(task Task) (int, error) {
	pt := paramtable.Get()
	username := task.Username()

	// Try to merge task if task is mergeable.
	if t := tryIntoMergeTask(task); t != nil {
		// Try to merge with same group first.
		maxNQ := pt.QueryNodeCfg.MaxGroupNQ.GetAsInt64()
		if p.queue.tryMergeWithSameGroup(username, t, maxNQ) {
			return 0, nil
		}

		// Try to merge with other group if option is enabled.
		enableCrossGroupMerge := pt.QueryNodeCfg.SchedulePolicyEnableCrossUserGrouping.GetAsBool()
		if enableCrossGroupMerge && p.queue.tryMergeWithOtherGroup(username, t, maxNQ) {
			return 0, nil
		}
	}

	// Check if length of user queue is greater than limit.
	taskGroupLen := p.queue.groupLen(username)
	if taskGroupLen > 0 {
		limit := pt.QueryNodeCfg.SchedulePolicyMaxPendingTaskPerUser.GetAsInt()
		if limit > 0 && taskGroupLen >= limit {
			return 0, merr.WrapErrServiceRequestLimitExceeded(
				int32(limit),
				fmt.Sprintf("limit by %s", pt.QueryNodeCfg.SchedulePolicyMaxPendingTaskPerUser.Key),
			)
		}
	}

	// Add a new task into queue.
	p.queue.push(username, task)
	return 1, nil
}

// Pop get the task next ready to run.
func (p *userTaskPollingPolicy) Pop() Task {
	expire := paramtable.Get().QueryNodeCfg.SchedulePolicyTaskQueueExpire.GetAsDuration(time.Second)
	return p.queue.pop(expire)
}

// Len get ready task counts.
func (p *userTaskPollingPolicy) Len() int {
	return p.queue.len()
}
