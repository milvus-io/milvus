package scheduler

import (
	"fmt"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func (p *userTaskPollingPolicy) Cleanup(now time.Time) []*queuedTask {
	return p.queue.cleanup(now)
}

func (p *userTaskPollingPolicy) Remove(filter TaskFilter, now time.Time) []*queuedTask {
	return p.queue.remove(filter, now)
}

// Push add a new task into scheduler, an error will be returned if scheduler reaches some limit.
func (p *userTaskPollingPolicy) Push(task *queuedTask) (int, error) {
	pt := paramtable.Get()
	username := task.Username()

	// Try to merge task if task is mergeable.
	if t := tryIntoMergeTask(task.Task); t != nil {
		// Try to merge with same group first.
		maxNQ := pt.QueryNodeCfg.MaxGroupNQ.GetAsInt64()
		nqMergeRatio := pt.QueryNodeCfg.NQMergeRatio.GetAsFloat()
		maxDeadlineMergeGap := pt.QueryNodeCfg.MaxDeadlineMergeGap.GetAsDurationByParse()
		if p.queue.tryMergeWithSameGroup(username, task, maxNQ, nqMergeRatio, maxDeadlineMergeGap) {
			return 0, nil
		}

		// Try to merge with other group if option is enabled.
		enableCrossGroupMerge := pt.QueryNodeCfg.SchedulePolicyEnableCrossUserGrouping.GetAsBool()
		if enableCrossGroupMerge && p.queue.tryMergeWithOtherGroup(username, task, maxNQ, nqMergeRatio, maxDeadlineMergeGap) {
			return 0, nil
		}
	}

	// Check if length of user queue is greater than limit.
	taskGroupLen := p.queue.groupLen(username)
	if taskGroupLen > 0 {
		limit := pt.QueryNodeCfg.SchedulePolicyMaxPendingTaskPerUser.GetAsInt()
		if limit > 0 && taskGroupLen >= limit {
			return 0, merr.WrapErrTooManyRequests(
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
func (p *userTaskPollingPolicy) Pop(now time.Time) *queuedTask {
	expire := paramtable.Get().QueryNodeCfg.SchedulePolicyTaskQueueExpire.GetAsDuration(time.Second)
	return p.queue.pop(expire)
}

// Len get ready task counts.
func (p *userTaskPollingPolicy) Len() int {
	return p.queue.len()
}
