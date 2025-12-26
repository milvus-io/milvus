package scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var _ schedulePolicy = &userTaskPollingPolicy{}

// newUserTaskPollingPolicy create a new user task polling schedule policy.
func newUserTaskPollingPolicy() *userTaskPollingPolicy {
	cpuPoolSize := paramtable.Get().QueryNodeCfg.MaxReadConcurrency.GetAsInt64()
	gpuPoolSize := paramtable.Get().QueryNodeCfg.MaxGpuReadConcurrency.GetAsInt64()
	return &userTaskPollingPolicy{
		queue: newFairPollingTaskQueue(),
		metrics: &userTaskPollingPolicyExecutingMetrics{
			cpuPoolSize: cpuPoolSize,
			gpuPoolSize: gpuPoolSize,
			cpuCounts:   make(map[string]int64),
			gpuCounts:   make(map[string]int64),
		},
	}
}

// userTaskPollingPolicy is a user based polling schedule policy.
type userTaskPollingPolicy struct {
	queue   *fairPollingTaskQueue
	metrics *userTaskPollingPolicyExecutingMetrics
}

type userTaskPollingPolicyExecutingMetrics struct {
	mu sync.Mutex

	cpuPoolSize int64
	gpuPoolSize int64
	cpuCounts   map[string]int64
	gpuCounts   map[string]int64
}

func (m *userTaskPollingPolicyExecutingMetrics) Incr(t Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t.IsGpuIndex() {
		m.gpuCounts[t.Username()]++
	} else {
		m.cpuCounts[t.Username()]++
	}
}

func (m *userTaskPollingPolicyExecutingMetrics) Decr(t Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t.IsGpuIndex() {
		m.gpuCounts[t.Username()]--
		if m.gpuCounts[t.Username()] == 0 {
			delete(m.gpuCounts, t.Username())
		}
	} else {
		m.cpuCounts[t.Username()]--
		if m.cpuCounts[t.Username()] == 0 {
			delete(m.cpuCounts, t.Username())
		}
	}
}

func (m *userTaskPollingPolicyExecutingMetrics) IsExceedConcurrentLimit(t Task) bool {
	ratio := paramtable.Get().QueryNodeCfg.SchedulePolicyMaxConcurrentRatioPerUser.GetAsFloat()

	m.mu.Lock()
	defer m.mu.Unlock()

	var current, limit int64
	if t.IsGpuIndex() {
		limit = int64(ratio * float64(m.gpuPoolSize))
		current = m.gpuCounts[t.Username()]
	} else {
		limit = int64(ratio * float64(m.cpuPoolSize))
		current = m.cpuCounts[t.Username()]
	}
	if limit <= 0 {
		limit = 1
	}
	return current >= limit
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
func (p *userTaskPollingPolicy) Pop() Task {
	expire := paramtable.Get().QueryNodeCfg.SchedulePolicyTaskQueueExpire.GetAsDuration(time.Second)
	task := p.queue.pop(expire, p.metrics.IsExceedConcurrentLimit)
	if task == nil {
		return nil
	}
	p.metrics.Incr(task)
	return &taskWrapper{
		Task: task,
		callbackWhenDone: func() {
			p.metrics.Decr(task)
		},
	}
}

// Len get ready task counts.
func (p *userTaskPollingPolicy) Len() int {
	return p.queue.len()
}
