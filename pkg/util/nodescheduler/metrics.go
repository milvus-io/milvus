// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nodescheduler

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TaskTypeProvider lets wrapper tasks expose the stable task type that should
// be used by node scheduler metrics.
type TaskTypeProvider interface {
	SchedulerTaskType() string
}

// TaskTypeName returns the stable, low-cardinality type name used by node
// scheduler metrics.
func TaskTypeName(task Task) string {
	if provider, ok := task.(TaskTypeProvider); ok {
		if taskType := provider.SchedulerTaskType(); taskType != "" {
			return taskType
		}
	}
	if task == nil {
		return "<nil>"
	}
	return reflect.TypeOf(task).String()
}

// schedulerMetrics keeps each instance's live gauge contributions so a scheduler
// created before session registration can move them to its assigned node ID.
// Observations made before registration keep their original label.
type schedulerMetrics struct {
	mu          sync.Mutex
	nodeID      string
	concurrency int
	pending     map[string]int
	running     map[string]int
}

func newSchedulerMetrics() *schedulerMetrics {
	return &schedulerMetrics{
		nodeID:  paramtable.GetStringNodeID(),
		pending: make(map[string]int),
		running: make(map[string]int),
	}
}

func (m *schedulerMetrics) refreshNodeIDLocked() {
	nodeID := paramtable.GetStringNodeID()
	if nodeID == m.nodeID {
		return
	}
	metrics.NodeSchedulerConcurrency.WithLabelValues(m.nodeID).Sub(float64(m.concurrency))
	metrics.NodeSchedulerConcurrency.WithLabelValues(nodeID).Add(float64(m.concurrency))
	for taskType, count := range m.pending {
		metrics.NodeSchedulerPendingTaskCount.WithLabelValues(m.nodeID, taskType).Sub(float64(count))
		metrics.NodeSchedulerPendingTaskCount.WithLabelValues(nodeID, taskType).Add(float64(count))
	}
	for taskType, count := range m.running {
		metrics.NodeSchedulerRunningTaskCount.WithLabelValues(m.nodeID, taskType).Sub(float64(count))
		metrics.NodeSchedulerRunningTaskCount.WithLabelValues(nodeID, taskType).Add(float64(count))
	}
	m.nodeID = nodeID
}

func (m *schedulerMetrics) refreshNodeID() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshNodeIDLocked()
}

func (m *schedulerMetrics) adjustConcurrency(delta int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshNodeIDLocked()
	m.concurrency += delta
	metrics.NodeSchedulerConcurrency.WithLabelValues(m.nodeID).Add(float64(delta))
}

func (m *schedulerMetrics) observeEnqueued(taskType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshNodeIDLocked()
	m.pending[taskType]++
	metrics.NodeSchedulerPendingTaskCount.WithLabelValues(m.nodeID, taskType).Inc()
}

func (m *schedulerMetrics) observeDequeued(taskType string, queueDuration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshNodeIDLocked()
	m.pending[taskType]--
	metrics.NodeSchedulerPendingTaskCount.WithLabelValues(m.nodeID, taskType).Dec()
	metrics.NodeSchedulerTaskQueueDurationSeconds.WithLabelValues(m.nodeID, taskType).Observe(queueDuration.Seconds())
}

func (m *schedulerMetrics) observeExecutionStarted(taskType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshNodeIDLocked()
	m.running[taskType]++
	metrics.NodeSchedulerRunningTaskCount.WithLabelValues(m.nodeID, taskType).Inc()
}

func (m *schedulerMetrics) observeExecutionFinished(taskType, status string, executionDuration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshNodeIDLocked()
	m.running[taskType]--
	metrics.NodeSchedulerRunningTaskCount.WithLabelValues(m.nodeID, taskType).Dec()
	metrics.NodeSchedulerTaskExecutionDurationSeconds.WithLabelValues(m.nodeID, taskType).Observe(executionDuration.Seconds())
	metrics.NodeSchedulerTaskExecutionTotal.WithLabelValues(m.nodeID, taskType, status).Inc()
}

func taskExecutionStatus(ctxErr, taskErr error) string {
	if ctxErr != nil {
		return metrics.CancelLabel
	}
	if taskErr == nil {
		return metrics.SuccessLabel
	}
	switch {
	case errors.Is(taskErr, context.Canceled),
		errors.Is(taskErr, context.DeadlineExceeded):
		return metrics.CancelLabel
	case errors.Is(taskErr, ErrDelay):
		return metrics.RetryLabel
	default:
		return metrics.FailLabel
	}
}
