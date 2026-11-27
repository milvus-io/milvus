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

package metrics

import "github.com/prometheus/client_golang/prometheus"

const subsystemNodeScheduler = "node_scheduler"

var (
	NodeSchedulerConcurrency = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subsystemNodeScheduler,
			Name:      "concurrency",
			Help:      "Configured maximum number of concurrently executing node scheduler tasks",
		}, []string{nodeIDLabelName})

	NodeSchedulerPendingTaskCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subsystemNodeScheduler,
			Name:      "pending_task_count",
			Help:      "Number of node scheduler tasks waiting in the queue or retry backoff",
		}, []string{nodeIDLabelName, TaskTypeLabel})

	NodeSchedulerRunningTaskCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: subsystemNodeScheduler,
			Name:      "running_task_count",
			Help:      "Number of node scheduler tasks currently executing",
		}, []string{nodeIDLabelName, TaskTypeLabel})

	NodeSchedulerTaskQueueDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: subsystemNodeScheduler,
			Name:      "task_queue_duration_seconds",
			Help:      "Time in the queue or retry backoff until execution or cancellation",
			Buckets:   prometheus.ExponentialBucketsRange(0.0001, 600, 24),
		}, []string{nodeIDLabelName, TaskTypeLabel})

	NodeSchedulerTaskExecutionDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: subsystemNodeScheduler,
			Name:      "task_execution_duration_seconds",
			Help:      "Time spent in each node scheduler task execution attempt",
			Buckets:   prometheus.ExponentialBucketsRange(0.0001, 600, 24),
		}, []string{nodeIDLabelName, TaskTypeLabel})

	NodeSchedulerTaskExecutionTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: subsystemNodeScheduler,
			Name:      "task_execution_total",
			Help:      "Total number of node scheduler task execution attempts by status",
		}, []string{nodeIDLabelName, TaskTypeLabel, statusLabelName})
)

func registerNodeScheduler(registry prometheus.Registerer) {
	registry.MustRegister(NodeSchedulerConcurrency)
	registry.MustRegister(NodeSchedulerPendingTaskCount)
	registry.MustRegister(NodeSchedulerRunningTaskCount)
	registry.MustRegister(NodeSchedulerTaskQueueDurationSeconds)
	registry.MustRegister(NodeSchedulerTaskExecutionDurationSeconds)
	registry.MustRegister(NodeSchedulerTaskExecutionTotal)
}
