// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

// The import V3 control-plane observability surface. The label constant for a
// job (job_version) or a task (task_type) is curried once, so the state
// machine and the task adapters report semantic events instead of building
// label sets at every call site. The metric names and label values match the
// legacy import path so both share the same series.

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// importV3JobMetrics reports one job's stage and total latency.
type importV3JobMetrics struct {
	version string
}

func newImportV3JobMetrics(job ImportJob) *importV3JobMetrics {
	return &importV3JobMetrics{version: job.GetVersion().String()}
}

func (m *importV3JobMetrics) observeStage(stage string, d time.Duration) {
	metrics.ImportJobLatency.WithLabelValues(stage, m.version).Observe(float64(d.Milliseconds()))
}

func (m *importV3JobMetrics) observeTotal(d time.Duration) {
	m.observeStage(metrics.TotalLabel, d)
}

// importTaskMetrics reports one task's stage latency, keyed by its task type.
type importTaskMetrics struct {
	taskType string
}

func newImportTaskMetrics(taskType TaskType) importTaskMetrics {
	return importTaskMetrics{taskType: taskType.String()}
}

func (m importTaskMetrics) observe(stage string, d time.Duration) {
	metrics.ImportTaskLatency.WithLabelValues(stage, m.taskType).Observe(float64(d.Milliseconds()))
}

// importV3Stats reports the checker's per-tick job and task state snapshots.
type importV3Stats struct{}

func (importV3Stats) setJobState(state string, version datapb.ImportJobVersion, num int) {
	metrics.ImportJobs.WithLabelValues(state, version.String()).Set(float64(num))
}

func (importV3Stats) setTaskCount(taskType TaskType, state datapb.ImportTaskStateV2, num int) {
	metrics.ImportTasks.WithLabelValues(taskType.String(), state.String()).Set(float64(num))
}
