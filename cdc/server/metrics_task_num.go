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

package server

import (
	"github.com/milvus-io/milvus/cdc/server/model/meta"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type TaskNumMetric struct {
	metricDesc *prometheus.Desc
	initialNum int
	runningNum int
	pauseNum   int
}

func (t *TaskNumMetric) AddInitial() {
	t.initialNum++
}

// add it should be adapted if you modify the meta.MinTaskState or the meta.MaxTaskState
func (t *TaskNumMetric) add(s meta.TaskState, errTip string) {
	innerLog := log.With(zap.String("tip", errTip), zap.Int("state", int(s)))
	if !s.IsValidTaskState() {
		innerLog.Warn("invalid task state")
		return
	}
	switch s {
	case meta.TaskStateInitial:
		t.initialNum++
	case meta.TaskStateRunning:
		t.runningNum++
	case meta.TaskStatePaused:
		t.pauseNum++
	default:
		innerLog.Warn("add, not handle the state")
		return
	}
}

// reduce it should be adapted if you modify the meta.MinTaskState or the meta.MaxTaskState
func (t *TaskNumMetric) reduce(s meta.TaskState, errTip string) {
	innerLog := log.With(zap.String("tip", errTip), zap.Int("state", int(s)))
	if !s.IsValidTaskState() {
		innerLog.Warn("invalid task state")
		return
	}
	switch s {
	case meta.TaskStateInitial:
		t.initialNum--
	case meta.TaskStateRunning:
		t.runningNum--
	case meta.TaskStatePaused:
		t.pauseNum--
	default:
		innerLog.Warn("reduce, not handle the state", zap.Int("state", int(s)))
		return
	}
}

func (t *TaskNumMetric) UpdateState(newState meta.TaskState, oldStates meta.TaskState) {
	t.add(newState, "update state, new state")
	t.reduce(oldStates, "update state, old state")
}

func (t *TaskNumMetric) Delete(state meta.TaskState) {
	t.reduce(state, "delete")
}

// Describe it should be adapted if you modify the meta.MinTaskState or the meta.MaxTaskState
func (t *TaskNumMetric) Describe(descs chan<- *prometheus.Desc) {
	descs <- t.metricDesc
	descs <- t.metricDesc
	descs <- t.metricDesc
}

// Collect it should be adapted if you modify the meta.MinTaskState or the meta.MaxTaskState
func (t *TaskNumMetric) Collect(metrics chan<- prometheus.Metric) {
	metrics <- prometheus.MustNewConstMetric(t.metricDesc, prometheus.GaugeValue,
		float64(t.initialNum), meta.TaskStateInitial.String())
	metrics <- prometheus.MustNewConstMetric(t.metricDesc, prometheus.GaugeValue,
		float64(t.runningNum), meta.TaskStateRunning.String())
	metrics <- prometheus.MustNewConstMetric(t.metricDesc, prometheus.GaugeValue,
		float64(t.pauseNum), meta.TaskStatePaused.String())
}
