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

import (
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type TaskType int

const (
	PreImportTaskType TaskType = 0
	ImportTaskType    TaskType = 1
	ReshardTaskType   TaskType = 2
	ImportTaskV3Type  TaskType = 3
)

var ImportTaskTypeName = map[TaskType]string{
	0: "PreImportTask",
	1: "ImportTask",
	2: "ReshardTask",
	3: "ImportTaskV3",
}

func (t TaskType) String() string {
	return ImportTaskTypeName[t]
}

type ImportTaskFilter func(task ImportTask) bool

func WithType(taskType TaskType) ImportTaskFilter {
	return func(task ImportTask) bool {
		return task.GetType() == taskType
	}
}

func WithStates(states ...datapb.ImportTaskStateV2) ImportTaskFilter {
	return func(task ImportTask) bool {
		for _, state := range states {
			if task.GetState() == state {
				return true
			}
		}
		return false
	}
}

func WithRequestSource() ImportTaskFilter {
	return func(task ImportTask) bool {
		return task.GetSource() == datapb.ImportTaskSourceV2_Request
	}
}

type UpdateAction func(task ImportTask)

type taskStateSetter interface {
	setState(datapb.ImportTaskStateV2)
}

type taskReasonSetter interface {
	setReason(string)
}

type taskCompleteTimeSetter interface {
	setCompleteTime(string)
}

type taskNodeIDSetter interface {
	setNodeID(int64)
}

type taskRunIDSetter interface {
	setRunID(int64)
}

type taskLogRangeSetter interface {
	setLogRange(*datapb.IDRange)
}

type taskFileStatsSetter interface {
	setFileStats([]*datapb.ImportFileStats)
}

type taskSegmentIDsSetter interface {
	setSegmentIDs([]UniqueID)
}

type taskStatsSegmentIDsSetter interface {
	setStatsSegmentIDs([]UniqueID)
}

// Compile-time assertions documenting which task types implement which
// UpdateAction setters. When adding a task type, add it to the corresponding
// assertion group instead of touching every Update* function.
var (
	_ taskStateSetter           = (*preImportTask)(nil)
	_ taskStateSetter           = (*importTask)(nil)
	_ taskStateSetter           = (*reshardTask)(nil)
	_ taskStateSetter           = (*importTaskV3)(nil)
	_ taskReasonSetter          = (*preImportTask)(nil)
	_ taskReasonSetter          = (*importTask)(nil)
	_ taskReasonSetter          = (*reshardTask)(nil)
	_ taskReasonSetter          = (*importTaskV3)(nil)
	_ taskCompleteTimeSetter    = (*preImportTask)(nil)
	_ taskCompleteTimeSetter    = (*importTask)(nil)
	_ taskNodeIDSetter          = (*preImportTask)(nil)
	_ taskNodeIDSetter          = (*importTask)(nil)
	_ taskNodeIDSetter          = (*reshardTask)(nil)
	_ taskNodeIDSetter          = (*importTaskV3)(nil)
	_ taskRunIDSetter           = (*reshardTask)(nil)
	_ taskRunIDSetter           = (*importTaskV3)(nil)
	_ taskLogRangeSetter        = (*importTaskV3)(nil)
	_ taskFileStatsSetter       = (*preImportTask)(nil)
	_ taskSegmentIDsSetter      = (*importTask)(nil)
	_ taskSegmentIDsSetter      = (*importTaskV3)(nil)
	_ taskStatsSegmentIDsSetter = (*importTask)(nil)
)

func UpdateState(state datapb.ImportTaskStateV2) UpdateAction {
	return func(t ImportTask) {
		if setter, ok := t.(taskStateSetter); ok {
			setter.setState(state)
		}
	}
}

func UpdateReason(reason string) UpdateAction {
	return func(t ImportTask) {
		if setter, ok := t.(taskReasonSetter); ok {
			setter.setReason(reason)
		}
	}
}

func UpdateCompleteTime(completeTime string) UpdateAction {
	return func(t ImportTask) {
		if setter, ok := t.(taskCompleteTimeSetter); ok {
			setter.setCompleteTime(completeTime)
		}
	}
}

func UpdateNodeID(nodeID int64) UpdateAction {
	return func(t ImportTask) {
		if setter, ok := t.(taskNodeIDSetter); ok {
			setter.setNodeID(nodeID)
		}
	}
}

func UpdateRunID(runID int64) UpdateAction {
	return func(t ImportTask) {
		if setter, ok := t.(taskRunIDSetter); ok {
			setter.setRunID(runID)
		}
	}
}

func UpdateLogRange(logRange *datapb.IDRange) UpdateAction {
	return func(t ImportTask) {
		if setter, ok := t.(taskLogRangeSetter); ok {
			setter.setLogRange(logRange)
		}
	}
}

func UpdateFileStats(fileStats []*datapb.ImportFileStats) UpdateAction {
	return func(t ImportTask) {
		if setter, ok := t.(taskFileStatsSetter); ok {
			setter.setFileStats(fileStats)
		}
	}
}

func UpdateSegmentIDs(segmentIDs []UniqueID) UpdateAction {
	return func(t ImportTask) {
		if setter, ok := t.(taskSegmentIDsSetter); ok {
			setter.setSegmentIDs(segmentIDs)
		}
	}
}

func UpdateStatsSegmentIDs(segmentIDs []UniqueID) UpdateAction {
	return func(t ImportTask) {
		if setter, ok := t.(taskStatsSegmentIDsSetter); ok {
			setter.setStatsSegmentIDs(segmentIDs)
		}
	}
}

type ImportTask interface {
	task.Task
	GetJobID() int64
	GetTaskID() int64
	GetCollectionID() int64
	GetNodeID() int64
	GetType() TaskType
	GetState() datapb.ImportTaskStateV2
	GetReason() string
	GetFileStats() []*datapb.ImportFileStats
	GetTR() *timerecord.TimeRecorder
	Clone() ImportTask
	GetSource() datapb.ImportTaskSourceV2
}
