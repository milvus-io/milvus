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
	"google.golang.org/protobuf/proto"

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

func WithJob(jobID int64) ImportTaskFilter {
	return func(task ImportTask) bool {
		return task.GetJobID() == jobID
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

func UpdateState(state datapb.ImportTaskStateV2) UpdateAction {
	return func(t ImportTask) {
		switch t.GetType() {
		case PreImportTaskType:
			t.(*preImportTask).task.Load().State = state
		case ImportTaskType:
			t.(*importTask).task.Load().State = state
		case ReshardTaskType:
			t.(*reshardTask).setState(state)
		case ImportTaskV3Type:
			t.(*importTaskV3).setState(state)
		}
	}
}

func UpdateReason(reason string) UpdateAction {
	return func(t ImportTask) {
		switch t.GetType() {
		case PreImportTaskType:
			t.(*preImportTask).task.Load().Reason = reason
		case ImportTaskType:
			t.(*importTask).task.Load().Reason = reason
		case ReshardTaskType:
			t.(*reshardTask).task.Load().Reason = reason
		case ImportTaskV3Type:
			t.(*importTaskV3).task.Load().Reason = reason
		}
	}
}

func UpdateCompleteTime(completeTime string) UpdateAction {
	return func(t ImportTask) {
		switch t.GetType() {
		case PreImportTaskType:
			t.(*preImportTask).task.Load().CompleteTime = completeTime
		case ImportTaskType:
			t.(*importTask).task.Load().CompleteTime = completeTime
		}
	}
}

func UpdateNodeID(nodeID int64) UpdateAction {
	return func(t ImportTask) {
		switch t.GetType() {
		case PreImportTaskType:
			t.(*preImportTask).task.Load().NodeID = nodeID
		case ImportTaskType:
			t.(*importTask).task.Load().NodeID = nodeID
		case ReshardTaskType:
			t.(*reshardTask).task.Load().NodeId = nodeID
		case ImportTaskV3Type:
			t.(*importTaskV3).task.Load().NodeId = nodeID
		}
	}
}

func updateV3FailureCode(code int32) UpdateAction {
	return func(t ImportTask) {
		switch t.GetType() {
		case ReshardTaskType:
			t.(*reshardTask).task.Load().FailureCode = code
		case ImportTaskV3Type:
			t.(*importTaskV3).task.Load().FailureCode = code
		}
	}
}

func UpdateV3Result(ref string, digest []byte) UpdateAction {
	return func(t ImportTask) {
		switch t.GetType() {
		case ReshardTaskType:
			task := t.(*reshardTask).task.Load()
			task.ResultRef = ref
			task.ResultDigest = append([]byte(nil), digest...)
		case ImportTaskV3Type:
			task := t.(*importTaskV3).task.Load()
			task.ResultRef = ref
			task.ResultDigest = append([]byte(nil), digest...)
		}
	}
}

func updateReshardRun(runID int64, outputPrefix string) UpdateAction {
	return func(t ImportTask) {
		task := t.(*reshardTask).task.Load()
		task.RunId = runID
		task.OutputPrefix = outputPrefix
		task.NodeId = NullNodeID
		task.State = datapb.ReshardTask_Pending
		task.ResultRef = ""
		task.ResultDigest = nil
		task.Reason = ""
		task.FailureCode = 0
	}
}

func updateImportV3Run(runID int64, outputPrefix string, outputSegmentIDs []int64, logIDRange *datapb.IDRange) UpdateAction {
	return func(t ImportTask) {
		task := t.(*importTaskV3).task.Load()
		task.RunId = runID
		task.OutputPrefix = outputPrefix
		task.OutputSegmentIds = append([]int64(nil), outputSegmentIDs...)
		task.LogIdRange = proto.Clone(logIDRange).(*datapb.IDRange)
		task.NodeId = NullNodeID
		task.State = datapb.ImportTaskV3_Pending
		task.ResultRef = ""
		task.ResultDigest = nil
		task.Reason = ""
		task.FailureCode = 0
	}
}

func UpdateFileStats(fileStats []*datapb.ImportFileStats) UpdateAction {
	return func(t ImportTask) {
		if task, ok := t.(*preImportTask); ok {
			task.task.Load().FileStats = fileStats
		}
	}
}

func UpdateSegmentIDs(segmentIDs []UniqueID) UpdateAction {
	return func(t ImportTask) {
		if task, ok := t.(*importTask); ok {
			task.task.Load().SegmentIDs = segmentIDs
		}
	}
}

func UpdateStatsSegmentIDs(segmentIDs []UniqueID) UpdateAction {
	return func(t ImportTask) {
		if task, ok := t.(*importTask); ok {
			task.task.Load().SortedSegmentIDs = segmentIDs
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
