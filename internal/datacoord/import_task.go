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

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type TaskType int

const (
	PreImportTaskType TaskType = 0
	ImportTaskType    TaskType = 1
)

var ImportTaskTypeName = map[TaskType]string{
	0: "PreImportTask",
	1: "ImportTask",
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

type UpdateAction func(task ImportTask)

func UpdateState(state datapb.ImportTaskStateV2) UpdateAction {
	return func(t ImportTask) {
		switch t.GetType() {
		case PreImportTaskType:
			t.(*preImportTask).PreImportTask.State = state
		case ImportTaskType:
			t.(*importTask).ImportTaskV2.State = state
		}
	}
}

func UpdateReason(reason string) UpdateAction {
	return func(t ImportTask) {
		switch t.GetType() {
		case PreImportTaskType:
			t.(*preImportTask).PreImportTask.Reason = reason
		case ImportTaskType:
			t.(*importTask).ImportTaskV2.Reason = reason
		}
	}
}

func UpdateCompleteTime(completeTime string) UpdateAction {
	return func(t ImportTask) {
		if task, ok := t.(*importTask); ok {
			task.ImportTaskV2.CompleteTime = completeTime
		}
	}
}

func UpdateNodeID(nodeID int64) UpdateAction {
	return func(t ImportTask) {
		switch t.GetType() {
		case PreImportTaskType:
			t.(*preImportTask).PreImportTask.NodeID = nodeID
		case ImportTaskType:
			t.(*importTask).ImportTaskV2.NodeID = nodeID
		}
	}
}

func UpdateFileStats(fileStats []*datapb.ImportFileStats) UpdateAction {
	return func(t ImportTask) {
		if task, ok := t.(*preImportTask); ok {
			task.PreImportTask.FileStats = fileStats
		}
	}
}

func UpdateSegmentIDs(segmentIDs []UniqueID) UpdateAction {
	return func(t ImportTask) {
		if task, ok := t.(*importTask); ok {
			task.ImportTaskV2.SegmentIDs = segmentIDs
		}
	}
}

type ImportTask interface {
	GetJobID() int64
	GetTaskID() int64
	GetCollectionID() int64
	GetNodeID() int64
	GetType() TaskType
	GetState() datapb.ImportTaskStateV2
	GetReason() string
	GetFileStats() []*datapb.ImportFileStats
	Clone() ImportTask
}

type preImportTask struct {
	*datapb.PreImportTask
}

func (p *preImportTask) GetType() TaskType {
	return PreImportTaskType
}

func (p *preImportTask) Clone() ImportTask {
	return &preImportTask{
		PreImportTask: proto.Clone(p.PreImportTask).(*datapb.PreImportTask),
	}
}

type importTask struct {
	*datapb.ImportTaskV2
}

func (t *importTask) GetType() TaskType {
	return ImportTaskType
}

func (t *importTask) Clone() ImportTask {
	return &importTask{
		ImportTaskV2: proto.Clone(t.ImportTaskV2).(*datapb.ImportTaskV2),
	}
}
