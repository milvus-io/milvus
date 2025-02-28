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

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
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

func WithRequestSource() ImportTaskFilter {
	return func(task ImportTask) bool {
		return task.GetSource() == datapb.ImportTaskSourceV2_Request
	}
}

func WithL0CompactionSource() ImportTaskFilter {
	return func(task ImportTask) bool {
		return task.GetSource() == datapb.ImportTaskSourceV2_L0Compaction
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
		switch t.GetType() {
		case PreImportTaskType:
			t.(*preImportTask).PreImportTask.CompleteTime = completeTime
		case ImportTaskType:
			t.(*importTask).ImportTaskV2.CompleteTime = completeTime
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

func UpdateStatsSegmentIDs(segmentIDs []UniqueID) UpdateAction {
	return func(t ImportTask) {
		if task, ok := t.(*importTask); ok {
			task.ImportTaskV2.StatsSegmentIDs = segmentIDs
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
	GetTR() *timerecord.TimeRecorder
	GetSlots() int64
	Clone() ImportTask
	GetSource() datapb.ImportTaskSourceV2
}

type preImportTask struct {
	*datapb.PreImportTask
	tr *timerecord.TimeRecorder
}

func (p *preImportTask) GetType() TaskType {
	return PreImportTaskType
}

func (p *preImportTask) GetTR() *timerecord.TimeRecorder {
	return p.tr
}

func (p *preImportTask) GetSlots() int64 {
	return int64(funcutil.Min(len(p.GetFileStats()), paramtable.Get().DataNodeCfg.MaxTaskSlotNum.GetAsInt()))
}

func (p *preImportTask) Clone() ImportTask {
	return &preImportTask{
		PreImportTask: proto.Clone(p.PreImportTask).(*datapb.PreImportTask),
		tr:            p.tr,
	}
}

func (p *preImportTask) GetSource() datapb.ImportTaskSourceV2 {
	return datapb.ImportTaskSourceV2_Request
}

func (p *preImportTask) MarshalJSON() ([]byte, error) {
	importTask := metricsinfo.ImportTask{
		JobID:        p.GetJobID(),
		TaskID:       p.GetTaskID(),
		CollectionID: p.GetCollectionID(),
		NodeID:       p.GetNodeID(),
		State:        p.GetState().String(),
		Reason:       p.GetReason(),
		TaskType:     p.GetType().String(),
		CreatedTime:  p.GetCreatedTime(),
		CompleteTime: p.GetCompleteTime(),
	}
	return json.Marshal(importTask)
}

type importTask struct {
	*datapb.ImportTaskV2
	tr *timerecord.TimeRecorder
}

func (t *importTask) GetType() TaskType {
	return ImportTaskType
}

func (t *importTask) GetTR() *timerecord.TimeRecorder {
	return t.tr
}

func (t *importTask) GetSlots() int64 {
	// Consider the following two scenarios:
	// 1. Importing a large number of small files results in
	//    a small total data size, making file count unsuitable as a slot number.
	// 2. Importing a file with many shards number results in many segments and a small total data size,
	//    making segment count unsuitable as a slot number.
	// Taking these factors into account, we've decided to use the
	// minimum value between segment count and file count as the slot number.
	return int64(funcutil.Min(len(t.GetFileStats()), len(t.GetSegmentIDs()), paramtable.Get().DataNodeCfg.MaxTaskSlotNum.GetAsInt()))
}

func (t *importTask) Clone() ImportTask {
	return &importTask{
		ImportTaskV2: proto.Clone(t.ImportTaskV2).(*datapb.ImportTaskV2),
		tr:           t.tr,
	}
}

func (t *importTask) MarshalJSON() ([]byte, error) {
	importTask := metricsinfo.ImportTask{
		JobID:        t.GetJobID(),
		TaskID:       t.GetTaskID(),
		CollectionID: t.GetCollectionID(),
		NodeID:       t.GetNodeID(),
		State:        t.GetState().String(),
		Reason:       t.GetReason(),
		TaskType:     t.GetType().String(),
		CreatedTime:  t.GetCreatedTime(),
		CompleteTime: t.GetCompleteTime(),
	}
	return json.Marshal(importTask)
}
