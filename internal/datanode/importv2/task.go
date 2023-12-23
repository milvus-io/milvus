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

package importv2

import (
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/samber/lo"

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

type TaskFilter func(task Task) bool

func WithType(taskType TaskType) TaskFilter {
	return func(task Task) bool {
		return task.GetType() == taskType
	}
}

func WithReq(reqID int64) TaskFilter {
	return func(task Task) bool {
		return task.GetRequestID() == reqID
	}
}

func WithStates(states ...datapb.ImportState) TaskFilter {
	return func(task Task) bool {
		for _, state := range states {
			if task.GetState() == state {
				return true
			}
		}
		return false
	}
}

type UpdateAction func(task Task)

func UpdateState(state datapb.ImportState) UpdateAction {
	return func(t Task) {
		switch t.GetType() {
		case PreImportTaskType:
			t.(*PreImportTask).PreImportTask.State = state
		case ImportTaskType:
			t.(*ImportTask).ImportTaskV2.State = state
		}
	}
}

func UpdateReason(reason string) UpdateAction {
	return func(t Task) {
		switch t.GetType() {
		case PreImportTaskType:
			t.(*PreImportTask).PreImportTask.Reason = reason
		case ImportTaskType:
			t.(*ImportTask).ImportTaskV2.Reason = reason
		}
	}
}

func UpdateFileStats(fileStats []*datapb.ImportFileStats) UpdateAction {
	return func(t Task) {
		switch t.GetType() {
		case PreImportTaskType:
			t.(*PreImportTask).PreImportTask.FileStats = fileStats
		case ImportTaskType:
			t.(*ImportTask).ImportTaskV2.FileStats = fileStats
		}
	}
}

func UpdateFileStat(idx int, fileStat *datapb.ImportFileStats) UpdateAction {
	return func(t Task) {
		switch t.GetType() {
		case PreImportTaskType:
			t.(*PreImportTask).PreImportTask.FileStats[idx] = fileStat
		case ImportTaskType:
			t.(*ImportTask).ImportTaskV2.FileStats[idx] = fileStat
		}
	}
}

func UpdateSegmentIDs(segmentIDs []int64) UpdateAction {
	return func(task Task) {
		if it, ok := task.(*ImportTask); ok {
			it.ImportTaskV2.SegmentIDs = segmentIDs
		}
	}
}

func UpdateSegmentInfo(info *datapb.ImportSegmentInfo) UpdateAction {
	return func(task Task) {
		if it, ok := task.(*ImportTask); ok {
			for i := range it.segmentsInfo {
				if it.segmentsInfo[i].GetSegmentID() == info.GetSegmentID() {
					it.segmentsInfo[i].ImportedRows += info.GetImportedRows()
					it.segmentsInfo[i].Binlogs = append(it.segmentsInfo[i].Binlogs, info.GetBinlogs()...)
					it.segmentsInfo[i].Statslogs = append(it.segmentsInfo[i].Statslogs, info.GetStatslogs()...)
					return
				}
			}
			it.segmentsInfo = append(it.segmentsInfo, info)
		}
	}
}

type Task interface {
	GetRequestID() int64
	GetTaskID() int64
	GetCollectionID() int64
	GetType() TaskType
	GetState() datapb.ImportState
	GetReason() string
	GetSchema() *schemapb.CollectionSchema
	Clone() Task
}

type PreImportTask struct {
	*datapb.PreImportTask
	schema *schemapb.CollectionSchema
}

func NewPreImportTask(req *datapb.PreImportRequest) Task {
	fileStats := lo.Map(req.GetImportFiles(), func(file *datapb.ImportFile, _ int) *datapb.ImportFileStats {
		return &datapb.ImportFileStats{
			ImportFile: file,
		}
	})
	return &PreImportTask{
		PreImportTask: &datapb.PreImportTask{
			RequestID:    req.GetRequestID(),
			TaskID:       req.GetTaskID(),
			CollectionID: req.GetCollectionID(),
			PartitionID:  req.GetPartitionID(),
			State:        datapb.ImportState_Pending,
			FileStats:    fileStats,
		},
		schema: req.GetSchema(),
	}
}

func (p *PreImportTask) GetType() TaskType {
	return PreImportTaskType
}

func (p *PreImportTask) Clone() Task {
	return &PreImportTask{
		PreImportTask: proto.Clone(p.PreImportTask).(*datapb.PreImportTask),
	}
}

func (p *PreImportTask) GetSchema() *schemapb.CollectionSchema {
	return p.schema
}

type ImportTask struct {
	*datapb.ImportTaskV2
	schema       *schemapb.CollectionSchema
	segmentsInfo []*datapb.ImportSegmentInfo
	req          *datapb.ImportRequest
	metaCaches   map[string]metacache.MetaCache
}

func NewImportTask(req *datapb.ImportRequest) Task {
	return &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			RequestID:    req.GetRequestID(),
			TaskID:       req.GetTaskID(),
			CollectionID: req.GetCollectionID(),
			State:        datapb.ImportState_Pending,
		},
		schema:     req.GetSchema(),
		req:        req,
		metaCaches: InitMetaCaches(req),
	}
}

func (t *ImportTask) GetType() TaskType {
	return ImportTaskType
}

func (t *ImportTask) Clone() Task {
	return &ImportTask{
		ImportTaskV2: proto.Clone(t.ImportTaskV2).(*datapb.ImportTaskV2),
	}
}

func (t *ImportTask) GetSchema() *schemapb.CollectionSchema {
	return t.schema
}

func (t *ImportTask) GetSegmentsInfo() []*datapb.ImportSegmentInfo {
	return t.segmentsInfo
}
