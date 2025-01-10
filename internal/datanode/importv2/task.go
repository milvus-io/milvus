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
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/conc"
)

type TaskType int

const (
	PreImportTaskType   TaskType = 0
	ImportTaskType      TaskType = 1
	L0PreImportTaskType TaskType = 2
	L0ImportTaskType    TaskType = 3
)

var ImportTaskTypeName = map[TaskType]string{
	0: "PreImportTask",
	1: "ImportTask",
	2: "L0PreImportTaskType",
	3: "L0ImportTaskType",
}

func (t TaskType) String() string {
	return ImportTaskTypeName[t]
}

type TaskFilter func(task Task) bool

func WithStates(states ...datapb.ImportTaskStateV2) TaskFilter {
	return func(task Task) bool {
		for _, state := range states {
			if task.GetState() == state {
				return true
			}
		}
		return false
	}
}

func WithType(taskType TaskType) TaskFilter {
	return func(task Task) bool {
		return task.GetType() == taskType
	}
}

type UpdateAction func(task Task)

func UpdateState(state datapb.ImportTaskStateV2) UpdateAction {
	return func(t Task) {
		switch t.GetType() {
		case PreImportTaskType:
			t.(*PreImportTask).PreImportTask.State = state
		case ImportTaskType:
			t.(*ImportTask).ImportTaskV2.State = state
		case L0PreImportTaskType:
			t.(*L0PreImportTask).PreImportTask.State = state
		case L0ImportTaskType:
			t.(*L0ImportTask).ImportTaskV2.State = state
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
		case L0PreImportTaskType:
			t.(*L0PreImportTask).PreImportTask.Reason = reason
		case L0ImportTaskType:
			t.(*L0ImportTask).ImportTaskV2.Reason = reason
		}
	}
}

func UpdateFileStat(idx int, fileStat *datapb.ImportFileStats) UpdateAction {
	return func(task Task) {
		var t *datapb.PreImportTask
		switch it := task.(type) {
		case *PreImportTask:
			t = it.PreImportTask
		case *L0PreImportTask:
			t = it.PreImportTask
		}
		if t != nil {
			t.FileStats[idx].FileSize = fileStat.GetFileSize()
			t.FileStats[idx].TotalRows = fileStat.GetTotalRows()
			t.FileStats[idx].TotalMemorySize = fileStat.GetTotalMemorySize()
			t.FileStats[idx].HashedStats = fileStat.GetHashedStats()
		}
	}
}

func UpdateSegmentInfo(info *datapb.ImportSegmentInfo) UpdateAction {
	mergeFn := func(current []*datapb.FieldBinlog, new []*datapb.FieldBinlog) []*datapb.FieldBinlog {
		for _, binlog := range new {
			fieldBinlogs, ok := lo.Find(current, func(log *datapb.FieldBinlog) bool {
				return log.GetFieldID() == binlog.GetFieldID()
			})
			if !ok || fieldBinlogs == nil {
				current = append(current, binlog)
			} else {
				fieldBinlogs.Binlogs = append(fieldBinlogs.Binlogs, binlog.Binlogs...)
			}
		}
		return current
	}
	return func(task Task) {
		var segmentsInfo map[int64]*datapb.ImportSegmentInfo
		switch it := task.(type) {
		case *ImportTask:
			segmentsInfo = it.segmentsInfo
		case *L0ImportTask:
			segmentsInfo = it.segmentsInfo
		}
		if segmentsInfo != nil {
			segment := info.GetSegmentID()
			if _, ok := segmentsInfo[segment]; ok {
				segmentsInfo[segment].ImportedRows = info.GetImportedRows()
				segmentsInfo[segment].Binlogs = mergeFn(segmentsInfo[segment].Binlogs, info.GetBinlogs())
				segmentsInfo[segment].Statslogs = mergeFn(segmentsInfo[segment].Statslogs, info.GetStatslogs())
				segmentsInfo[segment].Deltalogs = mergeFn(segmentsInfo[segment].Deltalogs, info.GetDeltalogs())
				return
			}
			segmentsInfo[segment] = info
		}
	}
}

type Task interface {
	Execute() []*conc.Future[any]
	GetJobID() int64
	GetTaskID() int64
	GetCollectionID() int64
	GetPartitionIDs() []int64
	GetVchannels() []string
	GetType() TaskType
	GetState() datapb.ImportTaskStateV2
	GetReason() string
	GetSchema() *schemapb.CollectionSchema
	GetSlots() int64
	Cancel()
	Clone() Task
}

func WrapLogFields(task Task, fields ...zap.Field) []zap.Field {
	res := []zap.Field{
		zap.Int64("taskID", task.GetTaskID()),
		zap.Int64("jobID", task.GetJobID()),
		zap.Int64("collectionID", task.GetCollectionID()),
		zap.String("type", task.GetType().String()),
	}
	res = append(res, fields...)
	return res
}
