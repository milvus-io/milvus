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
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
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

func UpdateFileStat(idx int, fileStat *datapb.ImportFileStats) UpdateAction {
	return func(task Task) {
		if it, ok := task.(*PreImportTask); ok {
			it.PreImportTask.FileStats[idx].TotalRows = fileStat.GetTotalRows()
			it.PreImportTask.FileStats[idx].HashedRows = fileStat.GetHashedRows()
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
	GetPartitionIDs() []int64
	GetVchannels() []string
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
			PartitionIDs: req.GetPartitionIDs(),
			Vchannels:    req.GetVchannels(),
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
	vchannels    []string
	partitions   []int64
	metaCaches   map[string]metacache.MetaCache
}

func NewImportTask(req *datapb.ImportRequest) Task {
	task := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			RequestID:    req.GetRequestID(),
			TaskID:       req.GetTaskID(),
			CollectionID: req.GetCollectionID(),
			State:        datapb.ImportState_Pending,
		},
		schema: req.GetSchema(),
		req:    req,
	}
	task.Init(req)
	return task
}

func (t *ImportTask) Init(req *datapb.ImportRequest) {
	metaCaches := make(map[string]metacache.MetaCache)
	channels := make(map[string]struct{})
	partitions := make(map[int64]struct{})
	for _, info := range req.GetSegmentsInfo() {
		channels[info.GetVchannel()] = struct{}{}
		partitions[info.GetPartitionID()] = struct{}{}
	}
	for _, channel := range lo.Keys(channels) {
		info := &datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{
				CollectionID: req.GetCollectionID(),
				ChannelName:  channel,
			},
			Schema: req.GetSchema(),
		}
		metaCache := metacache.NewMetaCache(info, func(segment *datapb.SegmentInfo) *metacache.BloomFilterSet {
			return metacache.NewBloomFilterSet()
		})
		metaCaches[channel] = metaCache
	}
	t.vchannels = lo.Keys(channels)
	t.partitions = lo.Keys(partitions)
	t.metaCaches = metaCaches
}

func (t *ImportTask) GetType() TaskType {
	return ImportTaskType
}

func (t *ImportTask) Clone() Task {
	return &ImportTask{
		ImportTaskV2: proto.Clone(t.ImportTaskV2).(*datapb.ImportTaskV2),
	}
}

func (t *ImportTask) GetPartitionIDs() []int64 {
	return t.partitions
}

func (t *ImportTask) GetVchannels() []string {
	return t.vchannels
}

func (t *ImportTask) GetSchema() *schemapb.CollectionSchema {
	return t.schema
}

func (t *ImportTask) GetSegmentsInfo() []*datapb.ImportSegmentInfo {
	return t.segmentsInfo
}
