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
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

func WithStates(states ...internalpb.ImportState) TaskFilter {
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

func UpdateState(state internalpb.ImportState) UpdateAction {
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
			it.PreImportTask.FileStats[idx].HashedStats = fileStat.GetHashedStats()
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
		if it, ok := task.(*ImportTask); ok {
			segment := info.GetSegmentID()
			if _, ok = it.segmentsInfo[segment]; ok {
				it.segmentsInfo[segment].ImportedRows = info.GetImportedRows()
				it.segmentsInfo[segment].Binlogs = mergeFn(it.segmentsInfo[segment].Binlogs, info.GetBinlogs())
				it.segmentsInfo[segment].Statslogs = mergeFn(it.segmentsInfo[segment].Statslogs, info.GetStatslogs())
				return
			}
			it.segmentsInfo[segment] = info
		}
	}
}

type Task interface {
	GetJobID() int64
	GetTaskID() int64
	GetCollectionID() int64
	GetPartitionIDs() []int64
	GetVchannels() []string
	GetType() TaskType
	GetState() internalpb.ImportState
	GetReason() string
	GetSchema() *schemapb.CollectionSchema
	GetCtx() context.Context
	GetOptions() []*commonpb.KeyValuePair
	Cancel()
	Clone() Task
}

type PreImportTask struct {
	*datapb.PreImportTask
	ctx    context.Context
	cancel context.CancelFunc
	schema *schemapb.CollectionSchema
}

func NewPreImportTask(req *datapb.PreImportRequest) Task {
	fileStats := lo.Map(req.GetImportFiles(), func(file *internalpb.ImportFile, _ int) *datapb.ImportFileStats {
		return &datapb.ImportFileStats{
			ImportFile: file,
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	return &PreImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:        req.GetJobID(),
			TaskID:       req.GetTaskID(),
			CollectionID: req.GetCollectionID(),
			PartitionIDs: req.GetPartitionIDs(),
			Vchannels:    req.GetVchannels(),
			State:        internalpb.ImportState_Pending,
			FileStats:    fileStats,
			Options:      req.GetOptions(),
		},
		ctx:    ctx,
		cancel: cancel,
		schema: req.GetSchema(),
	}
}

func (p *PreImportTask) GetType() TaskType {
	return PreImportTaskType
}

func (p *PreImportTask) GetSchema() *schemapb.CollectionSchema {
	return p.schema
}

func (p *PreImportTask) GetCtx() context.Context {
	return p.ctx
}

func (p *PreImportTask) Cancel() {
	p.cancel()
}

func (p *PreImportTask) Clone() Task {
	ctx, cancel := context.WithCancel(p.GetCtx())
	return &PreImportTask{
		PreImportTask: proto.Clone(p.PreImportTask).(*datapb.PreImportTask),
		ctx:           ctx,
		cancel:        cancel,
		schema:        p.GetSchema(),
	}
}

type ImportTask struct {
	*datapb.ImportTaskV2
	ctx          context.Context
	cancel       context.CancelFunc
	schema       *schemapb.CollectionSchema
	segmentsInfo map[int64]*datapb.ImportSegmentInfo
	req          *datapb.ImportRequest
	vchannels    []string
	partitions   []int64
	metaCaches   map[string]metacache.MetaCache
}

func NewImportTask(req *datapb.ImportRequest) Task {
	ctx, cancel := context.WithCancel(context.Background())
	task := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:        req.GetJobID(),
			TaskID:       req.GetTaskID(),
			CollectionID: req.GetCollectionID(),
			State:        internalpb.ImportState_Pending,
			Options:      req.GetOptions(),
		},
		ctx:          ctx,
		cancel:       cancel,
		schema:       req.GetSchema(),
		segmentsInfo: make(map[int64]*datapb.ImportSegmentInfo),
		req:          req,
	}
	task.Init(req)
	return task
}

func (t *ImportTask) Init(req *datapb.ImportRequest) {
	metaCaches := make(map[string]metacache.MetaCache)
	channels := make(map[string]struct{})
	partitions := make(map[int64]struct{})
	for _, info := range req.GetRequestSegments() {
		channels[info.GetVchannel()] = struct{}{}
		partitions[info.GetPartitionID()] = struct{}{}
	}
	schema := typeutil.AppendSystemFields(req.GetSchema())
	for _, channel := range lo.Keys(channels) {
		info := &datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{
				CollectionID: req.GetCollectionID(),
				ChannelName:  channel,
			},
			Schema: schema,
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

func (t *ImportTask) GetPartitionIDs() []int64 {
	return t.partitions
}

func (t *ImportTask) GetVchannels() []string {
	return t.vchannels
}

func (t *ImportTask) GetSchema() *schemapb.CollectionSchema {
	return t.schema
}

func (t *ImportTask) GetCtx() context.Context {
	return t.ctx
}

func (t *ImportTask) Cancel() {
	t.cancel()
}

func (t *ImportTask) GetSegmentsInfo() []*datapb.ImportSegmentInfo {
	return lo.Values(t.segmentsInfo)
}

func (t *ImportTask) Clone() Task {
	ctx, cancel := context.WithCancel(t.GetCtx())
	return &ImportTask{
		ImportTaskV2: proto.Clone(t.ImportTaskV2).(*datapb.ImportTaskV2),
		ctx:          ctx,
		cancel:       cancel,
		schema:       t.GetSchema(),
		segmentsInfo: t.segmentsInfo,
		req:          t.req,
		vchannels:    t.GetVchannels(),
		partitions:   t.GetPartitionIDs(),
		metaCaches:   t.metaCaches,
	}
}
