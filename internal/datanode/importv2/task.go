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
	"github.com/milvus-io/milvus/internal/util/importutilv2"
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
			it.PreImportTask.FileStats[idx].FileSize = fileStat.GetFileSize()
			it.PreImportTask.FileStats[idx].TotalRows = fileStat.GetTotalRows()
			it.PreImportTask.FileStats[idx].TotalMemorySize = fileStat.GetTotalMemorySize()
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
	GetState() datapb.ImportTaskStateV2
	GetReason() string
	GetSchema() *schemapb.CollectionSchema
	GetCtx() context.Context
	GetOptions() []*commonpb.KeyValuePair
	Cancel()
	Clone() Task
}

type PreImportTask struct {
	*datapb.PreImportTask
	ctx          context.Context
	cancel       context.CancelFunc
	partitionIDs []int64
	vchannels    []string
	schema       *schemapb.CollectionSchema
	options      []*commonpb.KeyValuePair
}

func NewPreImportTask(req *datapb.PreImportRequest) Task {
	fileStats := lo.Map(req.GetImportFiles(), func(file *internalpb.ImportFile, _ int) *datapb.ImportFileStats {
		return &datapb.ImportFileStats{
			ImportFile: file,
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	// During binlog import, even if the primary key's autoID is set to true,
	// the primary key from the binlog should be used instead of being reassigned.
	if importutilv2.IsBackup(req.GetOptions()) {
		UnsetAutoID(req.GetSchema())
	}
	return &PreImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:        req.GetJobID(),
			TaskID:       req.GetTaskID(),
			CollectionID: req.GetCollectionID(),
			State:        datapb.ImportTaskStateV2_Pending,
			FileStats:    fileStats,
		},
		ctx:          ctx,
		cancel:       cancel,
		partitionIDs: req.GetPartitionIDs(),
		vchannels:    req.GetVchannels(),
		schema:       req.GetSchema(),
		options:      req.GetOptions(),
	}
}

func (p *PreImportTask) GetPartitionIDs() []int64 {
	return p.partitionIDs
}

func (p *PreImportTask) GetVchannels() []string {
	return p.vchannels
}

func (p *PreImportTask) GetType() TaskType {
	return PreImportTaskType
}

func (p *PreImportTask) GetSchema() *schemapb.CollectionSchema {
	return p.schema
}

func (p *PreImportTask) GetOptions() []*commonpb.KeyValuePair {
	return p.options
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
		partitionIDs:  p.GetPartitionIDs(),
		vchannels:     p.GetVchannels(),
		schema:        p.GetSchema(),
		options:       p.GetOptions(),
	}
}

type ImportTask struct {
	*datapb.ImportTaskV2
	ctx          context.Context
	cancel       context.CancelFunc
	segmentsInfo map[int64]*datapb.ImportSegmentInfo
	req          *datapb.ImportRequest
	metaCaches   map[string]metacache.MetaCache
}

func NewImportTask(req *datapb.ImportRequest) Task {
	ctx, cancel := context.WithCancel(context.Background())
	// During binlog import, even if the primary key's autoID is set to true,
	// the primary key from the binlog should be used instead of being reassigned.
	if importutilv2.IsBackup(req.GetOptions()) {
		UnsetAutoID(req.GetSchema())
	}
	task := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:        req.GetJobID(),
			TaskID:       req.GetTaskID(),
			CollectionID: req.GetCollectionID(),
			State:        datapb.ImportTaskStateV2_Pending,
		},
		ctx:          ctx,
		cancel:       cancel,
		segmentsInfo: make(map[int64]*datapb.ImportSegmentInfo),
		req:          req,
	}
	task.initMetaCaches(req)
	return task
}

func (t *ImportTask) initMetaCaches(req *datapb.ImportRequest) {
	metaCaches := make(map[string]metacache.MetaCache)
	schema := typeutil.AppendSystemFields(req.GetSchema())
	for _, channel := range req.GetVchannels() {
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
	t.metaCaches = metaCaches
}

func (t *ImportTask) GetType() TaskType {
	return ImportTaskType
}

func (t *ImportTask) GetPartitionIDs() []int64 {
	return t.req.GetPartitionIDs()
}

func (t *ImportTask) GetVchannels() []string {
	return t.req.GetVchannels()
}

func (t *ImportTask) GetSchema() *schemapb.CollectionSchema {
	return t.req.GetSchema()
}

func (t *ImportTask) GetOptions() []*commonpb.KeyValuePair {
	return t.req.GetOptions()
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
		segmentsInfo: t.segmentsInfo,
		req:          t.req,
		metaCaches:   t.metaCaches,
	}
}
