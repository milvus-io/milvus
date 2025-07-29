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
	"fmt"
	"io"
	"runtime/debug"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ImportTask struct {
	*datapb.ImportTaskV2
	ctx          context.Context
	cancel       context.CancelFunc
	segmentsInfo map[int64]*datapb.ImportSegmentInfo
	req          *datapb.ImportRequest

	allocator  allocator.Interface
	manager    TaskManager
	syncMgr    syncmgr.SyncManager
	cm         storage.ChunkManager
	metaCaches map[string]metacache.MetaCache
}

func NewImportTask(req *datapb.ImportRequest,
	manager TaskManager,
	syncMgr syncmgr.SyncManager,
	cm storage.ChunkManager,
) Task {
	ctx, cancel := context.WithCancel(context.Background())
	// During binlog import, even if the primary key's autoID is set to true,
	// the primary key from the binlog should be used instead of being reassigned.
	if importutilv2.IsBackup(req.GetOptions()) {
		UnsetAutoID(req.GetSchema())
	}
	// Allocator for autoIDs and logIDs.
	alloc := allocator.NewLocalAllocator(req.GetIDRange().GetBegin(), req.GetIDRange().GetEnd())
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
		allocator:    alloc,
		manager:      manager,
		syncMgr:      syncMgr,
		cm:           cm,
	}
	task.metaCaches = NewMetaCache(req)
	return task
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

func (t *ImportTask) GetSlots() int64 {
	return t.req.GetTaskSlot()
}

func (t *ImportTask) GetBufferSize() int64 {
	// Calculate the task buffer size based on the number of vchannels and partitions
	baseBufferSize := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt()
	vchannelNum := len(t.GetVchannels())
	partitionNum := len(t.GetPartitionIDs())
	taskBufferSize := int64(baseBufferSize * vchannelNum * partitionNum)

	// If the file size is smaller than the task buffer size, use the file size
	fileSize := lo.MaxBy(t.GetFileStats(), func(a, b *datapb.ImportFileStats) bool {
		return a.GetTotalMemorySize() > b.GetTotalMemorySize()
	}).GetTotalMemorySize()
	if fileSize != 0 && fileSize < taskBufferSize {
		taskBufferSize = fileSize
	}

	// Task buffer size should not exceed the memory limit
	percentage := paramtable.Get().DataNodeCfg.ImportMemoryLimitPercentage.GetAsFloat()
	memoryLimit := int64(float64(hardware.GetMemoryCount()) * percentage / 100.0)
	if taskBufferSize > memoryLimit {
		return memoryLimit
	}
	return taskBufferSize
}

func (t *ImportTask) Cancel() {
	t.cancel()
}

func (t *ImportTask) GetSegmentsInfo() []*datapb.ImportSegmentInfo {
	return lo.Values(t.segmentsInfo)
}

func (t *ImportTask) Clone() Task {
	ctx, cancel := context.WithCancel(t.ctx)
	infos := make(map[int64]*datapb.ImportSegmentInfo)
	for id, info := range t.segmentsInfo {
		infos[id] = typeutil.Clone(info)
	}
	return &ImportTask{
		ImportTaskV2: typeutil.Clone(t.ImportTaskV2),
		ctx:          ctx,
		cancel:       cancel,
		segmentsInfo: infos,
		req:          t.req,
		allocator:    t.allocator,
		manager:      t.manager,
		syncMgr:      t.syncMgr,
		cm:           t.cm,
		metaCaches:   t.metaCaches,
	}
}

func (t *ImportTask) Execute() []*conc.Future[any] {
	bufferSize := t.GetBufferSize()
	log.Info("start to import", WrapLogFields(t,
		zap.Int64("bufferSize", bufferSize),
		zap.Int64("taskSlot", t.GetSlots()),
		zap.Any("files", t.req.GetFiles()),
		zap.Any("schema", t.GetSchema()),
	)...)
	t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress))

	req := t.req

	fn := func(file *internalpb.ImportFile) error {
		reader, err := importutilv2.NewReader(t.ctx, t.cm, t.GetSchema(), file, req.GetOptions(), int(bufferSize), t.req.GetStorageConfig())
		if err != nil {
			log.Warn("new reader failed", WrapLogFields(t, zap.String("file", file.String()), zap.Error(err))...)
			reason := fmt.Sprintf("error: %v, file: %s", err, file.String())
			t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason))
			return err
		}
		defer reader.Close()
		start := time.Now()
		err = t.importFile(reader)
		if err != nil {
			log.Warn("do import failed", WrapLogFields(t, zap.String("file", file.String()), zap.Error(err))...)
			reason := fmt.Sprintf("error: %v, file: %s", err, file.String())
			t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason))
			return err
		}
		log.Info("import file done", WrapLogFields(t, zap.Strings("files", file.GetPaths()),
			zap.Duration("dur", time.Since(start)))...)
		return nil
	}

	futures := make([]*conc.Future[any], 0, len(req.GetFiles()))
	for _, file := range req.GetFiles() {
		file := file
		f := GetExecPool().Submit(func() (any, error) {
			// Use blocking allocation - this will wait until memory is available
			GetMemoryAllocator().BlockingAllocate(t.GetTaskID(), bufferSize)
			defer func() {
				GetMemoryAllocator().Release(t.GetTaskID(), bufferSize)
				debug.FreeOSMemory()
			}()
			err := fn(file)
			return err, err
		})
		futures = append(futures, f)
	}
	return futures
}

func (t *ImportTask) importFile(reader importutilv2.Reader) error {
	syncFutures := make([]*conc.Future[struct{}], 0)
	syncTasks := make([]syncmgr.Task, 0)
	for {
		data, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		rowNum, _ := GetInsertDataRowCount(data, t.GetSchema())
		if rowNum == 0 {
			log.Info("0 row was imported, the data may have been deleted", WrapLogFields(t)...)
			continue
		}
		err = AppendSystemFieldsData(t, data, rowNum)
		if err != nil {
			return err
		}
		err = AppendNullableDefaultFieldsData(t.GetSchema(), data, rowNum)
		if err != nil {
			return err
		}
		err = FillDynamicData(t.GetSchema(), data, rowNum)
		if err != nil {
			return err
		}
		if !importutilv2.IsBackup(t.req.GetOptions()) {
			err = RunEmbeddingFunction(t, data)
			if err != nil {
				return err
			}
		}
		hashedData, err := HashData(t, data)
		if err != nil {
			return err
		}
		fs, sts, err := t.sync(hashedData)
		if err != nil {
			return err
		}
		syncFutures = append(syncFutures, fs...)
		syncTasks = append(syncTasks, sts...)
	}
	err := conc.AwaitAll(syncFutures...)
	if err != nil {
		return err
	}
	for _, syncTask := range syncTasks {
		segmentInfo, err := NewImportSegmentInfo(syncTask, t.metaCaches)
		if err != nil {
			return err
		}
		t.manager.Update(t.GetTaskID(), UpdateSegmentInfo(segmentInfo))
		log.Info("sync import data done", WrapLogFields(t, zap.Any("segmentInfo", segmentInfo))...)
	}
	return nil
}

func (t *ImportTask) sync(hashedData HashedData) ([]*conc.Future[struct{}], []syncmgr.Task, error) {
	log.Ctx(context.TODO()).Info("start to sync import data", WrapLogFields(t)...)
	futures := make([]*conc.Future[struct{}], 0)
	syncTasks := make([]syncmgr.Task, 0)
	for channelIdx, datas := range hashedData {
		channel := t.GetVchannels()[channelIdx]
		for partitionIdx, data := range datas {
			if data.GetRowNum() == 0 {
				continue
			}
			partitionID := t.GetPartitionIDs()[partitionIdx]
			segmentID, err := PickSegment(t.req.GetRequestSegments(), channel, partitionID)
			if err != nil {
				return nil, nil, err
			}
			bm25Stats := make(map[int64]*storage.BM25Stats)
			for _, fn := range t.req.GetSchema().GetFunctions() {
				if fn.GetType() == schemapb.FunctionType_BM25 {
					// BM25 function guarantees single output field
					outputSparseFieldId := fn.GetOutputFieldIds()[0]
					bm25Stats[outputSparseFieldId] = storage.NewBM25Stats()
					bm25Stats[outputSparseFieldId].AppendFieldData(data.Data[outputSparseFieldId].(*storage.SparseFloatVectorFieldData))
				}
			}
			syncTask, err := NewSyncTask(t.ctx, t.allocator, t.metaCaches, t.req.GetTs(),
				segmentID, partitionID, t.GetCollectionID(), channel, data, nil,
				bm25Stats, t.req.GetStorageVersion(), t.req.GetStorageConfig())
			if err != nil {
				return nil, nil, err
			}
			future, err := t.syncMgr.SyncDataWithChunkManager(t.ctx, syncTask, t.cm)
			if err != nil {
				log.Ctx(context.TODO()).Error("sync data failed", WrapLogFields(t, zap.Error(err))...)
				return nil, nil, err
			}
			futures = append(futures, future)
			syncTasks = append(syncTasks, syncTask)
		}
	}
	return futures, syncTasks, nil
}
