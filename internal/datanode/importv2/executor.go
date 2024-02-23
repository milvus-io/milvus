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
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type Executor interface {
	Start()
	Slots() int64
	Close()
}

type executor struct {
	manager TaskManager
	syncMgr syncmgr.SyncManager
	cm      storage.ChunkManager

	pool *conc.Pool[any]

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewExecutor(manager TaskManager, syncMgr syncmgr.SyncManager, cm storage.ChunkManager) Executor {
	pool := conc.NewPool[any](
		hardware.GetCPUNum()*2,
		conc.WithPreAlloc(false),
		conc.WithDisablePurge(false),
	)
	return &executor{
		manager:   manager,
		syncMgr:   syncMgr,
		cm:        cm,
		pool:      pool,
		closeChan: make(chan struct{}),
	}
}

func (e *executor) Start() {
	log.Info("start import executor")
	var (
		exeTicker = time.NewTicker(1 * time.Second)
		logTicker = time.NewTicker(10 * time.Minute)
	)
	defer exeTicker.Stop()
	defer logTicker.Stop()
	for {
		select {
		case <-e.closeChan:
			log.Info("import executor exited")
			return
		case <-exeTicker.C:
			tasks := e.manager.GetBy(WithStates(datapb.ImportTaskStateV2_Pending))
			wg := &sync.WaitGroup{}
			for _, task := range tasks {
				wg.Add(1)
				go func(task Task) {
					defer wg.Done()
					switch task.GetType() {
					case PreImportTaskType:
						e.PreImport(task)
					case ImportTaskType:
						e.Import(task)
					}
				}(task)
			}
			wg.Wait()
		case <-logTicker.C:
			LogStats(e.manager)
		}
	}
}

func (e *executor) Slots() int64 {
	tasks := e.manager.GetBy(WithStates(datapb.ImportTaskStateV2_Pending, datapb.ImportTaskStateV2_InProgress))
	return paramtable.Get().DataNodeCfg.MaxConcurrentImportTaskNum.GetAsInt64() - int64(len(tasks))
}

func (e *executor) Close() {
	e.closeOnce.Do(func() {
		close(e.closeChan)
	})
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

func (e *executor) handleErr(task Task, err error, msg string) {
	log.Warn(msg, WrapLogFields(task, zap.Error(err))...)
	e.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(err.Error()))
}

func (e *executor) PreImport(task Task) {
	bufferSize := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt()
	log.Info("start to preimport", WrapLogFields(task,
		zap.Int("bufferSize", bufferSize),
		zap.Any("schema", task.GetSchema()))...)
	e.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress))
	files := lo.Map(task.(*PreImportTask).GetFileStats(),
		func(fileStat *datapb.ImportFileStats, _ int) *internalpb.ImportFile {
			return fileStat.GetImportFile()
		})

	fn := func(i int, file *internalpb.ImportFile) error {
		reader, err := importutilv2.NewReader(task.GetCtx(), e.cm, task.GetSchema(), file, task.GetOptions(), bufferSize)
		if err != nil {
			e.handleErr(task, err, "new reader failed")
			return err
		}
		defer reader.Close()
		start := time.Now()
		err = e.readFileStat(reader, task, i)
		if err != nil {
			e.handleErr(task, err, "preimport failed")
			return err
		}
		log.Info("read file stat done", WrapLogFields(task, zap.Strings("files", file.GetPaths()),
			zap.Duration("dur", time.Since(start)))...)
		return nil
	}

	futures := make([]*conc.Future[any], 0, len(files))
	for i, file := range files {
		i := i
		file := file
		f := e.pool.Submit(func() (any, error) {
			err := fn(i, file)
			return err, err
		})
		futures = append(futures, f)
	}
	err := conc.AwaitAll(futures...)
	if err != nil {
		return
	}

	e.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed))
	log.Info("executor preimport done",
		WrapLogFields(task, zap.Any("fileStats", task.(*PreImportTask).GetFileStats()))...)
}

func (e *executor) readFileStat(reader importutilv2.Reader, task Task, fileIdx int) error {
	totalRows := 0
	totalSize := 0
	hashedStats := make(map[string]*datapb.PartitionImportStats)
	for {
		data, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		err = CheckRowsEqual(task.GetSchema(), data)
		if err != nil {
			return err
		}
		rowsCount, err := GetRowsStats(task, data)
		if err != nil {
			return err
		}
		MergeHashedStats(rowsCount, hashedStats)
		rows := data.GetRowNum()
		size := data.GetMemorySize()
		totalRows += rows
		totalSize += size
		log.Info("reading file stat...", WrapLogFields(task, zap.Int("readRows", rows), zap.Int("readSize", size))...)
	}

	stat := &datapb.ImportFileStats{
		TotalRows:       int64(totalRows),
		TotalMemorySize: int64(totalSize),
		HashedStats:     hashedStats,
	}
	e.manager.Update(task.GetTaskID(), UpdateFileStat(fileIdx, stat))
	return nil
}

func (e *executor) Import(task Task) {
	bufferSize := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt()
	log.Info("start to import", WrapLogFields(task,
		zap.Int("bufferSize", bufferSize),
		zap.Any("schema", task.GetSchema()))...)
	e.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress))

	req := task.(*ImportTask).req

	fn := func(file *internalpb.ImportFile) error {
		reader, err := importutilv2.NewReader(task.GetCtx(), e.cm, task.GetSchema(), file, task.GetOptions(), bufferSize)
		if err != nil {
			e.handleErr(task, err, fmt.Sprintf("new reader failed, file: %s", file.String()))
			return err
		}
		defer reader.Close()
		start := time.Now()
		err = e.importFile(reader, task)
		if err != nil {
			e.handleErr(task, err, fmt.Sprintf("do import failed, file: %s", file.String()))
			return err
		}
		log.Info("import file done", WrapLogFields(task, zap.Strings("files", file.GetPaths()),
			zap.Duration("dur", time.Since(start)))...)
		return nil
	}

	futures := make([]*conc.Future[any], 0, len(req.GetFiles()))
	for _, file := range req.GetFiles() {
		file := file
		f := e.pool.Submit(func() (any, error) {
			err := fn(file)
			return err, err
		})
		futures = append(futures, f)
	}
	err := conc.AwaitAll(futures...)
	if err != nil {
		return
	}

	e.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed))
	log.Info("import done", WrapLogFields(task)...)
}

func (e *executor) importFile(reader importutilv2.Reader, task Task) error {
	iTask := task.(*ImportTask)
	futures := make([]*conc.Future[error], 0)
	syncTasks := make([]syncmgr.Task, 0)
	for {
		data, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		err = AppendSystemFieldsData(iTask, data)
		if err != nil {
			return err
		}
		hashedData, err := HashData(iTask, data)
		if err != nil {
			return err
		}
		fs, sts, err := e.Sync(iTask, hashedData)
		if err != nil {
			return err
		}
		futures = append(futures, fs...)
		syncTasks = append(syncTasks, sts...)
	}
	err := conc.AwaitAll(futures...)
	if err != nil {
		return err
	}
	for _, syncTask := range syncTasks {
		segmentInfo, err := NewImportSegmentInfo(syncTask, iTask)
		if err != nil {
			return err
		}
		e.manager.Update(task.GetTaskID(), UpdateSegmentInfo(segmentInfo))
		log.Info("sync import data done", WrapLogFields(task, zap.Any("segmentInfo", segmentInfo))...)
	}
	return nil
}

func (e *executor) Sync(task *ImportTask, hashedData HashedData) ([]*conc.Future[error], []syncmgr.Task, error) {
	log.Info("start to sync import data", WrapLogFields(task)...)
	futures := make([]*conc.Future[error], 0)
	syncTasks := make([]syncmgr.Task, 0)
	segmentImportedSizes := make(map[int64]int)
	for channelIdx, datas := range hashedData {
		channel := task.GetVchannels()[channelIdx]
		for partitionIdx, data := range datas {
			partitionID := task.GetPartitionIDs()[partitionIdx]
			size := data.GetMemorySize()
			segmentID := PickSegment(task, segmentImportedSizes, channel, partitionID, size)
			syncTask, err := NewSyncTask(task.GetCtx(), task, segmentID, partitionID, channel, data)
			if err != nil {
				return nil, nil, err
			}
			segmentImportedSizes[segmentID] += size
			future := e.syncMgr.SyncData(task.GetCtx(), syncTask)
			futures = append(futures, future)
			syncTasks = append(syncTasks, syncTask)
		}
	}
	return futures, syncTasks, nil
}
