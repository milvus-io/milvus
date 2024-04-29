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
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type Scheduler interface {
	Start()
	Slots() int64
	Close()
}

type scheduler struct {
	manager TaskManager
	syncMgr syncmgr.SyncManager
	cm      storage.ChunkManager

	pool *conc.Pool[any]

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewScheduler(manager TaskManager, syncMgr syncmgr.SyncManager, cm storage.ChunkManager) Scheduler {
	pool := conc.NewPool[any](
		paramtable.Get().DataNodeCfg.MaxConcurrentImportTaskNum.GetAsInt(),
		conc.WithPreAlloc(true),
	)
	return &scheduler{
		manager:   manager,
		syncMgr:   syncMgr,
		cm:        cm,
		pool:      pool,
		closeChan: make(chan struct{}),
	}
}

func (s *scheduler) Start() {
	log.Info("start import scheduler")
	var (
		exeTicker = time.NewTicker(1 * time.Second)
		logTicker = time.NewTicker(10 * time.Minute)
	)
	defer exeTicker.Stop()
	defer logTicker.Stop()
	for {
		select {
		case <-s.closeChan:
			log.Info("import scheduler exited")
			return
		case <-exeTicker.C:
			tasks := s.manager.GetBy(WithStates(datapb.ImportTaskStateV2_Pending))
			futures := make(map[int64][]*conc.Future[any])
			for _, task := range tasks {
				switch task.GetType() {
				case PreImportTaskType:
					fs := s.PreImport(task)
					futures[task.GetTaskID()] = fs
					tryFreeFutures(futures)
				case ImportTaskType:
					fs := s.Import(task)
					futures[task.GetTaskID()] = fs
					tryFreeFutures(futures)
				}
			}
			for taskID, fs := range futures {
				err := conc.AwaitAll(fs...)
				if err != nil {
					continue
				}
				s.manager.Update(taskID, UpdateState(datapb.ImportTaskStateV2_Completed))
				log.Info("preimport/import done", zap.Int64("taskID", taskID))
			}
		case <-logTicker.C:
			LogStats(s.manager)
		}
	}
}

func (s *scheduler) Slots() int64 {
	tasks := s.manager.GetBy(WithStates(datapb.ImportTaskStateV2_Pending, datapb.ImportTaskStateV2_InProgress))
	return paramtable.Get().DataNodeCfg.MaxConcurrentImportTaskNum.GetAsInt64() - int64(len(tasks))
}

func (s *scheduler) Close() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
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

func tryFreeFutures(futures map[int64][]*conc.Future[any]) {
	for k, fs := range futures {
		fs = lo.Filter(fs, func(f *conc.Future[any], _ int) bool {
			if f.Done() {
				_, err := f.Await()
				return err != nil
			}
			return true
		})
		futures[k] = fs
	}
}

func (s *scheduler) handleErr(task Task, err error, msg string) {
	log.Warn(msg, WrapLogFields(task, zap.Error(err))...)
	s.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(err.Error()))
}

func (s *scheduler) PreImport(task Task) []*conc.Future[any] {
	bufferSize := paramtable.Get().DataNodeCfg.ReadBufferSizeInMB.GetAsInt() * 1024 * 1024
	log.Info("start to preimport", WrapLogFields(task,
		zap.Int("bufferSize", bufferSize),
		zap.Any("schema", task.GetSchema()))...)
	s.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress))
	files := lo.Map(task.(*PreImportTask).GetFileStats(),
		func(fileStat *datapb.ImportFileStats, _ int) *internalpb.ImportFile {
			return fileStat.GetImportFile()
		})

	fn := func(i int, file *internalpb.ImportFile) error {
		reader, err := importutilv2.NewReader(task.GetCtx(), s.cm, task.GetSchema(), file, task.GetOptions(), bufferSize)
		if err != nil {
			s.handleErr(task, err, "new reader failed")
			return err
		}
		defer reader.Close()
		start := time.Now()
		err = s.readFileStat(reader, task, i)
		if err != nil {
			s.handleErr(task, err, "preimport failed")
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
		f := s.pool.Submit(func() (any, error) {
			err := fn(i, file)
			return err, err
		})
		futures = append(futures, f)
	}
	return futures
}

func (s *scheduler) readFileStat(reader importutilv2.Reader, task Task, fileIdx int) error {
	fileSize, err := reader.Size()
	if err != nil {
		return err
	}
	maxSize := paramtable.Get().DataNodeCfg.MaxImportFileSizeInGB.GetAsFloat() * 1024 * 1024 * 1024
	if fileSize > int64(maxSize) {
		return errors.New(fmt.Sprintf(
			"The import file size has reached the maximum limit allowed for importing, "+
				"fileSize=%d, maxSize=%d", fileSize, int64(maxSize)))
	}

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
		FileSize:        fileSize,
		TotalRows:       int64(totalRows),
		TotalMemorySize: int64(totalSize),
		HashedStats:     hashedStats,
	}
	s.manager.Update(task.GetTaskID(), UpdateFileStat(fileIdx, stat))
	return nil
}

func (s *scheduler) Import(task Task) []*conc.Future[any] {
	bufferSize := paramtable.Get().DataNodeCfg.ReadBufferSizeInMB.GetAsInt() * 1024 * 1024
	log.Info("start to import", WrapLogFields(task,
		zap.Int("bufferSize", bufferSize),
		zap.Any("schema", task.GetSchema()))...)
	s.manager.Update(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress))

	req := task.(*ImportTask).req

	fn := func(file *internalpb.ImportFile) error {
		reader, err := importutilv2.NewReader(task.GetCtx(), s.cm, task.GetSchema(), file, task.GetOptions(), bufferSize)
		if err != nil {
			s.handleErr(task, err, fmt.Sprintf("new reader failed, file: %s", file.String()))
			return err
		}
		defer reader.Close()
		start := time.Now()
		err = s.importFile(reader, task)
		if err != nil {
			s.handleErr(task, err, fmt.Sprintf("do import failed, file: %s", file.String()))
			return err
		}
		log.Info("import file done", WrapLogFields(task, zap.Strings("files", file.GetPaths()),
			zap.Duration("dur", time.Since(start)))...)
		return nil
	}

	futures := make([]*conc.Future[any], 0, len(req.GetFiles()))
	for _, file := range req.GetFiles() {
		file := file
		f := s.pool.Submit(func() (any, error) {
			err := fn(file)
			return err, err
		})
		futures = append(futures, f)
	}
	return futures
}

func (s *scheduler) importFile(reader importutilv2.Reader, task Task) error {
	iTask := task.(*ImportTask)
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
		err = AppendSystemFieldsData(iTask, data)
		if err != nil {
			return err
		}
		hashedData, err := HashData(iTask, data)
		if err != nil {
			return err
		}
		fs, sts, err := s.Sync(iTask, hashedData)
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
		segmentInfo, err := NewImportSegmentInfo(syncTask, iTask)
		if err != nil {
			return err
		}
		s.manager.Update(task.GetTaskID(), UpdateSegmentInfo(segmentInfo))
		log.Info("sync import data done", WrapLogFields(task, zap.Any("segmentInfo", segmentInfo))...)
	}
	return nil
}

func (s *scheduler) Sync(task *ImportTask, hashedData HashedData) ([]*conc.Future[struct{}], []syncmgr.Task, error) {
	log.Info("start to sync import data", WrapLogFields(task)...)
	futures := make([]*conc.Future[struct{}], 0)
	syncTasks := make([]syncmgr.Task, 0)
	segmentImportedSizes := make(map[int64]int)
	for channelIdx, datas := range hashedData {
		channel := task.GetVchannels()[channelIdx]
		for partitionIdx, data := range datas {
			if data.GetRowNum() == 0 {
				continue
			}
			partitionID := task.GetPartitionIDs()[partitionIdx]
			size := data.GetMemorySize()
			segmentID := PickSegment(task, segmentImportedSizes, channel, partitionID, size)
			syncTask, err := NewSyncTask(task.GetCtx(), task, segmentID, partitionID, channel, data)
			if err != nil {
				return nil, nil, err
			}
			segmentImportedSizes[segmentID] += size
			future := s.syncMgr.SyncData(task.GetCtx(), syncTask)
			futures = append(futures, future)
			syncTasks = append(syncTasks, syncTask)
		}
	}
	return futures, syncTasks, nil
}
