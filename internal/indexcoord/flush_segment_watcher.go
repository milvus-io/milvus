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

package indexcoord

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/proto/querypb"

	"github.com/milvus-io/milvus/internal/util"

	"github.com/milvus-io/milvus/internal/metastore/model"

	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type flushedSegmentWatcher struct {
	ctx    context.Context
	cancel context.CancelFunc

	kvClient         kv.MetaKv
	wg               sync.WaitGroup
	taskMutex        sync.RWMutex
	scheduleDuration time.Duration
	notify           chan struct{}

	etcdRevision int64
	watchChan    clientv3.WatchChan

	meta    *metaTable
	builder *indexBuilder
	ic      *IndexCoord
	// segmentID -> indexID -> flushedSegmentTask, if there is no index or no need to build index, indexID is zero.
	flushedSegments map[UniqueID]map[UniqueID]*flushedSegmentTask
}

type flushedSegmentTask struct {
	// just use init, inProgress, Done
	state       indexTaskState
	segmentInfo *querypb.SegmentInfo
	binLogs     []*datapb.FieldBinlog
	initTask    bool
}

func newFlushSegmentWatcher(ctx context.Context, kv kv.MetaKv, meta *metaTable, builder *indexBuilder, ic *IndexCoord) (*flushedSegmentWatcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	fsw := &flushedSegmentWatcher{
		ctx:              ctx,
		cancel:           cancel,
		kvClient:         kv,
		wg:               sync.WaitGroup{},
		taskMutex:        sync.RWMutex{},
		scheduleDuration: time.Second * 10,
		notify:           make(chan struct{}, 10),
		meta:             meta,
		builder:          builder,
		ic:               ic,
	}
	err := fsw.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return fsw, nil
}

func (fsw *flushedSegmentWatcher) reloadFromKV() error {
	log.Info("flushSegmentWatcher reloadFromKV")
	fsw.flushedSegments = make(map[UniqueID]map[UniqueID]*flushedSegmentTask)
	_, values, version, err := fsw.kvClient.LoadWithRevision(flushedSegmentPrefix)
	if err != nil {
		log.Error("flushSegmentWatcher reloadFromKV fail", zap.String("prefix", flushedSegmentPrefix), zap.Error(err))
		return err
	}
	for _, value := range values {
		segmentInfo := &datapb.SegmentInfo{}
		if err = proto.Unmarshal([]byte(value), segmentInfo); err != nil {
			log.Error("flushSegmentWatcher unmarshal segment info fail", zap.Error(err))
			return err
		}
		fsw.enqueue(segmentInfo)
	}
	fsw.etcdRevision = version
	return nil
}

func (fsw *flushedSegmentWatcher) Start() {
	fsw.wg.Add(1)
	go fsw.scheduler()
}

func (fsw *flushedSegmentWatcher) Stop() {
	fsw.cancel()
	fsw.wg.Wait()
}

func (fsw *flushedSegmentWatcher) enqueue(segmentInfo *datapb.SegmentInfo) {
	fsw.taskMutex.Lock()
	defer fsw.taskMutex.Unlock()

	// -1 means the task is need to init
	if _, ok := fsw.flushedSegments[segmentInfo.ID]; !ok {
		fsw.flushedSegments[segmentInfo.ID] = make(map[UniqueID]*flushedSegmentTask)
	}
	fsw.flushedSegments[segmentInfo.ID][-1] = &flushedSegmentTask{
		segmentInfo: &querypb.SegmentInfo{
			SegmentID:           segmentInfo.ID,
			CollectionID:        segmentInfo.CollectionID,
			PartitionID:         segmentInfo.PartitionID,
			NumRows:             segmentInfo.NumOfRows,
			CompactionFrom:      segmentInfo.CompactionFrom,
			CreatedByCompaction: segmentInfo.CreatedByCompaction,
			SegmentState:        segmentInfo.State,
			EnableIndex:         false,
		},
		binLogs:  segmentInfo.GetBinlogs(),
		state:    indexTaskWait,
		initTask: true,
	}
	fsw.notify <- struct{}{}
}

func (fsw *flushedSegmentWatcher) scheduler() {
	log.Info("IndexCoord flushedSegmentWatcher scheduler start...")
	defer fsw.wg.Done()

	ticker := time.NewTicker(fsw.scheduleDuration)
	defer ticker.Stop()

	for {
		select {
		case <-fsw.ctx.Done():
			log.Warn("IndexCoord flushedSegmentWatcher context done")
			return
		case <-ticker.C:
			fsw.run()
		case <-fsw.notify:
			fsw.run()
		}
	}
}

func (fsw *flushedSegmentWatcher) run() {
	fsw.taskMutex.Lock()
	defer fsw.taskMutex.Unlock()
	if len(fsw.flushedSegments) > 0 {
		log.Debug("IndexCoord flushedSegmentWatcher schedule task", zap.Int("task num", len(fsw.flushedSegments)))
		for _, tasks := range fsw.flushedSegments {
			for _, t := range tasks {
				fsw.process(t)
			}
		}
	}
}

func (fsw *flushedSegmentWatcher) process(task *flushedSegmentTask) {
	// check if the segment needs index?
	segID := task.segmentInfo.SegmentID
	switch task.state {
	case indexTaskWait:
		if err := fsw.constructTask(task); err != nil {
			log.Error("IndexCoord flushedSegmentWatcher construct task fail", zap.Int64("segID", segID),
				zap.Int64s("compactFrom", task.segmentInfo.CompactionFrom), zap.Error(err))
			return
		}
		fsw.flushedSegments[segID][-1].state = indexTaskDone
		return
	case indexTaskInit:
		log.Debug("debug for init ", zap.Any("task", task.segmentInfo), zap.Any("binlog", task.binLogs))
		createTs, err := fsw.ic.tsoAllocator.AllocOne()
		if err != nil {
			log.Warn("IndexCoord flushedSegmentWatcher alloc timestamp fail", zap.Error(err))
			return
		}

		//get binLogs
		binLogs := make([]string, 0)
		for _, fieldBinLog := range task.binLogs {
			if fieldBinLog.GetFieldID() == task.segmentInfo.IndexInfos[0].FieldID {
				for _, binLog := range fieldBinLog.GetBinlogs() {
					binLogs = append(binLogs, binLog.LogPath)
				}
				break
			}
		}
		segIdx := &model.SegmentIndex{
			Segment: model.Segment{
				SegmentID:    segID,
				CollectionID: task.segmentInfo.CollectionID,
				PartitionID:  task.segmentInfo.PartitionID,
				NumRows:      task.segmentInfo.NumRows,
				BinLogs:      binLogs,
			},
			IndexID:    task.segmentInfo.IndexID,
			CreateTime: createTs,
		}

		//create index task for metaTable
		// send to indexBuilder
		if err := fsw.ic.createIndexForSegment(segIdx); err != nil {
			log.Warn("IndexCoord create index for segment fail", zap.Int64("segID", segID),
				zap.Int64("indexID", task.segmentInfo.IndexID), zap.Error(err))
			return
		}
		fsw.flushedSegments[segID][task.segmentInfo.IndexID].state = indexTaskInProgress

	case indexTaskInProgress:
		filePath, err := fsw.meta.GetIndexFilePathInfo(segID, task.segmentInfo.IndexID)
		if err != nil {
			log.Warn("IndexCoord get index file path fail, maybe it is in progress", zap.Int64("collID", task.segmentInfo.CollectionID),
				zap.Int64("partID", task.segmentInfo.PartitionID), zap.Int64("segID", segID), zap.Error(err))
			return
		}
		fsw.flushedSegments[segID][task.segmentInfo.IndexID].segmentInfo.IndexInfos[0].IndexFilePaths = filePath.IndexFilePaths
		fsw.flushedSegments[segID][task.segmentInfo.IndexID].segmentInfo.IndexInfos[0].IndexSize = int64(filePath.SerializedSize)
		fsw.flushedSegments[segID][task.segmentInfo.IndexID].state = indexTaskDone

		return
	case indexTaskDone:
		if task.initTask {
			if len(fsw.flushedSegments[segID]) == 1 {
				if err := fsw.removeFlushedSegment(task); err != nil {
					log.Warn("IndexCoord remove flushed segment fail, wait to retry", zap.Int64("collID", task.segmentInfo.CollectionID),
						zap.Int64("partID", task.segmentInfo.PartitionID), zap.Int64("segID", segID), zap.Error(err))
					return
				}
				delete(fsw.flushedSegments, segID)
			}
			return
		}

		if err := fsw.writeHandoffSegment(task); err != nil {
			log.Warn("IndexCoord writeHandoffSegment fail, wait to retry", zap.Int64("collID", task.segmentInfo.CollectionID),
				zap.Int64("partID", task.segmentInfo.PartitionID), zap.Int64("segID", task.segmentInfo.SegmentID), zap.Error(err))
			return
		}
		delete(fsw.flushedSegments[task.segmentInfo.SegmentID], task.segmentInfo.IndexID)
		return
	}
}

func (fsw *flushedSegmentWatcher) constructTask(t *flushedSegmentTask) error {
	buildIDs, err := fsw.meta.MarkSegmentsIndexAsDeleted(t.segmentInfo.CompactionFrom)
	if err != nil {
		log.Error("IndexCoord mark compacted segments' index fail", zap.Int64("segID", t.segmentInfo.SegmentID),
			zap.Int64s("compactFrom", t.segmentInfo.CompactionFrom), zap.Error(err))
		return err
	}
	for _, buildID := range buildIDs {
		fsw.builder.markTaskAsDeleted(buildID)
	}

	fieldIndexes := fsw.meta.GetIndexesForCollection(t.segmentInfo.CollectionID, "")
	if t.segmentInfo.NumRows < Params.IndexCoordCfg.MinSegmentNumRowsToEnableIndex || len(fieldIndexes) == 0 {
		log.Warn("segment no need to build index", zap.Int64("segmentID", t.segmentInfo.SegmentID),
			zap.Int64("num of rows", t.segmentInfo.NumRows), zap.Int("collection indexes num", len(fieldIndexes)))
		fsw.flushedSegments[t.segmentInfo.SegmentID][0] = &flushedSegmentTask{
			segmentInfo: t.segmentInfo,
			state:       indexTaskDone,
			binLogs:     t.binLogs,
			initTask:    false,
		}
		fsw.notify <- struct{}{}
		return nil
	}
	for _, index := range fieldIndexes {
		fsw.flushedSegments[t.segmentInfo.SegmentID][index.IndexID] = &flushedSegmentTask{
			segmentInfo: &querypb.SegmentInfo{
				SegmentID:           t.segmentInfo.SegmentID,
				CollectionID:        t.segmentInfo.CollectionID,
				PartitionID:         t.segmentInfo.PartitionID,
				NumRows:             t.segmentInfo.NumRows,
				CompactionFrom:      t.segmentInfo.CompactionFrom,
				CreatedByCompaction: t.segmentInfo.CreatedByCompaction,
				SegmentState:        t.segmentInfo.SegmentState,
				EnableIndex:         true,
				IndexName:           index.IndexName,
				IndexID:             index.IndexID,
				IndexInfos: []*querypb.FieldIndexInfo{
					{
						FieldID:     index.FieldID,
						EnableIndex: true,
						IndexName:   index.IndexName,
						IndexID:     index.IndexID,
						IndexParams: index.IndexParams,
					},
				},
			},
			state:   indexTaskInit,
			binLogs: t.binLogs,
		}
		hasIndex, _ := fsw.meta.CheckBuiltIndex(t.segmentInfo.SegmentID, index.IndexID)
		if hasIndex {
			state := fsw.meta.GetSegmentIndexState(t.segmentInfo.SegmentID, index.IndexID)
			switch state.state {
			case commonpb.IndexState_IndexStateNone:
				fsw.flushedSegments[t.segmentInfo.SegmentID][index.IndexID].state = indexTaskInit

			case commonpb.IndexState_InProgress, commonpb.IndexState_Unissued, commonpb.IndexState_Retry:
				fsw.flushedSegments[t.segmentInfo.SegmentID][index.IndexID].state = indexTaskInProgress

			case commonpb.IndexState_Finished, commonpb.IndexState_Failed:
				fsw.flushedSegments[t.segmentInfo.SegmentID][index.IndexID].state = indexTaskDone
			default:
				// can not to here
			}
		}
	}
	fsw.notify <- struct{}{}
	return nil
}

func (fsw *flushedSegmentWatcher) writeHandoffSegment(t *flushedSegmentTask) error {
	key := fmt.Sprintf("%s/%d/%d/%d", util.HandoffSegmentPrefix, t.segmentInfo.CollectionID, t.segmentInfo.PartitionID, t.segmentInfo.SegmentID)
	value, err := proto.Marshal(t.segmentInfo)
	if err != nil {
		log.Error("IndexCoord marshal handoff task fail", zap.Int64("collID", t.segmentInfo.CollectionID),
			zap.Int64("partID", t.segmentInfo.PartitionID), zap.Int64("segID", t.segmentInfo.SegmentID), zap.Error(err))
		return err
	}
	err = fsw.kvClient.Save(key, string(value))
	if err != nil {
		log.Error("IndexCoord save handoff task fail", zap.Int64("collID", t.segmentInfo.CollectionID),
			zap.Int64("partID", t.segmentInfo.PartitionID), zap.Int64("segID", t.segmentInfo.SegmentID), zap.Error(err))
		return err
	}

	log.Info("IndexCoord write handoff task success", zap.Int64("collID", t.segmentInfo.CollectionID),
		zap.Int64("partID", t.segmentInfo.PartitionID), zap.Int64("segID", t.segmentInfo.SegmentID))
	return nil
}

func (fsw *flushedSegmentWatcher) removeFlushedSegment(t *flushedSegmentTask) error {
	deletedKeys := fmt.Sprintf("%s/%d/%d/%d", util.FlushedSegmentPrefix, t.segmentInfo.CollectionID, t.segmentInfo.PartitionID, t.segmentInfo.SegmentID)
	err := fsw.kvClient.RemoveWithPrefix(deletedKeys)
	if err != nil {
		log.Error("IndexCoord remove flushed segment fail", zap.Int64("collID", t.segmentInfo.CollectionID),
			zap.Int64("partID", t.segmentInfo.PartitionID), zap.Int64("segID", t.segmentInfo.SegmentID), zap.Error(err))
		return err
	}
	log.Info("IndexCoord remove flushed segment success", zap.Int64("collID", t.segmentInfo.CollectionID),
		zap.Int64("partID", t.segmentInfo.PartitionID), zap.Int64("segID", t.segmentInfo.SegmentID))
	return nil
}
