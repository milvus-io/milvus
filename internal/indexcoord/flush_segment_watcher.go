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

	"github.com/milvus-io/milvus/internal/proto/indexpb"

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
	SegmentInfo *datapb.SegmentInfo
	IndexInfo   *indexpb.IndexInfo

	// just use init, inProgress, Done
	state indexTaskState

	indexFilePaths []string
	serializedSize uint64
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
		SegmentInfo: segmentInfo,
		state:       indexTaskWait,
	}
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
			fsw.taskMutex.Lock()
			if len(fsw.flushedSegments) > 0 {
				log.Info("IndexCoord flushedSegmentWatcher schedule task", zap.Int("task num", len(fsw.flushedSegments)))
				for _, tasks := range fsw.flushedSegments {
					if len(tasks) == 1 {
						if t, ok := tasks[-1]; ok {
							if err := fsw.removeFlushedSegment(t); err != nil {
								log.Warn("IndexCoord remove flushed segment fail, wait to retry", zap.Int64("collID", t.SegmentInfo.CollectionID),
									zap.Int64("partID", t.SegmentInfo.PartitionID), zap.Int64("segID", t.SegmentInfo.ID), zap.Error(err))
								continue
							}
							delete(fsw.flushedSegments, t.SegmentInfo.ID)
						}
						continue
					}
					for indexID, t := range tasks {
						if -1 == indexID {
							continue
						}
						fsw.process(t)
					}
				}
			}

			fsw.taskMutex.Unlock()
		}
	}
}

func (fsw *flushedSegmentWatcher) process(task *flushedSegmentTask) {
	// check if the segment needs index?

	switch task.state {
	case indexTaskWait:
		if err := fsw.constructTask(task.SegmentInfo); err != nil {
			log.Error("IndexCoord flushedSegmentWatcher construct task fail", zap.Int64("segID", task.SegmentInfo.ID),
				zap.Int64s("compactFrom", task.SegmentInfo.CompactionFrom), zap.Error(err))
			return
		}
		fsw.flushedSegments[task.SegmentInfo.ID][-1].state = indexTaskDone
		return
	case indexTaskInit:
		createTs, err := fsw.ic.tsoAllocator.AllocOne()
		if err != nil {
			log.Warn("IndexCoord flushedSegmentWatcher alloc timestamp fail", zap.Error(err))
			return
		}

		//get binLogs
		binLogs := make([]string, 0)
		for _, fieldBinLog := range task.SegmentInfo.GetBinlogs() {
			if fieldBinLog.GetFieldID() == task.IndexInfo.FieldID {
				for _, binLog := range fieldBinLog.GetBinlogs() {
					binLogs = append(binLogs, binLog.LogPath)
				}
				break
			}
		}
		segIdx := &model.SegmentIndex{
			Segment: model.Segment{
				SegmentID:    task.SegmentInfo.ID,
				CollectionID: task.SegmentInfo.CollectionID,
				PartitionID:  task.SegmentInfo.PartitionID,
				NumRows:      task.SegmentInfo.NumOfRows,
				BinLogs:      binLogs,
			},
			IndexID:    task.IndexInfo.IndexID,
			CreateTime: createTs,
		}

		//create index task for metaTable
		// send to indexBuilder
		if err := fsw.ic.createIndexForSegment(segIdx); err != nil {
			log.Warn("IndexCoord create index for segment fail", zap.Int64("segID", task.SegmentInfo.ID),
				zap.Int64("indexID", task.IndexInfo.IndexID), zap.Error(err))
			return
		}
		fsw.flushedSegments[task.SegmentInfo.ID][task.IndexInfo.IndexID].state = indexTaskInProgress

	case indexTaskInProgress:
		filePath, err := fsw.meta.GetIndexFilePathInfo(task.SegmentInfo.ID, task.IndexInfo.IndexID)
		if err != nil {
			log.Warn("IndexCoord get index file path fail, maybe it is in progress", zap.Int64("collID", task.SegmentInfo.CollectionID),
				zap.Int64("partID", task.SegmentInfo.PartitionID), zap.Int64("segID", task.SegmentInfo.ID), zap.Error(err))
			return
		}
		fsw.flushedSegments[task.SegmentInfo.ID][task.IndexInfo.IndexID].indexFilePaths = filePath.IndexFilePaths
		fsw.flushedSegments[task.SegmentInfo.ID][task.IndexInfo.IndexID].serializedSize = filePath.SerializedSize
		fsw.flushedSegments[task.SegmentInfo.ID][task.IndexInfo.IndexID].state = indexTaskDone

		return
	case indexTaskDone:
		if err := fsw.writeHandoffSegment(task); err != nil {
			log.Warn("IndexCoord writeHandoffSegment fail, wait to retry", zap.Int64("collID", task.SegmentInfo.CollectionID),
				zap.Int64("partID", task.SegmentInfo.PartitionID), zap.Int64("segID", task.SegmentInfo.ID), zap.Error(err))
			return
		}
		delete(fsw.flushedSegments[task.SegmentInfo.ID], task.IndexInfo.IndexID)
		return
	}
}

func (fsw *flushedSegmentWatcher) constructTask(segmentInfo *datapb.SegmentInfo) error {
	buildIDs, err := fsw.meta.MarkSegmentsIndexAsDeleted(segmentInfo.CompactionFrom)
	if err != nil {
		log.Error("IndexCoord mark compacted segments' index fail", zap.Int64("segID", segmentInfo.ID),
			zap.Int64s("compactFrom", segmentInfo.CompactionFrom), zap.Error(err))
		return err
	}
	for _, buildID := range buildIDs {
		fsw.builder.markTaskAsDeleted(buildID)
	}

	fieldIndexes := fsw.meta.GetIndexesForCollection(segmentInfo.CollectionID, "")
	if segmentInfo.NumOfRows < Params.IndexCoordCfg.MinSegmentNumRowsToEnableIndex || len(fieldIndexes) == 0 {
		log.Warn("segemnt no need to build index", zap.Int64("segmentID", segmentInfo.ID),
			zap.Int64("num of rows", segmentInfo.NumOfRows), zap.Int("collection indexes num", len(fieldIndexes)))
		fsw.flushedSegments[segmentInfo.ID][0] = &flushedSegmentTask{
			SegmentInfo: segmentInfo,
			state:       indexTaskDone,
		}
		fsw.notify <- struct{}{}
		return nil
	}
	for _, index := range fieldIndexes {
		fsw.flushedSegments[segmentInfo.ID][index.IndexID] = &flushedSegmentTask{
			SegmentInfo: segmentInfo,
			IndexInfo: &indexpb.IndexInfo{
				CollectionID: segmentInfo.CollectionID,
				FieldID:      index.FieldID,
				IndexName:    index.IndexName,
				IndexID:      index.IndexID,
				TypeParams:   index.TypeParams,
				IndexParams:  index.IndexParams,
			},
			state: indexTaskInit,
		}
		hasIndex, _ := fsw.meta.CheckBuiltIndex(segmentInfo.ID, index.IndexID)
		if hasIndex {
			state := fsw.meta.GetSegmentIndexState(segmentInfo.ID, index.IndexID)
			switch state.state {
			case commonpb.IndexState_IndexStateNone:
				fsw.flushedSegments[segmentInfo.ID][index.IndexID].state = indexTaskInit

			case commonpb.IndexState_InProgress, commonpb.IndexState_Unissued, commonpb.IndexState_Retry:
				fsw.flushedSegments[segmentInfo.ID][index.IndexID].state = indexTaskInProgress

			case commonpb.IndexState_Finished, commonpb.IndexState_Failed:
				fsw.flushedSegments[segmentInfo.ID][index.IndexID].state = indexTaskDone

			}
		}
	}
	fsw.notify <- struct{}{}
	return nil
}

func (fsw *flushedSegmentWatcher) writeHandoffSegment(t *flushedSegmentTask) error {
	handoffSegment := &querypb.SegmentInfo{
		SegmentID:           t.SegmentInfo.ID,
		CollectionID:        t.SegmentInfo.CollectionID,
		PartitionID:         t.SegmentInfo.PartitionID,
		NumRows:             t.SegmentInfo.NumOfRows,
		IndexName:           t.IndexInfo.IndexName,
		IndexID:             t.IndexInfo.IndexID,
		CompactionFrom:      t.SegmentInfo.CompactionFrom,
		CreatedByCompaction: t.SegmentInfo.CreatedByCompaction,
		SegmentState:        t.SegmentInfo.GetState(),
		IndexInfos: []*querypb.FieldIndexInfo{
			{
				FieldID:     t.IndexInfo.FieldID,
				EnableIndex: true,
				IndexName:   t.IndexInfo.IndexName,
				IndexID:     t.IndexInfo.IndexID,
				// unnecessary
				BuildID:        0,
				IndexParams:    t.IndexInfo.IndexParams,
				IndexFilePaths: t.indexFilePaths,
				IndexSize:      int64(t.serializedSize),
			},
		},
	}
	key := fmt.Sprintf("%s/%d/%d/%d", util.HandoffSegmentPrefix, t.SegmentInfo.CollectionID, t.SegmentInfo.PartitionID, t.SegmentInfo.ID)
	value, err := proto.Marshal(handoffSegment)
	if err != nil {
		log.Error("IndexCoord marshal handoff task fail", zap.Int64("collID", t.SegmentInfo.CollectionID),
			zap.Int64("partID", t.SegmentInfo.PartitionID), zap.Int64("segID", t.SegmentInfo.ID), zap.Error(err))
		return err
	}
	err = fsw.kvClient.Save(key, string(value))
	if err != nil {
		log.Error("IndexCoord save handoff task fail", zap.Int64("collID", t.SegmentInfo.CollectionID),
			zap.Int64("partID", t.SegmentInfo.PartitionID), zap.Int64("segID", t.SegmentInfo.ID), zap.Error(err))
		return err
	}

	log.Info("IndexCoord write handoff task success", zap.Int64("collID", t.SegmentInfo.CollectionID),
		zap.Int64("partID", t.SegmentInfo.PartitionID), zap.Int64("segID", t.SegmentInfo.ID))
	return nil
}

func (fsw *flushedSegmentWatcher) removeFlushedSegment(t *flushedSegmentTask) error {
	deletedKeys := fmt.Sprintf("%s/%d/%d/%d", util.FlushedSegmentPrefix, t.SegmentInfo.CollectionID, t.SegmentInfo.PartitionID, t.SegmentInfo.ID)
	err := fsw.kvClient.RemoveWithPrefix(deletedKeys)
	if err != nil {
		log.Error("IndexCoord remove flushed segment fail", zap.Int64("collID", t.SegmentInfo.CollectionID),
			zap.Int64("partID", t.SegmentInfo.PartitionID), zap.Int64("segID", t.SegmentInfo.ID), zap.Error(err))
		return err
	}
	log.Info("IndexCoord remove flushed segment success", zap.Int64("collID", t.SegmentInfo.CollectionID),
		zap.Int64("partID", t.SegmentInfo.PartitionID), zap.Int64("segID", t.SegmentInfo.ID))
	return nil
}
