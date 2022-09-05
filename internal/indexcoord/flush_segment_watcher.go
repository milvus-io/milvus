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

	"github.com/golang/protobuf/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util"
)

type flushedSegmentWatcher struct {
	ctx    context.Context
	cancel context.CancelFunc

	kvClient         kv.MetaKv
	wg               sync.WaitGroup
	scheduleDuration time.Duration

	internalTaskMutex sync.RWMutex
	childrenTaskMutex sync.RWMutex
	internalNotify    chan struct{}
	childrenNotify    chan struct{}

	etcdRevision int64
	watchChan    clientv3.WatchChan

	meta    *metaTable
	builder *indexBuilder
	ic      *IndexCoord

	internalTasks map[UniqueID]*internalTask
	// segmentID -> indexID -> flushedSegmentTask, if there is no index or no need to build index, indexID is zero.
	childrenTasks map[UniqueID]map[UniqueID]*childrenTask
}

type internalTask struct {
	state       indexTaskState
	segmentInfo *datapb.SegmentInfo
}

type childrenTask struct {
	internalTask
	indexInfo *querypb.FieldIndexInfo
}

func newFlushSegmentWatcher(ctx context.Context, kv kv.MetaKv, meta *metaTable, builder *indexBuilder, ic *IndexCoord) (*flushedSegmentWatcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	fsw := &flushedSegmentWatcher{
		ctx:               ctx,
		cancel:            cancel,
		kvClient:          kv,
		wg:                sync.WaitGroup{},
		internalTaskMutex: sync.RWMutex{},
		childrenTaskMutex: sync.RWMutex{},
		scheduleDuration:  time.Second * 10,
		internalNotify:    make(chan struct{}, 1),
		childrenNotify:    make(chan struct{}, 1),
		meta:              meta,
		builder:           builder,
		ic:                ic,
	}
	err := fsw.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return fsw, nil
}

func (fsw *flushedSegmentWatcher) reloadFromKV() error {
	log.Info("flushSegmentWatcher reloadFromKV")
	fsw.internalTasks = make(map[UniqueID]*internalTask)
	fsw.childrenTasks = make(map[int64]map[int64]*childrenTask)
	_, values, version, err := fsw.kvClient.LoadWithRevision(util.FlushedSegmentPrefix)
	if err != nil {
		log.Error("flushSegmentWatcher reloadFromKV fail", zap.String("prefix", util.FlushedSegmentPrefix), zap.Error(err))
		return err
	}
	for _, value := range values {
		segmentInfo := &datapb.SegmentInfo{}
		if err = proto.Unmarshal([]byte(value), segmentInfo); err != nil {
			log.Error("flushSegmentWatcher unmarshal segment info fail", zap.Error(err))
			return err
		}
		fsw.enqueueInternalTask(segmentInfo)
	}
	fsw.etcdRevision = version
	return nil
}

func (fsw *flushedSegmentWatcher) Start() {
	fsw.wg.Add(1)
	go fsw.internalScheduler()

	fsw.wg.Add(1)
	go fsw.childrenScheduler()
}

func (fsw *flushedSegmentWatcher) Stop() {
	fsw.cancel()
	close(fsw.internalNotify)
	close(fsw.childrenNotify)
	fsw.wg.Wait()
}

func (fsw *flushedSegmentWatcher) enqueueInternalTask(segmentInfo *datapb.SegmentInfo) {
	fsw.internalTaskMutex.Lock()
	defer fsw.internalTaskMutex.Unlock()

	fsw.internalTasks[segmentInfo.ID] = &internalTask{
		segmentInfo: segmentInfo,
		state:       indexTaskInit,
	}
	select {
	case fsw.internalNotify <- struct{}{}:
	default:
	}
}

func (fsw *flushedSegmentWatcher) enqueueChildrenTask(segmentInfo *datapb.SegmentInfo, index *model.Index) {
	fsw.childrenTaskMutex.Lock()
	defer fsw.childrenTaskMutex.Unlock()

	if _, ok := fsw.childrenTasks[segmentInfo.ID]; !ok {
		fsw.childrenTasks[segmentInfo.ID] = make(map[int64]*childrenTask)
	}
	fsw.childrenTasks[segmentInfo.ID][index.IndexID] = &childrenTask{
		internalTask: internalTask{
			state:       indexTaskInit,
			segmentInfo: segmentInfo,
		},
		indexInfo: &querypb.FieldIndexInfo{
			FieldID:     index.FieldID,
			EnableIndex: true,
			IndexName:   index.IndexName,
			IndexID:     index.IndexID,
			IndexParams: index.IndexParams,
		},
	}
	log.Debug("IndexCoord flushedSegmentWatcher create index for flushed segment",
		zap.Int64("segID", segmentInfo.ID), zap.Int64("indexID", index.IndexID))
	hasIndex, buildID := fsw.meta.HasSameIndex(segmentInfo.ID, index.IndexID)
	if hasIndex {
		fsw.childrenTasks[segmentInfo.ID][index.IndexID].indexInfo.BuildID = buildID
		state := fsw.meta.GetSegmentIndexState(segmentInfo.ID, index.IndexID)
		switch state.state {
		case commonpb.IndexState_IndexStateNone:
			fsw.childrenTasks[segmentInfo.ID][index.IndexID].state = indexTaskInit

		case commonpb.IndexState_InProgress, commonpb.IndexState_Unissued, commonpb.IndexState_Retry:
			fsw.childrenTasks[segmentInfo.ID][index.IndexID].state = indexTaskInProgress

		case commonpb.IndexState_Finished, commonpb.IndexState_Failed:
			fsw.childrenTasks[segmentInfo.ID][index.IndexID].state = indexTaskDone
		default:
			// can not to here
		}
	}
	select {
	case fsw.childrenNotify <- struct{}{}:
	default:
	}
}

func (fsw *flushedSegmentWatcher) internalScheduler() {
	log.Info("IndexCoord flushedSegmentWatcher internalScheduler start...")
	defer fsw.wg.Done()

	ticker := time.NewTicker(fsw.scheduleDuration)
	defer ticker.Stop()

	for {
		select {
		case <-fsw.ctx.Done():
			log.Warn("IndexCoord flushedSegmentWatcher context done")
			return
		case <-ticker.C:
			fsw.internalRun()
		case <-fsw.internalNotify:
			fsw.internalRun()
		}
	}
}

func (fsw *flushedSegmentWatcher) childrenScheduler() {
	log.Info("IndexCoord flushedSegmentWatcher childrenScheduler start...")
	defer fsw.wg.Done()

	ticker := time.NewTicker(fsw.scheduleDuration)
	defer ticker.Stop()

	for {
		select {
		case <-fsw.ctx.Done():
			log.Warn("IndexCoord flushedSegmentWatcher context done")
			return
		case <-ticker.C:
			fsw.childrenRun()
		case <-fsw.childrenNotify:
			fsw.childrenRun()
		}
	}
}

func (fsw *flushedSegmentWatcher) internalRun() {
	fsw.internalTaskMutex.Lock()
	defer fsw.internalTaskMutex.Unlock()
	if len(fsw.internalTasks) > 0 {
		log.Debug("IndexCoord flushedSegmentWatcher schedule internal tasks", zap.Int("internal task num", len(fsw.internalTasks)))
		for _, t := range fsw.internalTasks {
			fsw.internalProcess(t)
		}
	}
}

func (fsw *flushedSegmentWatcher) childrenRun() {
	fsw.childrenTaskMutex.Lock()
	defer fsw.childrenTaskMutex.Unlock()
	if len(fsw.childrenTasks) > 0 {
		log.Debug("IndexCoord flushedSegmentWatcher schedule children tasks", zap.Int("internal task num", len(fsw.childrenTasks)))
		for segID, tasks := range fsw.childrenTasks {
			for _, t := range tasks {
				fsw.childrenProcess(t)
			}
			if len(fsw.childrenTasks[segID]) == 0 {
				delete(fsw.childrenTasks, segID)
			}
		}
	}
}

func (fsw *flushedSegmentWatcher) removeCompactedTasks(t *internalTask) {
	log.Debug("IndexCoord flushedSegmentWatcher mark task as deleted which is compacted", zap.Int64("segID", t.segmentInfo.ID),
		zap.Int64s("compactionFrom", t.segmentInfo.CompactionFrom))
	fsw.builder.markTasksAsDeleted(fsw.meta.GetBuildIDsFromSegIDs(t.segmentInfo.CompactionFrom))
	for _, segID := range t.segmentInfo.CompactionFrom {
		if _, ok := fsw.internalTasks[segID]; ok {
			fsw.internalTasks[segID].state = indexTaskDeleted
		}
	}
}

func (fsw *flushedSegmentWatcher) internalProcess(t *internalTask) {
	log.Debug("IndexCoord flushedSegmentWatcher process internal task", zap.Int64("segID", t.segmentInfo.ID),
		zap.String("state", t.state.String()))
	switch t.state {
	case indexTaskInit:
		if t.segmentInfo.CreatedByCompaction {
			fsw.removeCompactedTasks(t)
		}
		if err := fsw.constructTask(t); err != nil {
			log.Error("IndexCoord flushedSegmentWatcher construct task fail", zap.Int64("segID", t.segmentInfo.ID),
				zap.Int64s("compactFrom", t.segmentInfo.CompactionFrom), zap.Error(err))
			return
		}
		fsw.internalTasks[t.segmentInfo.ID].state = indexTaskInProgress
		select {
		case fsw.internalNotify <- struct{}{}:
		default:
		}
		return
	case indexTaskInProgress:
		fsw.childrenTaskMutex.RLock()
		defer fsw.childrenTaskMutex.RUnlock()
		if tasks, ok := fsw.childrenTasks[t.segmentInfo.ID]; !ok || len(tasks) == 0 {
			fsw.internalTasks[t.segmentInfo.ID].state = indexTaskDone
			select {
			case fsw.internalNotify <- struct{}{}:
			default:
			}
		}
		return
	case indexTaskDone:
		handoffTask := &querypb.SegmentInfo{
			SegmentID:           t.segmentInfo.ID,
			CollectionID:        t.segmentInfo.CollectionID,
			PartitionID:         t.segmentInfo.PartitionID,
			NumRows:             t.segmentInfo.NumOfRows,
			DmChannel:           t.segmentInfo.GetInsertChannel(),
			IndexName:           "",
			IndexID:             0,
			CompactionFrom:      t.segmentInfo.CompactionFrom,
			CreatedByCompaction: t.segmentInfo.CreatedByCompaction,
			SegmentState:        t.segmentInfo.State,
			IndexInfos:          nil,
			EnableIndex:         false,
		}
		if err := fsw.writeHandoffSegment(handoffTask); err != nil {
			log.Error("IndexCoord flushSegmentWatcher writeHandoffSegment with no index fail",
				zap.Int64("segID", t.segmentInfo.ID), zap.Error(err))
			return
		}
		if err := fsw.removeFlushedSegment(t); err != nil {
			log.Error("IndexCoord flushSegmentWatcher removeFlushedSegment fail",
				zap.Int64("segID", t.segmentInfo.ID), zap.Error(err))
			return
		}
		delete(fsw.internalTasks, t.segmentInfo.ID)
		return
	case indexTaskDeleted:
		if t.segmentInfo.CreatedByCompaction {
			fsw.removeCompactedTasks(t)
		}
		fsw.childrenTaskMutex.Lock()
		delete(fsw.childrenTasks, t.segmentInfo.ID)
		fsw.childrenTaskMutex.Unlock()
		fsw.internalTasks[t.segmentInfo.ID].state = indexTaskDone
		return
	default:
		log.Debug("IndexCoord flushedSegmentWatcher internal task get invalid state", zap.Int64("segID", t.segmentInfo.ID),
			zap.String("state", t.state.String()))
	}
}

func (fsw *flushedSegmentWatcher) childrenProcess(task *childrenTask) {
	log.Debug("IndexCoord flushedSegmentWatcher process child task", zap.Int64("segID", task.segmentInfo.ID),
		zap.Int64("indexID", task.indexInfo.IndexID), zap.String("state", task.state.String()))
	segID := task.segmentInfo.ID
	switch task.state {
	case indexTaskInit:
		//get binLogs
		binLogs := make([]string, 0)
		for _, fieldBinLog := range task.segmentInfo.Binlogs {
			if fieldBinLog.GetFieldID() == task.indexInfo.FieldID {
				for _, binLog := range fieldBinLog.GetBinlogs() {
					binLogs = append(binLogs, binLog.LogPath)
				}
				break
			}
		}
		segIdx := &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: task.segmentInfo.CollectionID,
			PartitionID:  task.segmentInfo.PartitionID,
			NumRows:      task.segmentInfo.NumOfRows,
			IndexID:      task.indexInfo.IndexID,
			CreateTime:   task.segmentInfo.StartPosition.Timestamp,
		}

		//create index task for metaTable
		// send to indexBuilder
		have, buildID, err := fsw.ic.createIndexForSegment(segIdx)
		if err != nil {
			log.Warn("IndexCoord create index for segment fail", zap.Int64("segID", segID),
				zap.Int64("indexID", task.indexInfo.IndexID), zap.Error(err))
			return
		}
		if !have {
			fsw.builder.enqueue(buildID)
		}

		fsw.childrenTasks[segID][task.indexInfo.IndexID].state = indexTaskInProgress
		return
	case indexTaskInProgress:
		filePath, err := fsw.meta.GetIndexFilePathInfo(segID, task.indexInfo.IndexID)
		if err != nil {
			log.Warn("IndexCoord get index file path fail", zap.Int64("collID", task.segmentInfo.CollectionID),
				zap.Int64("partID", task.segmentInfo.PartitionID), zap.Int64("segID", segID), zap.Error(err))
			return
		}
		fsw.childrenTasks[segID][task.indexInfo.IndexID].indexInfo.IndexFilePaths = filePath.IndexFilePaths
		fsw.childrenTasks[segID][task.indexInfo.IndexID].indexInfo.IndexSize = int64(filePath.SerializedSize)
		fsw.childrenTasks[segID][task.indexInfo.IndexID].state = indexTaskDone

		return
	case indexTaskDone:
		handoffTask := &querypb.SegmentInfo{
			SegmentID:           task.segmentInfo.ID,
			CollectionID:        task.segmentInfo.CollectionID,
			PartitionID:         task.segmentInfo.PartitionID,
			NumRows:             task.segmentInfo.NumOfRows,
			IndexName:           task.indexInfo.IndexName,
			IndexID:             task.indexInfo.IndexID,
			CompactionFrom:      task.segmentInfo.CompactionFrom,
			CreatedByCompaction: task.segmentInfo.CreatedByCompaction,
			SegmentState:        task.segmentInfo.State,
			IndexInfos:          []*querypb.FieldIndexInfo{task.indexInfo},
			EnableIndex:         true,
		}
		if err := fsw.writeHandoffSegment(handoffTask); err != nil {
			log.Warn("IndexCoord writeHandoffSegment fail, wait to retry", zap.Int64("collID", task.segmentInfo.CollectionID),
				zap.Int64("partID", task.segmentInfo.PartitionID), zap.Int64("segID", task.segmentInfo.ID), zap.Error(err))
			return
		}
		log.Warn("IndexCoord writeHandoffSegment success", zap.Int64("collID", task.segmentInfo.CollectionID),
			zap.Int64("partID", task.segmentInfo.PartitionID), zap.Int64("segID", task.segmentInfo.ID))
		delete(fsw.childrenTasks[task.segmentInfo.ID], task.indexInfo.IndexID)
		return
	default:
		log.Debug("IndexCoord flushedSegmentWatcher internal task get invalid state", zap.Int64("segID", task.segmentInfo.ID),
			zap.String("state", task.state.String()))
	}
}

func (fsw *flushedSegmentWatcher) constructTask(t *internalTask) error {
	log.Debug("IndexCoord flushedSegmentWatcher construct tasks by segment info", zap.Int64("segID", t.segmentInfo.ID),
		zap.Int64s("compactionFrom", t.segmentInfo.CompactionFrom))
	fieldIndexes := fsw.meta.GetIndexesForCollection(t.segmentInfo.CollectionID, "")
	if t.segmentInfo.NumOfRows < Params.IndexCoordCfg.MinSegmentNumRowsToEnableIndex || len(fieldIndexes) == 0 {
		log.Debug("segment no need to build index", zap.Int64("segmentID", t.segmentInfo.ID),
			zap.Int64("num of rows", t.segmentInfo.NumOfRows), zap.Int("collection indexes num", len(fieldIndexes)))
		// no need to build index
		return nil
	}

	for _, index := range fieldIndexes {
		fsw.enqueueChildrenTask(t.segmentInfo, index)
	}
	return nil
}

func (fsw *flushedSegmentWatcher) writeHandoffSegment(t *querypb.SegmentInfo) error {
	key := fmt.Sprintf("%s/%d/%d/%d", util.HandoffSegmentPrefix, t.CollectionID, t.PartitionID, t.SegmentID)
	value, err := proto.Marshal(t)
	if err != nil {
		log.Error("IndexCoord marshal handoff task fail", zap.Int64("collID", t.CollectionID),
			zap.Int64("partID", t.PartitionID), zap.Int64("segID", t.SegmentID), zap.Error(err))
		return err
	}
	err = fsw.kvClient.Save(key, string(value))
	if err != nil {
		log.Error("IndexCoord save handoff task fail", zap.Int64("collID", t.CollectionID),
			zap.Int64("partID", t.PartitionID), zap.Int64("segID", t.SegmentID), zap.Error(err))
		return err
	}

	log.Info("IndexCoord write handoff task success", zap.Int64("collID", t.CollectionID),
		zap.Int64("partID", t.PartitionID), zap.Int64("segID", t.SegmentID))
	return nil
}

func (fsw *flushedSegmentWatcher) removeFlushedSegment(t *internalTask) error {
	if t.segmentInfo.CreatedByCompaction {
		log.Debug("IndexCoord flushedSegmentWatcher mark the segments indexes as deleted which is compacted", zap.Int64("segID", t.segmentInfo.ID),
			zap.Int64s("compactionFrom", t.segmentInfo.CompactionFrom))
		if err := fsw.meta.MarkSegmentsIndexAsDeleted(t.segmentInfo.CompactionFrom); err != nil {
			log.Error("IndexCoord mark compacted segments' index fail", zap.Int64("segID", t.segmentInfo.ID),
				zap.Int64s("compactFrom", t.segmentInfo.CompactionFrom), zap.Error(err))
			return err
		}
	}

	deletedKeys := fmt.Sprintf("%s/%d/%d/%d", util.FlushedSegmentPrefix, t.segmentInfo.CollectionID, t.segmentInfo.PartitionID, t.segmentInfo.ID)
	err := fsw.kvClient.RemoveWithPrefix(deletedKeys)
	if err != nil {
		log.Error("IndexCoord remove flushed segment fail", zap.Int64("collID", t.segmentInfo.CollectionID),
			zap.Int64("partID", t.segmentInfo.PartitionID), zap.Int64("segID", t.segmentInfo.ID), zap.Error(err))
		return err
	}
	log.Info("IndexCoord remove flushed segment success", zap.Int64("collID", t.segmentInfo.CollectionID),
		zap.Int64("partID", t.segmentInfo.PartitionID), zap.Int64("segID", t.segmentInfo.ID))
	return nil
}
