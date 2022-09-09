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
	"errors"
	"fmt"
	"sort"
	"strconv"
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
		scheduleDuration:  time.Second,
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
		segID, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			log.Error("flushSegmentWatcher parse segmentID fail", zap.String("value", value), zap.Error(err))
			return err
		}
		fsw.enqueueInternalTask(segID)
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
	fsw.wg.Wait()
}

func (fsw *flushedSegmentWatcher) enqueueInternalTask(segmentID UniqueID) {
	fsw.internalTaskMutex.Lock()

	fsw.internalTasks[segmentID] = &internalTask{
		state:       indexTaskInit,
		segmentInfo: nil,
	}
	fsw.internalTaskMutex.Unlock()

	fsw.prepare(segmentID)
	fsw.internalNotifyFunc()
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
	fsw.childrenNotifyFunc()
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
	fsw.internalTaskMutex.RLock()
	segmentIDs := make([]UniqueID, 0, len(fsw.internalTasks))
	if len(fsw.internalTasks) > 0 {
		log.Debug("IndexCoord flushedSegmentWatcher schedule internal tasks", zap.Int("internal task num", len(fsw.internalTasks)))
		for segID := range fsw.internalTasks {
			segmentIDs = append(segmentIDs, segID)
		}
		sort.Slice(segmentIDs, func(i, j int) bool {
			return segmentIDs[i] < segmentIDs[j]
		})
	}
	fsw.internalTaskMutex.RUnlock()

	for _, segID := range segmentIDs {
		fsw.internalProcess(segID)
		break
	}
}

func (fsw *flushedSegmentWatcher) childrenRun() {
	fsw.childrenTaskMutex.RLock()
	segmentIDs := make([]UniqueID, 0, len(fsw.childrenTasks))
	if len(fsw.childrenTasks) > 0 {
		log.Debug("IndexCoord flushedSegmentWatcher schedule children tasks", zap.Int("children task num", len(fsw.childrenTasks)))
		for segID := range fsw.childrenTasks {
			segmentIDs = append(segmentIDs, segID)
		}
		sort.Slice(segmentIDs, func(i, j int) bool {
			return segmentIDs[i] < segmentIDs[j]
		})
	}
	fsw.childrenTaskMutex.RUnlock()
	for _, segID := range segmentIDs {
		tasks := fsw.getChildrenTasks(segID)
		for _, t := range tasks {
			fsw.childrenProcess(t)
		}
	}
}

func (fsw *flushedSegmentWatcher) internalNotifyFunc() {
	select {
	case fsw.internalNotify <- struct{}{}:
	default:
	}
}

func (fsw *flushedSegmentWatcher) childrenNotifyFunc() {
	select {
	case fsw.childrenNotify <- struct{}{}:
	default:
	}
}

func (fsw *flushedSegmentWatcher) updateInternalTaskState(segID UniqueID, state indexTaskState) {
	fsw.internalTaskMutex.Lock()
	defer fsw.internalTaskMutex.Unlock()
	if _, ok := fsw.internalTasks[segID]; ok {
		fsw.internalTasks[segID].state = state
	}
}

func (fsw *flushedSegmentWatcher) deleteInternalTask(segID UniqueID) {
	fsw.internalTaskMutex.Lock()
	defer fsw.internalTaskMutex.Unlock()

	delete(fsw.internalTasks, segID)
}

func (fsw *flushedSegmentWatcher) getInternalTask(segID UniqueID) *internalTask {
	fsw.internalTaskMutex.RLock()
	defer fsw.internalTaskMutex.RUnlock()

	return &internalTask{
		state:       fsw.internalTasks[segID].state,
		segmentInfo: fsw.internalTasks[segID].segmentInfo,
	}
}

func (fsw *flushedSegmentWatcher) setInternalTaskSegmentInfo(segID UniqueID, segInfo *datapb.SegmentInfo) {
	fsw.internalTaskMutex.Lock()
	defer fsw.internalTaskMutex.Unlock()

	if _, ok := fsw.internalTasks[segID]; ok {
		fsw.internalTasks[segID].segmentInfo = segInfo
	}
}

func (fsw *flushedSegmentWatcher) updateChildrenTaskState(segID, indexID UniqueID, state indexTaskState) {
	fsw.childrenTaskMutex.Lock()
	defer fsw.childrenTaskMutex.Unlock()

	if tasks, ok := fsw.childrenTasks[segID]; ok {
		if _, ok = tasks[indexID]; ok {
			fsw.childrenTasks[segID][indexID].state = state

		}
	}
}

func (fsw *flushedSegmentWatcher) hasChildrenTaskDone(segID UniqueID) bool {
	fsw.childrenTaskMutex.RLock()
	defer fsw.childrenTaskMutex.RUnlock()
	if tasks, ok := fsw.childrenTasks[segID]; !ok || len(tasks) == 0 {
		return true
	}
	return false
}

func (fsw *flushedSegmentWatcher) getChildrenTasks(segID UniqueID) map[UniqueID]*childrenTask {
	fsw.childrenTaskMutex.RLock()
	defer fsw.childrenTaskMutex.RUnlock()
	tasks := make(map[UniqueID]*childrenTask)
	if ts, ok := fsw.childrenTasks[segID]; ok {
		for k, v := range ts {
			tasks[k] = v
		}
	}
	return tasks
}

func (fsw *flushedSegmentWatcher) deleteChildTask(segID, indexID UniqueID) {
	fsw.childrenTaskMutex.Lock()
	defer fsw.childrenTaskMutex.Unlock()
	if _, ok := fsw.childrenTasks[segID]; ok {
		delete(fsw.childrenTasks[segID], indexID)
	}
}

func (fsw *flushedSegmentWatcher) deleteChildrenTask(segID UniqueID) {
	fsw.childrenTaskMutex.Lock()
	defer fsw.childrenTaskMutex.Unlock()

	delete(fsw.childrenTasks, segID)
}

func (fsw *flushedSegmentWatcher) internalProcess(segID UniqueID) {
	// first pull segmentInfo
	if err := fsw.pullSegmentInfo(segID); err != nil {
		return
	}
	t := fsw.getInternalTask(segID)
	log.Debug("IndexCoord flushedSegmentWatcher process internal task", zap.Int64("segID", segID),
		zap.String("state", t.state.String()))

	switch t.state {
	case indexTaskInit:
		fsw.constructTask(t)
		fsw.updateInternalTaskState(segID, indexTaskInProgress)
		fsw.internalNotifyFunc()
	case indexTaskInProgress:
		if fsw.hasChildrenTaskDone(segID) {
			fsw.updateInternalTaskState(segID, indexTaskDone)
			fsw.internalNotifyFunc()
		}
	case indexTaskDone:
		handoffTask := &querypb.SegmentInfo{
			SegmentID:           segID,
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
				zap.Int64("segID", segID), zap.Error(err))
			return
		}
		if err := fsw.removeFlushedSegment(t); err != nil {
			log.Error("IndexCoord flushSegmentWatcher removeFlushedSegment fail",
				zap.Int64("segID", segID), zap.Error(err))
			return
		}
		fsw.deleteInternalTask(segID)
		fsw.internalNotifyFunc()
	case indexTaskDeleted:
		if t.segmentInfo.CreatedByCompaction {
			fsw.removeCompactedTasks(t)
		}
		fsw.updateInternalTaskState(segID, indexTaskDone)
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

		fsw.updateChildrenTaskState(segID, task.indexInfo.IndexID, indexTaskInProgress)
		fsw.childrenNotifyFunc()
	case indexTaskInProgress:
		state := fsw.meta.GetSegmentIndexState(task.segmentInfo.ID, task.indexInfo.IndexID)
		if state.state == commonpb.IndexState_IndexStateNone {
			log.Debug("task is no need to build index, remove task", zap.Int64("segID", task.segmentInfo.ID),
				zap.Int64("indexID", task.indexInfo.IndexID))
			fsw.deleteChildTask(task.segmentInfo.ID, task.indexInfo.IndexID)
			fsw.childrenNotifyFunc()
			return
		}
		if state.state != commonpb.IndexState_Finished && state.state != commonpb.IndexState_Failed {
			log.Debug("the index on segment is not finish", zap.Int64("segID", segID),
				zap.String("state", state.state.String()), zap.String("fail reason", state.failReason))
			return
		}
		// don't set index files, QueryCoord get index files from IndexCoord by grpc.
		//fsw.childrenTasks[segID][task.indexInfo.IndexID].indexInfo.IndexFilePaths = filePath.IndexFilePaths
		//fsw.childrenTasks[segID][task.indexInfo.IndexID].indexInfo.IndexSize = int64(filePath.SerializedSize)
		fsw.updateChildrenTaskState(segID, task.indexInfo.IndexID, indexTaskDone)
		fsw.childrenNotifyFunc()
	case indexTaskDone:
		handoffTask := &querypb.SegmentInfo{
			SegmentID:           task.segmentInfo.ID,
			CollectionID:        task.segmentInfo.CollectionID,
			PartitionID:         task.segmentInfo.PartitionID,
			NumRows:             task.segmentInfo.NumOfRows,
			DmChannel:           task.segmentInfo.GetInsertChannel(),
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
		log.Debug("IndexCoord writeHandoffSegment success", zap.Int64("collID", task.segmentInfo.CollectionID),
			zap.Int64("partID", task.segmentInfo.PartitionID), zap.Int64("segID", task.segmentInfo.ID))
		fsw.deleteChildTask(task.segmentInfo.ID, task.indexInfo.IndexID)
		fsw.childrenNotifyFunc()
	default:
		log.Debug("IndexCoord flushedSegmentWatcher internal task get invalid state", zap.Int64("segID", task.segmentInfo.ID),
			zap.String("state", task.state.String()))
	}
}

func (fsw *flushedSegmentWatcher) constructTask(t *internalTask) {
	log.Debug("IndexCoord flushedSegmentWatcher construct tasks by segment info", zap.Int64("segID", t.segmentInfo.ID),
		zap.Int64s("compactionFrom", t.segmentInfo.CompactionFrom))
	fieldIndexes := fsw.meta.GetIndexesForCollection(t.segmentInfo.CollectionID, "")
	if len(fieldIndexes) == 0 {
		log.Debug("segment no need to build index", zap.Int64("segmentID", t.segmentInfo.ID),
			zap.Int64("num of rows", t.segmentInfo.NumOfRows), zap.Int("collection indexes num", len(fieldIndexes)))
		// no need to build index
		return
	}

	for _, index := range fieldIndexes {
		fsw.enqueueChildrenTask(t.segmentInfo, index)
	}
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

func (fsw *flushedSegmentWatcher) pullSegmentInfo(segmentID UniqueID) error {
	t := fsw.getInternalTask(segmentID)
	if t.segmentInfo != nil {
		return nil
	}
	resp, err := fsw.ic.dataCoordClient.GetSegmentInfo(fsw.ctx, &datapb.GetSegmentInfoRequest{
		SegmentIDs:       []int64{segmentID},
		IncludeUnHealthy: true,
	})
	if err != nil {
		log.Error("flushedSegmentWatcher get segment info fail", zap.Int64("segID", segmentID), zap.Error(err))
		return err
	}
	if resp.Status.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Error("flushedSegmentWatcher get segment info fail", zap.Int64("segID", segmentID),
			zap.String("fail reason", resp.Status.GetReason()))
		if resp.Status.GetReason() == msgSegmentNotFound(segmentID) {
			return errSegmentNotFound(segmentID)
		}
		return errors.New(resp.Status.GetReason())
	}
	for _, info := range resp.Infos {
		if info.ID == segmentID {
			fsw.setInternalTaskSegmentInfo(segmentID, info)
			return nil
		}
	}
	errMsg := fmt.Sprintf("flushedSegmentWatcher get segment info fail, the segment is not include in the response with ID: %d", segmentID)
	log.Error(errMsg)
	return errors.New(errMsg)
}

func (fsw *flushedSegmentWatcher) prepare(segID UniqueID) {
	if err := fsw.pullSegmentInfo(segID); err != nil {
		log.Error("flushedSegmentWatcher get segment info fail", zap.Int64("segID", segID),
			zap.Error(err))
		if errors.Is(err, ErrSegmentNotFound) {
			fsw.deleteInternalTask(segID)
			return
		}
		return
	}
	t := fsw.getInternalTask(segID)
	if t.segmentInfo.CreatedByCompaction {
		fsw.removeCompactedTasks(t)
	}
}

func (fsw *flushedSegmentWatcher) removeCompactedTasks(t *internalTask) {
	log.Debug("IndexCoord flushedSegmentWatcher mark task as deleted which is compacted", zap.Int64("segID", t.segmentInfo.ID),
		zap.Int64s("compactionFrom", t.segmentInfo.CompactionFrom))
	fsw.builder.markTasksAsDeleted(fsw.meta.GetBuildIDsFromSegIDs(t.segmentInfo.CompactionFrom))
	for _, segID := range t.segmentInfo.CompactionFrom {
		fsw.deleteChildrenTask(segID)
		if _, ok := fsw.internalTasks[segID]; ok {
			fsw.updateInternalTaskState(segID, indexTaskDeleted)
		}
	}
}
