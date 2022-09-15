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

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/logutil"
)

type flushedSegmentWatcher struct {
	ctx    context.Context
	cancel context.CancelFunc

	kvClient         kv.MetaKv
	wg               sync.WaitGroup
	scheduleDuration time.Duration

	internalTaskMutex sync.RWMutex
	internalNotify    chan struct{}

	etcdRevision int64
	watchChan    clientv3.WatchChan

	meta    *metaTable
	builder *indexBuilder
	ic      *IndexCoord

	internalTasks map[UniqueID]*internalTask
}

type internalTask struct {
	state       indexTaskState
	segmentInfo *datapb.SegmentInfo
}

func newFlushSegmentWatcher(ctx context.Context, kv kv.MetaKv, meta *metaTable, builder *indexBuilder, ic *IndexCoord) (*flushedSegmentWatcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	fsw := &flushedSegmentWatcher{
		ctx:               ctx,
		cancel:            cancel,
		kvClient:          kv,
		wg:                sync.WaitGroup{},
		internalTaskMutex: sync.RWMutex{},
		scheduleDuration:  time.Second,
		internalNotify:    make(chan struct{}, 1),
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
}

func (fsw *flushedSegmentWatcher) Stop() {
	fsw.cancel()
	fsw.wg.Wait()
}

func (fsw *flushedSegmentWatcher) enqueueInternalTask(segmentID UniqueID) {
	defer fsw.internalNotifyFunc()
	fsw.internalTaskMutex.Lock()
	defer fsw.internalTaskMutex.Unlock()

	logutil.Logger(fsw.ctx).Info("flushedSegmentWatcher enqueueInternalTask", zap.Int64("segmentID", segmentID))

	if _, ok := fsw.internalTasks[segmentID]; !ok {
		fsw.internalTasks[segmentID] = &internalTask{
			state:       indexTaskPrepare,
			segmentInfo: nil,
		}
		return
	}
	logutil.Logger(fsw.ctx).Info("flushedSegmentWatcher already have the task", zap.Int64("segmentID", segmentID))
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

	for _, segmentID := range segmentIDs {
		fsw.internalProcess(segmentID)
	}
}

func (fsw *flushedSegmentWatcher) internalNotifyFunc() {
	select {
	case fsw.internalNotify <- struct{}{}:
	default:
	}
}

func (fsw *flushedSegmentWatcher) updateInternalTaskState(segID UniqueID, state indexTaskState) {
	fsw.internalTaskMutex.Lock()
	defer fsw.internalTaskMutex.Unlock()
	log.Debug("flushedSegmentWatcher updateInternalTaskState", zap.Int64("segID", segID), zap.String("state", state.String()))
	if _, ok := fsw.internalTasks[segID]; ok {
		fsw.internalTasks[segID].state = state
	}
}

func (fsw *flushedSegmentWatcher) deleteInternalTask(segID UniqueID) {
	fsw.internalTaskMutex.Lock()
	defer fsw.internalTaskMutex.Unlock()

	delete(fsw.internalTasks, segID)
	log.Debug("flushedSegmentWatcher delete the internal task", zap.Int64("segID", segID))
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
	log.Debug("flushedSegmentWatcher set internal task segment info success", zap.Int64("segID", segID))
}

func (fsw *flushedSegmentWatcher) allParentsDone(segIDs []UniqueID) bool {
	fsw.internalTaskMutex.RLock()
	defer fsw.internalTaskMutex.RUnlock()
	done := true
	for _, segID := range segIDs {
		if _, ok := fsw.internalTasks[segID]; ok {
			done = false
			break
		}
	}
	return done
}

func (fsw *flushedSegmentWatcher) internalProcess(segID UniqueID) {
	t := fsw.getInternalTask(segID)
	log.Debug("IndexCoord flushedSegmentWatcher process internal task", zap.Int64("segID", segID),
		zap.String("state", t.state.String()))

	switch t.state {
	case indexTaskPrepare:
		if err := fsw.prepare(segID); err != nil {
			log.Error("flushedSegmentWatcher prepare internal task fail", zap.Int64("segID", segID), zap.Error(err))
			return
		}
		fsw.updateInternalTaskState(segID, indexTaskInit)
	case indexTaskInit:
		if err := fsw.constructTask(t); err != nil {
			log.Error("flushedSegmentWatcher construct task fail", zap.Int64("segID", segID), zap.Error(err))
			return
		}
		fsw.updateInternalTaskState(segID, indexTaskInProgress)
		fsw.internalNotifyFunc()
	case indexTaskInProgress:
		state := fsw.meta.GetSegmentIndexState(segID)
		if state.state == commonpb.IndexState_Finished || state.state == commonpb.IndexState_Failed || state.state == commonpb.IndexState_IndexStateNone {
			log.Debug("all tasks are finished", zap.Int64("segID", segID), zap.String("state", state.state.String()))
			fsw.updateInternalTaskState(segID, indexTaskDone)
			fsw.internalNotifyFunc()
		}
	case indexTaskDone:
		if !fsw.allParentsDone(t.segmentInfo.CompactionFrom) {
			log.Debug("flushed segment create index done, but there are still parent task that haven't written handoff event",
				zap.Int64("segID", segID), zap.Int64s("compactionFrom", t.segmentInfo.CompactionFrom))
			return
		}
		indexInfos := fsw.meta.GetSegmentIndexes(segID)
		enableIndex := len(indexInfos) > 0
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
			IndexInfos:          make([]*querypb.FieldIndexInfo, 0),
			EnableIndex:         enableIndex,
		}
		for _, indexInfo := range indexInfos {
			handoffTask.IndexInfos = append(handoffTask.IndexInfos, &querypb.FieldIndexInfo{
				FieldID:     fsw.meta.GetFieldIDByIndexID(t.segmentInfo.CollectionID, indexInfo.IndexID),
				EnableIndex: true,
				IndexName:   fsw.meta.GetIndexNameByID(t.segmentInfo.CollectionID, indexInfo.IndexID),
				IndexID:     indexInfo.IndexID,
				BuildID:     indexInfo.BuildID,
				IndexParams: fsw.meta.GetIndexParams(t.segmentInfo.CollectionID, indexInfo.IndexID),
				//IndexFilePaths: nil,
				//IndexSize:      0,
			})
		}

		if err := fsw.writeHandoffSegment(handoffTask); err != nil {
			log.Error("IndexCoord flushSegmentWatcher writeHandoffSegment with no index info fail",
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
	default:
		log.Debug("IndexCoord flushedSegmentWatcher internal task get invalid state", zap.Int64("segID", segID),
			zap.String("state", t.state.String()))
	}
}

func (fsw *flushedSegmentWatcher) constructTask(t *internalTask) error {
	log.Debug("IndexCoord flushedSegmentWatcher construct tasks by segment info", zap.Int64("segID", t.segmentInfo.ID),
		zap.Int64s("compactionFrom", t.segmentInfo.CompactionFrom))
	fieldIndexes := fsw.meta.GetIndexesForCollection(t.segmentInfo.CollectionID, "")
	if len(fieldIndexes) == 0 {
		log.Debug("segment no need to build index", zap.Int64("segmentID", t.segmentInfo.ID),
			zap.Int64("num of rows", t.segmentInfo.NumOfRows), zap.Int("collection indexes num", len(fieldIndexes)))
		// no need to build index
		return nil
	}

	for _, index := range fieldIndexes {
		segIdx := &model.SegmentIndex{
			SegmentID:    t.segmentInfo.ID,
			CollectionID: t.segmentInfo.CollectionID,
			PartitionID:  t.segmentInfo.PartitionID,
			NumRows:      t.segmentInfo.NumOfRows,
			IndexID:      index.IndexID,
			CreateTime:   t.segmentInfo.StartPosition.Timestamp,
		}

		//create index task for metaTable
		// send to indexBuilder
		have, buildID, err := fsw.ic.createIndexForSegment(segIdx)
		if err != nil {
			log.Warn("IndexCoord create index for segment fail", zap.Int64("segID", t.segmentInfo.ID),
				zap.Int64("indexID", index.IndexID), zap.Error(err))
			return err
		}
		if !have {
			fsw.builder.enqueue(buildID)
		}
	}
	log.Debug("flushedSegmentWatcher construct children task success", zap.Int64("segID", t.segmentInfo.ID),
		zap.Int("tasks num", len(fieldIndexes)))
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

func (fsw *flushedSegmentWatcher) prepare(segID UniqueID) error {
	defer fsw.internalNotifyFunc()
	log.Debug("prepare flushed segment task", zap.Int64("segID", segID))
	if err := fsw.pullSegmentInfo(segID); err != nil {
		log.Error("flushedSegmentWatcher get segment info fail", zap.Int64("segID", segID),
			zap.Error(err))
		if errors.Is(err, ErrSegmentNotFound) {
			fsw.deleteInternalTask(segID)
			return err
		}
		return err
	}
	//t := fsw.getInternalTask(segID)
	//if t.segmentInfo.CreatedByCompaction {
	//	if err := fsw.removeCompactedTasks(t); err != nil {
	//		return err
	//	}
	//}
	return nil
}

//func (fsw *flushedSegmentWatcher) removeCompactedTasks(t *internalTask) error {
//	log.Debug("IndexCoord flushedSegmentWatcher mark task as deleted which is compacted", zap.Int64("segID", t.segmentInfo.ID),
//		zap.Int64s("compactionFrom", t.segmentInfo.CompactionFrom))
//	if err := fsw.builder.markTasksAsDeleted(fsw.meta.GetBuildIDsFromSegIDs(t.segmentInfo.CompactionFrom)); err != nil {
//		log.Error("mark index meta fail, try again", zap.Int64s("compacted segIDs", t.segmentInfo.CompactionFrom),
//			zap.Error(err))
//		return err
//	}
//	for _, segID := range t.segmentInfo.CompactionFrom {
//		fsw.deleteChildrenTask(segID)
//		if _, ok := fsw.internalTasks[segID]; ok {
//			fsw.updateInternalTaskState(segID, indexTaskDeleted)
//		}
//	}
//	return nil
//}
