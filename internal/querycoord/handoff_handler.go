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

package querycoord

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util"
)

type extraIndexInfo struct {
	indexID        UniqueID
	indexName      string
	indexParams    []*commonpb.KeyValuePair
	indexSize      uint64
	indexFilePaths []string
}

type handoffTaskState int32

const (
	// when we watched a handoff task
	handoffTaskInit handoffTaskState = iota
	// when we notified a segment is index
	handoffTaskReady
	// we've send handoff task to scheduler, and wait for response.
	handoffTaskTriggered
	// task done, wait to be cleaned
	handoffTaskDone
	// handoff canceled due to collection released or other behavior
	handoffTaskCancel
)

var (
	// Errors
	ErrHandoffRequestInvalid = errors.New("invalid handoff request")
)

type HandOffTask struct {
	segmentInfo *querypb.SegmentInfo
	state       handoffTaskState
	locked      bool
}

type HandoffHandler struct {
	ctx    context.Context
	cancel context.CancelFunc
	client kv.MetaKv

	revision int64

	taskMutex sync.Mutex
	tasks     map[int64]*HandOffTask
	notify    chan struct{}
	closed    bool

	meta      Meta
	scheduler *TaskScheduler
	cluster   Cluster

	broker *globalMetaBroker

	wg sync.WaitGroup
}

func newHandoffHandler(ctx context.Context, client kv.MetaKv, meta Meta, cluster Cluster, scheduler *TaskScheduler, broker *globalMetaBroker) (*HandoffHandler, error) {
	childCtx, cancel := context.WithCancel(ctx)

	checker := &HandoffHandler{
		ctx:    childCtx,
		cancel: cancel,
		client: client,

		tasks:  make(map[int64]*HandOffTask, 1024),
		notify: make(chan struct{}, 1024),

		meta:      meta,
		scheduler: scheduler,
		cluster:   cluster,

		broker: broker,
	}
	err := checker.reloadFromKV()
	if err != nil {
		log.Error("index checker reload from kv failed", zap.Error(err))
		return nil, err
	}

	return checker, nil
}

func (handler *HandoffHandler) Start() {
	handler.wg.Add(1)
	go handler.schedule()
}

func (handler *HandoffHandler) Stop() {
	handler.taskMutex.Lock()
	handler.closed = true
	close(handler.notify)
	handler.taskMutex.Unlock()
	handler.cancel()
	handler.wg.Wait()
}

// reloadFromKV  reload collection/partition, remove from etcd if failed.
func (handler *HandoffHandler) reloadFromKV() error {
	_, handoffReqValues, version, err := handler.client.LoadWithRevision(util.HandoffSegmentPrefix)
	if err != nil {
		log.Error("reloadFromKV: LoadWithRevision from kv failed", zap.Error(err))
		return err
	}
	handler.revision = version
	handler.taskMutex.Lock()
	defer handler.taskMutex.Unlock()
	for _, value := range handoffReqValues {
		segmentInfo := &querypb.SegmentInfo{}
		err := proto.Unmarshal([]byte(value), segmentInfo)
		if err != nil {
			log.Error("reloadFromKV: unmarshal failed", zap.Any("error", err.Error()))
			return err
		}
		isValid, _ := handler.verifyRequest(segmentInfo)
		if isValid && Params.QueryCoordCfg.AutoHandoff {
			// push the req to handoffReqChan and then wait to load after index created
			// in case handoffReqChan is full, and block start process
			handler.tasks[segmentInfo.SegmentID] = &HandOffTask{
				segmentInfo, handoffTaskInit, false,
			}
			log.Info("reloadFromKV: process handoff request done", zap.Int64("segmentId", segmentInfo.SegmentID))
		} else {
			handler.tasks[segmentInfo.SegmentID] = &HandOffTask{
				segmentInfo, handoffTaskCancel, false,
			}
			log.Info("reloadFromKV: collection/partition has not been loaded, task canceled", zap.Int64("segmentId", segmentInfo.SegmentID))
		}
	}

	return nil
}

func (handler *HandoffHandler) verifyRequest(req *querypb.SegmentInfo) (bool, *querypb.CollectionInfo) {
	// if collection has not been loaded, then skip the segment
	collectionInfo, err := handler.meta.getCollectionInfoByID(req.CollectionID)
	if err != nil {
		msgType := handler.scheduler.triggerTaskQueue.willLoadOrRelease(req.CollectionID)
		switch msgType {
		case commonpb.MsgType_LoadCollection, commonpb.MsgType_LoadPartitions:
			// collection/partition may be loaded, return valid and let handoff task do the check
			schema, err := handler.broker.describeCollection(handler.ctx, req.CollectionID)
			if err != nil {
				return false, nil
			}
			collectionInfo = &querypb.CollectionInfo{
				CollectionID: req.CollectionID,
				Schema:       schema,
				// use load collection to by-pass partition id check
				LoadType: querypb.LoadType_LoadCollection,
			}
		default:
			return false, nil
		}
	}

	// if partition has not been loaded or released, then skip handoff the segment
	if collectionInfo.LoadType == querypb.LoadType_LoadPartition {
		for _, id := range collectionInfo.PartitionIDs {
			if id == req.PartitionID {
				return true, collectionInfo
			}
		}
	} else {
		// Should not happen?
		partitionReleased := false
		for _, id := range collectionInfo.ReleasedPartitionIDs {
			if id == req.PartitionID {
				partitionReleased = true
			}
		}
		if !partitionReleased {
			return true, collectionInfo
		}
	}

	return false, nil
}

func (handler *HandoffHandler) enqueue(req *querypb.SegmentInfo) {
	handler.taskMutex.Lock()
	defer handler.taskMutex.Unlock()
	if handler.closed {
		return
	}
	handler.tasks[req.SegmentID] = &HandOffTask{
		req, handoffTaskInit, false,
	}
	handler.notify <- struct{}{}
}

func (handler *HandoffHandler) schedule() {
	defer handler.wg.Done()
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	for {
		select {
		case <-handler.ctx.Done():
			return
		case _, ok := <-handler.notify:
			if ok {
				handler.taskMutex.Lock()
				if len(handler.tasks) != 0 {
					log.Info("handoff task scheduled: ", zap.Int("task number", len(handler.tasks)))
					for segmentID := range handler.tasks {
						handler.process(segmentID)
					}
				}
				handler.taskMutex.Unlock()
			}
		case <-ticker.C:
			handler.taskMutex.Lock()
			if len(handler.tasks) != 0 {
				log.Info("handoff task scheduled: ", zap.Int("task number", len(handler.tasks)))
				for segmentID := range handler.tasks {
					handler.process(segmentID)
				}
			}
			handler.taskMutex.Unlock()
		}
	}
}

// must hold the lock
func (handler *HandoffHandler) process(segmentID int64) error {
	if handler.closed {
		return nil
	}
	task := handler.tasks[segmentID]
	// if task is cancel and success, clean up
	switch task.state {
	case handoffTaskCancel, handoffTaskDone:
		overrideSegments := handler.getOverrideSegments(task)
		for _, daughterSegmentID := range overrideSegments {
			handler.clean(daughterSegmentID)
		}
		handler.clean(segmentID)

	case handoffTaskInit:
		isValid, collectionInfo := handler.verifyRequest(task.segmentInfo)
		if !isValid || !Params.QueryCoordCfg.AutoHandoff {
			task.state = handoffTaskCancel
			log.Info("HandoffHandler: collection/partition has not been loaded, task canceled", zap.Int64("segmentID", task.segmentInfo.SegmentID))
			return ErrHandoffRequestInvalid
		}
		// TODO, add segment lock here, if segment lock add failed,
		if !task.locked {
			if err := handler.broker.acquireSegmentsReferLock(handler.ctx, task.segmentInfo.SegmentID, []UniqueID{task.segmentInfo.SegmentID}); err != nil {
				// if task can not be holded, there are three possible situation
				// 1. temporary fail of data coord -> retry
				// 2. collection is dropped -> verify Handoff Req valid should find the collection is released, the verifyHandoffReqValid before lock should handle it.
				// 3. compaction happened -> we should soon received another handoff task to handle current task.
				log.Warn("HandoffHandler: acquire segment reference lock failed", zap.Int64("segmentID", task.segmentInfo.SegmentID), zap.Error(err))
				return fmt.Errorf("failed to acquire segment refer lock")
			}
		}
		task.locked = true
		// TODO we should not directly poll the index info, wait for notification should be a better idea.
		indexInfo, err := handler.broker.getIndexInfo(handler.ctx, task.segmentInfo.CollectionID, task.segmentInfo.SegmentID, collectionInfo.Schema)
		if err == nil {
			// if index exist or not enableIndex, ready to load
			task.segmentInfo.IndexInfos = indexInfo
			// NOTICE, this is the trick, if compaction happened recursively, it should be all in our task list.
			task := &HandOffTask{
				task.segmentInfo, handoffTaskReady, true,
			}
			handler.tasks[task.segmentInfo.SegmentID] = task
			handler.notify <- struct{}{}
			log.Info("HandoffHandler: enqueue indexed segments", zap.Int64("segmentID", task.segmentInfo.SegmentID))
		}

	case handoffTaskReady:
		validHandoffReq, _ := handler.verifyRequest(task.segmentInfo)
		if !validHandoffReq || !Params.QueryCoordCfg.AutoHandoff {
			task.state = handoffTaskCancel
			log.Info("HandoffHandler: collection/partition has not been loaded, task canceled", zap.Int64("segmentID", task.segmentInfo.SegmentID))
			return ErrHandoffRequestInvalid
		}
		handler.triggerHandoff(task)
	}
	// handoffTaskTriggered state don't need to be handled in the loop, it will handled by the go routine in triggerHandoff

	return nil
}

func (handler *HandoffHandler) clean(segmentID int64) {
	task := handler.tasks[segmentID]

	// this is the trick, we go through all the task, check if any of the task is shaded by current task, then we handle both.
	if task.locked {
		if err := handler.broker.releaseSegmentReferLock(handler.ctx, task.segmentInfo.SegmentID, []UniqueID{task.segmentInfo.SegmentID}); err != nil {
			log.Warn("HandoffHandler: release segment reference lock failed", zap.Int64("segmentID", task.segmentInfo.SegmentID), zap.Error(err))
			return
		}
		task.locked = false
	}

	buildQuerySegmentPath := fmt.Sprintf("%s/%d/%d/%d", util.HandoffSegmentPrefix, task.segmentInfo.CollectionID, task.segmentInfo.PartitionID, task.segmentInfo.SegmentID)
	err := handler.client.Remove(buildQuerySegmentPath)
	if err != nil {
		log.Warn("HandoffHandler: remove handoff segment from etcd failed", zap.Int64("segmentID", task.segmentInfo.SegmentID), zap.Error(err))
		// just wait for next loop
		return
	}
	log.Info("HandoffHandler: clean task", zap.Int32("state", int32(task.state)), zap.Int64("segmentID", task.segmentInfo.SegmentID))
	delete(handler.tasks, segmentID)
}

func (handler *HandoffHandler) getOverrideSegments(parentTask *HandOffTask) []int64 {
	var toRelease []int64
	for segmentID, task := range handler.tasks {
		for _, compactFrom := range parentTask.segmentInfo.CompactionFrom {
			if segmentID == compactFrom {
				releasedSegmentIDs := handler.getOverrideSegments(task)
				toRelease = append(toRelease, releasedSegmentIDs...)
				toRelease = append(toRelease, segmentID)
			}
		}
	}

	if len(toRelease) > 0 {
		log.Info("HandoffHandler: find recursive compaction ",
			zap.Int64("TargetSegment", parentTask.segmentInfo.SegmentID),
			zap.Int64s("CompactedSegments", toRelease))
	}

	return toRelease
}

func (handler *HandoffHandler) triggerHandoff(task *HandOffTask) {
	log.Info("HandoffHandler: trigger handoff", zap.Any("segmentInfo", task.segmentInfo))
	baseTask := newBaseTask(handler.ctx, querypb.TriggerCondition_Handoff)

	// if recursive compaction happened, previous segment also need to be released
	toRelease := handler.getOverrideSegments(task)
	toRelease = append(toRelease, task.segmentInfo.GetCompactionFrom()...)

	handoffReq := &querypb.HandoffSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_HandoffSegments,
		},
		SegmentInfos:     []*querypb.SegmentInfo{task.segmentInfo},
		ReleasedSegments: toRelease,
	}
	handoffTask := &handoffTask{
		baseTask:               baseTask,
		HandoffSegmentsRequest: handoffReq,
		broker:                 handler.broker,
		cluster:                handler.cluster,
		meta:                   handler.meta,
	}
	err := handler.scheduler.Enqueue(handoffTask)
	if err != nil {
		// we just wait for next cycle for reschedule
		log.Error("HandoffHandler: handoffTask enqueue failed",
			zap.Int64("segmentID", task.segmentInfo.SegmentID),
			zap.Error(err))
		return
	}
	log.Info("HandoffHandler: handoff task triggered successfully", zap.Int64("segmentID", task.segmentInfo.SegmentID))
	handler.tasks[task.segmentInfo.SegmentID] = &HandOffTask{
		task.segmentInfo, handoffTaskTriggered, task.locked,
	}
	go func() {
		err := handoffTask.waitToFinish()
		handler.taskMutex.Lock()
		defer handler.taskMutex.Unlock()
		if err != nil {
			log.Warn("HandoffHandler: handoff task failed to execute",
				zap.Int64("segmentID", task.segmentInfo.SegmentID),
				zap.Error(err))
			// wait for reschedule
			handler.tasks[task.segmentInfo.SegmentID] = &HandOffTask{
				task.segmentInfo, handoffTaskReady, task.locked,
			}
			return
		}
		// wait for cleanup
		log.Info("HandoffHandler: handoffTask completed", zap.Int64("segmentID", task.segmentInfo.SegmentID))
		handler.tasks[task.segmentInfo.SegmentID] = &HandOffTask{
			task.segmentInfo, handoffTaskDone, task.locked,
		}
	}()
}
