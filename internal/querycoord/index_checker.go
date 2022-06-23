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
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type extraIndexInfo struct {
	indexID        UniqueID
	indexName      string
	indexParams    []*commonpb.KeyValuePair
	indexSize      uint64
	indexFilePaths []string
}

// IndexChecker checks index
type IndexChecker struct {
	ctx      context.Context
	cancel   context.CancelFunc
	client   kv.MetaKv
	revision int64

	meta      Meta
	scheduler *TaskScheduler
	cluster   Cluster

	broker      *globalMetaBroker
	idAllocator func() (UniqueID, error)

	wg sync.WaitGroup
}

func newIndexChecker(ctx context.Context, client kv.MetaKv, meta Meta, cluster Cluster, scheduler *TaskScheduler, broker *globalMetaBroker, allocator func() (UniqueID, error)) (*IndexChecker, error) {
	childCtx, cancel := context.WithCancel(ctx)

	checker := &IndexChecker{
		ctx:         childCtx,
		cancel:      cancel,
		client:      client,
		meta:        meta,
		scheduler:   scheduler,
		cluster:     cluster,
		broker:      broker,
		idAllocator: allocator,
	}
	err := checker.reloadFromKV()
	if err != nil {
		log.Error("index checker reload from kv failed", zap.Error(err))
		return nil, err
	}

	return checker, nil
}

func (ic *IndexChecker) start() {
}

func (ic *IndexChecker) close() {
	ic.cancel()
}

// reloadFromKV  reload collection/partition, remove from etcd if failed.
func (ic *IndexChecker) reloadFromKV() error {
	_, handoffReqValues, version, err := ic.client.LoadWithRevision(handoffSegmentPrefix)
	if err != nil {
		log.Error("reloadFromKV: LoadWithRevision from kv failed", zap.Error(err))
		return err
	}
	ic.revision = version

	for _, value := range handoffReqValues {
		segmentInfo := &querypb.SegmentInfo{}
		err := proto.Unmarshal([]byte(value), segmentInfo)
		if err != nil {
			log.Error("reloadFromKV: unmarshal failed", zap.Any("error", err.Error()))
			return err
		}
		validHandoffReq, _ := ic.verifyHandoffReqValid(segmentInfo)
		if validHandoffReq && Params.QueryCoordCfg.AutoHandoff {
			go ic.handleHandoffRequest(segmentInfo)
		} else {
			log.Info("reloadFromKV: collection/partition has not been loaded, remove req from etcd", zap.Any("segmentInfo", segmentInfo))
			buildQuerySegmentPath := fmt.Sprintf("%s/%d/%d/%d", handoffSegmentPrefix, segmentInfo.CollectionID, segmentInfo.PartitionID, segmentInfo.SegmentID)
			err = ic.client.Remove(buildQuerySegmentPath)
			if err != nil {
				log.Error("reloadFromKV: remove handoff segment from etcd failed", zap.Error(err))
				return err
			}
		}
		log.Info("reloadFromKV: process handoff request done", zap.Any("segmentInfo", segmentInfo))
	}

	return nil
}

func (ic *IndexChecker) handleHandoffRequest(info *querypb.SegmentInfo) error {
	// allocate a taskID for locking
	taskID, err := ic.idAllocator()
	if err != nil {
		log.Warn("handleHandoffRequest: allocate taskID failed", zap.Error(err))
		return err
	}

	// acquire reference lock and defer unlock
	if err = ic.broker.acquireSegmentsReferLock(ic.ctx, taskID, []UniqueID{info.SegmentID}); err != nil {
		log.Warn("acquire segment reference lock failed", zap.Int64("taskID", taskID), zap.Int64("segmentID", info.SegmentID))
		return err
	}
	defer func() {
		if err := ic.broker.releaseSegmentReferLock(ic.ctx, taskID, []UniqueID{info.SegmentID}); err != nil {
			log.Warn("release segment reference lock failed", zap.Int64("taskID", taskID), zap.Int64("segmentID", info.SegmentID))
			panic(err)
		}
	}()

	collectionInfo, err := ic.meta.getCollectionInfoByID(info.CollectionID)
	if err != nil {
		log.Warn("handleHandoffRequest: handoffTask failed", zap.Int64("taskID", taskID), zap.Int64("segmentID", info.SegmentID), zap.Error(err))
		return err
	}

	var indexInfo []*querypb.FieldIndexInfo
	indexInfo, err = ic.broker.getIndexInfo(ic.ctx, info.CollectionID, info.SegmentID, collectionInfo.Schema)
	if err != nil {
		// retry index check
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
	FOR:
		for {
			select {
			case <-ticker.C:
				indexInfo, err = ic.broker.getIndexInfo(ic.ctx, info.CollectionID, info.SegmentID, collectionInfo.Schema)
				if err == nil {
					break FOR // index check passed
				}
				if segmentState, err := ic.broker.getSegmentStates(ic.ctx, info.SegmentID); err == nil {
					if segmentState.State == commonpb.SegmentState_NotExist {
						// segment dropped, stop retry
						log.Warn("handleHandoffRequest: handoffTask failed because segment is dropped", zap.Int64("taskID", taskID), zap.Int64("segmentID", info.SegmentID))
						return fmt.Errorf("segment %d has been dropped", info.SegmentID)
					}
				}
			case <-ic.ctx.Done():
				return ic.ctx.Err()
			}
		}
	}
	info.IndexInfos = indexInfo

	// create task and enqueue
	baseTask := newBaseTask(ic.ctx, querypb.TriggerCondition_Handoff)
	baseTask.setTaskID(taskID)
	handoffReq := &querypb.HandoffSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_HandoffSegments,
		},
		SegmentInfos: []*querypb.SegmentInfo{info},
	}
	handoffTask := &handoffTask{
		baseTask:               baseTask,
		HandoffSegmentsRequest: handoffReq,
		broker:                 ic.broker,
		cluster:                ic.cluster,
		meta:                   ic.meta,
	}
	if err := ic.scheduler.Enqueue(handoffTask); err != nil {
		log.Error("handleHandoffRequest: handoffTask enqueue failed", zap.Error(err))
		panic(err)
	}

	// once task enqueue, etcd data can be cleaned, handoffTask will recover from taskScheduler's reloadFromKV()
	queryKey := fmt.Sprintf("%s/%d/%d/%d", handoffSegmentPrefix, info.CollectionID, info.PartitionID, info.SegmentID)
	if err = ic.client.Remove(queryKey); err != nil {
		log.Error("handleHandoffRequest: remove handoff segment from etcd failed", zap.Error(err))
		panic(err)
	}

	if err := handoffTask.waitToFinish(); err != nil {
		// collection or partition may have been released before handoffTask enqueue
		log.Warn("handleHandoffRequest: handoffTask failed", zap.Int64("taskID", taskID), zap.Int64("segmentID", info.SegmentID), zap.Error(err))
		return err
	}

	log.Info("handleHandoffRequest: handoffTask completed", zap.Int64("taskID", taskID), zap.Int64("segmentID", info.SegmentID))
	return nil
}

func (ic *IndexChecker) verifyHandoffReqValid(req *querypb.SegmentInfo) (bool, *querypb.CollectionInfo) {
	// if collection has not been loaded, then skip the segment
	collectionInfo, err := ic.meta.getCollectionInfoByID(req.CollectionID)
	if err == nil {
		// if partition has not been loaded or released, then skip handoff the segment
		if collectionInfo.LoadType == querypb.LoadType_LoadPartition {
			for _, id := range collectionInfo.PartitionIDs {
				if id == req.PartitionID {
					return true, collectionInfo
				}
			}
		} else {
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
	}

	return false, nil
}
