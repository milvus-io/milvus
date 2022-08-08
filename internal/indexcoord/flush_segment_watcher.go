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
	"sync"
	"time"

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
	handoffTask *indexpb.HandoffTask
	segID       UniqueID
	indexID     UniqueID
	fieldID     UniqueID
	// just use init, inProgress, Done
	state indexTaskState
}

func newFlushSegmentWatcher(ctx context.Context, kv kv.MetaKv) (*flushedSegmentWatcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	fsw := &flushedSegmentWatcher{
		ctx:              ctx,
		cancel:           cancel,
		kvClient:         kv,
		wg:               sync.WaitGroup{},
		taskMutex:        sync.RWMutex{},
		scheduleDuration: time.Second * 10,
	}
	return fsw, nil
}

func (fsw *flushedSegmentWatcher) constructTask(segmentInfo *datapb.SegmentInfo) {
	fieldIndexes := fsw.meta.GetIndexesForCollection(segmentInfo.CollectionID, "")
	if segmentInfo.NumOfRows < Params.IndexCoordCfg.MinSegmentNumRowsToEnableIndex || len(fieldIndexes) == 0 {
		log.Warn("segemnt no need to build index", zap.Int64("segmentID", segmentInfo.ID),
			zap.Int64("num of rows", segmentInfo.NumOfRows), zap.Int("collection indexes num", len(fieldIndexes)))
		fsw.flushedSegments[segmentInfo.ID][0] = &flushedSegmentTask{
			handoffTask: &indexpb.HandoffTask{
				SegmentInfo: segmentInfo,
			},
			state: indexTaskDone,
		}
		fsw.notify <- struct{}{}
		return
	}
	for _, index := range fieldIndexes {
		fsw.flushedSegments[segmentInfo.ID][index.IndexID] = &flushedSegmentTask{
			handoffTask: &indexpb.HandoffTask{
				SegmentInfo: segmentInfo,
				IndexInfo: &indexpb.IndexInfo{
					CollectionID: segmentInfo.CollectionID,
					FieldID:      index.FieldID,
					IndexName:    index.IndexName,
					TypeParams:   index.TypeParams,
					IndexParams:  index.IndexParams,
				},
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
	return
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
		fsw.constructTask(segmentInfo)
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

	fsw.constructTask(segmentInfo)
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
			for _, tasks := range fsw.flushedSegments {
				for _, t := range tasks {
					fsw.process(t)
				}
			}
			fsw.taskMutex.Unlock()
		}
	}
}

func (fsw *flushedSegmentWatcher) process(task *flushedSegmentTask) {
	// check if the segment needs index?

	switch task.state {
	case indexTaskInit:
		//get binLogs
		binLogs := make([]string, 0)
		for _, fieldBinLog := range task.handoffTask.SegmentInfo.GetBinlogs() {
			if fieldBinLog.GetFieldID() == task.fieldID {
				for _, binLog := range fieldBinLog.GetBinlogs() {
					binLogs = append(binLogs, binLog.LogPath)
				}
				break
			}
		}
		segIdx := &model.SegmentIndex{
			Segment: model.Segment{
				SegmentID:    task.handoffTask.SegmentInfo.ID,
				CollectionID: task.handoffTask.SegmentInfo.CollectionID,
				PartitionID:  task.handoffTask.SegmentInfo.PartitionID,
				NumRows:      task.handoffTask.SegmentInfo.NumOfRows,
				BinLogs:      binLogs,
			},
		}

		//create index task for metaTable
		// send to indexBuilder
		if err := fsw.ic.createIndex(segIdx); err != nil {
			log.Warn("IndexCoord create index for segment fail", zap.Int64("segID", task.handoffTask.SegmentInfo.ID),
				zap.Int64("indexID", task.indexID), zap.Error(err))
			return
		}
		fsw.flushedSegments[task.segID][task.indexID].state = indexTaskInProgress

	case indexTaskInProgress:

		return
	case indexTaskDone:
		// TODO @xiaocai2333: send hand off event to etcd, and remove flushed segment from etcd.
	}
	// buildIndex
}
