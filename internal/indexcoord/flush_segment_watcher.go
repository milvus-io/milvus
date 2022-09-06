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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util"
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

	// segmentID -> internalTask.
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
		scheduleDuration:  time.Second * 10,
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
}

func (fsw *flushedSegmentWatcher) Stop() {
	fsw.cancel()
	close(fsw.internalNotify)
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
	fsw.internalTaskMutex.Lock()
	defer fsw.internalTaskMutex.Unlock()
	if len(fsw.internalTasks) > 0 {
		log.Debug("IndexCoord flushedSegmentWatcher schedule internal tasks", zap.Int("internal task num", len(fsw.internalTasks)))
		for _, t := range fsw.internalTasks {
			fsw.internalProcess(t)
		}
	}
}

func (fsw *flushedSegmentWatcher) internalProcess(t *internalTask) {
	log.Debug("IndexCoord flushedSegmentWatcher process internal task", zap.Int64("segID", t.segmentInfo.ID),
		zap.String("state", t.state.String()))
	switch t.state {
	case indexTaskInit:
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
		if fsw.meta.IsIndexDone(t.segmentInfo.ID) {
			fsw.internalTasks[t.segmentInfo.ID].state = indexTaskDone
		}
		return
	case indexTaskDone:
		if err := fsw.removeFlushedSegment(t); err != nil {
			log.Error("IndexCoord flushSegmentWatcher removeFlushedSegment fail",
				zap.Int64("segID", t.segmentInfo.ID), zap.Error(err))
			return
		}
		delete(fsw.internalTasks, t.segmentInfo.ID)
		return
	default:
		log.Debug("IndexCoord flushedSegmentWatcher internal task get invalid state", zap.Int64("segID", t.segmentInfo.ID),
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
