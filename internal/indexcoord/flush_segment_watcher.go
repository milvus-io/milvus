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

	etcdRevision int64
	watchChan    clientv3.WatchChan

	meta            *metaTable
	flushedSegments map[UniqueID]*datapb.SegmentInfo
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

func (fsw *flushedSegmentWatcher) reloadFromKV() error {
	log.Info("flushSegmentWatcher reloadFromKV")
	fsw.flushedSegments = make(map[UniqueID]*datapb.SegmentInfo)
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
		fsw.flushedSegments[segmentInfo.ID] = segmentInfo
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

	fsw.flushedSegments[segmentInfo.ID] = segmentInfo
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
			for _, segmentInfo := range fsw.flushedSegments {
				fsw.process(segmentInfo)
			}
			fsw.taskMutex.Unlock()
		}
	}
}

func (fsw *flushedSegmentWatcher) process(segmentInfo *datapb.SegmentInfo) {
	// check if the segment needs index?

	// buildIndex
}
