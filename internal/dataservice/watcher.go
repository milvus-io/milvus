// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package dataservice

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/trace"
	"go.uber.org/zap"

	"golang.org/x/net/context"

	"github.com/milvus-io/milvus/internal/msgstream"
)

type dataNodeTimeTickWatcher struct {
	meta      *meta
	cluster   *dataNodeCluster
	allocator segmentAllocatorInterface
	msgQueue  chan *msgstream.TimeTickMsg
}

func newDataNodeTimeTickWatcher(meta *meta, allocator segmentAllocatorInterface, cluster *dataNodeCluster) *dataNodeTimeTickWatcher {
	return &dataNodeTimeTickWatcher{
		meta:      meta,
		allocator: allocator,
		cluster:   cluster,
		msgQueue:  make(chan *msgstream.TimeTickMsg, 1),
	}
}

func (watcher *dataNodeTimeTickWatcher) Watch(msg *msgstream.TimeTickMsg) {
	watcher.msgQueue <- msg
}

func (watcher *dataNodeTimeTickWatcher) StartBackgroundLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Debug("data node time tick watcher closed")
			return
		case msg := <-watcher.msgQueue:
			if err := watcher.handleTimeTickMsg(msg); err != nil {
				log.Error("handle time tick error", zap.Error(err))
			}
		}
	}
}

func (watcher *dataNodeTimeTickWatcher) handleTimeTickMsg(msg *msgstream.TimeTickMsg) error {
	ctx := context.TODO()
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	coll2Segs := make(map[UniqueID][]UniqueID)
	segments, err := watcher.allocator.GetFlushableSegments(ctx, msg.Base.Timestamp)
	if err != nil {
		return err
	}
	for _, id := range segments {
		sInfo, err := watcher.meta.GetSegment(id)
		if err != nil {
			log.Error("get segment from meta error", zap.Int64("segmentID", id), zap.Error(err))
			continue
		}
		collID, segID := sInfo.CollectionID, sInfo.ID
		coll2Segs[collID] = append(coll2Segs[collID], segID)
		watcher.allocator.DropSegment(ctx, id)
	}
	for collID, segIDs := range coll2Segs {
		watcher.cluster.FlushSegment(&datapb.FlushSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Flush,
				MsgID:     -1, // todo add msg id
				Timestamp: 0,  // todo
				SourceID:  Params.NodeID,
			},
			CollectionID: collID,
			SegmentIDs:   segIDs,
		})
	}
	return nil
}
