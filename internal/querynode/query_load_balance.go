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

package querynode

import (
	"context"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"go.uber.org/zap"
)

type loadBalanceStage struct {
	ctx    context.Context
	cancel context.CancelFunc

	collectionID UniqueID

	input chan *msgstream.LoadBalanceSegmentsMsg
}

func newLoadBalanceStage(ctx context.Context,
	cancel context.CancelFunc,
	collectionID UniqueID,
	input chan *msgstream.LoadBalanceSegmentsMsg) *loadBalanceStage {

	return &loadBalanceStage{
		ctx:          ctx,
		cancel:       cancel,
		collectionID: collectionID,
		input:        input,
	}
}

func (q *loadBalanceStage) start() {
	for {
		select {
		case <-q.ctx.Done():
			log.Debug("stop loadBalanceStage", zap.Int64("collectionID", q.collectionID))
			return
		case <-q.input:
			//TODO:: get loadBalance info from etcd
			//log.Debug("consume load balance message",
			//	zap.Int64("msgID", msg.ID()))
			//nodeID := Params.QueryNodeID
			//for _, info := range msg.Infos {
			//	segmentID := info.SegmentID
			//	if nodeID == info.SourceNodeID {
			//		err := s.historical.replica.removeSegment(segmentID)
			//		if err != nil {
			//			log.Error("loadBalance failed when remove segment",
			//				zap.Error(err),
			//				zap.Any("segmentID", segmentID))
			//		}
			//	}
			//	if nodeID == info.DstNodeID {
			//		segment, err := s.historical.replica.getSegmentByID(segmentID)
			//		if err != nil {
			//			log.Error("loadBalance failed when making segment on service",
			//				zap.Error(err),
			//				zap.Any("segmentID", segmentID))
			//			continue // not return, try to load balance all segment
			//		}
			//		segment.setOnService(true)
			//	}
			//}
			//log.Debug("load balance done",
			//	zap.Int64("msgID", msg.ID()),
			//	zap.Int("num of segment", len(msg.Infos)))
		}
	}
}
