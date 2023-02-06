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

package pipeline

import (
	"context"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

//pipeline used for querynode
type Pipeline interface {
	base.StreamPipeline
	ExcludedSegments(segInfos ...*datapb.SegmentInfo)
}
type pipeline struct {
	base.StreamPipeline

	excludedSegments *typeutil.ConcurrentMap[int64, *datapb.SegmentInfo]
	collectionID     UniqueID
}

func (p *pipeline) ExcludedSegments(segInfos ...*datapb.SegmentInfo) {
	for _, segInfo := range segInfos {
		log.Debug("pipeline add exclude info",
			zap.Int64("segmentID", segInfo.GetID()),
			zap.Uint64("tss", segInfo.GetDmlPosition().Timestamp),
		)
		p.excludedSegments.Insert(segInfo.GetID(), segInfo)
	}
}

func NewPipeLine(
	collectionID UniqueID,
	channel string,
	manager *DataManager,
	tSafeManager TSafeManager,
	factory msgstream.Factory,
	delegator delegator.ShardDelegator,
) (Pipeline, error) {
	pipelineQueueLength := paramtable.Get().QueryNodeCfg.FlowGraphMaxQueueLength.GetAsInt32()
	dmStream, err := factory.NewTtMsgStream(context.Background())
	if err != nil {
		return nil, err
	}
	excludedSegments := typeutil.NewConcurrentMap[int64, *datapb.SegmentInfo]()

	p := &pipeline{
		collectionID:     collectionID,
		excludedSegments: excludedSegments,
		StreamPipeline:   base.NewPipelineWithStream(dmStream, nodeCtxTtInterval, enableTtChecker, channel),
	}

	filterNode := newFilterNode(collectionID, channel, manager, excludedSegments, pipelineQueueLength)
	insertNode := newInsertNode(collectionID, channel, manager, delegator, pipelineQueueLength)
	deleteNode := newDeleteNode(collectionID, channel, manager, tSafeManager, delegator, pipelineQueueLength)
	p.Add(filterNode, insertNode, deleteNode)
	return p, nil
}
