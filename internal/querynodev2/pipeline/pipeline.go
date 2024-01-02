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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// pipeline used for querynode
type Pipeline interface {
	base.StreamPipeline
	ExcludedSegments(info map[int64]uint64)
}

type pipeline struct {
	base.StreamPipeline

	excludedSegments *typeutil.ConcurrentMap[int64, uint64]
	collectionID     UniqueID
}

func (p *pipeline) ExcludedSegments(excludeInfo map[int64]uint64) { //(segInfos ...*datapb.SegmentInfo) {
	for segmentID, ts := range excludeInfo {
		log.Debug("pipeline add exclude info",
			zap.Int64("segmentID", segmentID),
			zap.Uint64("ts", ts),
		)
		p.excludedSegments.Insert(segmentID, ts)
	}
}

func (p *pipeline) Close() {
	p.StreamPipeline.Close()
	metrics.CleanupQueryNodeCollectionMetrics(paramtable.GetNodeID(), p.collectionID)
}

func NewPipeLine(
	collectionID UniqueID,
	channel string,
	manager *DataManager,
	tSafeManager TSafeManager,
	dispatcher msgdispatcher.Client,
	delegator delegator.ShardDelegator,
) (Pipeline, error) {
	pipelineQueueLength := paramtable.Get().QueryNodeCfg.FlowGraphMaxQueueLength.GetAsInt32()
	excludedSegments := typeutil.NewConcurrentMap[int64, uint64]()

	p := &pipeline{
		collectionID:     collectionID,
		excludedSegments: excludedSegments,
		StreamPipeline:   base.NewPipelineWithStream(dispatcher, nodeCtxTtInterval, enableTtChecker, channel),
	}

	filterNode := newFilterNode(collectionID, channel, manager, excludedSegments, pipelineQueueLength)
	insertNode := newInsertNode(collectionID, channel, manager, delegator, pipelineQueueLength)
	deleteNode := newDeleteNode(collectionID, channel, manager, tSafeManager, delegator, pipelineQueueLength)
	p.Add(filterNode, insertNode, deleteNode)
	return p, nil
}
