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
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	base "github.com/milvus-io/milvus/internal/util/pipeline"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// pipeline used for querynode
type Pipeline interface {
	base.StreamPipeline
}

type pipeline struct {
	base.StreamPipeline

	collectionID UniqueID
}

func (p *pipeline) Close() {
	p.StreamPipeline.Close()
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

	p := &pipeline{
		collectionID:   collectionID,
		StreamPipeline: base.NewPipelineWithStream(dispatcher, nodeCtxTtInterval, enableTtChecker, channel),
	}

	filterNode := newFilterNode(collectionID, channel, manager, delegator, pipelineQueueLength)
	insertNode := newInsertNode(collectionID, channel, manager, delegator, pipelineQueueLength)
	deleteNode := newDeleteNode(collectionID, channel, manager, tSafeManager, delegator, pipelineQueueLength)
	p.Add(filterNode, insertNode, deleteNode)
	return p, nil
}
