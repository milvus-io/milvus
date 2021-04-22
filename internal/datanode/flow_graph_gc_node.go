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

package datanode

import (
	"github.com/milvus-io/milvus/internal/util/flowgraph"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

type gcNode struct {
	BaseNode
	replica Replica
}

func (gcNode *gcNode) Name() string {
	return "gcNode"
}

func (gcNode *gcNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {

	if len(in) != 1 {
		log.Error("Invalid operate message input in gcNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	gcMsg, ok := in[0].(*gcMsg)
	if !ok {
		log.Error("type assertion failed for gcMsg")
		// TODO: add error handling
	}

	if gcMsg == nil {
		return []Msg{}
	}

	// drop collections
	for _, collectionID := range gcMsg.gcRecord.collections {
		err := gcNode.replica.removeCollection(collectionID)
		if err != nil {
			log.Error("replica remove collection wrong", zap.Error(err))
		}
	}

	return nil
}

func newGCNode(replica Replica) *gcNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &gcNode{
		BaseNode: baseNode,
		replica:  replica,
	}
}
