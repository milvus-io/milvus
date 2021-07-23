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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

type gcNode struct {
	baseNode
	replica ReplicaInterface
}

func (gcNode *gcNode) Name() string {
	return "gcNode"
}

func (gcNode *gcNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	//log.Debug("Do gcNode operation")

	if len(in) != 1 {
		log.Error("Invalid operate message input in gcNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	_, ok := in[0].(*gcMsg)
	if !ok {
		log.Warn("type assertion failed for gcMsg")
		// TODO: add error handling
	}

	// Use `releasePartition` and `releaseCollection`,
	// because if we drop collections or partitions here, query service doesn't know this behavior,
	// which would lead the wrong result of `showCollections` or `showPartition`

	//// drop collections
	//for _, collectionID := range gcMsg.gcRecord.collections {
	//	err := gcNode.replica.removeCollection(collectionID)
	//	if err != nil {
	//		log.Println(err)
	//	}
	//}
	//
	//// drop partitions
	//for _, partition := range gcMsg.gcRecord.partitions {
	//	err := gcNode.replica.removePartition(partition.partitionID)
	//	if err != nil {
	//		log.Println(err)
	//	}
	//}

	return nil
}

func newGCNode(replica ReplicaInterface) *gcNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &gcNode{
		baseNode: baseNode,
		replica:  replica,
	}
}
