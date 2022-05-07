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

package querynode

import (
	"fmt"
	"reflect"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"go.uber.org/zap"
)

// serviceTimeNode is one of the nodes in delta flow graph
type serviceTimeNode struct {
	baseNode
	collectionID UniqueID
	vChannel     Channel
	tSafeReplica TSafeReplicaInterface
}

// Name returns the name of serviceTimeNode
func (stNode *serviceTimeNode) Name() string {
	return fmt.Sprintf("stNode-%d-%s", stNode.collectionID, stNode.vChannel)
}

// Operate handles input messages, to execute insert operations
func (stNode *serviceTimeNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	if len(in) != 1 {
		log.Warn("Invalid operate message input in serviceTimeNode, input length = ", zap.Int("input node", len(in)))
		return []Msg{}
	}

	serviceTimeMsg, ok := in[0].(*serviceTimeMsg)
	if !ok {
		if in[0] == nil {
			log.Debug("type assertion failed for serviceTimeMsg because it's nil")
		} else {
			log.Warn("type assertion failed for serviceTimeMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		}
		return []Msg{}
	}

	if serviceTimeMsg == nil {
		return []Msg{}
	}

	// update service time
	err := stNode.tSafeReplica.setTSafe(stNode.vChannel, serviceTimeMsg.timeRange.timestampMax)
	if err != nil {
		log.Error("serviceTimeNode setTSafe failed",
			zap.Any("collectionID", stNode.collectionID),
			zap.Error(err),
		)
	}
	p, _ := tsoutil.ParseTS(serviceTimeMsg.timeRange.timestampMax)
	log.Debug("update tSafe:",
		zap.Any("collectionID", stNode.collectionID),
		zap.Any("tSafe", serviceTimeMsg.timeRange.timestampMax),
		zap.Any("tSafe_p", p),
		zap.Any("channel", stNode.vChannel),
	)

	return []Msg{}
}

// newServiceTimeNode returns a new serviceTimeNode
func newServiceTimeNode(tSafeReplica TSafeReplicaInterface,
	collectionID UniqueID,
	channel Channel) *serviceTimeNode {

	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &serviceTimeNode{
		baseNode:     baseNode,
		collectionID: collectionID,
		vChannel:     channel,
		tSafeReplica: tSafeReplica,
	}
}
