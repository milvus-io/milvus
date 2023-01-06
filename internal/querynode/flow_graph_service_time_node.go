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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
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
	return fmt.Sprintf("stNode-%s", stNode.vChannel)
}

func (stNode *serviceTimeNode) IsValidInMsg(in []Msg) bool {
	if !stNode.baseNode.IsValidInMsg(in) {
		return false
	}
	_, ok := in[0].(*serviceTimeMsg)
	if !ok {
		log.Warn("type assertion failed for serviceTimeMsg", zap.String("msgType", reflect.TypeOf(in[0]).Name()), zap.String("name", stNode.Name()))
		return false
	}
	return true
}

// Operate handles input messages, to execute insert operations
func (stNode *serviceTimeNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {
	serviceTimeMsg := in[0].(*serviceTimeMsg)
	if serviceTimeMsg.IsCloseMsg() {
		log.Info("service node hit close msg",
			zap.Int64("collectionID", stNode.collectionID),
			zap.Uint64("tSafe", serviceTimeMsg.timeRange.timestampMax),
			zap.String("channel", stNode.vChannel),
		)
		return in
	}

	// update service time
	err := stNode.tSafeReplica.setTSafe(stNode.vChannel, serviceTimeMsg.timeRange.timestampMax)
	if err != nil {
		// should not happen, QueryNode should addTSafe before start flow graph
		panic(fmt.Errorf("serviceTimeNode setTSafe timeout, collectionID = %d, err = %s", stNode.collectionID, err))
	}
	rateCol.updateTSafe(stNode.vChannel, serviceTimeMsg.timeRange.timestampMax)
	p, _ := tsoutil.ParseTS(serviceTimeMsg.timeRange.timestampMax)
	log.RatedDebug(10.0, "update tSafe:",
		zap.Int64("collectionID", stNode.collectionID),
		zap.Uint64("tSafe", serviceTimeMsg.timeRange.timestampMax),
		zap.Time("tSafe_p", p),
		zap.Duration("tsLag", time.Since(p)),
		zap.String("channel", stNode.vChannel),
	)

	return in
}

// newServiceTimeNode returns a new serviceTimeNode
func newServiceTimeNode(tSafeReplica TSafeReplicaInterface,
	collectionID UniqueID,
	vchannel Channel) *serviceTimeNode {

	maxQueueLength := Params.QueryNodeCfg.FlowGraphMaxQueueLength.GetAsInt32()
	maxParallelism := Params.QueryNodeCfg.FlowGraphMaxParallelism.GetAsInt32()

	baseNode := baseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	return &serviceTimeNode{
		baseNode:     baseNode,
		collectionID: collectionID,
		vChannel:     vchannel,
		tSafeReplica: tSafeReplica,
	}
}
