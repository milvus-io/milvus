// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package proxy

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/util/merr"
)

type RoundRobinBalancer struct {
	// request num send to each node
	mutex        sync.RWMutex
	nodeWorkload map[int64]int64
}

func NewRoundRobinBalancer() *RoundRobinBalancer {
	return &RoundRobinBalancer{
		nodeWorkload: make(map[int64]int64),
	}
}

func (b *RoundRobinBalancer) SelectNode(availableNodes []int64, workload int64) (int64, error) {
	if len(availableNodes) == 0 {
		return -1, merr.ErrNoAvailableNode
	}
	b.mutex.Lock()
	defer b.mutex.Unlock()
	targetNode := int64(-1)
	targetNodeWorkload := int64(-1)
	for _, node := range availableNodes {
		if targetNodeWorkload == -1 || b.nodeWorkload[node] < targetNodeWorkload {
			targetNode = node
			targetNodeWorkload = b.nodeWorkload[node]
		}
	}

	b.nodeWorkload[targetNode] += workload
	return targetNode, nil
}
