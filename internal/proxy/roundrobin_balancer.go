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
	"context"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/atomic"
)

type RoundRobinBalancer struct {
	// request num send to each node
	nodeWorkload *typeutil.ConcurrentMap[int64, *atomic.Int64]
}

func NewRoundRobinBalancer() *RoundRobinBalancer {
	return &RoundRobinBalancer{
		nodeWorkload: typeutil.NewConcurrentMap[int64, *atomic.Int64](),
	}
}

func (b *RoundRobinBalancer) SelectNode(ctx context.Context, availableNodes []int64, cost int64) (int64, error) {
	if len(availableNodes) == 0 {
		return -1, merr.ErrNoAvailableNode
	}

	targetNode := int64(-1)
	var targetNodeWorkload *atomic.Int64
	for _, node := range availableNodes {
		workload, ok := b.nodeWorkload.Get(node)

		if !ok {
			workload = atomic.NewInt64(0)
			b.nodeWorkload.Insert(node, workload)
		}

		if targetNodeWorkload == nil || workload.Load() < targetNodeWorkload.Load() {
			targetNode = node
			targetNodeWorkload = workload
		}
	}

	targetNodeWorkload.Add(cost)
	return targetNode, nil
}

func (b *RoundRobinBalancer) CancelWorkload(node int64, nq int64) {
	load, ok := b.nodeWorkload.Get(node)

	if ok {
		load.Sub(nq)
	}
}

func (b *RoundRobinBalancer) UpdateCostMetrics(node int64, cost *internalpb.CostAggregation) {}

func (b *RoundRobinBalancer) Start(ctx context.Context) {}

func (b *RoundRobinBalancer) Close() {}
