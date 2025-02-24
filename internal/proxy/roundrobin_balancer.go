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

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type RoundRobinBalancer struct {
	idx atomic.Int64
}

func NewRoundRobinBalancer() *RoundRobinBalancer {
	return &RoundRobinBalancer{}
}

func (b *RoundRobinBalancer) RegisterNodeInfo(nodeInfos []nodeInfo) {}

func (b *RoundRobinBalancer) SelectNode(ctx context.Context, availableNodes []int64, cost int64) (int64, error) {
	if len(availableNodes) == 0 {
		return -1, merr.ErrNodeNotAvailable
	}

	idx := b.idx.Inc()
	return availableNodes[int(idx)%len(availableNodes)], nil
}

func (b *RoundRobinBalancer) CancelWorkload(node int64, nq int64) {
}

func (b *RoundRobinBalancer) UpdateCostMetrics(node int64, cost *internalpb.CostAggregation) {}

func (b *RoundRobinBalancer) Start(ctx context.Context) {}

func (b *RoundRobinBalancer) Close() {}
