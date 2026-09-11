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

package shardclient

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func selectWeightedNode(ctx context.Context, balancer LBBalancer, nodes []WeightedNode, nq int64) (int64, error) {
	if weightedBalancer, ok := balancer.(WeightedLBBalancer); ok {
		return weightedBalancer.SelectNodeWithWeights(ctx, nodes, nq)
	}
	nodeIDs := make([]int64, 0, len(nodes))
	for _, node := range nodes {
		if node.Weight > 0 {
			nodeIDs = append(nodeIDs, node.NodeID)
		}
	}
	return balancer.SelectNode(ctx, nodeIDs, nq)
}

func selectWeightedRoundRobin(nodes []WeightedNode, idx int64) (int64, error) {
	totalWeight := 0
	for _, node := range nodes {
		if node.Weight > 0 {
			totalWeight += node.Weight
		}
	}
	if totalWeight == 0 {
		return -1, merr.ErrNodeNotAvailable
	}

	offset := int(idx % int64(totalWeight))
	for _, node := range nodes {
		if node.Weight <= 0 {
			continue
		}
		if offset < node.Weight {
			return node.NodeID, nil
		}
		offset -= node.Weight
	}
	return -1, merr.ErrNodeNotAvailable
}
