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

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

type LBBalancer interface {
	RegisterNodeInfo(nodeInfos []NodeInfo)
	// SelectNode reserves nq on the selected node. On success, the caller must
	// call release exactly once, including when it abandons the selection.
	// The release function belongs to this reservation, not the node's current state.
	// On error, no workload is reserved and release is nil.
	SelectNode(ctx context.Context, availableNodes []int64, nq int64) (nodeID int64, release func(), err error)
	UpdateCostMetrics(node int64, cost *internalpb.CostAggregation)
	Start(ctx context.Context)
	Close()
}
