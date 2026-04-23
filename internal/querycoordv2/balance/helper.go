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

package balance

import (
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
)

// BalanceReplicaHelper provides shared helper methods for getting RW/RO nodes
// from a replica, handling the streaming service compatibility logic in one place.
type BalanceReplicaHelper struct {
	nodeManager *session.NodeManager
}

// GetRWNodesForChannels returns the RW nodes for channel balancing.
// When streaming service is enabled, it uses the compatibility helper;
// otherwise it returns the replica's RW nodes directly.
func (h *BalanceReplicaHelper) GetRWNodesForChannels(replica *meta.Replica) []int64 {
	if streamingutil.IsStreamingServiceEnabled() {
		rwNodes, _ := utils.GetChannelRWAndRONodesFor260(replica, h.nodeManager)
		return rwNodes
	}
	return replica.GetRWNodes()
}

// GetRWAndRONodesForChannels returns both RW and RO nodes for channel balancing.
// When streaming service is enabled, it uses the compatibility helper;
// otherwise it returns the replica's RW and RO nodes directly.
func (h *BalanceReplicaHelper) GetRWAndRONodesForChannels(replica *meta.Replica) (rwNodes []int64, roNodes []int64) {
	if streamingutil.IsStreamingServiceEnabled() {
		return utils.GetChannelRWAndRONodesFor260(replica, h.nodeManager)
	}
	return replica.GetRWNodes(), replica.GetRONodes()
}
