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
	"testing"

	"github.com/blang/semver/v4"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	ext "github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// The nodes a channel is balanced across follow where delegators live. On a
// stock binary with the streaming service on that is the replica's streaming
// query nodes; under an installed form it is the replica's regular query
// nodes, RO ones included, so a query node leaving its cluster is drained of
// its delegator exactly as it was before the streaming service existed.
func TestChannelBalanceNodesFollowWhereDelegatorsLive(t *testing.T) {
	streaming := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(true).Build()
	defer streaming.UnPatch()

	replica := meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 1, ResourceGroup: "cluster_a",
		Nodes: []int64{11}, RoNodes: []int64{12}, RwSqNodes: []int64{7},
	})
	// Registered at a current version: a query node the node manager does not
	// know, or one older than 2.6, is listed for draining by the upgrade path
	// (utils.GetChannelRWAndRONodesFor260), which is not what this is about.
	nodeMgr := session.NewNodeManager()
	for _, nodeID := range []int64{11, 12} {
		nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID: nodeID, Address: "localhost", Hostname: "localhost", Version: semver.MustParse("2.6.0"),
		}))
	}
	helper := &BalanceReplicaHelper{nodeManager: nodeMgr}

	t.Run("stock binary", func(t *testing.T) {
		ext.ResetForTest()
		t.Cleanup(ext.ResetForTest)
		assert.Equal(t, []int64{7}, helper.GetRWNodesForChannels(replica))
		rw, ro := helper.GetRWAndRONodesForChannels(replica)
		assert.Equal(t, []int64{7}, rw)
		assert.Equal(t, []int64{12}, ro, "the regular RO node is still one to move channels off")
	})

	t.Run("installed form", func(t *testing.T) {
		ext.ResetForTest()
		t.Cleanup(ext.ResetForTest)
		ext.SetForm()
		assert.Equal(t, []int64{11}, helper.GetRWNodesForChannels(replica))
		rw, ro := helper.GetRWAndRONodesForChannels(replica)
		assert.Equal(t, []int64{11}, rw)
		assert.Equal(t, []int64{12}, ro)
	})
}
