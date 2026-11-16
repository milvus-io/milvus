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
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// useStreamingQueryNodes exposes this fixture's registered nodes as SQNs.
// Keep the WAL owner outside the replica to exercise candidate scoring.
func useStreamingQueryNodes(t *testing.T, nodeManager *session.NodeManager) {
	t.Helper()
	nodes := mockey.Mock((*snmanager.StreamingNodeManager).GetStreamingQueryNodeIDs).To(
		func(*snmanager.StreamingNodeManager) typeutil.UniqueSet {
			ids := typeutil.NewUniqueSet()
			for _, node := range nodeManager.GetAll() {
				ids.Insert(node.ID())
			}
			return ids
		}).Build()
	t.Cleanup(func() { nodes.UnPatch() })
	wal := mockey.Mock((*snmanager.StreamingNodeManager).GetWALLocated).Return(int64(10000)).Build()
	t.Cleanup(func() { wal.UnPatch() })
}

// createStreamingTestReplica gives the existing workload fixture explicit SQNs.
func createStreamingTestReplica(id, collectionID int64, nodes []int64) *meta.Replica {
	replica := utils.CreateTestReplica(id, collectionID, nodes).CopyForWrite()
	replica.AddRWSQNode(nodes...)
	return replica.IntoReplica()
}

func recoverStreamingTestReplicas(t *testing.T, m *meta.Meta) {
	t.Helper()
	utils.RecoverAllCollection(m)
	ctx := context.Background()
	require.NoError(t, m.RecoverSQNodesInCollections(ctx, m.GetAll(ctx),
		map[string]typeutil.UniqueSet{
			meta.DefaultResourceGroupName: snmanager.StaticStreamingNodeManager.GetStreamingQueryNodeIDs(),
		}))
}
