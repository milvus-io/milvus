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

package task

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestOrphanSegmentCleanupAfterNodeReassigned(t *testing.T) {
	for _, rpcReturned := range []bool{false, true} {
		name := "before dispatch"
		if rpcReturned {
			name = "reloaded before distribution observed deletion"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			const collectionID, nodeID, segmentID = int64(100), int64(6), int64(10)
			action := NewSegmentActionWithScope(nodeID, ActionTypeReduce, "ch", segmentID, querypb.DataScope_Historical, 100)
			cleanup, err := NewSegmentTask(ctx, time.Minute, WrapIDSource(0), collectionID, meta.NilReplica, commonpb.LoadPriority_LOW, action)
			require.NoError(t, err)
			t.Cleanup(func() { cleanup.Cancel(nil) })

			nodes := session.NewNodeManager()
			nodes.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: nodeID}))
			dist := meta.NewDistributionManager(nil)
			dist.SegmentDistManager.Update(nodeID, utils.CreateTestSegment(collectionID, 1, segmentID, nodeID, 1, "ch"))
			scheduler := &taskScheduler{
				ctx: ctx, meta: &meta.Meta{ReplicaManager: &meta.ReplicaManager{}}, nodeMgr: nodes, distMgr: dist,
			}
			loaded := true
			loadedPatch := mockey.Mock((*meta.CollectionManager).Exist).To(
				func(_ *meta.CollectionManager, _ context.Context, collection int64) bool {
					require.Equal(t, collectionID, collection)
					return loaded
				}).Build()
			t.Cleanup(func() { loadedPatch.UnPatch() })
			var owner *meta.Replica
			patch := mockey.Mock((*meta.ReplicaManager).GetByCollectionAndNode).To(
				func(_ *meta.ReplicaManager, _ context.Context, collection, node int64) *meta.Replica {
					require.Equal(t, collectionID, collection)
					require.Equal(t, nodeID, node)
					return owner
				}).Build()
			t.Cleanup(func() { patch.UnPatch() })

			// A genuinely orphaned copy still needs cleanup.
			require.NoError(t, scheduler.checkStale(cleanup, true))
			action.rpcReturned.Store(rpcReturned)
			require.False(t, action.IsFinished(dist))

			// An RG change assigns this node to a new replica. Its copy is now
			// owned again, possibly reloaded without an observed absent report.
			owner = utils.CreateTestReplica(20, collectionID, []int64{nodeID})
			require.ErrorContains(t, scheduler.checkStale(cleanup, true), "reassigned")

			// ReleaseCollection removes load metadata before replicas. Cleanup
			// must continue even while the replica still contains this node.
			loaded = false
			require.NoError(t, scheduler.checkStale(cleanup, true))
			loaded = true
			require.False(t, scheduler.preProcess(cleanup))
			require.Equal(t, TaskStatusCanceled, cleanup.Status(), "retire the old cleanup instead of holding its task slot until timeout")
		})
	}
}
