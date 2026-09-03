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

package checkers

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

const sqnShard = "by-dev-rootcoord-dml_0_100v0"

// sealedSegmentCandidates runs createSegmentLoadTasks against a replica whose
// node sets the caller chooses, and reports the nodes the assignment policy
// was offered. Everything else is stubbed: what is under test is which nodes
// reach the policy, not what it does with them.
func sealedSegmentCandidates(t *testing.T, replica *meta.Replica, streaming bool) []int64 {
	t.Helper()

	enabled := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(streaming).Build()
	defer enabled.UnPatch()
	// A shard leader must exist, or the loop skips before it picks nodes.
	leader := mockey.Mock((*meta.ChannelDistManager).GetShardLeader).Return(&meta.DmChannel{}).Build()
	defer leader.UnPatch()

	var offered []int64
	policy := assign.NewMockAssignPolicy(t)
	policy.EXPECT().
		AssignSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ int64, _ []*meta.Segment, nodes []int64, _ bool) {
			offered = nodes
		}).Return(nil).Maybe()

	c := &SegmentChecker{
		dist:         meta.NewDistributionManager(session.NewNodeManager()),
		assignPolicy: policy,
	}
	c.createSegmentLoadTasks(context.Background(),
		[]*datapb.SegmentInfo{{ID: 1, InsertChannel: sqnShard}},
		[]commonpb.LoadPriority{commonpb.LoadPriority_HIGH},
		replica)
	return offered
}

// A replica whose resource group's only compute is a streaming node has no
// regular query node at all - milvus keeps the query node embedded in a
// streaming node out of the resource manager. Its sealed segments must still
// have somewhere to go, or the load is accepted and never converges.
func TestSealedSegmentsReachAStreamingQueryNodeWhenThereIsNoOther(t *testing.T) {
	replica := meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 100, RwSqNodes: []int64{7},
	})
	require.Empty(t, replica.GetRWNodes(), "the case under test is a replica with no regular node")

	assert.Equal(t, []int64{7}, sealedSegmentCandidates(t, replica, true),
		"the group's streaming query node is the only compute the replica has")
}

// With a regular query node present, nothing changes: sealed segments stay off
// the streaming node, which is the split milvus intends.
func TestSealedSegmentsStayOffTheStreamingNodeWhenARegularOneExists(t *testing.T) {
	replica := meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 100, Nodes: []int64{11}, RwSqNodes: []int64{7},
	})

	assert.Equal(t, []int64{11}, sealedSegmentCandidates(t, replica, true))
}

// With the streaming service off there are no streaming query nodes to fall
// back to, and the empty candidate set is what it always was.
func TestSealedSegmentCandidatesAreUnchangedWithTheStreamingServiceOff(t *testing.T) {
	replica := meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 100, RwSqNodes: []int64{7},
	})

	assert.Empty(t, sealedSegmentCandidates(t, replica, false))
}
