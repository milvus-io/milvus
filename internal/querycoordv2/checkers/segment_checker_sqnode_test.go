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
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	ext "github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const sqnShard = "by-dev-rootcoord-dml_0_100v0"

// formHook is the smallest thing a distribution can install: the placement
// only asks whether a hook is there, never what it does.
type formHook struct{ hook.Hook }

// setForm makes this test's binary one a distribution compiled itself into,
// or a stock one, and restores a stock binary when the test ends.
func setForm(t *testing.T, installed bool) {
	t.Helper()
	ext.ResetForTest()
	t.Cleanup(ext.ResetForTest)
	if installed {
		ext.SetHook(formHook{})
	}
}

// sealedSegmentPlacement runs createSegmentLoadTasks against a replica whose
// node sets the caller chooses, with one sealed segment to place, and reports
// the nodes the assignment policy was offered and the tasks that came out.
// The policy itself is stubbed to place the segment on the first node it is
// offered: what is under test is which nodes reach it, not what it does with
// them.
func sealedSegmentPlacement(t *testing.T, replica *meta.Replica, streaming bool) (offered []int64, tasks []task.Task) {
	t.Helper()
	paramtable.Init()

	enabled := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(streaming).Build()
	defer enabled.UnPatch()
	// A shard leader must exist, or the loop skips before it picks nodes.
	leader := mockey.Mock((*meta.ChannelDistManager).GetShardLeader).Return(&meta.DmChannel{}).Build()
	defer leader.UnPatch()

	policy := assign.NewMockAssignPolicy(t)
	policy.EXPECT().
		AssignSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ int64, segments []*meta.Segment, nodes []int64, _ bool) []assign.SegmentAssignPlan {
			offered = nodes
			if len(nodes) == 0 {
				return nil
			}
			plans := make([]assign.SegmentAssignPlan, 0, len(segments))
			for _, segment := range segments {
				plans = append(plans, assign.SegmentAssignPlan{Segment: segment, From: -1, To: nodes[0]})
			}
			return plans
		}).Maybe()

	c := &SegmentChecker{
		dist:         meta.NewDistributionManager(session.NewNodeManager()),
		assignPolicy: policy,
	}
	tasks = c.createSegmentLoadTasks(context.Background(),
		[]*datapb.SegmentInfo{{ID: 1, CollectionID: 100, InsertChannel: sqnShard}},
		[]commonpb.LoadPriority{commonpb.LoadPriority_HIGH},
		replica)
	return offered, tasks
}

// A replica whose resource group's only compute is a streaming node has no
// regular query node at all - milvus keeps the query node embedded in a
// streaming node out of the resource manager. For an installed form, its
// sealed segments must still have somewhere to go, or the load is accepted and
// never converges: the segment is placed on the streaming node's query node.
func TestSealedSegmentsReachAStreamingQueryNodeWhenThereIsNoOther(t *testing.T) {
	setForm(t, true)
	replica := meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 100, RwSqNodes: []int64{7},
	})
	require.Empty(t, replica.GetRWNodes(), "the case under test is a replica with no regular node")

	offered, tasks := sealedSegmentPlacement(t, replica, true)
	assert.Equal(t, []int64{7}, offered,
		"the group's streaming query node is the only compute the replica has")
	require.Len(t, tasks, 1, "the sealed segment must be placed, not silently dropped")
	require.Len(t, tasks[0].Actions(), 1)
	assert.EqualValues(t, 7, tasks[0].Actions()[0].Node(), "and placed on the streaming node's query node")
	assert.EqualValues(t, 1, tasks[0].(*task.SegmentTask).SegmentID())
}

// A stock binary keeps the empty candidate set it always had: a replica with
// no regular node produces no plan and no task, and nothing lands on a
// streaming node's query node, where the balancers would never find it again.
func TestAStockBinaryPlacesNoSealedSegmentOnAStreamingQueryNode(t *testing.T) {
	setForm(t, false)
	replica := meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 100, RwSqNodes: []int64{7},
	})

	offered, tasks := sealedSegmentPlacement(t, replica, true)
	assert.Empty(t, offered)
	assert.Empty(t, tasks)
}

// With a regular query node present, nothing changes even for a form: sealed
// segments stay off the streaming node, which is the split milvus intends.
func TestSealedSegmentsStayOffTheStreamingNodeWhenARegularOneExists(t *testing.T) {
	setForm(t, true)
	replica := meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 100, Nodes: []int64{11}, RwSqNodes: []int64{7},
	})

	offered, tasks := sealedSegmentPlacement(t, replica, true)
	assert.Equal(t, []int64{11}, offered)
	require.Len(t, tasks, 1)
	assert.EqualValues(t, 11, tasks[0].Actions()[0].Node())
}

// With the streaming service off there are no streaming query nodes to fall
// back to, and the empty candidate set is what it always was.
func TestSealedSegmentCandidatesAreUnchangedWithTheStreamingServiceOff(t *testing.T) {
	setForm(t, true)
	replica := meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 100, RwSqNodes: []int64{7},
	})

	offered, tasks := sealedSegmentPlacement(t, replica, false)
	assert.Empty(t, offered)
	assert.Empty(t, tasks)
}
