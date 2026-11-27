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
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (suite *BalanceTestSuite) TestMultiTargetBalanceWaitsForReplicaSegmentTasks() {
	ctx := context.Background()
	balancer := NewMultiTargetBalancer(suite.mockScheduler, suite.nodeManager, suite.dist, suite.targetMgr)
	replica := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 1, Nodes: []int64{1, 2}})
	otherReplica := meta.NewReplica(&querypb.Replica{ID: 2, CollectionID: 1, Nodes: []int64{3, 4}})
	otherTask, err := task.NewSegmentTask(ctx, time.Minute, task.WrapIDSource(0), 1, otherReplica,
		commonpb.LoadPriority_LOW, task.NewSegmentAction(3, task.ActionTypeGrow, "channel", 100))
	suite.Require().NoError(err)
	defer otherTask.Cancel(nil)
	pending := []task.Task{otherTask}
	// Exercise the production filter against real tasks, including another
	// replica of the same collection, rather than hardcoding a task count.
	suite.mockScheduler.EXPECT().GetSegmentTaskNum(mock.Anything).RunAndReturn(func(filters ...task.TaskFilter) int {
		count := 0
		for _, pendingTask := range pending {
			matches := true
			for _, filter := range filters {
				matches = matches && filter(pendingTask)
			}
			if matches {
				count++
			}
		}
		return count
	})
	suite.targetMgr.EXPECT().CanSegmentBeMoved(mock.Anything, int64(1), mock.Anything).Return(true).Maybe()
	segments := make([]*meta.Segment, 0, 4)
	for id := int64(1); id <= 4; id++ {
		segments = append(segments, &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{ID: id, CollectionID: 1, InsertChannel: "channel", NumOfRows: 100},
			Node:        1,
		})
	}
	suite.dist.SegmentDistManager.Update(1, segments...)
	plans := balancer.balanceSegments(ctx, NewBalanceReport(), replica)
	suite.Require().Len(plans, 2)
	moves := CreateSegmentTasksFromPlans(ctx, task.WrapIDSource(0), time.Minute, plans)
	suite.Require().Len(moves, 2)
	for _, move := range moves {
		defer move.Cancel(nil)
	}
	pending = append(pending, moves...)

	// Match CI: the first pair is pending, but all four segments are still
	// reported on the source. Repeated rounds must not pick the other pair.
	for i := 0; i < 20; i++ {
		suite.Empty(balancer.balanceSegments(ctx, NewBalanceReport(), replica))
	}

	// Completing or canceling only one task must not reopen planning.
	pending = []task.Task{otherTask, moves[1]}
	suite.Empty(balancer.balanceSegments(ctx, NewBalanceReport(), replica))

	// A canceled/failed batch is removed by the scheduler. With the original
	// distribution still unbalanced, a new batch must be allowed to retry.
	pending = []task.Task{otherTask}
	suite.Len(balancer.balanceSegments(ctx, NewBalanceReport(), replica), 2)

	// Successful completion publishes the moved segments before task removal.
	moved := typeutil.NewSet(plans[0].Segment.GetID(), plans[1].Segment.GetID())
	var source, destination []*meta.Segment
	for _, segment := range segments {
		if moved.Contain(segment.GetID()) {
			clone := *segment
			clone.Node = 2
			destination = append(destination, &clone)
		} else {
			source = append(source, segment)
		}
	}
	suite.dist.SegmentDistManager.Update(1, source...)
	suite.dist.SegmentDistManager.Update(2, destination...)
	suite.Empty(balancer.balanceSegments(ctx, NewBalanceReport(), replica))
}

func (suite *BalanceTestSuite) TestMultiTargetPendingSegmentsDoNotBlockChannels() {
	ctx := context.Background()
	params := paramtable.Get()
	suite.Require().NoError(params.Save(params.QueryCoordCfg.AutoBalanceChannel.Key, "true"))
	defer params.Reset(params.QueryCoordCfg.AutoBalanceChannel.Key)
	balancer := NewMultiTargetBalancer(suite.mockScheduler, suite.nodeManager, suite.dist, suite.targetMgr)
	replica := meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 1, Nodes: []int64{1, 2}})
	for _, id := range []int64{1, 2} {
		node := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: id, Version: common.Version})
		node.SetState(session.NodeStateNormal)
		suite.nodeManager.Add(node)
	}
	suite.dist.ChannelDistManager.Update(1,
		&meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "channel-1"}, Node: 1},
		&meta.DmChannel{VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "channel-2"}, Node: 1},
	)
	suite.mockScheduler.EXPECT().GetSegmentTaskNum(mock.Anything).Return(1).Maybe()
	segmentPlans, channelPlans := balancer.BalanceReplica(ctx, replica)
	suite.Empty(segmentPlans)
	suite.Len(channelPlans, 1)
	suite.mockScheduler.AssertNotCalled(suite.T(), "GetSegmentTaskNum", mock.Anything)
}
