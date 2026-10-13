// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkers

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const recoveryTestChannel = "recovery_1v0"

func (suite *ChannelCheckerTestSuite) prepareChannelRecoveryTarget(replicas ...*meta.Replica) {
	ctx := context.Background()
	suite.Require().NoError(suite.meta.PutCollection(ctx, utils.CreateTestCollection(1, int32(len(replicas)))))
	suite.Require().NoError(suite.meta.PutPartition(ctx, utils.CreateTestPartition(1, 1)))
	suite.Require().NoError(suite.meta.Put(ctx, replicas...))
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		[]*datapb.VchannelInfo{{CollectionID: 1, ChannelName: recoveryTestChannel}}, nil, nil)
	suite.Require().NoError(suite.checker.targetMgr.UpdateCollectionNextTarget(ctx, 1))
}

func (suite *ChannelCheckerTestSuite) TestChannelRecoveryAcrossReplicas() {
	suite.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "0")
	suite.setNodeAvailable(1, 2)
	suite.prepareChannelRecoveryTarget(
		utils.CreateTestReplica(1, 1, []int64{1}),
		utils.CreateTestReplica(2, 1, []int64{2}),
	)
	// An unchanged distribution means both replicas still need the channel on
	// the next tick too. The first replica must not consume the second's claim.
	for tick := 0; tick < 2; tick++ {
		tasks := suite.checker.Check(context.Background())
		suite.Require().Len(tasks, 2)
		targets := make(map[int64]int64)
		for _, work := range tasks {
			defer work.Cancel(nil)
			suite.Require().Len(work.Actions(), 1)
			action := work.Actions()[0].(*task.ChannelAction)
			suite.Equal(task.ActionTypeGrow, action.Type())
			suite.Equal(recoveryTestChannel, action.ChannelName())
			targets[work.ReplicaID()] = action.Node()
		}
		suite.Equal(map[int64]int64{1: 1, 2: 2}, targets)
	}
}

func (suite *ChannelCheckerTestSuite) TestChannelRecoverySkipsUnknownReport() {
	suite.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "0")
	suite.setNodeAvailable(1, 2)
	suite.prepareChannelRecoveryTarget(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	suite.nodeMgr.BeginLoadReportSync(0)
	for _, node := range suite.nodeMgr.GetAll() {
		node.UpdateLoadReport(nil, true, true, time.Now())
	}
	suite.True(suite.nodeMgr.LoadReportSyncReady(), "the startup wait has expired")
	suite.Empty(suite.checker.Check(context.Background()), "unknown reports cannot admit recovery")

	// The candidate with the smaller ID stays unknown. A report-only update
	// makes node 2 usable without changing target/distribution versions.
	suite.nodeMgr.Get(2).UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, true, time.Now())
	suite.False(suite.nodeMgr.Get(1).LoadAdmissionReady())
	suite.True(suite.nodeMgr.Get(2).LoadAdmissionReady())
	tasks := suite.checker.Check(context.Background())
	suite.Require().Len(tasks, 1)
	defer tasks[0].Cancel(nil)
	suite.EqualValues(2, tasks[0].Actions()[0].Node())
}

func (suite *ChannelCheckerTestSuite) TestChannelRecoveryFallsBackFromUnknownWALOwner() {
	suite.T().Setenv(streamingutil.MilvusStreamingServiceEnabled, "1")
	snmanager.ResetStreamingNodeManager()
	suite.T().Cleanup(snmanager.ResetStreamingNodeManager)
	b := mock_balancer.NewMockBalancer(suite.T())
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		cb(balancer.WatchChannelAssignmentsCallbackParam{
			Version:            typeutil.VersionInt64Pair{Global: 1, Local: 1},
			CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "recovery"}},
			Relations: []types.PChannelInfoAssigned{{
				Channel: types.PChannelInfo{Name: "recovery", Term: 1},
				Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
			}},
		})
		<-ctx.Done()
		return context.Cause(ctx)
	}).Maybe()
	streamingNodes := make(map[int64]*types.StreamingNodeInfoWithResourceGroup)
	for _, node := range []int64{1, 2} {
		streamingNodes[node] = &types.StreamingNodeInfoWithResourceGroup{
			StreamingNodeInfo: types.StreamingNodeInfo{ServerID: node, Address: "localhost:1"},
		}
	}
	b.EXPECT().GetAllStreamingNodes(mock.Anything).Return(streamingNodes, nil).Maybe()
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(streamingNodes, nil).Maybe()
	b.EXPECT().Close().Return().Maybe()
	balance.Register(b)

	suite.setNodeAvailable(1, 2)
	suite.prepareChannelRecoveryTarget(meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 1, ResourceGroup: meta.DefaultResourceGroupName, RwSqNodes: []int64{1, 2},
	}))
	suite.nodeMgr.BeginLoadReportSync(0)
	suite.nodeMgr.Get(1).UpdateLoadReport(nil, true, true, time.Now())
	suite.nodeMgr.Get(2).UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{}, true, true, time.Now())
	suite.EqualValues(1, snmanager.StaticStreamingNodeManager.GetWALLocated(recoveryTestChannel))
	tasks := suite.checker.Check(context.Background())
	suite.Require().Len(tasks, 1)
	defer tasks[0].Cancel(nil)
	suite.EqualValues(2, tasks[0].Actions()[0].Node(), "an unknown WAL owner must not hide the ready fallback")
}
