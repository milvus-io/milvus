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
	"os"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	ext "github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type delegatorFormHook struct{ hook.Hook }

// channelOnAQueryCluster loads one channel of collection 1 into a replica that
// holds a regular query node and a streaming query node, with the WAL on a
// third node, and answers which node the delegator is grown on.
func (suite *ChannelCheckerTestSuite) channelOnAQueryCluster(regular, streamingQuery, walNode int64) int64 {
	streamingutil.SetStreamingServiceEnabled()
	suite.T().Cleanup(func() { os.Unsetenv(streamingutil.MilvusStreamingServiceEnabled) })

	// Feed the pchannel's WAL assignment, otherwise GetWALLocated blocks forever.
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(suite.T())
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		cb(balancer.WatchChannelAssignmentsCallbackParam{
			Version:            typeutil.VersionInt64Pair{Global: 1, Local: 1},
			CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: "pchannel0"}},
			Relations: []types.PChannelInfoAssigned{{
				Channel: types.PChannelInfo{Name: "pchannel0", Term: 1},
				Node:    types.StreamingNodeInfo{ServerID: walNode, Address: "localhost:1"},
			}},
		})
		<-ctx.Done()
		return context.Cause(ctx)
	}).Maybe()
	streamingNodes := map[int64]*types.StreamingNodeInfoWithResourceGroup{}
	for _, node := range []int64{walNode, streamingQuery} {
		streamingNodes[node] = &types.StreamingNodeInfoWithResourceGroup{
			StreamingNodeInfo: types.StreamingNodeInfo{ServerID: node, Address: "localhost:1"},
		}
	}
	b.EXPECT().GetAllStreamingNodes(mock.Anything).Return(streamingNodes, nil).Maybe()
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(streamingNodes, nil).Maybe()
	b.EXPECT().Close().Return().Maybe()
	balance.Register(b)

	ctx := context.Background()
	checker := suite.checker
	checker.scheduler.(*task.MockScheduler).EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	checker.meta.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	suite.meta.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.Put(ctx, meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: 1, ResourceGroup: meta.DefaultResourceGroupName,
		Nodes: []int64{regular}, RwSqNodes: []int64{streamingQuery},
	}))
	suite.setNodeAvailable(regular, streamingQuery)

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		[]*datapb.VchannelInfo{{CollectionID: 1, ChannelName: "pchannel0_1v0"}}, nil, nil)
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

	tasks := checker.Check(ctx)
	suite.Require().Len(tasks, 1)
	suite.Require().Len(tasks[0].Actions(), 1)
	action := tasks[0].Actions()[0].(*task.ChannelAction)
	suite.Equal(task.ActionTypeGrow, action.Type())
	return action.Node()
}

// A form keeps its one streaming node for DDL and the write ahead log and
// serves queries from regular query nodes, one replica per query cluster. The
// delegator of a replica goes onto that replica's regular query node, which
// watches the channel and reads the WAL remotely - never onto the streaming
// query node the replica manager may still have handed the replica.
func (suite *ChannelCheckerTestSuite) TestUnderAFormADelegatorGoesToARegularQueryNode() {
	ext.ResetForTest()
	suite.T().Cleanup(ext.ResetForTest)
	ext.SetHook(delegatorFormHook{})

	suite.EqualValues(11, suite.channelOnAQueryCluster(11, 1051, 1007))
}

// A stock binary keeps milvus's placement: beside the WAL's readers, on the
// replica's streaming query node.
func (suite *ChannelCheckerTestSuite) TestOnAStockBinaryADelegatorGoesToTheStreamingQueryNode() {
	ext.ResetForTest()
	suite.T().Cleanup(ext.ResetForTest)

	suite.EqualValues(1051, suite.channelOnAQueryCluster(11, 1051, 1007))
}
