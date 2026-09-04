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

package job

import (
	"context"
	"os"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type UpdateLoadConfigJobSuite struct {
	suite.Suite
}

func (suite *UpdateLoadConfigJobSuite) SetupSuite() {
	paramtable.Init()
	// The update-target loop is not under test; keep it quiet.
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.UpdateNextTargetInterval.Key, "3600")
}

// TestScaleUpRecoversSQNodesSynchronously verifies that UpdateLoadConfigJob
// assigns streaming query nodes to newly spawned replicas before returning,
// instead of leaving them to the replica observer's node-changed/timeout loop
// (queryCoord.checkNodeInReplicaInterval, default 60s). Without the synchronous
// recovery, channel delegators on the new replicas cannot be set up for up to
// a minute after a scale-up.
func (suite *UpdateLoadConfigJobSuite) TestScaleUpRecoversSQNodesSynchronously() {
	streamingutil.SetStreamingServiceEnabled()
	defer os.Unsetenv(streamingutil.MilvusStreamingServiceEnabled)

	// Register two RG-labeled streaming nodes via the streaming node manager.
	snmanager.ResetStreamingNodeManager()
	b := mock_balancer.NewMockBalancer(suite.T())
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	}).Maybe()
	streamingNodes := map[int64]*types.StreamingNodeInfoWithResourceGroup{
		101: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 101, Address: "localhost:101"}, ResourceGroup: "rg1"},
		102: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 102, Address: "localhost:102"}, ResourceGroup: "rg2"},
	}
	b.EXPECT().GetAllStreamingNodes(mock.Anything).Return(streamingNodes, nil).Maybe()
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(streamingNodes, nil).Maybe()
	balance.Register(b)

	ctx := context.Background()
	collectionID := int64(1000)

	catalog := mocks.NewQueryCoordCatalog(suite.T())
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil).Maybe()
	// SaveReplica flattens its variadic replicas argument, so register one
	// expectation per possible call arity (single or batched replica saves).
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil).Maybe()

	nodeMgr := session.NewNodeManager()
	for _, id := range []int64{1, 2} {
		nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   id,
			Address:  "localhost",
			Hostname: "localhost",
		}))
	}
	nextID := int64(200)
	m := meta.NewMeta(func() (int64, error) {
		id := nextID
		nextID++
		return id, nil
	}, catalog, nodeMgr)

	// Two resource groups requesting one query node each; HandleNodeUp fills them.
	rgCfg := func() *rgpb.ResourceGroupConfig {
		return &rgpb.ResourceGroupConfig{
			Requests: &rgpb.ResourceGroupLimit{NodeNum: 1},
			Limits:   &rgpb.ResourceGroupLimit{NodeNum: 1},
		}
	}
	for _, rgName := range []string{"rg1", "rg2"} {
		_, err := m.AddResourceGroup(ctx, rgName, rgCfg())
		suite.NoError(err)
	}
	m.HandleNodeUp(ctx, 1)
	m.HandleNodeUp(ctx, 2)
	for _, rgName := range []string{"rg1", "rg2"} {
		nodes, err := m.GetNodes(ctx, rgName)
		suite.NoError(err)
		suite.Len(nodes, 1, "each resource group should hold exactly one query node")
	}

	// Collection loaded with one replica in rg1 (the pre-scale-up state).
	err := m.PutCollection(ctx, utils.CreateTestCollection(collectionID, 1))
	suite.NoError(err)
	_, err = m.Spawn(ctx, collectionID, map[string]int{"rg1": 1}, nil, commonpb.LoadPriority_LOW)
	suite.NoError(err)
	utils.RecoverReplicaOfCollection(ctx, m, collectionID)

	// The target observer's dependencies are mocked out; it only needs to answer
	// the job's UpdateNextTarget call at the end of Execute.
	targetMgr := meta.NewMockTargetManager(suite.T())
	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, mock.Anything, mock.Anything).Return(map[string]*meta.DmChannel{}).Maybe()
	targetMgr.EXPECT().IsNextTargetExist(mock.Anything, mock.Anything).Return(false).Maybe()
	targetMgr.EXPECT().UpdateCollectionNextTarget(mock.Anything, mock.Anything).Return(nil).Maybe()
	targetMgr.EXPECT().IsCurrentTargetExist(mock.Anything, mock.Anything, mock.Anything).Return(true).Maybe()
	targetMgr.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(int64(0)).Maybe()
	targetObserver := observers.NewTargetObserver(m, targetMgr, meta.NewDistributionManager(nodeMgr), meta.NewMockBroker(suite.T()), session.NewMockCluster(suite.T()), nodeMgr)
	targetObserver.Start()
	defer targetObserver.Stop()

	// Scale up 1 -> 2 across rg1/rg2, mirroring the control-plane flow
	// (needWaitRGReady=true, i.e. freshly scaled-out query nodes).
	req := &querypb.UpdateLoadConfigRequest{
		Base:           &commonpb.MsgBase{MsgID: 1},
		CollectionIDs:  []int64{collectionID},
		ReplicaNumber:  2,
		ResourceGroups: []string{"rg1", "rg2"},
	}
	j := NewUpdateLoadConfigJob(ctx, req, m, targetMgr, targetObserver, nil, proxyutil.NewMockProxyClientManager(suite.T()), false, true)
	suite.NoError(j.Execute())

	replicas := m.GetByCollection(ctx, collectionID)
	suite.Len(replicas, 2)
	byRG := lo.GroupBy(replicas, func(r *meta.Replica) string { return r.GetResourceGroup() })
	for rgName, expectedSQNode := range map[string]int64{"rg1": 101, "rg2": 102} {
		suite.Require().Len(byRG[rgName], 1)
		replica := byRG[rgName][0]
		suite.NotEmpty(replica.GetRWNodes(), "replica in %s should have query nodes assigned", rgName)
		suite.Contains(replica.GetRWSQNodes(), expectedSQNode,
			"replica in %s should have its streaming query node assigned synchronously after scale-up", rgName)
	}
}

func TestUpdateLoadConfigJob(t *testing.T) {
	suite.Run(t, new(UpdateLoadConfigJobSuite))
}
