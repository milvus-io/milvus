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

package ops

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	grpcquerynode "github.com/milvus-io/milvus/internal/distributed/querynode"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	dim            = 128
	dbName         = ""
	collectionName = "test_suspend_node"
)

type SuspendNodeTestSuite struct {
	integration.MiniClusterSuite
}

func (s *SuspendNodeTestSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1000")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")

	s.Require().NoError(s.SetupEmbedEtcd())
}

func (s *SuspendNodeTestSuite) loadCollection(collectionName string, db string, replica int, rgs []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// load
	loadStatus, err := s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         db,
		CollectionName: collectionName,
		ReplicaNumber:  int32(replica),
		ResourceGroups: rgs,
	})
	s.NoError(err)
	s.True(merr.Ok(loadStatus))
	s.WaitForLoadWithDB(ctx, db, collectionName)
}

func (s *SuspendNodeTestSuite) releaseCollection(db, collectionName string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// load
	status, err := s.Cluster.Proxy.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		DbName:         db,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(status))
}

func (s *SuspendNodeTestSuite) TestSuspendNode() {
	ctx := context.Background()
	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       1,
		SegmentNum:       3,
		RowNumPerSegment: 2000,
	})

	qns := make([]*grpcquerynode.Server, 0)
	for i := 1; i < 3; i++ {
		qn := s.Cluster.AddQueryNode()
		qns = append(qns, qn)
	}

	// load collection without specified replica and rgs
	s.loadCollection(collectionName, dbName, 1, nil)
	resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 1)
	defer s.releaseCollection(dbName, collectionName)

	resp3, err := s.Cluster.QueryCoord.SuspendNode(ctx, &querypb.SuspendNodeRequest{
		NodeID: qns[0].GetQueryNode().GetNodeID(),
	})
	s.NoError(err)
	s.True(merr.Ok(resp3))

	// expect suspend node to be removed from resource group
	resp5, err := s.Cluster.QueryCoord.DescribeResourceGroup(ctx, &querypb.DescribeResourceGroupRequest{
		ResourceGroup: meta.DefaultResourceGroupName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp5.GetStatus()))
	s.Equal(2, len(resp5.GetResourceGroup().GetNodes()))

	resp6, err := s.Cluster.QueryCoord.ResumeNode(ctx, &querypb.ResumeNodeRequest{
		NodeID: qns[0].GetQueryNode().GetNodeID(),
	})
	s.NoError(err)
	s.True(merr.Ok(resp6))

	// expect node state to be resume
	resp7, err := s.Cluster.QueryCoord.ListQueryNode(ctx, &querypb.ListQueryNodeRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp7.GetStatus()))
	for _, node := range resp7.GetNodeInfos() {
		if node.GetID() == qns[0].GetQueryNode().GetNodeID() {
			s.Equal(session.NodeStateNormal.String(), node.GetState())
		}
	}

	// expect suspend node to be added to resource group
	resp8, err := s.Cluster.QueryCoord.DescribeResourceGroup(ctx, &querypb.DescribeResourceGroupRequest{
		ResourceGroup: meta.DefaultResourceGroupName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp8.GetStatus()))
	s.Equal(3, len(resp8.GetResourceGroup().GetNodes()))
}

func TestSuspendNode(t *testing.T) {
	suite.Run(t, new(SuspendNodeTestSuite))
}
