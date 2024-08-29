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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	dim            = 128
	dbName         = ""
	collectionName = "test_load_collection"
)

type LoadTestSuite struct {
	integration.MiniClusterSuite
}

func (s *LoadTestSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1000")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")

	s.Require().NoError(s.SetupEmbedEtcd())
}

func (s *LoadTestSuite) loadCollection(collectionName string, db string, replica int, rgs []string) {
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

func (s *LoadTestSuite) releaseCollection(db, collectionName string) {
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

func (s *LoadTestSuite) TestLoadWithDatabaseLevelConfig() {
	ctx := context.Background()
	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       1,
		SegmentNum:       3,
		RowNumPerSegment: 2000,
	})

	// prepare resource groups
	rgNum := 3
	rgs := make([]string, 0)
	for i := 0; i < rgNum; i++ {
		rgs = append(rgs, fmt.Sprintf("rg_%d", i))
		s.Cluster.QueryCoord.CreateResourceGroup(ctx, &milvuspb.CreateResourceGroupRequest{
			ResourceGroup: rgs[i],
			Config: &rgpb.ResourceGroupConfig{
				Requests: &rgpb.ResourceGroupLimit{
					NodeNum: 1,
				},
				Limits: &rgpb.ResourceGroupLimit{
					NodeNum: 1,
				},

				TransferFrom: []*rgpb.ResourceGroupTransfer{
					{
						ResourceGroup: meta.DefaultResourceGroupName,
					},
				},
				TransferTo: []*rgpb.ResourceGroupTransfer{
					{
						ResourceGroup: meta.DefaultResourceGroupName,
					},
				},
			},
		})
	}

	resp, err := s.Cluster.QueryCoord.ListResourceGroups(ctx, &milvuspb.ListResourceGroupsRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))
	s.Len(resp.GetResourceGroups(), rgNum+1)

	for i := 1; i < rgNum; i++ {
		s.Cluster.AddQueryNode()
	}

	s.Eventually(func() bool {
		matchCounter := 0
		for _, rg := range rgs {
			resp1, err := s.Cluster.QueryCoord.DescribeResourceGroup(ctx, &querypb.DescribeResourceGroupRequest{
				ResourceGroup: rg,
			})
			s.NoError(err)
			s.True(merr.Ok(resp.GetStatus()))
			if len(resp1.ResourceGroup.Nodes) == 1 {
				matchCounter += 1
			}
		}
		return matchCounter == rgNum
	}, 30*time.Second, time.Second)

	status, err := s.Cluster.Proxy.AlterDatabase(ctx, &milvuspb.AlterDatabaseRequest{
		DbName: "default",
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.DatabaseReplicaNumber,
				Value: "3",
			},
			{
				Key:   common.DatabaseResourceGroups,
				Value: strings.Join(rgs, ","),
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(status))

	resp1, err := s.Cluster.Proxy.DescribeDatabase(ctx, &milvuspb.DescribeDatabaseRequest{
		DbName: "default",
	})
	s.NoError(err)
	s.True(merr.Ok(resp1.Status))
	s.Len(resp1.GetProperties(), 2)

	// load collection without specified replica and rgs
	s.loadCollection(collectionName, dbName, 0, nil)
	resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 3)
	s.releaseCollection(dbName, collectionName)
}

func (s *LoadTestSuite) TestLoadWithPredefineCollectionLevelConfig() {
	ctx := context.Background()

	// prepare resource groups
	rgNum := 3
	rgs := make([]string, 0)
	for i := 0; i < rgNum; i++ {
		rgs = append(rgs, fmt.Sprintf("rg_%d", i))
		s.Cluster.QueryCoord.CreateResourceGroup(ctx, &milvuspb.CreateResourceGroupRequest{
			ResourceGroup: rgs[i],
			Config: &rgpb.ResourceGroupConfig{
				Requests: &rgpb.ResourceGroupLimit{
					NodeNum: 1,
				},
				Limits: &rgpb.ResourceGroupLimit{
					NodeNum: 1,
				},

				TransferFrom: []*rgpb.ResourceGroupTransfer{
					{
						ResourceGroup: meta.DefaultResourceGroupName,
					},
				},
				TransferTo: []*rgpb.ResourceGroupTransfer{
					{
						ResourceGroup: meta.DefaultResourceGroupName,
					},
				},
			},
		})
	}

	resp, err := s.Cluster.QueryCoord.ListResourceGroups(ctx, &milvuspb.ListResourceGroupsRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))
	s.Len(resp.GetResourceGroups(), rgNum+1)

	for i := 1; i < rgNum; i++ {
		s.Cluster.AddQueryNode()
	}

	s.Eventually(func() bool {
		matchCounter := 0
		for _, rg := range rgs {
			resp1, err := s.Cluster.QueryCoord.DescribeResourceGroup(ctx, &querypb.DescribeResourceGroupRequest{
				ResourceGroup: rg,
			})
			s.NoError(err)
			s.True(merr.Ok(resp.GetStatus()))
			if len(resp1.ResourceGroup.Nodes) == 1 {
				matchCounter += 1
			}
		}
		return matchCounter == rgNum
	}, 30*time.Second, time.Second)

	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       1,
		SegmentNum:       3,
		RowNumPerSegment: 2000,
		ReplicaNumber:    3,
		ResourceGroups:   rgs,
	})

	// load collection without specified replica and rgs
	s.loadCollection(collectionName, dbName, 0, nil)
	resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 3)
	s.releaseCollection(dbName, collectionName)
}

func (s *LoadTestSuite) TestLoadWithPredefineDatabaseLevelConfig() {
	ctx := context.Background()

	// prepare resource groups
	rgNum := 3
	rgs := make([]string, 0)
	for i := 0; i < rgNum; i++ {
		rgs = append(rgs, fmt.Sprintf("rg_%d", i))
		s.Cluster.QueryCoord.CreateResourceGroup(ctx, &milvuspb.CreateResourceGroupRequest{
			ResourceGroup: rgs[i],
			Config: &rgpb.ResourceGroupConfig{
				Requests: &rgpb.ResourceGroupLimit{
					NodeNum: 1,
				},
				Limits: &rgpb.ResourceGroupLimit{
					NodeNum: 1,
				},

				TransferFrom: []*rgpb.ResourceGroupTransfer{
					{
						ResourceGroup: meta.DefaultResourceGroupName,
					},
				},
				TransferTo: []*rgpb.ResourceGroupTransfer{
					{
						ResourceGroup: meta.DefaultResourceGroupName,
					},
				},
			},
		})
	}

	resp, err := s.Cluster.QueryCoord.ListResourceGroups(ctx, &milvuspb.ListResourceGroupsRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))
	s.Len(resp.GetResourceGroups(), rgNum+1)

	for i := 1; i < rgNum; i++ {
		s.Cluster.AddQueryNode()
	}

	s.Eventually(func() bool {
		matchCounter := 0
		for _, rg := range rgs {
			resp1, err := s.Cluster.QueryCoord.DescribeResourceGroup(ctx, &querypb.DescribeResourceGroupRequest{
				ResourceGroup: rg,
			})
			s.NoError(err)
			s.True(merr.Ok(resp.GetStatus()))
			if len(resp1.ResourceGroup.Nodes) == 1 {
				matchCounter += 1
			}
		}
		return matchCounter == rgNum
	}, 30*time.Second, time.Second)

	newDbName := "db_load_test_with_db_level_config"
	resp1, err := s.Cluster.Proxy.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: newDbName,
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.DatabaseReplicaNumber,
				Value: "3",
			},
			{
				Key:   common.DatabaseResourceGroups,
				Value: strings.Join(rgs, ","),
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(resp1))

	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           newDbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       1,
		SegmentNum:       3,
		RowNumPerSegment: 2000,
	})

	// load collection without specified replica and rgs
	s.loadCollection(collectionName, newDbName, 0, nil)
	resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         newDbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 3)
	s.releaseCollection(newDbName, collectionName)
}

func (s *LoadTestSuite) TestLoadWithPredefineClusterLevelConfig() {
	ctx := context.Background()

	// prepare resource groups
	rgNum := 3
	rgs := make([]string, 0)
	for i := 0; i < rgNum; i++ {
		rgs = append(rgs, fmt.Sprintf("rg_%d", i))
		s.Cluster.QueryCoord.CreateResourceGroup(ctx, &milvuspb.CreateResourceGroupRequest{
			ResourceGroup: rgs[i],
			Config: &rgpb.ResourceGroupConfig{
				Requests: &rgpb.ResourceGroupLimit{
					NodeNum: 1,
				},
				Limits: &rgpb.ResourceGroupLimit{
					NodeNum: 1,
				},

				TransferFrom: []*rgpb.ResourceGroupTransfer{
					{
						ResourceGroup: meta.DefaultResourceGroupName,
					},
				},
				TransferTo: []*rgpb.ResourceGroupTransfer{
					{
						ResourceGroup: meta.DefaultResourceGroupName,
					},
				},
			},
		})
	}

	resp, err := s.Cluster.QueryCoord.ListResourceGroups(ctx, &milvuspb.ListResourceGroupsRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))
	s.Len(resp.GetResourceGroups(), rgNum+1)

	for i := 1; i < rgNum; i++ {
		s.Cluster.AddQueryNode()
	}

	s.Eventually(func() bool {
		matchCounter := 0
		for _, rg := range rgs {
			resp1, err := s.Cluster.QueryCoord.DescribeResourceGroup(ctx, &querypb.DescribeResourceGroupRequest{
				ResourceGroup: rg,
			})
			s.NoError(err)
			s.True(merr.Ok(resp.GetStatus()))
			if len(resp1.ResourceGroup.Nodes) == 1 {
				matchCounter += 1
			}
		}
		return matchCounter == rgNum
	}, 30*time.Second, time.Second)

	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       1,
		SegmentNum:       3,
		RowNumPerSegment: 2000,
	})
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "3")
	defer paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, strings.Join(rgs, ","))
	defer paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)

	// load collection without specified replica and rgs
	s.loadCollection(collectionName, dbName, 0, nil)
	resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 3)
	s.releaseCollection(dbName, collectionName)
}

func (s *LoadTestSuite) TestAssignReplicaToProxy() {
	ctx := context.Background()

	// prepare resource groups
	rgNum := 4
	rgs := make([]string, 0)
	for i := 0; i < rgNum; i++ {
		rgs = append(rgs, fmt.Sprintf("rg_%d", i))
		s.Cluster.QueryCoord.CreateResourceGroup(ctx, &milvuspb.CreateResourceGroupRequest{
			ResourceGroup: rgs[i],
			Config: &rgpb.ResourceGroupConfig{
				Requests: &rgpb.ResourceGroupLimit{
					NodeNum: 1,
				},
				Limits: &rgpb.ResourceGroupLimit{
					NodeNum: 1,
				},

				TransferFrom: []*rgpb.ResourceGroupTransfer{
					{
						ResourceGroup: meta.DefaultResourceGroupName,
					},
				},
				TransferTo: []*rgpb.ResourceGroupTransfer{
					{
						ResourceGroup: meta.DefaultResourceGroupName,
					},
				},
			},
		})
	}

	resp, err := s.Cluster.QueryCoord.ListResourceGroups(ctx, &milvuspb.ListResourceGroupsRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))
	s.Len(resp.GetResourceGroups(), rgNum+1)

	for i := 1; i < rgNum; i++ {
		s.Cluster.AddQueryNode()
	}

	s.Eventually(func() bool {
		matchCounter := 0
		for _, rg := range rgs {
			resp1, err := s.Cluster.QueryCoord.DescribeResourceGroup(ctx, &querypb.DescribeResourceGroupRequest{
				ResourceGroup: rg,
			})
			s.NoError(err)
			s.True(merr.Ok(resp.GetStatus()))
			if len(resp1.ResourceGroup.Nodes) == 1 {
				matchCounter += 1
			}
		}
		return matchCounter == rgNum
	}, 30*time.Second, time.Second)

	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       1,
		SegmentNum:       3,
		RowNumPerSegment: 2000,
	})

	// load collection without specified replica and rgs
	s.loadCollection(collectionName, dbName, 4, rgs)
	resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 4)
	collectionID := resp2.GetReplicas()[0].CollectionID

	// mock 1 proxy without assign replica
	session1 := sessionutil.NewSessionWithEtcd(ctx, paramtable.Get().EtcdCfg.RootPath.Key, s.Cluster.EtcdCli, sessionutil.WithResueNodeID(false))
	session1.Init(typeutil.ProxyRole, "localhost", false, true)
	session1.Register()
	defer session1.Revoke(time.Second)
	resp3, err := s.Cluster.QueryCoord.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		Base: &commonpb.MsgBase{
			SourceID: session1.GetServerID(),
		},

		CollectionID: collectionID,
	})
	s.NoError(err)
	s.Len(resp3.GetShards(), 1)
	s.Len(resp3.GetShards()[0].NodeIds, 4)

	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.EnableAssignReplicaToProxy.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.EnableAssignReplicaToProxy.Key)
	resp3, err = s.Cluster.QueryCoord.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		Base: &commonpb.MsgBase{
			SourceID: session1.GetServerID(),
		},

		CollectionID: collectionID,
	})
	s.NoError(err)
	s.Len(resp3.GetShards(), 1)
	s.Len(resp3.GetShards()[0].NodeIds, 4)

	// mock 2 proxy without assign replica
	session2 := sessionutil.NewSessionWithEtcd(ctx, paramtable.Get().EtcdCfg.RootPath.Key, s.Cluster.EtcdCli, sessionutil.WithResueNodeID(false))
	session2.Init(typeutil.ProxyRole, "localhost", false, true)
	resp3, err = s.Cluster.QueryCoord.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		Base: &commonpb.MsgBase{
			SourceID: session2.GetServerID(),
		},

		CollectionID: collectionID,
	})
	s.NoError(err)
	s.Len(resp3.GetShards(), 1)
	s.Len(resp3.GetShards()[0].NodeIds, 2)

	// mock 3 proxy without assign replica
	session3 := sessionutil.NewSessionWithEtcd(ctx, paramtable.Get().EtcdCfg.RootPath.Key, s.Cluster.EtcdCli, sessionutil.WithResueNodeID(false))
	session3.Init(typeutil.ProxyRole, "localhost", false, true)
	resp3, err = s.Cluster.QueryCoord.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		Base: &commonpb.MsgBase{
			SourceID: session3.GetServerID(),
		},

		CollectionID: collectionID,
	})
	s.NoError(err)
	s.Len(resp3.GetShards(), 1)
	s.Len(resp3.GetShards()[0].NodeIds, 4)

	// mock 4 proxy without assign replica
	session4 := sessionutil.NewSessionWithEtcd(ctx, paramtable.Get().EtcdCfg.RootPath.Key, s.Cluster.EtcdCli, sessionutil.WithResueNodeID(false))
	session4.Init(typeutil.ProxyRole, "localhost", false, true)
	resp3, err = s.Cluster.QueryCoord.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
		Base: &commonpb.MsgBase{
			SourceID: session4.GetServerID(),
		},

		CollectionID: collectionID,
	})
	s.NoError(err)
	s.Len(resp3.GetShards(), 1)
	s.Len(resp3.GetShards()[0].NodeIds, 1)

	s.releaseCollection(dbName, collectionName)
}

func TestReplicas(t *testing.T) {
	suite.Run(t, new(LoadTestSuite))
}
