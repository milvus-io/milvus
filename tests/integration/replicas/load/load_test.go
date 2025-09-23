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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

const (
	dim            = 128
	dbName         = ""
	collectionName = "test_load_collection"
)

type LoadTestSuite struct {
	integration.MiniClusterSuite

	rgs []string
}

func (s *LoadTestSuite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, "16")
	s.WithMilvusConfig(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "100")
	s.WithMilvusConfig(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")
	s.WithMilvusConfig(paramtable.Get().QueryCoordCfg.CheckNodeInReplicaInterval.Key, "1")
	s.WithMilvusConfig(paramtable.Get().StreamingCfg.WALBalancerPolicyMinRebalanceIntervalThreshold.Key, "1ms")

	s.WithOptions(integration.WithDropAllCollectionsWhenTestTearDown())
	s.WithOptions(integration.WithoutResetDeploymentWhenTestTearDown())
	s.MiniClusterSuite.SetupSuite()
	// setup resource group
	s.initResourceGroup()
}

func (s *LoadTestSuite) initResourceGroup() {
	ctx := s.Cluster.GetContext()

	// prepare resource groups
	rgNum := 5
	rgs := make([]string, 0)
	for i := 0; i < rgNum; i++ {
		rgs = append(rgs, fmt.Sprintf("rg_%d", i))
		s.Cluster.MixCoordClient.CreateResourceGroup(ctx, &milvuspb.CreateResourceGroupRequest{
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

	resp, err := s.Cluster.MixCoordClient.ListResourceGroups(ctx, &milvuspb.ListResourceGroupsRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))
	s.Len(resp.GetResourceGroups(), rgNum+1)

	// global 6 qn for every resource group.
	for i := 0; i < rgNum; i++ {
		qn := s.Cluster.AddQueryNode(cluster.WithoutWaitForReady())
		defer qn.MustWaitForReady(s.Cluster.GetContext())
	}

	// because of the sn didn't manage by rg, so we keep global 5 sn.
	for i := 1; i < rgNum; i++ {
		sn := s.Cluster.AddStreamingNode(cluster.WithoutWaitForReady())
		defer sn.MustWaitForReady(s.Cluster.GetContext())
	}

	s.Eventually(func() bool {
		matchCounter := 0
		for _, rg := range rgs {
			resp1, err := s.Cluster.MixCoordClient.DescribeResourceGroup(ctx, &querypb.DescribeResourceGroupRequest{
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
	s.rgs = rgs
}

func (s *LoadTestSuite) loadCollection(collectionName string, db string, replica int, rgs []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// load
	loadStatus, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         db,
		CollectionName: collectionName,
		ReplicaNumber:  int32(replica),
		ResourceGroups: rgs,
	})
	if err = merr.CheckRPCCall(loadStatus, err); err != nil {
		panic(err)
	}
	s.WaitForLoadWithDB(ctx, db, collectionName)
}

func (s *LoadTestSuite) releaseCollection(db, collectionName string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// load
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		DbName:         db,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(status))
}

func (s *LoadTestSuite) TestLoadWithPredefineCollectionLevelConfig() {
	ctx := context.Background()
	rgs := s.rgs
	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       1,
		SegmentNum:       3,
		RowNumPerSegment: 2000,
		ReplicaNumber:    3,
		ResourceGroups:   rgs[:3],
	})

	// load collection without specified replica and rgs
	s.loadCollection(collectionName, dbName, 0, nil)
	resp2, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 3)

	// modify config, increase replica number
	resp3, err := s.Cluster.MilvusClient.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		CollectionName: collectionName,
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.CollectionReplicaNumber,
				Value: "5",
			},
			{
				Key:   common.CollectionResourceGroups,
				Value: strings.Join(rgs, ","),
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(resp3))
	s.Eventually(func() bool {
		resp2, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp2.Status))
		return len(resp2.GetReplicas()) == 5
	}, 30*time.Second, time.Second)

	// modify config, decrease replica number
	resp4, err := s.Cluster.MilvusClient.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		CollectionName: collectionName,
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.CollectionReplicaNumber,
				Value: "2",
			},
			{
				Key:   common.CollectionResourceGroups,
				Value: strings.Join(rgs[:2], ","),
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(resp4))
	s.Eventually(func() bool {
		resp2, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp2.Status))
		log.Info("get replicas", zap.Any("replicas", resp2.GetReplicas()))
		return len(resp2.GetReplicas()) == 2
	}, 30*time.Second, time.Second)

	s.releaseCollection(dbName, collectionName)
}

func (s *LoadTestSuite) TestLoadWithPredefineDatabaseLevelConfig() {
	ctx := context.Background()
	rgs := s.rgs

	newDbName := "db_load_test_with_db_level_config"
	resp1, err := s.Cluster.MilvusClient.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: newDbName,
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.DatabaseReplicaNumber,
				Value: "3",
			},
			{
				Key:   common.DatabaseResourceGroups,
				Value: strings.Join(rgs[:3], ","),
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
	resp2, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         newDbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 3)

	// modify config, increase replica number
	resp3, err := s.Cluster.MilvusClient.AlterDatabase(ctx, &milvuspb.AlterDatabaseRequest{
		DbName: newDbName,
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.DatabaseReplicaNumber,
				Value: "5",
			},
			{
				Key:   common.DatabaseResourceGroups,
				Value: strings.Join(rgs, ","),
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(resp3))
	s.Eventually(func() bool {
		resp2, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         newDbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp2.Status))
		return len(resp2.GetReplicas()) == 5
	}, 30*time.Second, time.Second)

	// modify config, decrease replica number
	resp4, err := s.Cluster.MilvusClient.AlterDatabase(ctx, &milvuspb.AlterDatabaseRequest{
		DbName: newDbName,
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.DatabaseReplicaNumber,
				Value: "2",
			},
			{
				Key:   common.DatabaseResourceGroups,
				Value: strings.Join(rgs[:2], ","),
			},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(resp4))
	s.Eventually(func() bool {
		resp2, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         newDbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp2.Status))
		return len(resp2.GetReplicas()) == 2
	}, 30*time.Second, time.Second)

	s.releaseCollection(newDbName, collectionName)
}

func (s *LoadTestSuite) TestLoadWithPredefineClusterLevelConfig() {
	ctx := context.Background()
	rgs := s.rgs

	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       1,
		SegmentNum:       3,
		RowNumPerSegment: 2000,
	})
	revertGuard := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key:  "3",
		paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key: strings.Join(rgs[:3], ","),
	})
	defer revertGuard()

	// load collection without specified replica and rgs
	s.loadCollection(collectionName, dbName, 0, nil)
	resp2, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 3)

	revertGuard()
	revertGuard = s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key:  "5",
		paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key: strings.Join(rgs, ","),
	})
	// modify load config, increase replicas
	s.Eventually(func() bool {
		resp3, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 5
	}, 30*time.Second, 1*time.Second)

	revertGuard()
	revertGuard = s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key:  "2",
		paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key: strings.Join(rgs[:2], ","),
	})
	defer revertGuard()

	// modify load config, decrease replicas
	s.Eventually(func() bool {
		resp3, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 2
	}, 30*time.Second, 1*time.Second)

	s.releaseCollection(dbName, collectionName)
}

func (s *LoadTestSuite) TestDynamicUpdateLoadConfigs() {
	ctx := context.Background()
	rgs := s.rgs

	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       1,
		SegmentNum:       3,
		RowNumPerSegment: 2000,
	})

	// load collection
	s.loadCollection(collectionName, dbName, 5, rgs)
	resp2, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 5)

	// test load collection with dynamic update
	s.loadCollection(collectionName, dbName, 3, rgs[:3])
	s.Eventually(func() bool {
		resp3, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 3
	}, 30*time.Second, 1*time.Second)

	s.Eventually(func() bool {
		segmentNum, channelNum := 0, 0
		for _, qn := range s.Cluster.GetAllStreamingAndQueryNodesClient() {
			resp, err := qn.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
			s.NoError(err)
			s.True(merr.Ok(resp.Status))
			segmentNum += len(resp.Segments)
			channelNum += len(resp.Channels)
		}
		return segmentNum == 9 && channelNum == 3
	}, 30*time.Second, 1*time.Second)

	s.loadCollection(collectionName, dbName, 2, rgs[3:])
	s.Eventually(func() bool {
		resp3, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 2
	}, 30*time.Second, 1*time.Second)

	s.Eventually(func() bool {
		segmentNum, channelNum := 0, 0
		for _, qn := range s.Cluster.GetAllStreamingAndQueryNodesClient() {
			resp, err := qn.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
			s.NoError(err)
			s.True(merr.Ok(resp.Status))
			segmentNum += len(resp.Segments)
			channelNum += len(resp.Channels)
		}
		return segmentNum == 6 && channelNum == 2
	}, 30*time.Second, 1*time.Second)

	// test load collection with dynamic update
	s.loadCollection(collectionName, dbName, 5, rgs)
	s.Eventually(func() bool {
		resp3, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 5
	}, 30*time.Second, 1*time.Second)

	s.releaseCollection(dbName, collectionName)
}

func (s *LoadTestSuite) TestDynamicUpdateLoadConfigs_WithoutRG() {
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
	for i := 1; i < 5; i++ {
		qn := s.Cluster.AddQueryNode(cluster.WithoutWaitForReady())
		defer qn.Stop(10 * time.Second)
	}
	s.Eventually(func() bool {
		resp, err := s.Cluster.MilvusClient.DescribeResourceGroup(ctx, &milvuspb.DescribeResourceGroupRequest{
			ResourceGroup: "__default_resource_group",
		})
		s.NoError(err)
		s.True(merr.Ok(resp.Status))
		return len(resp.ResourceGroup.Nodes) == 5
	}, 30*time.Second, time.Second)

	// load collection
	s.loadCollection(collectionName, dbName, 5, nil)
	resp2, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 5)

	// test load collection with dynamic update
	s.loadCollection(collectionName, dbName, 3, nil)
	s.Eventually(func() bool {
		resp3, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 3
	}, 30*time.Second, 1*time.Second)

	// test load collection with dynamic update
	s.loadCollection(collectionName, dbName, 5, nil)
	s.Eventually(func() bool {
		resp3, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 5
	}, 30*time.Second, 1*time.Second)

	s.releaseCollection(dbName, collectionName)
}

func (s *LoadTestSuite) TestDynamicUpdateLoadConfigs_WithRGLackOfNode() {
	ctx := context.Background()
	rgs := s.rgs

	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       1,
		SegmentNum:       3,
		RowNumPerSegment: 2000,
	})
	// load collection
	s.loadCollection(collectionName, dbName, 1, nil)
	resp2, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 1)

	// Stop all query nodes to create a lack of node situation
	s.Cluster.StopAllQueryNode()

	// test load collection with dynamic update
	loadStatus, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  int32(3),
		ResourceGroups: rgs[:3],
	})
	s.NoError(err)
	s.True(merr.Ok(loadStatus))
	s.Eventually(func() bool {
		resp3, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 3
	}, 30*time.Second, 1*time.Second)

	loadStatus, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  int32(2),
		ResourceGroups: rgs[3:],
	})
	s.NoError(err)
	s.True(merr.Ok(loadStatus))
	s.Eventually(func() bool {
		resp3, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 2
	}, 30*time.Second, 1*time.Second)

	// test load collection with dynamic update
	loadStatus, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  int32(5),
		ResourceGroups: rgs,
	})
	s.NoError(err)
	s.True(merr.Ok(loadStatus))
	s.Eventually(func() bool {
		resp3, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 5
	}, 30*time.Second, 1*time.Second)

	// add qn back,  expect each replica has shard leaders
	for i := 0; i < len(rgs)+1; i++ {
		s.Cluster.AddQueryNode(cluster.WithoutWaitForReady())
	}

	s.Eventually(func() bool {
		resp3, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		s.Equal(5, len(resp3.GetReplicas()))
		for _, replica := range resp3.GetReplicas() {
			if len(replica.GetNodeIds()) != 2 { // one sn and one qn at least
				log.Info("get replica info", zap.Any("replica", replica))
				return false
			}
		}
		return true
	}, 30*time.Second, 1*time.Second)

	s.releaseCollection(dbName, collectionName)
}

func (s *LoadTestSuite) TestDynamicUpdateLoadConfigs_OnLoadingCollection() {
	ctx := context.Background()
	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       1,
		SegmentNum:       1,
		RowNumPerSegment: 2000,
	})

	rgs := s.rgs
	rgNum := len(s.rgs)

	resp, err := s.Cluster.MixCoordClient.ListResourceGroups(ctx, &milvuspb.ListResourceGroupsRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))
	s.Len(resp.GetResourceGroups(), rgNum+1)

	// trigger collection loading, and modify collection's load config during loading
	loadStatus, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  1,
		ResourceGroups: rgs[:1],
	})
	s.NoError(err)
	s.True(merr.Ok(loadStatus))
	loadStatus, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  3,
		ResourceGroups: rgs[1:4],
	})
	s.NoError(err)
	s.True(merr.Ok(loadStatus))
	loadStatus, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  5,
		ResourceGroups: rgs,
	})
	s.NoError(err)
	s.True(merr.Ok(loadStatus))

	s.Eventually(func() bool {
		resp3, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 5
	}, 30*time.Second, 1*time.Second)

	s.Eventually(func() bool {
		segmentNum, channelNum := 0, 0
		for _, qn := range s.Cluster.GetAllStreamingAndQueryNodesClient() {
			resp, err := qn.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
			s.NoError(err)
			s.True(merr.Ok(resp.Status))
			segmentNum += len(resp.Segments)
			channelNum += len(resp.Channels)
		}
		log.Info("get data distribution", zap.Int("segmentNum", segmentNum), zap.Int("channelNum", channelNum))
		return segmentNum == 5 && channelNum == 5
	}, 30*time.Second, 1*time.Second)

	s.releaseCollection(dbName, collectionName)
}

func (s *LoadTestSuite) TestLoadWithCompact() {
	ctx := context.Background()
	collName := "test_load_with_compact"

	// Create collection with configuration
	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collName,
		ChannelNum:       1,
		SegmentNum:       3,
		RowNumPerSegment: 2000,
	})

	s.releaseCollection(dbName, collName)

	stopInsertCh := make(chan struct{}, 1)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	// Start a goroutine to continuously insert data and trigger compaction
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopInsertCh:
				return
			default:
				s.InsertAndFlush(ctx, dbName, collName, 2000, dim)
				_, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{
					CollectionName: collName,
				})
				s.NoError(err)
				time.Sleep(time.Second)
			}
		}
	}()

	time.Sleep(10 * time.Second)

	// Load the collection while data is being inserted and compacted
	s.loadCollection(collName, dbName, 1, nil)

	// Verify the collection is loaded
	s.Eventually(func() bool {
		resp, err := s.Cluster.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			CollectionNames: []string{collName},
			Type:            milvuspb.ShowType_InMemory,
		})
		s.NoError(err)

		return len(resp.InMemoryPercentages) == 1 && resp.InMemoryPercentages[0] == 100
	}, 30*time.Second, 1*time.Second)

	// Clean up
	close(stopInsertCh)
	wg.Wait()
	s.releaseCollection(dbName, collName)
}

func TestReplicas(t *testing.T) {
	suite.Run(t, new(LoadTestSuite))
}
