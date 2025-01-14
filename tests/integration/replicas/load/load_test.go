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
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	s.Require().NoError(s.SetupEmbedEtcd())

	// setup mini cluster to use embed etcd
	endpoints := etcd.GetEmbedEtcdEndpoints(s.EtcdServer)
	val := strings.Join(endpoints, ",")
	// setup env value to init etcd source
	s.T().Setenv("etcd.endpoints", val)

	for key, value := range integration.DefaultParams() {
		s.T().Setenv(key, value)
	}

	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1000")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.CheckNodeInReplicaInterval.Key, "5")
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

func (s *LoadTestSuite) TestLoadWithPredefineCollectionLevelConfig() {
	ctx := context.Background()

	// prepare resource groups
	rgNum := 5
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
		ResourceGroups:   rgs[:3],
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

	// modify config, increase replica number
	resp3, err := s.Cluster.Proxy.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		CollectionName: collectionName,
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
		resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp2.Status))
		return len(resp2.GetReplicas()) == 5
	}, 30*time.Second, time.Second)

	// modify config, decrease replica number
	resp4, err := s.Cluster.Proxy.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		CollectionName: collectionName,
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
		resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp2.Status))
		return len(resp2.GetReplicas()) == 2
	}, 30*time.Second, time.Second)

	s.releaseCollection(dbName, collectionName)
}

func (s *LoadTestSuite) TestLoadWithPredefineDatabaseLevelConfig() {
	ctx := context.Background()

	// prepare resource groups
	rgNum := 5
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
	resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         newDbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 3)

	// modify config, increase replica number
	resp3, err := s.Cluster.Proxy.AlterDatabase(ctx, &milvuspb.AlterDatabaseRequest{
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
		resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         newDbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp2.Status))
		return len(resp2.GetReplicas()) == 5
	}, 30*time.Second, time.Second)

	// modify config, decrease replica number
	resp4, err := s.Cluster.Proxy.AlterDatabase(ctx, &milvuspb.AlterDatabaseRequest{
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
		resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
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

	// prepare resource groups
	rgNum := 5
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
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, strings.Join(rgs[:3], ","))

	// load collection without specified replica and rgs
	s.loadCollection(collectionName, dbName, 0, nil)
	resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 3)

	paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)

	// modify load config, increase replicas
	endpoints := etcd.GetEmbedEtcdEndpoints(s.EtcdServer)
	configPrefix := path.Join(paramtable.Get().EtcdCfg.RootPath.GetValue(), "config")
	log.Info("endpoints", zap.Strings("endpoints", endpoints), zap.String("configPrefix", configPrefix))
	s.Cluster.EtcdCli.Put(ctx, path.Join(configPrefix, paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key), "5")
	s.Cluster.EtcdCli.Put(ctx, path.Join(configPrefix, paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key), strings.Join(rgs, ","))
	s.Eventually(func() bool {
		resp3, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 5
	}, 30*time.Second, 1*time.Second)

	// modify load config, decrease replicas
	endpoints = etcd.GetEmbedEtcdEndpoints(s.EtcdServer)
	log.Info("endpoints", zap.Strings("endpoints", endpoints), zap.String("configPrefix", configPrefix))
	s.Cluster.EtcdCli.Put(ctx, path.Join(configPrefix, paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key), "2")
	s.Cluster.EtcdCli.Put(ctx, path.Join(configPrefix, paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key), strings.Join(rgs[:2], ","))
	s.Eventually(func() bool {
		resp3, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
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
	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       1,
		SegmentNum:       3,
		RowNumPerSegment: 2000,
	})

	// prepare resource groups
	rgNum := 5
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

	nodesInRG := make(map[string][]int64)
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
				nodesInRG[rg] = []int64{resp1.ResourceGroup.Nodes[0].NodeId}
			}
		}
		return matchCounter == rgNum
	}, 30*time.Second, time.Second)

	// load collection
	s.loadCollection(collectionName, dbName, 5, rgs)
	resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 5)

	// test load collection with dynamic update
	s.loadCollection(collectionName, dbName, 3, rgs[:3])
	s.Eventually(func() bool {
		resp3, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 3
	}, 30*time.Second, 1*time.Second)

	s.Eventually(func() bool {
		segmentNum, channelNum := 0, 0
		for _, qn := range s.Cluster.GetAllQueryNodes() {
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
		resp3, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 2
	}, 30*time.Second, 1*time.Second)

	s.Eventually(func() bool {
		segmentNum, channelNum := 0, 0
		for _, qn := range s.Cluster.GetAllQueryNodes() {
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
		resp3, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
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
		s.Cluster.AddQueryNode()
	}

	// load collection
	s.loadCollection(collectionName, dbName, 5, nil)
	resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 5)

	// test load collection with dynamic update
	s.loadCollection(collectionName, dbName, 3, nil)
	s.Eventually(func() bool {
		resp3, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
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
		resp3, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
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
	resp2, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.True(merr.Ok(resp2.Status))
	s.Len(resp2.GetReplicas(), 1)

	recycleRG := "__recycle_rg"
	s.Cluster.QueryCoord.CreateResourceGroup(ctx, &milvuspb.CreateResourceGroupRequest{
		ResourceGroup: recycleRG,
		Config: &rgpb.ResourceGroupConfig{
			Requests: &rgpb.ResourceGroupLimit{
				NodeNum: 0,
			},
			Limits: &rgpb.ResourceGroupLimit{
				NodeNum: 100,
			},
		},
	})
	// prepare resource groups
	rgNum := 5
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
						ResourceGroup: recycleRG,
					},
				},
				TransferTo: []*rgpb.ResourceGroupTransfer{
					{
						ResourceGroup: recycleRG,
					},
				},
			},
		})
	}

	s.Cluster.QueryCoord.UpdateResourceGroups(ctx, &querypb.UpdateResourceGroupsRequest{
		ResourceGroups: map[string]*rgpb.ResourceGroupConfig{
			meta.DefaultResourceGroupName: {
				Requests: &rgpb.ResourceGroupLimit{
					NodeNum: 1,
				},
				Limits: &rgpb.ResourceGroupLimit{
					NodeNum: 1,
				},
			},
		},
	})

	resp, err := s.Cluster.QueryCoord.ListResourceGroups(ctx, &milvuspb.ListResourceGroupsRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))
	s.Len(resp.GetResourceGroups(), rgNum+2)

	// test load collection with dynamic update
	loadStatus, err := s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  int32(3),
		ResourceGroups: rgs[:3],
	})
	s.NoError(err)
	s.True(merr.Ok(loadStatus))
	s.Eventually(func() bool {
		resp3, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 3
	}, 30*time.Second, 1*time.Second)

	loadStatus, err = s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  int32(2),
		ResourceGroups: rgs[3:],
	})
	s.NoError(err)
	s.True(merr.Ok(loadStatus))
	s.Eventually(func() bool {
		resp3, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 2
	}, 30*time.Second, 1*time.Second)

	// test load collection with dynamic update
	loadStatus, err = s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  int32(5),
		ResourceGroups: rgs,
	})
	s.NoError(err)
	s.True(merr.Ok(loadStatus))
	s.Eventually(func() bool {
		resp3, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 5
	}, 30*time.Second, 1*time.Second)

	// add qn back,  expect each replica has shard leaders
	for i := 0; i < rgNum; i++ {
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

	s.Eventually(func() bool {
		resp3, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		s.Equal(5, len(resp3.GetReplicas()))
		for _, replica := range resp3.GetReplicas() {
			if len(replica.GetNodeIds()) != 1 {
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

	// prepare resource groups
	rgNum := 10
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

	nodesInRG := make(map[string][]int64)
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
				nodesInRG[rg] = []int64{resp1.ResourceGroup.Nodes[0].NodeId}
			}
		}
		return matchCounter == rgNum
	}, 30*time.Second, time.Second)

	// trigger collection loading, and modify collection's load config during loading
	loadStatus, err := s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  1,
		ResourceGroups: rgs[:1],
	})
	s.NoError(err)
	s.True(merr.Ok(loadStatus))
	loadStatus, err = s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  3,
		ResourceGroups: rgs[1:4],
	})
	s.NoError(err)
	s.True(merr.Ok(loadStatus))
	loadStatus, err = s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  5,
		ResourceGroups: rgs[4:9],
	})
	s.NoError(err)
	s.True(merr.Ok(loadStatus))

	s.Eventually(func() bool {
		resp3, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp3.Status))
		return len(resp3.GetReplicas()) == 5
	}, 30*time.Second, 1*time.Second)

	s.Eventually(func() bool {
		segmentNum, channelNum := 0, 0
		for _, qn := range s.Cluster.GetAllQueryNodes() {
			resp, err := qn.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
			s.NoError(err)
			s.True(merr.Ok(resp.Status))
			segmentNum += len(resp.Segments)
			channelNum += len(resp.Channels)
		}
		return segmentNum == 5 && channelNum == 5
	}, 30*time.Second, 1*time.Second)

	s.releaseCollection(dbName, collectionName)
}

func TestReplicas(t *testing.T) {
	suite.Run(t, new(LoadTestSuite))
}
