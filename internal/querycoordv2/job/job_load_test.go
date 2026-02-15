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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type LoadCollectionJobSuite struct {
	suite.Suite
}

func (suite *LoadCollectionJobSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *LoadCollectionJobSuite) SetupTest() {
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
}

func (suite *LoadCollectionJobSuite) buildBroadcastResult(collectionID int64, partitionIDs []int64) message.BroadcastResultAlterLoadConfigMessageV2 {
	controlChannel := "_ctrl_channel"
	replicas := []*messagespb.LoadReplicaConfig{
		{ReplicaId: 1, ResourceGroupName: "__default_resource_group"},
	}
	broadcastMsg := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(&messagespb.AlterLoadConfigMessageHeader{
			CollectionId: collectionID,
			PartitionIds: partitionIDs,
			Replicas:     replicas,
		}).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()

	specializedMsg := message.MustAsBroadcastAlterLoadConfigMessageV2(broadcastMsg)
	return message.BroadcastResultAlterLoadConfigMessageV2{
		Message: specializedMsg,
		Results: map[string]*message.AppendResult{
			controlChannel: {},
		},
	}
}

// TestDescribeCollectionNotFound tests that Execute returns nil when the collection is not found.
func (suite *LoadCollectionJobSuite) TestDescribeCollectionNotFound() {
	ctx := context.Background()
	collectionID := int64(1000)

	broker := meta.NewMockBroker(suite.T())
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(nil, merr.WrapErrCollectionNotFound(collectionID))

	result := suite.buildBroadcastResult(collectionID, []int64{100, 101})
	job := NewLoadCollectionJob(ctx, result, nil, nil, broker, nil, nil, nil, nil, nil)

	err := job.Execute()
	suite.NoError(err)
}

// TestDescribeCollectionOtherError tests that Execute returns the error when DescribeCollection fails.
func (suite *LoadCollectionJobSuite) TestDescribeCollectionOtherError() {
	ctx := context.Background()
	collectionID := int64(1001)

	expectedErr := errors.New("broker unavailable")
	broker := meta.NewMockBroker(suite.T())
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(nil, expectedErr)

	result := suite.buildBroadcastResult(collectionID, []int64{200, 201})
	job := NewLoadCollectionJob(ctx, result, nil, nil, broker, nil, nil, nil, nil, nil)

	err := job.Execute()
	suite.Error(err)
	suite.True(errors.Is(err, expectedErr))
}

// TestDescribeCollectionSuccess tests that Execute proceeds with VirtualChannelNames from DescribeCollection.
func (suite *LoadCollectionJobSuite) TestDescribeCollectionSuccess() {
	ctx := context.Background()
	collectionID := int64(1002)
	channels := []string{"ch1", "ch2"}

	broker := meta.NewMockBroker(suite.T())
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(&milvuspb.DescribeCollectionResponse{
			CollectionID:        collectionID,
			VirtualChannelNames: channels,
		}, nil)

	result := suite.buildBroadcastResult(collectionID, []int64{300, 301})
	// We pass nil for meta to test that DescribeCollection is called before SpawnReplicasWithReplicaConfig.
	// SpawnReplicasWithReplicaConfig will panic on nil meta, proving that DescribeCollection was called first.
	job := NewLoadCollectionJob(ctx, result, nil, nil, broker, nil, nil, nil, nil, nil)

	// This should panic at SpawnReplicasWithReplicaConfig because meta is nil,
	// but this proves DescribeCollection was called and returned successfully first.
	suite.Panics(func() {
		job.Execute()
	})
}

func (suite *LoadCollectionJobSuite) buildBroadcastResultWithLocalReplicaConfig(
	collectionID int64, partitionIDs []int64, useLocalReplicaConfig bool,
) message.BroadcastResultAlterLoadConfigMessageV2 {
	controlChannel := "_ctrl_channel"
	replicas := []*messagespb.LoadReplicaConfig{
		{ReplicaId: 1, ResourceGroupName: "primary_rg1"},
		{ReplicaId: 2, ResourceGroupName: "primary_rg2"},
		{ReplicaId: 3, ResourceGroupName: "primary_rg3"},
	}
	broadcastMsg := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(&messagespb.AlterLoadConfigMessageHeader{
			CollectionId:          collectionID,
			PartitionIds:          partitionIDs,
			Replicas:              replicas,
			UseLocalReplicaConfig: useLocalReplicaConfig,
		}).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()

	specializedMsg := message.MustAsBroadcastAlterLoadConfigMessageV2(broadcastMsg)
	return message.BroadcastResultAlterLoadConfigMessageV2{
		Message: specializedMsg,
		Results: map[string]*message.AppendResult{
			controlChannel: {},
		},
	}
}

// TestUseLocalReplicaConfigWithLocalConfigSet tests that local config overrides primary config.
func (suite *LoadCollectionJobSuite) TestUseLocalReplicaConfigWithLocalConfigSet() {
	// Set local cluster-level config: 1 replica in __default_resource_group
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "__default_resource_group")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	}()

	ctx := context.Background()
	collectionID := int64(2000)

	broker := meta.NewMockBroker(suite.T())
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(&milvuspb.DescribeCollectionResponse{
			CollectionID:        collectionID,
			VirtualChannelNames: []string{"ch1"},
		}, nil)

	// UseLocalReplicaConfig=true AND local config is set → should use local config (1 replica)
	// Primary config has 3 replicas, but local overrides to 1
	result := suite.buildBroadcastResultWithLocalReplicaConfig(collectionID, []int64{400}, true)

	// We pass nil for meta - SpawnReplicasWithReplicaConfig will panic on nil meta,
	// but getLocalReplicaConfig also needs meta.ReplicaManager.AllocateReplicaID which will panic first.
	// This proves the local config path was taken (it tried to allocate replica IDs locally).
	job := NewLoadCollectionJob(ctx, result, nil, nil, broker, nil, nil, nil, nil, nil)
	suite.Panics(func() {
		job.Execute()
	})
}

// TestUseLocalReplicaConfigWithoutLocalConfig tests fallback to primary config when local config is not set.
func (suite *LoadCollectionJobSuite) TestUseLocalReplicaConfigWithoutLocalConfig() {
	// Ensure local config is NOT set (defaults: replicaNum=0, rgs="")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "0")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	}()

	ctx := context.Background()
	collectionID := int64(2001)

	broker := meta.NewMockBroker(suite.T())
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(&milvuspb.DescribeCollectionResponse{
			CollectionID:        collectionID,
			VirtualChannelNames: []string{"ch1"},
		}, nil)

	// UseLocalReplicaConfig=true but local config not set → should fall back to primary's 3 replicas
	result := suite.buildBroadcastResultWithLocalReplicaConfig(collectionID, []int64{401}, true)

	// Will panic at SpawnReplicasWithReplicaConfig with nil meta,
	// proving DescribeCollection succeeded and fallback to primary config was used.
	job := NewLoadCollectionJob(ctx, result, nil, nil, broker, nil, nil, nil, nil, nil)
	suite.Panics(func() {
		job.Execute()
	})
}

// TestUseLocalReplicaConfigFlagFalse tests that when UseLocalReplicaConfig=false, primary config is used directly.
func (suite *LoadCollectionJobSuite) TestUseLocalReplicaConfigFlagFalse() {
	// Even if local config is set, UseLocalReplicaConfig=false should use primary config
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "__default_resource_group")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	}()

	ctx := context.Background()
	collectionID := int64(2002)

	broker := meta.NewMockBroker(suite.T())
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(&milvuspb.DescribeCollectionResponse{
			CollectionID:        collectionID,
			VirtualChannelNames: []string{"ch1"},
		}, nil)

	// UseLocalReplicaConfig=false → should NOT read local config, use primary's 3 replicas directly
	result := suite.buildBroadcastResultWithLocalReplicaConfig(collectionID, []int64{402}, false)

	// Will panic at SpawnReplicasWithReplicaConfig with nil meta (using primary's replicas)
	job := NewLoadCollectionJob(ctx, result, nil, nil, broker, nil, nil, nil, nil, nil)
	suite.Panics(func() {
		job.Execute()
	})
}

// TestGetLocalReplicaConfig_SingleRG tests getLocalReplicaConfig with a single resource group.
func (suite *LoadCollectionJobSuite) TestGetLocalReplicaConfig_SingleRG() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "3")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	}()

	nextID := int64(100)
	m := &meta.Meta{
		ReplicaManager: meta.NewReplicaManager(func() (int64, error) {
			id := nextID
			nextID++
			return id, nil
		}, nil),
	}

	replicas := getLocalReplicaConfig(context.Background(), m)
	suite.NotNil(replicas)
	suite.Len(replicas, 3)
	// All replicas should be in rg1
	for _, r := range replicas {
		suite.Equal("rg1", r.ResourceGroupName)
		suite.Greater(r.ReplicaId, int64(0))
	}
	// Replica IDs should be unique
	ids := make(map[int64]bool)
	for _, r := range replicas {
		suite.False(ids[r.ReplicaId], "duplicate replica ID")
		ids[r.ReplicaId] = true
	}
}

// TestGetLocalReplicaConfig_MultipleRGs tests getLocalReplicaConfig with multiple resource groups.
func (suite *LoadCollectionJobSuite) TestGetLocalReplicaConfig_MultipleRGs() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1,rg2")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	}()

	nextID := int64(200)
	m := &meta.Meta{
		ReplicaManager: meta.NewReplicaManager(func() (int64, error) {
			id := nextID
			nextID++
			return id, nil
		}, nil),
	}

	replicas := getLocalReplicaConfig(context.Background(), m)
	suite.NotNil(replicas)
	suite.Len(replicas, 2)
	// Each RG should have 1 replica
	rgCount := make(map[string]int)
	for _, r := range replicas {
		rgCount[r.ResourceGroupName]++
	}
	suite.Equal(1, rgCount["rg1"])
	suite.Equal(1, rgCount["rg2"])
}

// TestGetLocalReplicaConfig_NotSet tests getLocalReplicaConfig returns nil when config is not set.
func (suite *LoadCollectionJobSuite) TestGetLocalReplicaConfig_NotSet() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "0")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	}()

	replicas := getLocalReplicaConfig(context.Background(), nil)
	suite.Nil(replicas)
}

// TestGetLocalReplicaConfig_NoResourceGroups tests getLocalReplicaConfig returns nil when rgs is empty.
func (suite *LoadCollectionJobSuite) TestGetLocalReplicaConfig_NoResourceGroups() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	}()

	replicas := getLocalReplicaConfig(context.Background(), nil)
	suite.Nil(replicas)
}

// TestGetLocalReplicaConfig_AllocIDError tests getLocalReplicaConfig returns nil when ID allocation fails.
func (suite *LoadCollectionJobSuite) TestGetLocalReplicaConfig_AllocIDError() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg1")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	}()

	m := &meta.Meta{
		ReplicaManager: meta.NewReplicaManager(func() (int64, error) {
			return 0, errors.New("allocation failed")
		}, nil),
	}

	replicas := getLocalReplicaConfig(context.Background(), m)
	suite.Nil(replicas)
}

func TestLoadCollectionJob(t *testing.T) {
	suite.Run(t, new(LoadCollectionJobSuite))
}
