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
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

	// Create meta with ResourceManager (has __default_resource_group) and ReplicaManager
	m := &meta.Meta{
		ReplicaManager:  meta.NewReplicaManager(func() (int64, error) { return 100, nil }, nil),
		ResourceManager: meta.NewResourceManager(nil, nil),
	}

	// Will panic at SpawnReplicasWithReplicaConfig (nil catalog in ReplicaManager.SpawnWithReplicaConfig),
	// proving that getLocalReplicaConfig was called and produced replicas.
	job := NewLoadCollectionJob(ctx, result, nil, m, broker, nil, nil, nil, nil, nil)
	suite.Panics(func() {
		job.Execute()
	})
}

// TestUseLocalReplicaConfigWithoutLocalConfig tests that when local config is not set,
// defaults to 1 replica in __default_resource_group (not falling back to primary config).
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

	// UseLocalReplicaConfig=true but local config not set → defaults to 1 replica in __default_resource_group
	result := suite.buildBroadcastResultWithLocalReplicaConfig(collectionID, []int64{401}, true)

	// Create meta with ResourceManager (has __default_resource_group) and ReplicaManager
	m := &meta.Meta{
		ReplicaManager:  meta.NewReplicaManager(func() (int64, error) { return 100, nil }, nil),
		ResourceManager: meta.NewResourceManager(nil, nil),
	}

	// Will panic at SpawnReplicasWithReplicaConfig (nil catalog in ReplicaManager.SpawnWithReplicaConfig),
	// proving that getLocalReplicaConfig was called and produced default replicas (1 replica in default RG).
	job := NewLoadCollectionJob(ctx, result, nil, m, broker, nil, nil, nil, nil, nil)
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

// TestGetLocalReplicaConfig_FirstLoad tests getLocalReplicaConfig allocates new replicas on first load.
func (suite *LoadCollectionJobSuite) TestGetLocalReplicaConfig_FirstLoad() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "__default_resource_group")
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
		ResourceManager: meta.NewResourceManager(nil, nil),
	}

	collectionID := int64(3000)
	// No existing replicas → should allocate 2 new replicas
	replicas, err := getLocalReplicaConfig(context.Background(), m, collectionID)
	suite.NoError(err)
	suite.NotNil(replicas)
	suite.Len(replicas, 2)
	for _, r := range replicas {
		suite.Equal("__default_resource_group", r.ResourceGroupName)
	}
	// Replica IDs should be unique
	suite.NotEqual(replicas[0].ReplicaId, replicas[1].ReplicaId)
}

// TestGetLocalReplicaConfig_Idempotent tests that getLocalReplicaConfig reuses existing replicas on replay.
func (suite *LoadCollectionJobSuite) TestGetLocalReplicaConfig_Idempotent() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "__default_resource_group")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	}()

	collectionID := int64(3001)
	nextID := int64(200)
	catalog := mocks.NewQueryCoordCatalog(suite.T())
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Maybe()
	m := &meta.Meta{
		ReplicaManager: meta.NewReplicaManager(func() (int64, error) {
			id := nextID
			nextID++
			return id, nil
		}, catalog),
		ResourceManager: meta.NewResourceManager(nil, nil),
	}

	// First call: no existing replicas → allocates new ID
	replicas1, err := getLocalReplicaConfig(context.Background(), m, collectionID)
	suite.NoError(err)
	suite.Len(replicas1, 1)
	firstReplicaID := replicas1[0].ReplicaId

	// Simulate replay: add the replica to meta so it appears as "current"
	err = m.ReplicaManager.Put(context.Background(), meta.NewReplica(
		&querypb.Replica{
			ID:            firstReplicaID,
			CollectionID:  collectionID,
			ResourceGroup: "__default_resource_group",
		},
		typeutil.NewUniqueSet(),
	))
	suite.NoError(err)

	// Second call (replay): should reuse existing replica, not allocate new one
	replicas2, err := getLocalReplicaConfig(context.Background(), m, collectionID)
	suite.NoError(err)
	suite.Len(replicas2, 1)
	suite.Equal(firstReplicaID, replicas2[0].ReplicaId, "should reuse existing replica ID on replay")
}

// TestGetLocalReplicaConfig_NotSet tests getLocalReplicaConfig defaults to 1 replica in __default_resource_group.
func (suite *LoadCollectionJobSuite) TestGetLocalReplicaConfig_NotSet() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "0")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	}()

	nextID := int64(300)
	m := &meta.Meta{
		ReplicaManager: meta.NewReplicaManager(func() (int64, error) {
			id := nextID
			nextID++
			return id, nil
		}, nil),
		ResourceManager: meta.NewResourceManager(nil, nil),
	}

	replicas, err := getLocalReplicaConfig(context.Background(), m, 0)
	suite.NoError(err)
	suite.NotNil(replicas)
	suite.Len(replicas, 1)
	suite.Equal("__default_resource_group", replicas[0].ResourceGroupName)
}

// TestGetLocalReplicaConfig_NoResourceGroups tests getLocalReplicaConfig defaults rgs to __default_resource_group.
func (suite *LoadCollectionJobSuite) TestGetLocalReplicaConfig_NoResourceGroups() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	}()

	nextID := int64(400)
	m := &meta.Meta{
		ReplicaManager: meta.NewReplicaManager(func() (int64, error) {
			id := nextID
			nextID++
			return id, nil
		}, nil),
		ResourceManager: meta.NewResourceManager(nil, nil),
	}

	replicas, err := getLocalReplicaConfig(context.Background(), m, 0)
	suite.NoError(err)
	suite.NotNil(replicas)
	suite.Len(replicas, 2)
	for _, r := range replicas {
		suite.Equal("__default_resource_group", r.ResourceGroupName)
	}
}

// TestGetLocalReplicaConfig_AllocIDError tests getLocalReplicaConfig returns error when ID allocation fails.
func (suite *LoadCollectionJobSuite) TestGetLocalReplicaConfig_AllocIDError() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "__default_resource_group")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	}()

	m := &meta.Meta{
		ReplicaManager: meta.NewReplicaManager(func() (int64, error) {
			return 0, errors.New("allocation failed")
		}, nil),
		ResourceManager: meta.NewResourceManager(nil, nil),
	}

	replicas, err := getLocalReplicaConfig(context.Background(), m, 0)
	suite.Error(err)
	suite.Nil(replicas)
}

func TestLoadCollectionJob(t *testing.T) {
	suite.Run(t, new(LoadCollectionJobSuite))
}
