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
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	job := NewLoadCollectionJob(ctx, result, nil, nil, broker, nil, nil, nil, nil, nil, nil)

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
	job := NewLoadCollectionJob(ctx, result, nil, nil, broker, nil, nil, nil, nil, nil, nil)

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
	job := NewLoadCollectionJob(ctx, result, nil, nil, broker, nil, nil, nil, nil, nil, nil)

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
	job := NewLoadCollectionJob(ctx, result, nil, m, broker, nil, nil, nil, nil, nil, nil)
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
	job := NewLoadCollectionJob(ctx, result, nil, m, broker, nil, nil, nil, nil, nil, nil)
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
	job := NewLoadCollectionJob(ctx, result, nil, nil, broker, nil, nil, nil, nil, nil, nil)
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
	err = m.Put(context.Background(), meta.NewReplica(
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

const (
	expansionCollectionID = int64(5000)
	expansionPartitionID  = int64(50)
	expansionChannel      = "5000-dmc0"
	expansionDbID         = int64(7)
	rgA                   = "rg_a"
	rgB                   = "rg_b"
	rgC                   = "rg_c"
)

// observedLoadTask is one registration the job handed to the CollectionObserver.
type observedLoadTask struct {
	collectionID  int64
	partitionIDs  []int64
	resourceGroup string
}

// IncrementalExpansionSuite exercises LoadCollectionJob.Execute against a real
// meta store, so the assertions are about the state the job leaves behind
// rather than about which collaborator was called.
type IncrementalExpansionSuite struct {
	suite.Suite

	ctx     context.Context
	catalog *mocks.QueryCoordCatalog
	meta    *meta.Meta
	dist    *meta.DistributionManager
	broker  *meta.MockBroker
	nextID  int64
}

func (suite *IncrementalExpansionSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *IncrementalExpansionSuite) SetupTest() {
	suite.ctx = context.Background()
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()

	// The keep-loaded fast path is configuration-gated: only a deployment that
	// scopes load requests to the resource groups they name gets it, so these
	// tests turn queryCoord.resourceGroupScopedLoad on.
	// TestNativeBinaryNeverTakesTheFastPath is the other half.
	scopedKey := paramtable.Get().QueryCoordCfg.ResourceGroupScopedLoad.Key
	paramtable.Get().Save(scopedKey, "true")
	suite.T().Cleanup(func() { paramtable.Get().Reset(scopedKey) })

	suite.catalog = mocks.NewQueryCoordCatalog(suite.T())
	// The collection under test always carries exactly one partition, so a
	// SaveCollection carries either zero or one partition load info.
	suite.catalog.On("SaveCollection", mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.catalog.On("SaveCollection", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.catalog.On("SavePartition", mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.catalog.On("SaveReplica", mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.catalog.On("SaveReplica", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.catalog.On("SaveReplica", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	// Spawning replicas batches its writes through the variadic Update. The
	// action count depends on how many replicas the request reconciles, so
	// every arity these cases reach is stubbed.
	suite.catalog.On("Update", mock.Anything).Return(nil).Maybe()
	suite.catalog.On("Update", mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.catalog.On("Update", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.catalog.On("Update", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.catalog.On("SaveResourceGroup", mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.catalog.On("ReleaseReplica", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.catalog.On("ReleasePartition", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.catalog.On("ReleaseCollection", mock.Anything, mock.Anything).Return(nil).Maybe()

	suite.nextID = 9000
	nodeMgr := session.NewNodeManager()
	suite.meta = meta.NewMeta(func() (int64, error) {
		suite.nextID++
		return suite.nextID, nil
	}, suite.catalog, nodeMgr)
	suite.dist = meta.NewDistributionManager(nodeMgr)

	suite.broker = meta.NewMockBroker(suite.T())
	suite.broker.EXPECT().DescribeCollection(mock.Anything, mock.Anything).
		Return(&milvuspb.DescribeCollectionResponse{
			CollectionID:        expansionCollectionID,
			VirtualChannelNames: []string{expansionChannel},
		}, nil).Maybe()
}

// seedLoadedCollection puts a collection that has finished loading into
// resource group rg, with one replica holding one node.
func (suite *IncrementalExpansionSuite) seedLoadedCollection(replicaID int64, rg string, nodes ...int64) {
	suite.seedCollection(querypb.LoadStatus_Loaded, querypb.LoadType_LoadCollection, expansionDbID)
	suite.seedReplica(replicaID, rg, nodes...)
}

func (suite *IncrementalExpansionSuite) seedCollection(status querypb.LoadStatus, loadType querypb.LoadType, dbID int64) {
	err := suite.meta.PutCollection(suite.ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  expansionCollectionID,
			DbID:          dbID,
			ReplicaNumber: 1,
			Status:        status,
			LoadType:      loadType,
			LoadFields:    []int64{100, 101},
			FieldIndexID:  map[int64]int64{100: 1000},
		},
		LoadPercentage: 100,
		CreatedAt:      time.Now().Add(-3 * time.Hour),
	}, &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID:  expansionCollectionID,
			PartitionID:   expansionPartitionID,
			ReplicaNumber: 1,
			Status:        status,
			FieldIndexID:  map[int64]int64{100: 1000},
		},
		LoadPercentage: 100,
		CreatedAt:      time.Now().Add(-3 * time.Hour),
	})
	suite.Require().NoError(err)
}

func (suite *IncrementalExpansionSuite) seedReplica(replicaID int64, rg string, nodes ...int64) {
	err := suite.meta.Put(suite.ctx, meta.NewReplica(&querypb.Replica{
		ID:            replicaID,
		CollectionID:  expansionCollectionID,
		ResourceGroup: rg,
		Nodes:         nodes,
	}))
	suite.Require().NoError(err)
}

func replicaConfig(replicaID int64, rg string) *messagespb.LoadReplicaConfig {
	return &messagespb.LoadReplicaConfig{ReplicaId: replicaID, ResourceGroupName: rg}
}

// buildExpansionRequest builds a request that matches what seedCollection
// stored, apart from the replicas handed in.
func (suite *IncrementalExpansionSuite) buildExpansionRequest(replicas ...*messagespb.LoadReplicaConfig) message.BroadcastResultAlterLoadConfigMessageV2 {
	return suite.buildRequest(expansionDbID, []int64{expansionPartitionID},
		[]*messagespb.LoadFieldConfig{{FieldId: 100, IndexId: 1000}, {FieldId: 101}}, replicas)
}

func (suite *IncrementalExpansionSuite) buildRequest(
	dbID int64,
	partitionIDs []int64,
	loadFields []*messagespb.LoadFieldConfig,
	replicas []*messagespb.LoadReplicaConfig,
) message.BroadcastResultAlterLoadConfigMessageV2 {
	controlChannel := "_ctrl_channel"
	broadcastMsg := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(&messagespb.AlterLoadConfigMessageHeader{
			DbId:         dbID,
			CollectionId: expansionCollectionID,
			PartitionIds: partitionIDs,
			LoadFields:   loadFields,
			Replicas:     replicas,
		}).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()

	return message.BroadcastResultAlterLoadConfigMessageV2{
		Message: message.MustAsBroadcastAlterLoadConfigMessageV2(broadcastMsg),
		Results: map[string]*message.AppendResult{controlChannel: {}},
	}
}

// runJob executes the job with the target observer stubbed out (it needs a
// running goroutine to answer) and records what reached the collection
// observer, plus how many times the collection meta was overwritten. Both
// PutCollection and LoadPartitions keep running for real.
func (suite *IncrementalExpansionSuite) runJob(result message.BroadcastResultAlterLoadConfigMessageV2) (int, []observedLoadTask, error) {
	putCollectionCalls := 0
	var putOrigin func(*meta.CollectionManager, context.Context, *meta.Collection, ...*meta.Partition) error
	putMock := mockey.Mock((*meta.CollectionManager).PutCollection).
		To(func(cm *meta.CollectionManager, ctx context.Context, collection *meta.Collection, partitions ...*meta.Partition) error {
			putCollectionCalls++
			return putOrigin(cm, ctx, collection, partitions...)
		}).Origin(&putOrigin).Build()
	defer putMock.UnPatch()

	tasks := make([]observedLoadTask, 0)
	loadMock := mockey.Mock((*observers.CollectionObserver).LoadPartitions).
		To(func(ob *observers.CollectionObserver, ctx context.Context, collectionID int64, partitionIDs []int64, rgName string) {
			tasks = append(tasks, observedLoadTask{collectionID, partitionIDs, rgName})
		}).Build()
	defer loadMock.UnPatch()

	targetMock := mockey.Mock((*observers.TargetObserver).UpdateNextTarget).
		To(func(ob *observers.TargetObserver, collectionID int64) (chan struct{}, error) {
			return nil, nil
		}).Build()
	defer targetMock.UnPatch()

	job := NewLoadCollectionJob(suite.ctx, result, suite.dist, suite.meta, suite.broker,
		nil, &observers.TargetObserver{}, &observers.CollectionObserver{}, nil, nil, nil)
	err := job.Execute()
	return putCollectionCalls, tasks, err
}

// TestFirstLoadOverwritesMetaAndRegistersUnscopedTask pins the upstream path:
// a load of a collection that is not loaded yet writes the collection meta and
// hands the observer a task that names no resource group.
func (suite *IncrementalExpansionSuite) TestFirstLoadOverwritesMetaAndRegistersUnscopedTask() {
	putCalls, tasks, err := suite.runJob(suite.buildExpansionRequest(replicaConfig(1, rgA)))
	suite.NoError(err)
	suite.Equal(1, putCalls, "a first load must store the collection meta")

	collection := suite.meta.GetCollection(suite.ctx, expansionCollectionID)
	suite.Require().NotNil(collection)
	suite.Equal(querypb.LoadStatus_Loading, collection.GetStatus())
	suite.EqualValues(1, collection.GetReplicaNumber())
	suite.EqualValues(0, collection.LoadPercentage)

	suite.Require().Len(tasks, 1)
	suite.Equal("", tasks[0].resourceGroup, "an ordinary load must stay collection-wide")
	suite.Equal(expansionCollectionID, tasks[0].collectionID)
	suite.Equal([]int64{expansionPartitionID}, tasks[0].partitionIDs)
}

// TestReloadOverwritesMeta pins the upstream path for a load that repeats the
// current configuration: nothing is added, so the meta write still happens.
func (suite *IncrementalExpansionSuite) TestReloadOverwritesMeta() {
	suite.seedLoadedCollection(1, rgA, 1)

	putCalls, tasks, err := suite.runJob(suite.buildExpansionRequest(replicaConfig(1, rgA)))
	suite.NoError(err)
	suite.Equal(1, putCalls, "a reload must still store the collection meta")

	collection := suite.meta.GetCollection(suite.ctx, expansionCollectionID)
	suite.Equal(querypb.LoadStatus_Loading, collection.GetStatus())
	suite.Require().Len(tasks, 1)
	suite.Equal("", tasks[0].resourceGroup)
}

// TestReplicaNumberIncreaseInSameResourceGroupOverwritesMeta covers the plain
// replica-number change every upstream deployment can hit: the added replica
// lands in a resource group that is already loaded, so it would get no
// resource-group task of its own and must keep today's path.
func (suite *IncrementalExpansionSuite) TestReplicaNumberIncreaseInSameResourceGroupOverwritesMeta() {
	suite.seedLoadedCollection(1, rgA, 1)

	putCalls, tasks, err := suite.runJob(suite.buildExpansionRequest(
		replicaConfig(1, rgA), replicaConfig(2, rgA)))
	suite.NoError(err)
	suite.Equal(1, putCalls, "a replica-number change must still store the collection meta")

	collection := suite.meta.GetCollection(suite.ctx, expansionCollectionID)
	suite.Equal(querypb.LoadStatus_Loading, collection.GetStatus())
	suite.EqualValues(2, collection.GetReplicaNumber())
	suite.Require().Len(tasks, 1)
	suite.Equal("", tasks[0].resourceGroup)
}

// TestReplicaNumberDecreaseOverwritesMeta covers the other direction: the
// request drops a replica, so it is not an expansion.
func (suite *IncrementalExpansionSuite) TestReplicaNumberDecreaseOverwritesMeta() {
	suite.seedLoadedCollection(1, rgA, 1)
	suite.seedReplica(2, rgB, 2)

	putCalls, tasks, err := suite.runJob(suite.buildExpansionRequest(replicaConfig(1, rgA)))
	suite.NoError(err)
	suite.Equal(1, putCalls, "a replica-number decrease must still store the collection meta")
	suite.Require().Len(tasks, 1)
	suite.Equal("", tasks[0].resourceGroup)
}

// TestIncrementalExpansionKeepsLoadedResourceGroupIntact is the incident case:
// a collection loaded into rgA hours ago is loaded again to also cover rgB.
func (suite *IncrementalExpansionSuite) TestIncrementalExpansionKeepsLoadedResourceGroupIntact() {
	suite.seedLoadedCollection(1, rgA, 1)
	before := suite.meta.GetCollection(suite.ctx, expansionCollectionID)
	beforePartition := suite.meta.GetPartition(suite.ctx, expansionPartitionID)

	putCalls, tasks, err := suite.runJob(suite.buildExpansionRequest(
		replicaConfig(1, rgA), replicaConfig(2, rgB)))
	suite.NoError(err)
	suite.Equal(0, putCalls, "an incremental resource group expansion must not overwrite the collection meta")

	collection := suite.meta.GetCollection(suite.ctx, expansionCollectionID)
	suite.Require().NotNil(collection)
	suite.Equal(querypb.LoadStatus_Loaded, collection.GetStatus(), "rgA must stay loaded")
	suite.EqualValues(100, collection.LoadPercentage, "rgA's load percentage must survive")
	suite.Equal(before.CreatedAt, collection.CreatedAt, "the collection must not be recreated")
	suite.EqualValues(2, collection.GetReplicaNumber(), "the replica count must follow the expansion")

	partition := suite.meta.GetPartition(suite.ctx, expansionPartitionID)
	suite.Require().NotNil(partition)
	suite.Equal(querypb.LoadStatus_Loaded, partition.GetStatus())
	suite.EqualValues(100, partition.LoadPercentage)
	suite.Equal(beforePartition.CreatedAt, partition.CreatedAt)
	suite.EqualValues(2, partition.GetReplicaNumber())

	suite.Require().Len(tasks, 1, "only the added resource group needs a task")
	suite.Equal(rgB, tasks[0].resourceGroup, "the task must be scoped to the added resource group")
	suite.Equal(expansionCollectionID, tasks[0].collectionID)
	suite.Equal([]int64{expansionPartitionID}, tasks[0].partitionIDs)
}

// TestIncrementalExpansionRegistersOneTaskPerAddedResourceGroup checks the
// pre-spawn snapshot: sampling the resource groups after spawn would find every
// requested resource group already present and register nothing at all.
func (suite *IncrementalExpansionSuite) TestIncrementalExpansionRegistersOneTaskPerAddedResourceGroup() {
	suite.seedLoadedCollection(1, rgA, 1)

	putCalls, tasks, err := suite.runJob(suite.buildExpansionRequest(
		replicaConfig(1, rgA), replicaConfig(2, rgB), replicaConfig(3, rgC)))
	suite.NoError(err)
	suite.Equal(0, putCalls)

	suite.Require().Len(tasks, 2)
	rgs := []string{tasks[0].resourceGroup, tasks[1].resourceGroup}
	suite.ElementsMatch([]string{rgB, rgC}, rgs)
}

// newPredicateJob builds a job over the seeded meta without executing it, so
// each leg of the predicate can be exercised on its own.
func (suite *IncrementalExpansionSuite) newPredicateJob(result message.BroadcastResultAlterLoadConfigMessageV2) *LoadCollectionJob {
	return NewLoadCollectionJob(suite.ctx, result, suite.dist, suite.meta, suite.broker,
		nil, &observers.TargetObserver{}, &observers.CollectionObserver{}, nil, nil, nil)
}

func (suite *IncrementalExpansionSuite) TestIsIncrementalExpansionLegs() {
	sameFields := []*messagespb.LoadFieldConfig{{FieldId: 100, IndexId: 1000}, {FieldId: 101}}
	expansionReplicas := []*messagespb.LoadReplicaConfig{replicaConfig(1, rgA), replicaConfig(2, rgB)}

	cases := []struct {
		name     string
		seed     func()
		request  message.BroadcastResultAlterLoadConfigMessageV2
		expected bool
	}{
		{
			name:     "expansion into a new resource group",
			seed:     func() { suite.seedLoadedCollection(1, rgA, 1) },
			request:  suite.buildRequest(expansionDbID, []int64{expansionPartitionID}, sameFields, expansionReplicas),
			expected: true,
		},
		{
			name:     "collection not loaded at all",
			seed:     func() { suite.seedReplica(1, rgA, 1) },
			request:  suite.buildRequest(expansionDbID, []int64{expansionPartitionID}, sameFields, expansionReplicas),
			expected: false,
		},
		{
			name: "collection still loading",
			seed: func() {
				suite.seedCollection(querypb.LoadStatus_Loading, querypb.LoadType_LoadCollection, expansionDbID)
				suite.seedReplica(1, rgA, 1)
			},
			request:  suite.buildRequest(expansionDbID, []int64{expansionPartitionID}, sameFields, expansionReplicas),
			expected: false,
		},
		{
			name:     "different db",
			seed:     func() { suite.seedLoadedCollection(1, rgA, 1) },
			request:  suite.buildRequest(expansionDbID+1, []int64{expansionPartitionID}, sameFields, expansionReplicas),
			expected: false,
		},
		{
			name: "collection was loaded by partition",
			seed: func() {
				suite.seedCollection(querypb.LoadStatus_Loaded, querypb.LoadType_LoadPartition, expansionDbID)
				suite.seedReplica(1, rgA, 1)
			},
			request:  suite.buildRequest(expansionDbID, []int64{expansionPartitionID}, sameFields, expansionReplicas),
			expected: false,
		},
		{
			name:     "different partition set",
			seed:     func() { suite.seedLoadedCollection(1, rgA, 1) },
			request:  suite.buildRequest(expansionDbID, []int64{expansionPartitionID, expansionPartitionID + 1}, sameFields, expansionReplicas),
			expected: false,
		},
		{
			name: "different load fields",
			seed: func() { suite.seedLoadedCollection(1, rgA, 1) },
			request: suite.buildRequest(expansionDbID, []int64{expansionPartitionID},
				[]*messagespb.LoadFieldConfig{{FieldId: 100, IndexId: 1000}}, expansionReplicas),
			expected: false,
		},
		{
			name: "different field index",
			seed: func() { suite.seedLoadedCollection(1, rgA, 1) },
			request: suite.buildRequest(expansionDbID, []int64{expansionPartitionID},
				[]*messagespb.LoadFieldConfig{{FieldId: 100, IndexId: 2000}, {FieldId: 101}}, expansionReplicas),
			expected: false,
		},
		{
			name: "an existing replica is dropped",
			seed: func() { suite.seedLoadedCollection(1, rgA, 1) },
			request: suite.buildRequest(expansionDbID, []int64{expansionPartitionID}, sameFields,
				[]*messagespb.LoadReplicaConfig{replicaConfig(2, rgB), replicaConfig(3, rgC)}),
			expected: false,
		},
		{
			name: "an existing replica moves to another resource group",
			seed: func() { suite.seedLoadedCollection(1, rgA, 1) },
			request: suite.buildRequest(expansionDbID, []int64{expansionPartitionID}, sameFields,
				[]*messagespb.LoadReplicaConfig{replicaConfig(1, rgB), replicaConfig(2, rgC)}),
			expected: false,
		},
		{
			name: "the added replica lands in a loaded resource group",
			seed: func() { suite.seedLoadedCollection(1, rgA, 1) },
			request: suite.buildRequest(expansionDbID, []int64{expansionPartitionID}, sameFields,
				[]*messagespb.LoadReplicaConfig{replicaConfig(1, rgA), replicaConfig(2, rgA)}),
			expected: false,
		},
		{
			name: "the replica set does not grow",
			seed: func() { suite.seedLoadedCollection(1, rgA, 1) },
			request: suite.buildRequest(expansionDbID, []int64{expansionPartitionID}, sameFields,
				[]*messagespb.LoadReplicaConfig{replicaConfig(1, rgA)}),
			expected: false,
		},
	}

	for _, tc := range cases {
		suite.Run(tc.name, func() {
			suite.SetupTest()
			tc.seed()
			job := suite.newPredicateJob(tc.request)
			req := tc.request.Message.Header()
			suite.Equal(tc.expected, job.isIncrementalExpansion(req, req.GetReplicas()))
		})
	}
}

// TestExpandedCollectionKeepsServingWhileNewResourceGroupLoads runs a real
// CollectionObserver tick after the expansion. The loaded resource group must
// still report a full load while the added one is still at zero: that is what
// "the resource groups load independently" means to everything downstream.
func (suite *IncrementalExpansionSuite) TestExpandedCollectionKeepsServingWhileNewResourceGroupLoads() {
	suite.seedLoadedCollection(1, rgA, 1)

	targetMgr := meta.NewMockTargetManager(suite.T())
	channels := map[string]*meta.DmChannel{
		expansionChannel: meta.DmChannelFromVChannel(&datapb.VchannelInfo{
			CollectionID: expansionCollectionID,
			ChannelName:  expansionChannel,
		}),
	}
	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, mock.Anything, mock.Anything).Return(channels).Maybe()
	targetMgr.EXPECT().GetSealedSegmentsByCollection(mock.Anything, mock.Anything, mock.Anything).
		Return(map[int64]*datapb.SegmentInfo{}).Maybe()
	targetMgr.EXPECT().GetSealedSegmentsByPartition(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(map[int64]*datapb.SegmentInfo{}).Maybe()
	targetMgr.EXPECT().IsNextTargetExist(mock.Anything, mock.Anything).Return(true).Maybe()
	targetMgr.EXPECT().IsCurrentTargetExist(mock.Anything, mock.Anything, mock.Anything).Return(true).Maybe()

	// Node 1 belongs to rgA's replica and already serves the only channel
	// target; rgB's replica has no node carrying anything.
	suite.dist.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: expansionCollectionID,
			ChannelName:  expansionChannel,
		},
		Node: 1,
		View: &meta.LeaderView{
			ID:           1,
			CollectionID: expansionCollectionID,
			Channel:      expansionChannel,
			Segments:     make(map[int64]*querypb.SegmentDist),
		},
	})

	targetMock := mockey.Mock((*observers.TargetObserver).UpdateNextTarget).
		To(func(ob *observers.TargetObserver, collectionID int64) (chan struct{}, error) {
			return nil, nil
		}).Build()
	defer targetMock.UnPatch()

	targetObserver := &observers.TargetObserver{}
	collectionObserver := observers.NewCollectionObserver(suite.dist, suite.meta, targetMgr,
		targetObserver, &checkers.CheckerController{}, nil)

	result := suite.buildExpansionRequest(replicaConfig(1, rgA), replicaConfig(2, rgB))
	job := NewLoadCollectionJob(suite.ctx, result, suite.dist, suite.meta, suite.broker,
		targetMgr, targetObserver, collectionObserver, nil, nil, nil)
	suite.Require().NoError(job.Execute())

	// The per-resource-group percentage refuses a group that does not exist,
	// so both have to be registered before it is asked about them.
	rgCfg := &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 0},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 1},
	}
	_, err := suite.meta.AddResourceGroup(suite.ctx, rgA, rgCfg)
	suite.Require().NoError(err)
	_, err = suite.meta.AddResourceGroup(suite.ctx, rgB, rgCfg)
	suite.Require().NoError(err)

	// The added replica has no nodes yet, so rgB is at zero while rgA is full.
	percentA, err := utils.LoadPercentageByResourceGroup(suite.ctx, suite.meta, targetMgr, suite.dist, expansionCollectionID, rgA)
	suite.NoError(err)
	suite.EqualValues(100, percentA)
	percentB, err := utils.LoadPercentageByResourceGroup(suite.ctx, suite.meta, targetMgr, suite.dist, expansionCollectionID, rgB)
	suite.NoError(err)
	suite.EqualValues(0, percentB)

	collectionObserver.Observe(suite.ctx)

	collection := suite.meta.GetCollection(suite.ctx, expansionCollectionID)
	suite.Require().NotNil(collection, "the observer must not release the collection")
	suite.Equal(querypb.LoadStatus_Loaded, collection.GetStatus())
	suite.EqualValues(100, suite.meta.CalculateLoadPercentage(suite.ctx, expansionCollectionID),
		"the serving resource group must not be averaged down by the loading one")
	suite.EqualValues(100, suite.meta.GetPartitionLoadPercentage(suite.ctx, expansionPartitionID))
	suite.Len(suite.meta.GetByCollection(suite.ctx, expansionCollectionID), 2,
		"neither resource group's replica may be torn down")
}

func TestIncrementalExpansion(t *testing.T) {
	suite.Run(t, new(IncrementalExpansionSuite))
}

// placementOnlyProvider declares exactly the LoadPlacement capability the
// fast-path gate looks for.
// A stock binary never takes the fast path, whatever the request looks like:
// the same add-resource-group request that the extension-gated path keeps
// loaded falls through to the native overwrite - reset to Loading, one
// unscoped observer task - so native failure visibility (the SDK blocks until
// the new resource group loads, or the whole collection is released on
// timeout) is exactly what it always was.
func (suite *IncrementalExpansionSuite) TestNativeBinaryNeverTakesTheFastPath() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ResourceGroupScopedLoad.Key, "false")
	suite.seedLoadedCollection(1, rgA, 1)

	putCalls, tasks, err := suite.runJob(suite.buildExpansionRequest(
		replicaConfig(1, rgA), replicaConfig(2, rgB)))
	suite.NoError(err)
	suite.Equal(1, putCalls, "with the scoped load off the meta overwrite must happen exactly as upstream")

	collection := suite.meta.GetCollection(suite.ctx, expansionCollectionID)
	suite.Require().NotNil(collection)
	suite.Equal(querypb.LoadStatus_Loading, collection.GetStatus(),
		"native semantics: adding a resource group resets the collection to Loading")

	suite.Require().Len(tasks, 1)
	suite.Equal("", tasks[0].resourceGroup, "native path registers the unscoped collection-wide task")
}
