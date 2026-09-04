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

package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func loadedIn(collectionID int64, resourceGroups ...string) job.CurrentLoadConfig {
	replicas := make(map[int64]*meta.Replica, len(resourceGroups))
	for i, rgName := range resourceGroups {
		id := int64(i + 1)
		replicas[id] = meta.NewReplica(&querypb.Replica{
			ID:            id,
			CollectionID:  collectionID,
			ResourceGroup: rgName,
		})
	}
	return job.CurrentLoadConfig{Replicas: replicas}
}

func TestCompletePlacementKeepsOutOfScopeResourceGroups(t *testing.T) {
	paramtable.Init()
	expected := map[string]int{"rg_1": 1}
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_1"}, expected, loadedIn(7, "rg_0"))
	assert.Equal(t, map[string]int{"rg_0": 1, "rg_1": 1}, got,
		"the resource group the request did not name keeps the replica it holds")
	assert.Equal(t, map[string]int{"rg_1": 1}, expected,
		"the map AssignReplica returned must not be mutated")
}

func TestCompletePlacementDoesNotOverrideNamedResourceGroups(t *testing.T) {
	paramtable.Init()
	current := loadedIn(7, "rg_0", "rg_1")
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_1"}, map[string]int{"rg_1": 2}, current)
	assert.Equal(t, map[string]int{"rg_0": 1, "rg_1": 2}, got)
}

func TestCompletePlacementOnFirstLoadIsUnchanged(t *testing.T) {
	paramtable.Init()
	expected := map[string]int{"rg_0": 1}
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_0"}, expected, job.CurrentLoadConfig{})
	assert.Equal(t, map[string]int{"rg_0": 1}, got)
}

func TestCompletePlacementWhenRequestNamesEveryLoadedResourceGroup(t *testing.T) {
	paramtable.Init()
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_0"}, map[string]int{"rg_0": 1}, loadedIn(7, "rg_0"))
	assert.Equal(t, map[string]int{"rg_0": 1}, got)
}

func TestCompletePlacementKeepsEverySibling(t *testing.T) {
	paramtable.Init()
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_2"}, map[string]int{"rg_2": 1},
		loadedIn(7, "rg_0", "rg_1"))
	assert.Equal(t, map[string]int{"rg_0": 1, "rg_1": 1, "rg_2": 1}, got)
}

func TestCompletePlacementKeepsSiblingReplicaCount(t *testing.T) {
	paramtable.Init()
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_1"}, map[string]int{"rg_1": 1},
		loadedIn(7, "rg_0", "rg_0"))
	assert.Equal(t, map[string]int{"rg_0": 2, "rg_1": 1}, got)
}

type stubBroadcaster struct{}

func (stubBroadcaster) Broadcast(context.Context, message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	return nil, nil
}

func (stubBroadcaster) Close() {}

func TestLoadCollectionBroadcastAppliesTheCompletedPlacement(t *testing.T) {
	paramtable.Init()
	const collectionID = int64(7)
	broker := meta.NewMockBroker(t)
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(&milvuspb.DescribeCollectionResponse{CollectionID: collectionID}, nil).Maybe()
	broker.EXPECT().GetPartitions(mock.Anything, collectionID).Return([]int64{1}, nil).Maybe()
	s := &Server{broker: broker}
	var captured *job.AlterLoadConfigRequest
	mockey.PatchConvey("a load naming one resource group carries the other's placement", t, func() {
		mockey.Mock((*Server).startBroadcastWithCollectionIDLock).
			Return(stubBroadcaster{}, nil).Build()
		mockey.Mock(utils.AssignReplica).Return(map[string]int{"rg_1": 1}, nil).Build()
		mockey.Mock((*Server).getCurrentLoadConfig).Return(loadedIn(collectionID, "rg_0")).Build()
		mockey.Mock(job.GenerateAlterLoadConfigMessage).To(
			func(_ context.Context, req *job.AlterLoadConfigRequest) (message.BroadcastMutableMessage, error) {
				captured = req
				return nil, nil
			}).Build()
		err := s.broadcastAlterLoadConfigCollectionV2ForLoadCollection(context.Background(),
			&querypb.LoadCollectionRequest{
				CollectionID:   collectionID,
				ReplicaNumber:  1,
				ResourceGroups: []string{"rg_1"},
			})
		assert.NoError(t, err)
	})
	require.NotNil(t, captured, "the broadcast request must have been built")
	assert.Equal(t, map[string]int{"rg_0": 1, "rg_1": 1},
		captured.Expected.ExpectedReplicaNumber,
		"the load path must broadcast the completed placement, not the one it asked for")
}

func TestCompletePlacementForARequestNamingNoResourceGroup(t *testing.T) {
	paramtable.Init()
	expected := map[string]int{meta.DefaultResourceGroupName: 1}
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, nil, expected, loadedIn(7, "rg_0"))
	assert.Equal(t, map[string]int{meta.DefaultResourceGroupName: 1}, got,
		"a request naming no resource group states the whole placement, so nothing is carried over")
}

func TestCompletePlacementWhenTheDefaultResourceGroupIsNamedExplicitly(t *testing.T) {
	paramtable.Init()
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{meta.DefaultResourceGroupName},
		map[string]int{meta.DefaultResourceGroupName: 1}, loadedIn(7, "rg_0"))
	assert.Equal(t, map[string]int{meta.DefaultResourceGroupName: 1, "rg_0": 1}, got,
		"naming the default resource group explicitly is a scoped request like any other")
}

// The scoping list and the list AssignReplica works from come out of one call,
// and they differ exactly where the defaulting rewrote the request.
func TestLoadReplicaConfigSeparatesTheRequestedGroupsFromTheDefaultedOnes(t *testing.T) {
	paramtable.Init()
	s := &Server{}

	replicaNumber, resourceGroups, scoped, userSpecified, err := s.getLoadReplicaConfigForRequest(
		context.Background(), 1, nil, 7)
	require.NoError(t, err)
	assert.EqualValues(t, 1, replicaNumber)
	assert.Equal(t, []string{meta.DefaultResourceGroupName}, resourceGroups,
		"the defaulted list is what AssignReplica is given")
	assert.Empty(t, scoped, "a request naming no group states the whole placement")
	assert.True(t, userSpecified)

	_, resourceGroups, scoped, _, err = s.getLoadReplicaConfigForRequest(
		context.Background(), 1, []string{"rg_0"}, 7)
	require.NoError(t, err)
	assert.Equal(t, []string{"rg_0"}, resourceGroups)
	assert.Equal(t, []string{"rg_0"}, scoped, "a request naming a group is scoped to it")
}

// A cluster-level force override states the whole placement, whatever the
// request named, and the flag behind it is read once so the two answers cannot
// disagree.
func TestLoadReplicaConfigUnderClusterLevelForceOverride(t *testing.T) {
	paramtable.Init()
	p := paramtable.Get()
	require.NoError(t, p.Save(p.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key, "true"))
	require.NoError(t, p.Save(p.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "2"))
	require.NoError(t, p.Save(p.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg_0"))
	t.Cleanup(func() {
		p.Reset(p.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key)
		p.Reset(p.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		p.Reset(p.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	})
	s := &Server{}

	replicaNumber, resourceGroups, scoped, userSpecified, err := s.getLoadReplicaConfigForRequest(
		context.Background(), 1, []string{"rg_1"}, 7)
	require.NoError(t, err)
	assert.EqualValues(t, 2, replicaNumber)
	assert.Equal(t, []string{"rg_0"}, resourceGroups)
	assert.Empty(t, scoped)
	assert.False(t, userSpecified)
}

// TestLoadCollectionBroadcastOfABareRequestKeepsThePlacementItAsksFor is the
// LoadCollection half of the reviewer's scenario: a load naming no resource
// group must not be read as a load scoped to the default resource group, or
// the replicas the collection already has elsewhere are carried through and
// the collection ends up with one more replica than anybody asked for.
func TestLoadCollectionBroadcastOfABareRequestKeepsThePlacementItAsksFor(t *testing.T) {
	paramtable.Init()
	const collectionID = int64(8)
	broker := meta.NewMockBroker(t)
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(&milvuspb.DescribeCollectionResponse{CollectionID: collectionID}, nil).Maybe()
	broker.EXPECT().GetPartitions(mock.Anything, collectionID).Return([]int64{1}, nil).Maybe()
	s := &Server{broker: broker}
	var captured *job.AlterLoadConfigRequest
	mockey.PatchConvey("a load naming no resource group states the whole placement", t, func() {
		mockey.Mock((*Server).startBroadcastWithCollectionIDLock).
			Return(stubBroadcaster{}, nil).Build()
		mockey.Mock(utils.AssignReplica).
			Return(map[string]int{meta.DefaultResourceGroupName: 1}, nil).Build()
		mockey.Mock((*Server).getCurrentLoadConfig).Return(loadedIn(collectionID, "rg_a")).Build()
		mockey.Mock(job.GenerateAlterLoadConfigMessage).To(
			func(_ context.Context, req *job.AlterLoadConfigRequest) (message.BroadcastMutableMessage, error) {
				captured = req
				return nil, nil
			}).Build()
		err := s.broadcastAlterLoadConfigCollectionV2ForLoadCollection(context.Background(),
			&querypb.LoadCollectionRequest{
				CollectionID:  collectionID,
				ReplicaNumber: 1,
			})
		assert.NoError(t, err)
	})
	require.NotNil(t, captured, "the broadcast request must have been built")
	assert.Equal(t, map[string]int{meta.DefaultResourceGroupName: 1},
		captured.Expected.ExpectedReplicaNumber,
		"a bare load must not carry the replicas of a resource group it never named")
}

// TestLoadPartitionsBroadcastOfABareRequestIsNotRefused is the LoadPartitions
// half: reading a bare request as scoped to the default resource group makes
// the expected placement one replica larger than the collection has, and
// CheckIfLoadPartitionsExecutable refuses the load for "changing the replica
// number" of a resource group the caller never mentioned.
func TestLoadPartitionsBroadcastOfABareRequestIsNotRefused(t *testing.T) {
	paramtable.Init()
	const collectionID = int64(9)
	broker := meta.NewMockBroker(t)
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(&milvuspb.DescribeCollectionResponse{CollectionID: collectionID}, nil).Maybe()
	s := &Server{broker: broker}
	current := loadedIn(collectionID, "rg_a")
	current.Collection = &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: collectionID, ReplicaNumber: 1},
	}
	var captured *job.AlterLoadConfigRequest
	mockey.PatchConvey("a bare load_partitions on a collection loaded elsewhere", t, func() {
		mockey.Mock((*Server).startBroadcastWithCollectionIDLock).
			Return(stubBroadcaster{}, nil).Build()
		mockey.Mock(utils.AssignReplica).
			Return(map[string]int{meta.DefaultResourceGroupName: 1}, nil).Build()
		mockey.Mock((*Server).getCurrentLoadConfig).Return(current).Build()
		mockey.Mock(job.GenerateAlterLoadConfigMessage).To(
			func(_ context.Context, req *job.AlterLoadConfigRequest) (message.BroadcastMutableMessage, error) {
				captured = req
				return nil, nil
			}).Build()
		err := s.broadcastAlterLoadConfigCollectionV2ForLoadPartitions(context.Background(),
			&querypb.LoadPartitionsRequest{
				CollectionID:  collectionID,
				PartitionIDs:  []int64{1},
				ReplicaNumber: 1,
			})
		assert.NoError(t, err, "a bare load_partitions must not be refused for a replica number it never asked to change")
	})
	require.NotNil(t, captured, "the broadcast request must have been built")
	assert.Equal(t, map[string]int{meta.DefaultResourceGroupName: 1},
		captured.Expected.ExpectedReplicaNumber)
}

// TestLoadPartitionsBroadcastOfAScopedRequestCarriesTheSiblings pins the other
// direction on the same callback: a request that does name a resource group is
// scoped, and the groups it did not name keep their replicas.
func TestLoadPartitionsBroadcastOfAScopedRequestCarriesTheSiblings(t *testing.T) {
	paramtable.Init()
	const collectionID = int64(10)
	broker := meta.NewMockBroker(t)
	broker.EXPECT().DescribeCollection(mock.Anything, collectionID).
		Return(&milvuspb.DescribeCollectionResponse{CollectionID: collectionID}, nil).Maybe()
	s := &Server{broker: broker}
	var captured *job.AlterLoadConfigRequest
	mockey.PatchConvey("a load_partitions naming one resource group carries the other's placement", t, func() {
		mockey.Mock((*Server).startBroadcastWithCollectionIDLock).
			Return(stubBroadcaster{}, nil).Build()
		mockey.Mock(utils.AssignReplica).Return(map[string]int{"rg_b": 1}, nil).Build()
		mockey.Mock((*Server).getCurrentLoadConfig).Return(loadedIn(collectionID, "rg_a")).Build()
		mockey.Mock(job.GenerateAlterLoadConfigMessage).To(
			func(_ context.Context, req *job.AlterLoadConfigRequest) (message.BroadcastMutableMessage, error) {
				captured = req
				return nil, nil
			}).Build()
		err := s.broadcastAlterLoadConfigCollectionV2ForLoadPartitions(context.Background(),
			&querypb.LoadPartitionsRequest{
				CollectionID:   collectionID,
				PartitionIDs:   []int64{1},
				ReplicaNumber:  1,
				ResourceGroups: []string{"rg_b"},
			})
		assert.NoError(t, err)
	})
	require.NotNil(t, captured)
	assert.Equal(t, map[string]int{"rg_a": 1, "rg_b": 1}, captured.Expected.ExpectedReplicaNumber)
}
