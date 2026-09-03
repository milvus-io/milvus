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

// setResourceGroupScopedLoad turns the scoped-load configuration on or off for
// the test and restores the default afterwards.
func setResourceGroupScopedLoad(t *testing.T, on bool) {
	t.Helper()
	paramtable.Init()
	key := paramtable.Get().QueryCoordCfg.ResourceGroupScopedLoad.Key
	if on {
		paramtable.Get().Save(key, "true")
	} else {
		paramtable.Get().Save(key, "false")
	}
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

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

func TestCompletePlacementIsOffByDefault(t *testing.T) {
	paramtable.Init()
	assert.False(t, paramtable.Get().QueryCoordCfg.ResourceGroupScopedLoad.GetAsBool())
	expected := map[string]int{"rg_1": 1}
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_1"}, expected, loadedIn(7, "rg_0"))
	assert.Equal(t, map[string]int{"rg_1": 1}, got,
		"a sibling resource group's replica must not be carried over natively")
}

func TestCompletePlacementWhenConfiguredOff(t *testing.T) {
	setResourceGroupScopedLoad(t, false)
	expected := map[string]int{"rg_1": 1}
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_1"}, expected, loadedIn(7, "rg_0"))
	assert.Equal(t, map[string]int{"rg_1": 1}, got)
}

func TestCompletePlacementKeepsOutOfScopeResourceGroups(t *testing.T) {
	setResourceGroupScopedLoad(t, true)
	expected := map[string]int{"rg_1": 1}
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_1"}, expected, loadedIn(7, "rg_0"))
	assert.Equal(t, map[string]int{"rg_0": 1, "rg_1": 1}, got,
		"the resource group the request did not name keeps the replica it holds")
	assert.Equal(t, map[string]int{"rg_1": 1}, expected,
		"the map AssignReplica returned must not be mutated")
}

func TestCompletePlacementDoesNotOverrideNamedResourceGroups(t *testing.T) {
	setResourceGroupScopedLoad(t, true)
	current := loadedIn(7, "rg_0", "rg_1")
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_1"}, map[string]int{"rg_1": 2}, current)
	assert.Equal(t, map[string]int{"rg_0": 1, "rg_1": 2}, got)
}

func TestCompletePlacementOnFirstLoadIsUnchanged(t *testing.T) {
	setResourceGroupScopedLoad(t, true)
	expected := map[string]int{"rg_0": 1}
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_0"}, expected, job.CurrentLoadConfig{})
	assert.Equal(t, map[string]int{"rg_0": 1}, got)
}

func TestCompletePlacementWhenRequestNamesEveryLoadedResourceGroup(t *testing.T) {
	setResourceGroupScopedLoad(t, true)
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_0"}, map[string]int{"rg_0": 1}, loadedIn(7, "rg_0"))
	assert.Equal(t, map[string]int{"rg_0": 1}, got)
}

func TestCompletePlacementKeepsEverySibling(t *testing.T) {
	setResourceGroupScopedLoad(t, true)
	got := completePlacementForOutOfScopeResourceGroups(
		context.Background(), 7, []string{"rg_2"}, map[string]int{"rg_2": 1},
		loadedIn(7, "rg_0", "rg_1"))
	assert.Equal(t, map[string]int{"rg_0": 1, "rg_1": 1, "rg_2": 1}, got)
}

func TestCompletePlacementKeepsSiblingReplicaCount(t *testing.T) {
	setResourceGroupScopedLoad(t, true)
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
	setResourceGroupScopedLoad(t, true)
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
