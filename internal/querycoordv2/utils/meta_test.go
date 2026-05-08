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

package utils

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	etcdKV "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSpawnReplicasWithRG(t *testing.T) {
	paramtable.Init()
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	kv := etcdKV.NewEtcdKV(cli, config.MetaRootPath.GetValue())

	ctx := context.Background()
	store := querycoord.NewCatalog(kv)
	nodeMgr := session.NewNodeManager()
	m := meta.NewMeta(RandomIncrementIDAllocator(), store, nodeMgr)
	m.AddResourceGroup(ctx, "rg1", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 3},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 3},
	})
	m.AddResourceGroup(ctx, "rg2", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 3},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 3},
	})
	m.AddResourceGroup(ctx, "rg3", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 3},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 3},
	})

	for i := 1; i < 10; i++ {
		nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "localhost",
			Hostname: "localhost",
		}))
		if i%3 == 0 {
			m.HandleNodeUp(ctx, int64(i))
		}
		if i%3 == 1 {
			m.HandleNodeUp(ctx, int64(i))
		}
		if i%3 == 2 {
			m.HandleNodeUp(ctx, int64(i))
		}
	}

	type args struct {
		m              *meta.Meta
		collection     int64
		resourceGroups []string
		replicaNumber  int32
	}

	tests := []struct {
		name           string
		args           args
		wantReplicaNum int
		wantErr        bool
	}{
		{
			name:           "test 3 replica on 1 rg",
			args:           args{m, 1000, []string{"rg1"}, 3},
			wantReplicaNum: 3,
			wantErr:        false,
		},

		{
			name:           "test 3 replica on 2 rg",
			args:           args{m, 1001, []string{"rg1", "rg2"}, 3},
			wantReplicaNum: 0,
			wantErr:        true,
		},

		{
			name:           "test 3 replica on 3 rg",
			args:           args{m, 1002, []string{"rg1", "rg2", "rg3"}, 3},
			wantReplicaNum: 3,
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SpawnReplicasWithRG(ctx, tt.args.m, tt.args.collection, tt.args.resourceGroups, tt.args.replicaNumber, nil, commonpb.LoadPriority_LOW)
			if (err != nil) != tt.wantErr {
				t.Errorf("SpawnReplicasWithRG() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != tt.wantReplicaNum {
				t.Errorf("SpawnReplicasWithRG() = %v, want %d replicas", got, tt.args.replicaNumber)
			}
		})
	}
}

func TestReassignReplicaToRG_ScaleUpTransfersToSmallestRG(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	store := mocks.NewQueryCoordCatalog(t)
	store.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	nodeMgr := session.NewNodeManager()
	m := meta.NewMeta(RandomIncrementIDAllocator(), store, nodeMgr)

	// Setup: 1 replica in __default_resource_group
	m.PutCollection(ctx, CreateTestCollection(100, 1))
	m.Put(ctx, meta.NewReplica(
		&querypb.Replica{
			ID:            10,
			CollectionID:  100,
			Nodes:         []int64{1, 2, 3},
			ResourceGroup: meta.DefaultResourceGroupName,
		},
		typeutil.NewUniqueSet(),
	))

	// Create rg_for_replica_1, rg_for_replica_2, rg_for_replica_3
	for _, rg := range []string{"rg_for_replica_1", "rg_for_replica_2", "rg_for_replica_3"} {
		m.AddResourceGroup(ctx, rg, &rgpb.ResourceGroupConfig{
			Requests: &rgpb.ResourceGroupLimit{NodeNum: 3},
			Limits:   &rgpb.ResourceGroupLimit{NodeNum: 3},
		})
	}

	// Scale up: 1 replica -> 3 replicas on rg1/rg2/rg3
	toSpawn, toTransfer, toRelease, err := ReassignReplicaToRG(
		ctx, m, 100, 3,
		[]string{"rg_for_replica_1", "rg_for_replica_2", "rg_for_replica_3"},
	)
	require.NoError(t, err)
	assert.Empty(t, toRelease, "should not release any replica during scale-up")

	// The old replica (from __default_resource_group) should be transferred to rg_for_replica_1 (lex smallest)
	assert.Contains(t, toTransfer, "rg_for_replica_1", "old replica should be transferred to lex-smallest RG")
	assert.Equal(t, int64(10), toTransfer["rg_for_replica_1"][0].GetID(), "the transferred replica should be the original one")

	// rg_for_replica_2 and rg_for_replica_3 should spawn new replicas
	assert.Equal(t, 1, toSpawn["rg_for_replica_2"], "rg2 should spawn 1 new replica")
	assert.Equal(t, 1, toSpawn["rg_for_replica_3"], "rg3 should spawn 1 new replica")
}

func TestReassignReplicaToRG_ScaleDownPreservesSmallestRG(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	store := mocks.NewQueryCoordCatalog(t)
	store.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	nodeMgr := session.NewNodeManager()
	m := meta.NewMeta(RandomIncrementIDAllocator(), store, nodeMgr)

	// Setup: 3 replicas in rg1/rg2/rg3
	m.PutCollection(ctx, CreateTestCollection(100, 3))
	for i, rg := range []string{"rg_for_replica_1", "rg_for_replica_2", "rg_for_replica_3"} {
		m.AddResourceGroup(ctx, rg, &rgpb.ResourceGroupConfig{
			Requests: &rgpb.ResourceGroupLimit{NodeNum: 3},
			Limits:   &rgpb.ResourceGroupLimit{NodeNum: 3},
		})
		m.Put(ctx, meta.NewReplica(
			&querypb.Replica{
				ID:            int64(10 + i),
				CollectionID:  100,
				Nodes:         []int64{},
				ResourceGroup: rg,
			},
			typeutil.NewUniqueSet(),
		))
	}

	// Scale down: 3 replicas -> 1 replica on __default_resource_group
	_, toTransfer, toRelease, err := ReassignReplicaToRG(
		ctx, m, 100, 1,
		[]string{meta.DefaultResourceGroupName},
	)
	require.NoError(t, err)

	// Should release 2 replicas (from rg3 and rg2, in that order)
	assert.Len(t, toRelease, 2, "should release 2 replicas")
	// The replica from rg_for_replica_1 (lex smallest, ID=10) should be preserved (transferred to __default)
	assert.Contains(t, toTransfer, meta.DefaultResourceGroupName)
	assert.Equal(t, int64(10), toTransfer[meta.DefaultResourceGroupName][0].GetID(),
		"replica from lex-smallest RG should be preserved and transferred to __default")

	// The released replicas should be from rg3 (ID=12) first, then rg2 (ID=11)
	assert.Equal(t, int64(12), toRelease[0], "rg3's replica should be released first")
	assert.Equal(t, int64(11), toRelease[1], "rg2's replica should be released second")
}

func TestAddNodesToCollectionsInRGFailed(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	store := mocks.NewQueryCoordCatalog(t)
	store.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Times(4)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	nodeMgr := session.NewNodeManager()
	m := meta.NewMeta(RandomIncrementIDAllocator(), store, nodeMgr)
	m.AddResourceGroup(ctx, "rg", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 0},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 0},
	})
	m.PutCollection(ctx, CreateTestCollection(1, 2))
	m.PutCollection(ctx, CreateTestCollection(2, 2))
	m.Put(ctx, meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  1,
			Nodes:         []int64{},
			ResourceGroup: "rg",
		},
		typeutil.NewUniqueSet(),
	))

	m.Put(ctx, meta.NewReplica(
		&querypb.Replica{
			ID:            2,
			CollectionID:  1,
			Nodes:         []int64{},
			ResourceGroup: "rg",
		},
		typeutil.NewUniqueSet(),
	))

	m.Put(ctx, meta.NewReplica(
		&querypb.Replica{
			ID:            3,
			CollectionID:  2,
			Nodes:         []int64{},
			ResourceGroup: "rg",
		},
		typeutil.NewUniqueSet(),
	))

	m.Put(ctx, meta.NewReplica(
		&querypb.Replica{
			ID:            4,
			CollectionID:  2,
			Nodes:         []int64{},
			ResourceGroup: "rg",
		},
		typeutil.NewUniqueSet(),
	))

	storeErr := errors.New("store error")
	store.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(storeErr)
	RecoverAllCollection(m)

	assert.Len(t, m.ReplicaManager.Get(ctx, 1).GetNodes(), 0)
	assert.Len(t, m.ReplicaManager.Get(ctx, 2).GetNodes(), 0)
	assert.Len(t, m.ReplicaManager.Get(ctx, 3).GetNodes(), 0)
	assert.Len(t, m.ReplicaManager.Get(ctx, 4).GetNodes(), 0)
}

func TestRecoverReplicaOfCollection_WaitRGReady(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	store := mocks.NewQueryCoordCatalog(t)
	store.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	nodeMgr := session.NewNodeManager()
	m := meta.NewMeta(RandomIncrementIDAllocator(), store, nodeMgr)

	collectionID := int64(1000)
	m.PutCollection(ctx, CreateTestCollection(collectionID, 2))

	// Create rg1 first and fill it completely before creating rg2,
	// so that HandleNodeUp assigns nodes to the correct RG.
	m.AddResourceGroup(ctx, "rg1", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 2},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 2},
	})

	// Add 2 nodes to rg1 (fully ready)
	for i := 1; i <= 2; i++ {
		nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "127.0.0.1",
			Hostname: "localhost",
		}))
		m.HandleNodeUp(ctx, int64(i))
	}
	rg1 := m.GetResourceGroup(ctx, "rg1")
	require.Equal(t, 0, rg1.MissingNumOfNodes(), "rg1 should be fully ready")

	// Now create rg2 (new, nodes not ready yet)
	m.AddResourceGroup(ctx, "rg2", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 2},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 2},
	})

	// Add only 1 node to rg2 (not ready, missing 1 node)
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   3,
		Address:  "127.0.0.1",
		Hostname: "localhost",
	}))
	m.HandleNodeUp(ctx, 3)

	// Verify rg2 is missing nodes
	rg2 := m.GetResourceGroup(ctx, "rg2")
	require.Equal(t, 1, rg2.MissingNumOfNodes(), "rg2 should be missing 1 node")

	// Put existing replica in rg1 (has RW nodes, should recover normally)
	m.Put(ctx, meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  collectionID,
			Nodes:         []int64{1, 2},
			ResourceGroup: "rg1",
		},
	))

	// Spawn a new replica in rg2 with NeedWaitRGReady option
	newReplicas, err := m.Spawn(ctx, collectionID, map[string]int{"rg2": 1}, nil, commonpb.LoadPriority_LOW, meta.WithNeedWaitRGReady())
	require.NoError(t, err)
	require.Len(t, newReplicas, 1)
	newReplicaID := newReplicas[0].GetID()

	// Verify the new replica has the NeedWaitRGReady flag set
	newReplica := m.Get(ctx, newReplicaID)
	require.True(t, newReplica.NeedWaitRGReady(), "newly spawned replica should have NeedWaitRGReady flag")

	// First recovery: rg2 is not ready (missing 1 node), new replica should NOT get nodes
	RecoverReplicaOfCollection(ctx, m, collectionID)

	newReplica = m.Get(ctx, newReplicaID)
	assert.Empty(t, newReplica.GetRWNodes(), "new replica should have no RW nodes while RG is not ready")
	assert.True(t, newReplica.NeedWaitRGReady(), "flag should still be set since no nodes were assigned")

	// Existing replica in rg1 should still have its nodes
	existingReplica := m.Get(ctx, 1)
	assert.Len(t, existingReplica.GetRWNodes(), 2, "existing replica should keep its nodes")

	// Now add the second node to rg2 (RG becomes ready)
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   4,
		Address:  "127.0.0.1",
		Hostname: "localhost",
	}))
	m.HandleNodeUp(ctx, 4)

	rg2 = m.GetResourceGroup(ctx, "rg2")
	require.Equal(t, 0, rg2.MissingNumOfNodes(), "rg2 should now be fully ready")

	// Second recovery: rg2 is ready, new replica should now get all nodes and flag should be cleared
	RecoverReplicaOfCollection(ctx, m, collectionID)

	newReplica = m.Get(ctx, newReplicaID)
	assert.Len(t, newReplica.GetRWNodes(), 2, "new replica should now have 2 RW nodes")
	assert.False(t, newReplica.NeedWaitRGReady(), "flag should be explicitly cleared after first node assignment")
}

func TestRecoverReplicaOfCollection_ExistingReplicaNotAffectedByMissingNodes(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	store := mocks.NewQueryCoordCatalog(t)
	store.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	nodeMgr := session.NewNodeManager()
	m := meta.NewMeta(RandomIncrementIDAllocator(), store, nodeMgr)

	collectionID := int64(2000)
	m.PutCollection(ctx, CreateTestCollection(collectionID, 1))

	// Create RG with 3 requested nodes but only 2 available (simulates rolling update)
	m.AddResourceGroup(ctx, "rg1", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 3},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 3},
	})

	for i := 1; i <= 2; i++ {
		nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  "127.0.0.1",
			Hostname: "localhost",
		}))
		m.HandleNodeUp(ctx, int64(i))
	}

	// Existing replica (has RW nodes, does NOT have NeedWaitRGReady flag)
	m.Put(ctx, meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  collectionID,
			Nodes:         []int64{1, 2},
			ResourceGroup: "rg1",
		},
	))

	// rg1 is missing 1 node
	rg1 := m.GetResourceGroup(ctx, "rg1")
	require.Equal(t, 1, rg1.MissingNumOfNodes())

	// Recovery should still work for existing replica (no NeedWaitRGReady flag)
	RecoverReplicaOfCollection(ctx, m, collectionID)

	existingReplica := m.Get(ctx, 1)
	assert.Len(t, existingReplica.GetRWNodes(), 2, "existing replica should keep its nodes during rolling update")
}

func TestSpawnWithoutWaitRGReadyOption(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	store := mocks.NewQueryCoordCatalog(t)
	store.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	nodeMgr := session.NewNodeManager()
	m := meta.NewMeta(RandomIncrementIDAllocator(), store, nodeMgr)

	collectionID := int64(3000)
	m.PutCollection(ctx, CreateTestCollection(collectionID, 1))

	m.AddResourceGroup(ctx, "rg1", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 2},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 2},
	})

	// Spawn without WithNeedWaitRGReady — should NOT set the flag
	replicas, err := m.Spawn(ctx, collectionID, map[string]int{"rg1": 1}, nil, commonpb.LoadPriority_LOW)
	require.NoError(t, err)
	require.Len(t, replicas, 1)
	assert.False(t, replicas[0].NeedWaitRGReady(), "spawn without option should not set NeedWaitRGReady")

	// Spawn with WithNeedWaitRGReady — should set the flag
	replicas2, err := m.Spawn(ctx, collectionID, map[string]int{"rg1": 1}, nil, commonpb.LoadPriority_LOW, meta.WithNeedWaitRGReady())
	require.NoError(t, err)
	require.Len(t, replicas2, 1)
	assert.True(t, replicas2[0].NeedWaitRGReady(), "spawn with option should set NeedWaitRGReady")
}

func TestAddNodesToCollectionsInRG(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	store := mocks.NewQueryCoordCatalog(t)
	store.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil)
	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	nodeMgr := session.NewNodeManager()
	m := meta.NewMeta(RandomIncrementIDAllocator(), store, nodeMgr)
	m.AddResourceGroup(ctx, "rg", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 4},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 4},
	})
	m.PutCollection(ctx, CreateTestCollection(1, 2))
	m.PutCollection(ctx, CreateTestCollection(2, 2))
	m.Put(ctx, meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  1,
			Nodes:         []int64{},
			ResourceGroup: "rg",
		},
		typeutil.NewUniqueSet(),
	))

	m.Put(ctx, meta.NewReplica(
		&querypb.Replica{
			ID:            2,
			CollectionID:  1,
			Nodes:         []int64{},
			ResourceGroup: "rg",
		},
		typeutil.NewUniqueSet(),
	))

	m.Put(ctx, meta.NewReplica(
		&querypb.Replica{
			ID:            3,
			CollectionID:  2,
			Nodes:         []int64{},
			ResourceGroup: "rg",
		},
		typeutil.NewUniqueSet(),
	))

	m.Put(ctx, meta.NewReplica(
		&querypb.Replica{
			ID:            4,
			CollectionID:  2,
			Nodes:         []int64{},
			ResourceGroup: "rg",
		},
		typeutil.NewUniqueSet(),
	))
	for i := 1; i < 5; i++ {
		nodeID := int64(i)
		nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   nodeID,
			Address:  "127.0.0.1",
			Hostname: "localhost",
		}))
		m.HandleNodeUp(ctx, nodeID)
	}
	RecoverAllCollection(m)

	assert.Len(t, m.ReplicaManager.Get(ctx, 1).GetNodes(), 2)
	assert.Len(t, m.ReplicaManager.Get(ctx, 2).GetNodes(), 2)
	assert.Len(t, m.ReplicaManager.Get(ctx, 3).GetNodes(), 2)
	assert.Len(t, m.ReplicaManager.Get(ctx, 4).GetNodes(), 2)
}
