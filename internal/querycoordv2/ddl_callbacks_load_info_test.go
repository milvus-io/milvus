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
	"time"

	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (suite *ServiceSuite) TestDDLCallbacksLoadCollectionInfo() {
	ctx := context.Background()
	suite.expectGetRecoverInfoForAllCollections()

	// Test load collection
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
			continue
		}
		// Load with 1 replica
		req := &querypb.LoadCollectionRequest{
			CollectionID: collection,
			// It will be set to 1
			// ReplicaNumber: 1,
		}
		resp, err := suite.server.LoadCollection(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))
		suite.NoError(err)
		suite.assertCollectionLoaded(collection)
	}

	// Test load again
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
			continue
		}
		req := &querypb.LoadCollectionRequest{
			CollectionID: collection,
		}
		resp, err := suite.server.LoadCollection(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))
	}

	// Test load partition while collection exists
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
			continue
		}
		// Load with 1 replica
		req := &querypb.LoadPartitionsRequest{
			CollectionID:  collection,
			PartitionIDs:  suite.partitions[collection],
			ReplicaNumber: 1,
		}
		resp, err := suite.server.LoadPartitions(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))
	}

	// Test load existed collection with different replica number
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
			continue
		}
		req := &querypb.LoadCollectionRequest{
			CollectionID:  collection,
			ReplicaNumber: 3,
		}
		resp, err := suite.server.LoadCollection(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))
	}

	cfg := &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: 0,
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: 0,
		},
	}

	suite.meta.AddResourceGroup(ctx, "rg1", cfg)
	suite.meta.AddResourceGroup(ctx, "rg2", cfg)
	suite.meta.AddResourceGroup(ctx, "rg3", cfg)

	// Load with 3 replica on 1 rg
	req := &querypb.LoadCollectionRequest{
		CollectionID:   1001,
		ReplicaNumber:  3,
		ResourceGroups: []string{"rg1"},
	}
	resp, err := suite.server.LoadCollection(ctx, req)
	suite.Require().ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrResourceGroupNodeNotEnough)

	// Load with 3 replica on 3 rg
	req = &querypb.LoadCollectionRequest{
		CollectionID:   1001,
		ReplicaNumber:  3,
		ResourceGroups: []string{"rg1", "rg2", "rg3"},
	}

	resp, err = suite.server.LoadCollection(ctx, req)
	suite.Require().ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrResourceGroupNodeNotEnough)
}

func (suite *ServiceSuite) TestDDLCallbacksLoadCollectionWithReplicas() {
	ctx := context.Background()
	suite.expectGetRecoverInfoForAllCollections()

	// Test load collection
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
			continue
		}
		// Load with 3 replica
		req := &querypb.LoadCollectionRequest{
			CollectionID:  collection,
			ReplicaNumber: int32(len(suite.nodes) + 1),
		}
		resp, err := suite.server.LoadCollection(ctx, req)
		suite.Require().ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrResourceGroupNodeNotEnough)
	}
}

func (suite *ServiceSuite) TestDDLCallbacksLoadCollectionWithLoadFields() {
	ctx := context.Background()
	suite.expectGetRecoverInfoForAllCollections()

	suite.Run("init_load", func() {
		// Test load collection
		for _, collection := range suite.collections {
			if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
				continue
			}
			// Load with 1 replica
			req := &querypb.LoadCollectionRequest{
				CollectionID: collection,
				LoadFields:   []int64{100, 101, 102},
			}
			resp, err := suite.server.LoadCollection(ctx, req)
			suite.Require().NoError(merr.CheckRPCCall(resp, err))
			suite.assertCollectionLoaded(collection)
		}
	})

	suite.Run("load_again_same_fields", func() {
		for _, collection := range suite.collections {
			if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
				continue
			}
			req := &querypb.LoadCollectionRequest{
				CollectionID: collection,
				LoadFields:   []int64{102, 101, 100}, // field id order shall not matter
			}
			resp, err := suite.server.LoadCollection(ctx, req)
			suite.Require().NoError(merr.CheckRPCCall(resp, err))
		}
	})

	suite.Run("load_again_diff_fields", func() {
		// Test load existed collection with different load fields
		for _, collection := range suite.collections {
			if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
				continue
			}
			req := &querypb.LoadCollectionRequest{
				CollectionID: collection,
				LoadFields:   []int64{100, 101},
			}
			resp, err := suite.server.LoadCollection(ctx, req)
			suite.Require().NoError(merr.CheckRPCCall(resp, err))
		}
	})

	suite.Run("load_from_legacy_proxy", func() {
		// Test load again with legacy proxy
		for _, collection := range suite.collections {
			if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
				continue
			}
			req := &querypb.LoadCollectionRequest{
				CollectionID: collection,
				Schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{FieldID: 100},
						{FieldID: 101},
						{FieldID: 102},
					},
				},
			}
			resp, err := suite.server.LoadCollection(ctx, req)
			suite.Require().NoError(merr.CheckRPCCall(resp, err))
		}
	})
}

func (suite *ServiceSuite) TestDDLCallbacksLoadPartition() {
	ctx := context.Background()
	suite.expectGetRecoverInfoForAllCollections()

	// Test load partition
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}
		// Load with 1 replica
		req := &querypb.LoadPartitionsRequest{
			CollectionID:  collection,
			PartitionIDs:  suite.partitions[collection],
			ReplicaNumber: 1,
		}
		resp, err := suite.server.LoadPartitions(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))
		suite.Len(suite.server.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap()[collection].Replicas, 1)
		suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
		suite.assertQViewsLoadConfigured(collection, 1, suite.partitions[collection])
	}

	// Test load partition again
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}
		// Load with 1 replica
		req := &querypb.LoadPartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
			// ReplicaNumber: 1,
		}
		resp, err := suite.server.LoadPartitions(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))
	}

	// Test load partition with different replica number
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}

		req := &querypb.LoadPartitionsRequest{
			CollectionID:  collection,
			PartitionIDs:  suite.partitions[collection],
			ReplicaNumber: 3,
		}
		resp, err := suite.server.LoadPartitions(ctx, req)
		suite.Require().ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
	}

	// Test load partition with more partition
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}

		req := &querypb.LoadPartitionsRequest{
			CollectionID:  collection,
			PartitionIDs:  append(suite.partitions[collection], 200),
			ReplicaNumber: 1,
		}
		resp, err := suite.server.LoadPartitions(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))
	}

	// Test load collection while partitions exists
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}

		req := &querypb.LoadCollectionRequest{
			CollectionID:  collection,
			ReplicaNumber: 1,
		}
		resp, err := suite.server.LoadCollection(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))
	}

	cfg := &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: 1,
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: 1,
		},
	}
	suite.meta.AddResourceGroup(ctx, "rg1", cfg)
	suite.meta.AddResourceGroup(ctx, "rg2", cfg)
	suite.meta.AddResourceGroup(ctx, "rg3", cfg)

	// test load 3 replica in 1 rg, should pass rg check
	req := &querypb.LoadPartitionsRequest{
		CollectionID:   999,
		PartitionIDs:   []int64{888},
		ReplicaNumber:  3,
		ResourceGroups: []string{"rg1"},
	}
	resp, err := suite.server.LoadPartitions(ctx, req)
	suite.Require().ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrResourceGroupNodeNotEnough)

	// test load 3 replica in 3 rg, should pass rg check
	req = &querypb.LoadPartitionsRequest{
		CollectionID:   999,
		PartitionIDs:   []int64{888},
		ReplicaNumber:  3,
		ResourceGroups: []string{"rg1", "rg2", "rg3"},
	}
	resp, err = suite.server.LoadPartitions(ctx, req)
	suite.Require().ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrResourceGroupNodeNotEnough)
}

func (suite *ServiceSuite) TestLoadPartitionWithLoadFields() {
	ctx := context.Background()
	suite.expectGetRecoverInfoForAllCollections()

	suite.Run("init_load", func() {
		// Test load partition
		for _, collection := range suite.collections {
			if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
				continue
			}
			// Load with 1 replica
			req := &querypb.LoadPartitionsRequest{
				CollectionID:  collection,
				PartitionIDs:  suite.partitions[collection],
				ReplicaNumber: 1,
				LoadFields:    []int64{100, 101, 102},
			}
			resp, err := suite.server.LoadPartitions(ctx, req)
			suite.Require().NoError(merr.CheckRPCCall(resp, err))
			suite.Len(suite.server.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap()[collection].Replicas, 1)
			suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
			suite.assertQViewsLoadConfigured(collection, 1, suite.partitions[collection])
		}
	})

	suite.Run("load_with_same_load_fields", func() {
		for _, collection := range suite.collections {
			if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
				continue
			}
			// Load with 1 replica
			req := &querypb.LoadPartitionsRequest{
				CollectionID:  collection,
				PartitionIDs:  suite.partitions[collection],
				ReplicaNumber: 1,
				LoadFields:    []int64{102, 101, 100},
			}
			resp, err := suite.server.LoadPartitions(ctx, req)
			suite.Require().NoError(merr.CheckRPCCall(resp, err))
		}
	})

	suite.Run("load_with_diff_load_fields", func() {
		// Test load partition with different load fields
		for _, collection := range suite.collections {
			if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
				continue
			}

			req := &querypb.LoadPartitionsRequest{
				CollectionID: collection,
				PartitionIDs: suite.partitions[collection],
				LoadFields:   []int64{100, 101},
			}
			resp, err := suite.server.LoadPartitions(ctx, req)
			suite.Require().NoError(merr.CheckRPCCall(resp, err))
		}
	})

	suite.Run("load_legacy_proxy", func() {
		for _, collection := range suite.collections {
			if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
				continue
			}
			// Load with 1 replica
			req := &querypb.LoadPartitionsRequest{
				CollectionID:  collection,
				PartitionIDs:  suite.partitions[collection],
				ReplicaNumber: 1,
				Schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{FieldID: 100},
						{FieldID: 101},
						{FieldID: 102},
					},
				},
			}
			resp, err := suite.server.LoadPartitions(ctx, req)
			suite.Require().NoError(merr.CheckRPCCall(resp, err))
		}
	})
}

func (suite *ServiceSuite) TestDynamicLoad() {
	ctx := context.Background()
	suite.expectGetRecoverInfoForAllCollections()

	collection := suite.collections[0]
	p0, p1, p2 := suite.partitions[collection][0], suite.partitions[collection][1], suite.partitions[collection][2]
	newLoadPartJob := func(partitions ...int64) *querypb.LoadPartitionsRequest {
		req := &querypb.LoadPartitionsRequest{
			CollectionID:  collection,
			PartitionIDs:  partitions,
			ReplicaNumber: 1,
		}
		return req
	}
	newLoadColJob := func() *querypb.LoadCollectionRequest {
		return &querypb.LoadCollectionRequest{
			CollectionID:  collection,
			ReplicaNumber: 1,
		}
	}

	// loaded: none
	// action: load p0, p1, p2
	// expect: p0, p1, p2 loaded
	req := newLoadPartJob(p0, p1, p2)
	resp, err := suite.server.LoadPartitions(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
	suite.assertPartitionLoaded(ctx, collection, p0, p1, p2)

	// loaded: p0, p1, p2
	// action: load p0, p1, p2
	// expect: do nothing, p0, p1, p2 loaded
	req = newLoadPartJob(p0, p1, p2)
	resp, err = suite.server.LoadPartitions(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.assertPartitionLoaded(ctx, collection, p0, p1, p2)

	// loaded: p0, p1
	// action: load p2
	// expect: p0, p1, p2 loaded
	suite.releaseAll()
	req = newLoadPartJob(p0, p1)
	resp, err = suite.server.LoadPartitions(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
	suite.assertPartitionLoaded(ctx, collection, p0, p1)
	req = newLoadPartJob(p2)
	resp, err = suite.server.LoadPartitions(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
	suite.assertPartitionLoaded(ctx, collection, p0, p1, p2)

	// loaded: p0, p1
	// action: load p1, p2
	// expect: p0, p1, p2 loaded
	suite.releaseAll()
	req = newLoadPartJob(p0, p1)
	resp, err = suite.server.LoadPartitions(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
	suite.assertPartitionLoaded(ctx, collection, p0, p1)
	req = newLoadPartJob(p1, p2)
	resp, err = suite.server.LoadPartitions(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
	suite.assertPartitionLoaded(ctx, collection, p0, p1, p2)

	// loaded: p0, p1
	// action: load col
	// expect: col loaded
	suite.releaseAll()
	req = newLoadPartJob(p0, p1)
	resp, err = suite.server.LoadPartitions(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
	suite.assertPartitionLoaded(ctx, collection, p0, p1)
	colJob := newLoadColJob()
	resp, err = suite.server.LoadCollection(ctx, colJob)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
	suite.assertPartitionLoaded(ctx, collection, p0, p1, p2)
}

func (suite *ServiceSuite) TestLoadPartitionWithReplicas() {
	ctx := context.Background()
	suite.expectGetRecoverInfoForAllCollections()

	// Test load partitions
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}
		// Load with 3 replica
		req := &querypb.LoadPartitionsRequest{
			CollectionID:  collection,
			PartitionIDs:  suite.partitions[collection],
			ReplicaNumber: 11,
		}
		resp, err := suite.server.LoadPartitions(ctx, req)
		suite.Require().ErrorIs(merr.CheckRPCCall(resp, err), merr.ErrResourceGroupNodeNotEnough)
	}
}

func (suite *ServiceSuite) TestDDLCallbacksReleaseCollection() {
	ctx := context.Background()
	suite.expectGetRecoverInfoForAllCollections()
	suite.loadAll()

	// Test release collection and partition
	for _, collection := range suite.collections {
		req := &querypb.ReleaseCollectionRequest{
			CollectionID: collection,
		}
		resp, err := suite.server.ReleaseCollection(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))
		suite.assertCollectionReleased(collection)
	}

	// Test release again
	for _, collection := range suite.collections {
		req := &querypb.ReleaseCollectionRequest{
			CollectionID: collection,
		}
		resp, err := suite.server.ReleaseCollection(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))
		suite.assertCollectionReleased(collection)
	}
}

func (suite *ServiceSuite) mockReadyQueryViews() {
	// This suite checks metadata updates; emulate successful worker handoff.
	// TestReleasePartitionsWaitsForHandoff separately verifies delayed/current
	// version acknowledgements, replay, cancellation and broadcast failure.
	patch := mockey.Mock((*coordview.ShardViewRegistry).SnapshotForShards).To(func(_ *coordview.ShardViewRegistry, shards []qviews.ShardID) *coordview.ShardViewSnapshot {
		stats := make(map[qviews.ShardID]*coordview.ShardStats)
		store := suite.server.qviewsRuntime.loadConfigStore
		for id, cfg := range store.Snapshot().ConfigsMap() {
			for _, replica := range cfg.Replicas {
				for _, shard := range shards {
					if shard.ReplicaID == replica.ReplicaID {
						stats[shard] = &coordview.ShardStats{UpVersion: &qviews.QueryViewVersion{}, UpLoadInfoVersion: store.GetConfigVersion(id)}
					}
				}
			}
		}
		return coordview.NewShardViewSnapshot(1, stats)
	}).Build()
	suite.T().Cleanup(func() { patch.UnPatch() })
}

func (suite *ServiceSuite) TestDDLCallbacksReleasePartition() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	suite.mockReadyQueryViews()
	suite.expectGetRecoverInfoForAllCollections()
	suite.loadAll()

	// Test release partition
	for _, collection := range suite.collections {
		req := &querypb.ReleasePartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
		}
		resp, err := suite.server.ReleasePartitions(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))
		suite.assertPartitionReleased(collection, suite.partitions[collection]...)
	}

	// Test release again
	for _, collection := range suite.collections {
		req := &querypb.ReleasePartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
		}
		resp, err := suite.server.ReleasePartitions(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))
		suite.assertPartitionReleased(collection, suite.partitions[collection]...)
	}

	// Test release partial partitions
	suite.releaseAll()
	suite.loadAll()
	for _, collection := range suite.collections {
		req := &querypb.ReleasePartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection][1:],
		}
		resp, err := suite.server.ReleasePartitions(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))

		suite.assertPartitionLoaded(ctx, collection, suite.partitions[collection][0])
		suite.assertPartitionReleased(collection, suite.partitions[collection][1:]...)
	}
}

func (suite *ServiceSuite) TestDynamicRelease() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	suite.mockReadyQueryViews()
	suite.expectGetRecoverInfoForAllCollections()

	col0, col1 := suite.collections[0], suite.collections[1]
	p0, p1, p2 := suite.partitions[col0][0], suite.partitions[col0][1], suite.partitions[col0][2]
	p3, p4, p5 := suite.partitions[col1][0], suite.partitions[col1][1], suite.partitions[col1][2]
	newReleasePartJob := func(col int64, partitions ...int64) *querypb.ReleasePartitionsRequest {
		return &querypb.ReleasePartitionsRequest{
			CollectionID: col,
			PartitionIDs: partitions,
		}
	}
	newReleaseColJob := func(col int64) *querypb.ReleaseCollectionRequest {
		return &querypb.ReleaseCollectionRequest{
			CollectionID: col,
		}
	}

	// loaded: p0, p1, p2
	// action: release p0
	// expect: p0 released, p1, p2 loaded
	suite.loadAll()

	req := newReleasePartJob(col0, p0)
	resp, err := suite.server.ReleasePartitions(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.assertPartitionReleased(col0, p0)
	suite.assertPartitionLoaded(ctx, col0, p1, p2)

	// loaded: p1, p2
	// action: release p0, p1
	// expect: p1 released, p2 loaded
	req = newReleasePartJob(col0, p0, p1)
	resp, err = suite.server.ReleasePartitions(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.assertPartitionReleased(col0, p0, p1)
	suite.assertPartitionLoaded(ctx, col0, p2)

	// loaded: p2
	// action: release p2
	// expect: loadType=col: col loaded, p2 released, full collection should be released.
	req = newReleasePartJob(col0, p2)
	resp, err = suite.server.ReleasePartitions(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.assertPartitionReleased(col0, p0, p1, p2)
	suite.assertCollectionReleased(col0)

	// loaded: p0, p1, p2
	// action: release col
	// expect: col released
	suite.releaseAll()
	suite.loadAll()

	req2 := newReleaseColJob(col0)
	resp, err = suite.server.ReleaseCollection(ctx, req2)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.assertCollectionReleased(col0)
	suite.assertPartitionReleased(col0, p0, p1, p2)

	// loaded: p3, p4, p5
	// action: release p3, p4, p5
	// expect: loadType=partition: col released
	suite.releaseAll()
	suite.loadAll()

	req = newReleasePartJob(col1, p3, p4, p5)
	resp, err = suite.server.ReleasePartitions(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.assertCollectionReleased(col1)
	suite.assertPartitionReleased(col1, p3, p4, p5)
}

func (suite *ServiceSuite) releaseAll() {
	ctx := context.Background()
	for _, collection := range suite.collections {
		resp, err := suite.server.ReleaseCollection(ctx, &querypb.ReleaseCollectionRequest{
			CollectionID: collection,
		})
		suite.Require().NoError(merr.CheckRPCCall(resp, err))
		suite.assertCollectionReleased(collection)
	}
}

func (suite *ServiceSuite) assertCollectionReleased(collection int64) {
	suite.False(suite.server.qviewsRuntime.loadConfigStore.Contains(collection))
}

func (suite *ServiceSuite) assertPartitionReleased(collection int64, partitionIDs ...int64) {
	cfg := suite.server.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap()[collection]
	if cfg == nil {
		return
	}
	for _, partition := range partitionIDs {
		suite.NotContains(cfg.PartitionIDs, partition)
	}
}

func (suite *ServiceSuite) TestDDLCallbacksLoadCollectionWithUserSpecifiedReplicaMode() {
	ctx := context.Background()
	suite.expectGetRecoverInfoForAllCollections()

	// Test load collection with userSpecifiedReplicaMode = true
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
			continue
		}

		req := &querypb.LoadCollectionRequest{
			CollectionID:  collection,
			ReplicaNumber: 1,
		}
		resp, err := suite.server.LoadCollection(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))

		suite.assertCollectionLoaded(collection)
		cfg := suite.server.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap()[collection]
		suite.True(cfg.UserSpecifiedReplicaMode)
	}
}

func (suite *ServiceSuite) TestDDLCallbacksLoadCollectionForceOverrideUserSpecifiedReplicaMode() {
	ctx := context.Background()
	suite.expectGetRecoverInfoForAllCollections()

	paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1")
	paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, meta.DefaultResourceGroupName)
	paramtable.Get().Save(Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key, "true")
	defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
	defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	defer paramtable.Get().Reset(Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key)

	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
			continue
		}

		req := &querypb.LoadCollectionRequest{
			CollectionID:   collection,
			ReplicaNumber:  2,
			ResourceGroups: []string{meta.DefaultResourceGroupName},
		}
		resp, err := suite.server.LoadCollection(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))

		loadedCollection := suite.server.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap()[collection]
		suite.Require().NotNil(loadedCollection)
		suite.False(loadedCollection.UserSpecifiedReplicaMode)
		suite.Len(suite.server.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap()[collection].Replicas, 1)
	}
}

func (suite *ServiceSuite) TestLoadPartitionWithUserSpecifiedReplicaMode() {
	ctx := context.Background()
	suite.expectGetRecoverInfoForAllCollections()

	// Test load partition with userSpecifiedReplicaMode = true
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}

		req := &querypb.LoadPartitionsRequest{
			CollectionID:  collection,
			PartitionIDs:  suite.partitions[collection],
			ReplicaNumber: 1,
		}
		resp, err := suite.server.LoadPartitions(ctx, req)
		suite.Require().NoError(merr.CheckRPCCall(resp, err))

		// Verify UserSpecifiedReplicaMode is set correctly
		loadedCollection := suite.server.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap()[collection]
		suite.Require().NotNil(loadedCollection)
		suite.True(loadedCollection.UserSpecifiedReplicaMode)

		suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
		suite.assertQViewsLoadConfigured(collection, 1, suite.partitions[collection])
	}
}

func (suite *ServiceSuite) TestLoadPartitionUpdateUserSpecifiedReplicaMode() {
	ctx := context.Background()
	suite.expectGetRecoverInfoForAllCollections()

	// First load partition with userSpecifiedReplicaMode = false
	collection := suite.collections[1] // Use partition load type collection
	if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
		return
	}

	req := &querypb.LoadPartitionsRequest{
		CollectionID: collection,
		PartitionIDs: suite.partitions[collection][:1], // Load first partition
	}

	resp, err := suite.server.LoadPartitions(ctx, req)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))

	// Verify UserSpecifiedReplicaMode is false
	loadedCollection := suite.server.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap()[collection]
	suite.Require().NotNil(loadedCollection)
	suite.False(loadedCollection.UserSpecifiedReplicaMode)

	// Load another partition with userSpecifiedReplicaMode = true
	req2 := &querypb.LoadPartitionsRequest{
		CollectionID:  collection,
		PartitionIDs:  suite.partitions[collection][1:2], // Load second partition
		ReplicaNumber: 1,
	}
	resp, err = suite.server.LoadPartitions(ctx, req2)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))

	// Verify UserSpecifiedReplicaMode is updated to true
	updatedCollection := suite.server.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap()[collection]
	suite.Require().NotNil(updatedCollection)
	suite.True(updatedCollection.UserSpecifiedReplicaMode)
}

func (suite *ServiceSuite) assertCollectionLoaded(collection int64) {
	suite.assertQViewsLoadConfigured(collection, 0, suite.partitions[collection])
}
