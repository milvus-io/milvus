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

	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
		suite.EqualValues(1, suite.meta.GetReplicaNumber(ctx, collection))
		suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
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

	suite.meta.ResourceManager.AddResourceGroup(ctx, "rg1", cfg)
	suite.meta.ResourceManager.AddResourceGroup(ctx, "rg2", cfg)
	suite.meta.ResourceManager.AddResourceGroup(ctx, "rg3", cfg)

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
			suite.EqualValues(1, suite.meta.GetReplicaNumber(ctx, collection))
			suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
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
		suite.EqualValues(1, suite.meta.GetReplicaNumber(ctx, collection))
		suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
		suite.assertCollectionLoaded(collection)
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
	suite.meta.ResourceManager.AddResourceGroup(ctx, "rg1", cfg)
	suite.meta.ResourceManager.AddResourceGroup(ctx, "rg2", cfg)
	suite.meta.ResourceManager.AddResourceGroup(ctx, "rg3", cfg)

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
			suite.EqualValues(1, suite.meta.GetReplicaNumber(ctx, collection))
			suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
			suite.assertCollectionLoaded(collection)
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
	suite.assertPartitionLoaded(ctx, collection)

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
	suite.assertPartitionLoaded(ctx, collection, p2)

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
	suite.assertPartitionLoaded(ctx, collection, p2)

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
	suite.assertPartitionLoaded(ctx, collection, p2)
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

func (suite *ServiceSuite) TestDDLCallbacksReleasePartition() {
	ctx := context.Background()
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
	for _, collectionID := range suite.collections {
		// make collection able to get into loaded state
		suite.updateChannelDist(ctx, collectionID)
		suite.updateSegmentDist(collectionID, 3000, suite.partitions[collectionID]...)
		job.WaitCurrentTargetUpdated(ctx, suite.targetObserver, collectionID)
	}
	for _, collection := range suite.collections {
		req := &querypb.ReleasePartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection][1:],
		}
		ch := make(chan struct{})
		go func() {
			defer close(ch)
			time.Sleep(100 * time.Millisecond)
			suite.updateChannelDist(ctx, collection)
			suite.updateSegmentDist(collection, 3000, suite.partitions[collection][:1]...)
		}()
		resp, err := suite.server.ReleasePartitions(ctx, req)
		<-ch
		suite.Require().NoError(merr.CheckRPCCall(resp, err))

		suite.True(suite.meta.Exist(ctx, collection))
		partitions := suite.meta.GetPartitionsByCollection(ctx, collection)
		suite.Len(partitions, 1)
		suite.Equal(suite.partitions[collection][0], partitions[0].GetPartitionID())
		suite.assertPartitionReleased(collection, suite.partitions[collection][1:]...)
	}
}

func (suite *ServiceSuite) TestDynamicRelease() {
	ctx := context.Background()
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
	for _, collectionID := range suite.collections {
		// make collection able to get into loaded state
		suite.updateChannelDist(ctx, collectionID)
		suite.updateSegmentDist(collectionID, 3000, suite.partitions[collectionID]...)
		job.WaitCurrentTargetUpdated(ctx, suite.targetObserver, collectionID)
	}

	req := newReleasePartJob(col0, p0)
	// update segments
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		time.Sleep(100 * time.Millisecond)
		suite.updateSegmentDist(col0, 3000, p1, p2)
		suite.updateChannelDist(ctx, col0)
	}()
	resp, err := suite.server.ReleasePartitions(ctx, req)
	<-ch
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.assertPartitionReleased(col0, p0)
	suite.assertPartitionLoaded(ctx, col0, p1, p2)

	// loaded: p1, p2
	// action: release p0, p1
	// expect: p1 released, p2 loaded
	req = newReleasePartJob(col0, p0, p1)
	ch = make(chan struct{})
	go func() {
		defer close(ch)
		time.Sleep(100 * time.Millisecond)
		suite.updateSegmentDist(col0, 3000, p2)
		suite.updateChannelDist(ctx, col0)
	}()
	resp, err = suite.server.ReleasePartitions(ctx, req)
	<-ch
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.assertPartitionReleased(col0, p0, p1)
	suite.assertPartitionLoaded(ctx, col0, p2)

	// loaded: p2
	// action: release p2
	// expect: loadType=col: col loaded, p2 released, full collection should be released.
	req = newReleasePartJob(col0, p2)
	ch = make(chan struct{})
	go func() {
		defer close(ch)
		time.Sleep(100 * time.Millisecond)
		suite.releaseSegmentDist(3000)
		suite.releaseAllChannelDist()
	}()
	resp, err = suite.server.ReleasePartitions(ctx, req)
	<-ch
	suite.Require().NoError(merr.CheckRPCCall(resp, err))
	suite.assertPartitionReleased(col0, p0, p1, p2)
	suite.False(suite.meta.Exist(ctx, col0))

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
	ctx := context.Background()
	suite.False(suite.meta.Exist(ctx, collection))
	suite.Equal(0, len(suite.meta.ReplicaManager.GetByCollection(ctx, collection)))
	for _, channel := range suite.channels[collection] {
		suite.Nil(suite.targetMgr.GetDmChannel(ctx, collection, channel, meta.CurrentTarget))
	}
	for _, partitions := range suite.segments[collection] {
		for _, segment := range partitions {
			suite.Nil(suite.targetMgr.GetSealedSegment(ctx, collection, segment, meta.CurrentTarget))
		}
	}
}

func (suite *ServiceSuite) assertPartitionReleased(collection int64, partitionIDs ...int64) {
	ctx := context.Background()
	for _, partition := range partitionIDs {
		suite.Nil(suite.meta.GetPartition(ctx, partition))
		segments := suite.segments[collection][partition]
		for _, segment := range segments {
			suite.Nil(suite.targetMgr.GetSealedSegment(ctx, collection, segment, meta.CurrentTarget))
		}
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

		// Verify UserSpecifiedReplicaMode is set correctly
		loadedCollection := suite.meta.GetCollection(ctx, collection)
		suite.NotNil(loadedCollection)
		suite.True(loadedCollection.GetUserSpecifiedReplicaMode())

		suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
		suite.assertCollectionLoaded(collection)
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
		loadedCollection := suite.meta.GetCollection(ctx, collection)
		suite.NotNil(loadedCollection)
		suite.True(loadedCollection.GetUserSpecifiedReplicaMode())

		suite.targetMgr.UpdateCollectionCurrentTarget(ctx, collection)
		suite.assertCollectionLoaded(collection)
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
	loadedCollection := suite.meta.GetCollection(ctx, collection)
	suite.NotNil(loadedCollection)
	suite.False(loadedCollection.GetUserSpecifiedReplicaMode())

	// Load another partition with userSpecifiedReplicaMode = true
	req2 := &querypb.LoadPartitionsRequest{
		CollectionID:  collection,
		PartitionIDs:  suite.partitions[collection][1:2], // Load second partition
		ReplicaNumber: 1,
	}
	resp, err = suite.server.LoadPartitions(ctx, req2)
	suite.Require().NoError(merr.CheckRPCCall(resp, err))

	// Verify UserSpecifiedReplicaMode is updated to true
	updatedCollection := suite.meta.GetCollection(ctx, collection)
	suite.NotNil(updatedCollection)
	suite.True(updatedCollection.GetUserSpecifiedReplicaMode())
}

func (suite *ServiceSuite) TestSyncNewCreatedPartition() {
	newPartition := int64(999)
	ctx := context.Background()

	// test sync new created partition
	suite.loadAll()
	collectionID := suite.collections[0]
	// make collection able to get into loaded state
	suite.updateChannelDist(ctx, collectionID)
	suite.updateSegmentDist(collectionID, 3000, suite.partitions[collectionID]...)

	req := &querypb.SyncNewCreatedPartitionRequest{
		CollectionID: collectionID,
		PartitionID:  newPartition,
	}
	syncJob := job.NewSyncNewCreatedPartitionJob(
		ctx,
		req,
		suite.meta,
		suite.broker,
		suite.targetObserver,
		suite.targetMgr,
	)
	suite.jobScheduler.Add(syncJob)
	err := syncJob.Wait()
	suite.NoError(err)
	partition := suite.meta.CollectionManager.GetPartition(ctx, newPartition)
	suite.NotNil(partition)
	suite.Equal(querypb.LoadStatus_Loaded, partition.GetStatus())

	// test collection not loaded
	req = &querypb.SyncNewCreatedPartitionRequest{
		CollectionID: int64(888),
		PartitionID:  newPartition,
	}
	syncJob = job.NewSyncNewCreatedPartitionJob(
		ctx,
		req,
		suite.meta,
		suite.broker,
		suite.targetObserver,
		suite.targetMgr,
	)
	suite.jobScheduler.Add(syncJob)
	err = syncJob.Wait()
	suite.NoError(err)

	// test collection loaded, but its loadType is loadPartition
	req = &querypb.SyncNewCreatedPartitionRequest{
		CollectionID: suite.collections[1],
		PartitionID:  newPartition,
	}
	syncJob = job.NewSyncNewCreatedPartitionJob(
		ctx,
		req,
		suite.meta,
		suite.broker,
		suite.targetObserver,
		suite.targetMgr,
	)
	suite.jobScheduler.Add(syncJob)
	err = syncJob.Wait()
	suite.NoError(err)
}

func (suite *ServiceSuite) assertCollectionLoaded(collection int64) {
	ctx := context.Background()
	suite.True(suite.meta.Exist(ctx, collection))
	suite.NotEqual(0, len(suite.meta.ReplicaManager.GetByCollection(ctx, collection)))
	for _, channel := range suite.channels[collection] {
		suite.NotNil(suite.targetMgr.GetDmChannel(ctx, collection, channel, meta.CurrentTarget))
	}
	for _, segments := range suite.segments[collection] {
		for _, segment := range segments {
			suite.NotNil(suite.targetMgr.GetSealedSegment(ctx, collection, segment, meta.CurrentTarget))
		}
	}
}
