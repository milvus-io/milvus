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

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/etcd"
)

const (
	defaultVecFieldID = 1
	defaultIndexID    = 1
)

type JobSuite struct {
	suite.Suite

	// Data
	collections []int64
	partitions  map[int64][]int64
	channels    map[int64][]string
	segments    map[int64]map[int64][]int64 // CollectionID, PartitionID -> Segments
	loadTypes   map[int64]querypb.LoadType

	// Dependencies
	kv             kv.MetaKv
	store          meta.Store
	dist           *meta.DistributionManager
	meta           *meta.Meta
	targetMgr      *meta.TargetManager
	targetObserver *observers.TargetObserver
	broker         *meta.MockBroker
	nodeMgr        *session.NodeManager

	// Test objects
	scheduler *Scheduler
}

func (suite *JobSuite) SetupSuite() {
	Params.Init()

	suite.collections = []int64{1000, 1001}
	suite.partitions = map[int64][]int64{
		1000: {100, 101},
		1001: {102, 103},
	}
	suite.channels = map[int64][]string{
		1000: {"1000-dmc0", "1000-dmc1"},
		1001: {"1001-dmc0", "1001-dmc1"},
	}
	suite.segments = map[int64]map[int64][]int64{
		1000: {
			100: {1, 2},
			101: {3, 4},
		},
		1001: {
			102: {5, 6},
			103: {7, 8},
		},
	}
	suite.loadTypes = map[int64]querypb.LoadType{
		1000: querypb.LoadType_LoadCollection,
		1001: querypb.LoadType_LoadPartition,
	}

	suite.broker = meta.NewMockBroker(suite.T())
	for collection, partitions := range suite.segments {
		vChannels := []*datapb.VchannelInfo{}
		for _, channel := range suite.channels[collection] {
			vChannels = append(vChannels, &datapb.VchannelInfo{
				CollectionID: collection,
				ChannelName:  channel,
			})
		}
		for partition, segments := range partitions {
			segmentBinlogs := []*datapb.SegmentBinlogs{}
			for _, segment := range segments {
				segmentBinlogs = append(segmentBinlogs, &datapb.SegmentBinlogs{
					SegmentID:     segment,
					InsertChannel: suite.channels[collection][segment%2],
				})
			}

			suite.broker.EXPECT().
				GetRecoveryInfo(mock.Anything, collection, partition).
				Return(vChannels, segmentBinlogs, nil)
		}
	}
}

func (suite *JobSuite) SetupTest() {
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())

	suite.store = meta.NewMetaStore(suite.kv)
	suite.dist = meta.NewDistributionManager()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(RandomIncrementIDAllocator(), suite.store, suite.nodeMgr)
	suite.targetMgr = meta.NewTargetManager(suite.broker, suite.meta)
	suite.targetObserver = observers.NewTargetObserver(suite.meta,
		suite.targetMgr,
		suite.dist,
		suite.broker,
	)
	suite.scheduler = NewScheduler()

	suite.scheduler.Start(context.Background())
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()

	suite.nodeMgr.Add(session.NewNodeInfo(1000, "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(2000, "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(3000, "localhost"))

	err = suite.meta.AssignNode(meta.DefaultResourceGroupName, 1000)
	suite.NoError(err)
	err = suite.meta.AssignNode(meta.DefaultResourceGroupName, 2000)
	suite.NoError(err)
	err = suite.meta.AssignNode(meta.DefaultResourceGroupName, 3000)
	suite.NoError(err)
}

func (suite *JobSuite) TearDownTest() {
	suite.kv.Close()
	suite.scheduler.Stop()
}

func (suite *JobSuite) BeforeTest(suiteName, testName string) {
	switch testName {
	case "TestLoadCollection":
		for collection, partitions := range suite.partitions {
			if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
				continue
			}
			suite.broker.EXPECT().
				GetPartitions(mock.Anything, collection).
				Return(partitions, nil)
		}
	}
}

func (suite *JobSuite) TestLoadCollection() {
	ctx := context.Background()

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
		job := NewLoadCollectionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.EqualValues(1, suite.meta.GetReplicaNumber(collection))
		suite.targetMgr.UpdateCollectionCurrentTarget(collection)
		suite.assertLoaded(collection)
	}

	// Test load again
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
			continue
		}
		req := &querypb.LoadCollectionRequest{
			CollectionID: collection,
		}
		job := NewLoadCollectionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, ErrCollectionLoaded)
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
		job := NewLoadCollectionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, ErrLoadParameterMismatched)
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
		job := NewLoadPartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, ErrLoadParameterMismatched)
	}

	suite.meta.ResourceManager.AddResourceGroup("rg1")
	suite.meta.ResourceManager.AddResourceGroup("rg2")
	suite.meta.ResourceManager.AddResourceGroup("rg3")

	// Load with 3 replica on 1 rg
	req := &querypb.LoadCollectionRequest{
		CollectionID:   1001,
		ReplicaNumber:  3,
		ResourceGroups: []string{"rg1"},
	}
	job := NewLoadCollectionJob(
		ctx,
		req,
		suite.dist,
		suite.meta,
		suite.targetMgr,
		suite.broker,
		suite.nodeMgr,
	)
	suite.scheduler.Add(job)
	err := job.Wait()
	suite.ErrorContains(err, meta.ErrNodeNotEnough.Error())

	// Load with 3 replica on 3 rg
	req = &querypb.LoadCollectionRequest{
		CollectionID:   1002,
		ReplicaNumber:  3,
		ResourceGroups: []string{"rg1", "rg2", "rg3"},
	}
	job = NewLoadCollectionJob(
		ctx,
		req,
		suite.dist,
		suite.meta,
		suite.targetMgr,
		suite.broker,
		suite.nodeMgr,
	)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.ErrorContains(err, meta.ErrNodeNotEnough.Error())
}

func (suite *JobSuite) TestLoadCollectionWithReplicas() {
	ctx := context.Background()

	// Test load collection
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
			continue
		}
		// Load with 3 replica
		req := &querypb.LoadCollectionRequest{
			CollectionID:  collection,
			ReplicaNumber: 5,
		}
		job := NewLoadCollectionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorContains(err, meta.ErrNodeNotEnough.Error())
	}
}

func (suite *JobSuite) TestLoadCollectionWithDiffIndex() {
	ctx := context.Background()

	// Test load collection
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
			continue
		}
		// Load with 1 replica
		req := &querypb.LoadCollectionRequest{
			CollectionID: collection,
			FieldIndexID: map[int64]int64{
				defaultVecFieldID: defaultIndexID,
			},
		}
		job := NewLoadCollectionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.EqualValues(1, suite.meta.GetReplicaNumber(collection))
		suite.targetMgr.UpdateCollectionCurrentTarget(collection, suite.partitions[collection]...)
		suite.assertLoaded(collection)
	}

	// Test load with different index
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
			continue
		}
		req := &querypb.LoadCollectionRequest{
			CollectionID: collection,
			FieldIndexID: map[int64]int64{
				defaultVecFieldID: -defaultIndexID,
			},
		}
		job := NewLoadCollectionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, ErrLoadParameterMismatched)
	}
}

func (suite *JobSuite) TestLoadPartition() {
	ctx := context.Background()

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
		job := NewLoadPartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.EqualValues(1, suite.meta.GetReplicaNumber(collection))
		suite.targetMgr.UpdateCollectionCurrentTarget(collection, suite.partitions[collection]...)
		suite.assertLoaded(collection)
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
		job := NewLoadPartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, ErrCollectionLoaded)
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
		job := NewLoadPartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, ErrLoadParameterMismatched)
	}

	// Test load partition with more partition
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}

		req := &querypb.LoadPartitionsRequest{
			CollectionID:  collection,
			PartitionIDs:  append(suite.partitions[collection], 200),
			ReplicaNumber: 3,
		}
		job := NewLoadPartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, ErrLoadParameterMismatched)
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
		job := NewLoadCollectionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, ErrLoadParameterMismatched)
	}

	suite.meta.ResourceManager.AddResourceGroup("rg1")
	suite.meta.ResourceManager.AddResourceGroup("rg2")
	suite.meta.ResourceManager.AddResourceGroup("rg3")

	// test load 3 replica in 1 rg, should pass rg check
	req := &querypb.LoadPartitionsRequest{
		CollectionID:   100,
		PartitionIDs:   []int64{1001},
		ReplicaNumber:  3,
		ResourceGroups: []string{"rg1"},
	}
	job := NewLoadPartitionJob(
		ctx,
		req,
		suite.dist,
		suite.meta,
		suite.targetMgr,
		suite.broker,
		suite.nodeMgr,
	)
	suite.scheduler.Add(job)
	err := job.Wait()
	suite.Contains(err.Error(), meta.ErrNodeNotEnough.Error())

	// test load 3 replica in 3 rg, should pass rg check
	req = &querypb.LoadPartitionsRequest{
		CollectionID:   102,
		PartitionIDs:   []int64{1001},
		ReplicaNumber:  3,
		ResourceGroups: []string{"rg1", "rg2", "rg3"},
	}
	job = NewLoadPartitionJob(
		ctx,
		req,
		suite.dist,
		suite.meta,
		suite.targetMgr,
		suite.broker,
		suite.nodeMgr,
	)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.Contains(err.Error(), meta.ErrNodeNotEnough.Error())
}

func (suite *JobSuite) TestLoadPartitionWithReplicas() {
	ctx := context.Background()

	// Test load partitions
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}
		// Load with 3 replica
		req := &querypb.LoadPartitionsRequest{
			CollectionID:  collection,
			PartitionIDs:  suite.partitions[collection],
			ReplicaNumber: 5,
		}
		job := NewLoadPartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorContains(err, meta.ErrNodeNotEnough.Error())
	}
}

func (suite *JobSuite) TestLoadPartitionWithDiffIndex() {
	ctx := context.Background()

	// Test load partition
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}
		// Load with 1 replica
		req := &querypb.LoadPartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
			FieldIndexID: map[int64]int64{
				defaultVecFieldID: defaultIndexID,
			},
		}
		job := NewLoadPartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.EqualValues(1, suite.meta.GetReplicaNumber(collection))
		suite.targetMgr.UpdateCollectionCurrentTarget(collection, suite.partitions[collection]...)
		suite.assertLoaded(collection)
	}

	// Test load partition with different index
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}
		// Load with 1 replica
		req := &querypb.LoadPartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
			FieldIndexID: map[int64]int64{
				defaultVecFieldID: -defaultIndexID,
			},
		}
		job := NewLoadPartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, ErrLoadParameterMismatched)
	}
}

func (suite *JobSuite) TestReleaseCollection() {
	ctx := context.Background()

	suite.loadAll()

	// Test release collection and partition
	for _, collection := range suite.collections {
		req := &querypb.ReleaseCollectionRequest{
			CollectionID: collection,
		}
		job := NewReleaseCollectionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.targetObserver,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.assertReleased(collection)
	}

	// Test release again
	for _, collection := range suite.collections {
		req := &querypb.ReleaseCollectionRequest{
			CollectionID: collection,
		}
		job := NewReleaseCollectionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.targetObserver,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.assertReleased(collection)
	}
}

func (suite *JobSuite) TestReleasePartition() {
	ctx := context.Background()

	suite.loadAll()

	// Test release partition
	for _, collection := range suite.collections {
		req := &querypb.ReleasePartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
		}
		job := NewReleasePartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.targetObserver,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
			suite.ErrorIs(err, ErrLoadParameterMismatched)
			suite.assertLoaded(collection)
		} else {
			suite.NoError(err)
			suite.assertReleased(collection)
		}
	}

	// Test release again
	for _, collection := range suite.collections {
		req := &querypb.ReleasePartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
		}
		job := NewReleasePartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.targetObserver,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
			suite.ErrorIs(err, ErrLoadParameterMismatched)
			suite.assertLoaded(collection)
		} else {
			suite.NoError(err)
			suite.assertReleased(collection)
		}
	}

	// Test release partial partitions
	suite.releaseAll()
	suite.loadAll()
	for _, collection := range suite.collections {
		req := &querypb.ReleasePartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection][1:],
		}
		job := NewReleasePartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.targetObserver,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
			suite.ErrorIs(err, ErrLoadParameterMismatched)
			suite.assertLoaded(collection)
		} else {
			suite.NoError(err)
			suite.True(suite.meta.Exist(collection))
			partitions := suite.meta.GetPartitionsByCollection(collection)
			suite.Len(partitions, 1)
			suite.Equal(suite.partitions[collection][0], partitions[0].GetPartitionID())
		}
	}
}

func (suite *JobSuite) TestLoadCollectionStoreFailed() {
	// Store collection failed
	store := meta.NewMockStore(suite.T())
	suite.meta = meta.NewMeta(RandomIncrementIDAllocator(), store, suite.nodeMgr)

	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil)
	err := suite.meta.AssignNode(meta.DefaultResourceGroupName, 1000)
	suite.NoError(err)
	err = suite.meta.AssignNode(meta.DefaultResourceGroupName, 2000)
	suite.NoError(err)
	err = suite.meta.AssignNode(meta.DefaultResourceGroupName, 3000)
	suite.NoError(err)

	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
			continue
		}

		err := errors.New("failed to store collection")
		store.EXPECT().SaveReplica(mock.Anything).Return(nil)
		store.EXPECT().SaveCollection(&querypb.CollectionLoadInfo{
			CollectionID:  collection,
			ReplicaNumber: 1,
			Status:        querypb.LoadStatus_Loading,
		}).Return(err)
		store.EXPECT().ReleaseReplicas(collection).Return(nil)

		req := &querypb.LoadCollectionRequest{
			CollectionID: collection,
		}
		job := NewLoadCollectionJob(
			context.Background(),
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		loadErr := job.Wait()
		suite.ErrorIs(loadErr, err)
	}
}

func (suite *JobSuite) TestLoadPartitionStoreFailed() {
	// Store partition failed
	store := meta.NewMockStore(suite.T())
	suite.meta = meta.NewMeta(RandomIncrementIDAllocator(), store, suite.nodeMgr)

	store.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil)
	err := suite.meta.AssignNode(meta.DefaultResourceGroupName, 1000)
	suite.NoError(err)
	err = suite.meta.AssignNode(meta.DefaultResourceGroupName, 2000)
	suite.NoError(err)
	err = suite.meta.AssignNode(meta.DefaultResourceGroupName, 3000)
	suite.NoError(err)

	err = errors.New("failed to store collection")
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}

		store.EXPECT().SaveReplica(mock.Anything).Return(nil)
		store.EXPECT().SavePartition(mock.Anything, mock.Anything).Return(err)
		store.EXPECT().ReleaseReplicas(collection).Return(nil)

		req := &querypb.LoadPartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
		}
		job := NewLoadPartitionJob(
			context.Background(),
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		loadErr := job.Wait()
		suite.ErrorIs(loadErr, err)
	}
}

func (suite *JobSuite) TestLoadCreateReplicaFailed() {
	// Store replica failed
	suite.meta = meta.NewMeta(ErrorIDAllocator(), suite.store, session.NewNodeManager())
	for _, collection := range suite.collections {
		req := &querypb.LoadCollectionRequest{
			CollectionID: collection,
		}
		job := NewLoadCollectionJob(
			context.Background(),
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.broker,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, ErrFailedAllocateID)
	}
}

func (suite *JobSuite) loadAll() {
	ctx := context.Background()
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
			req := &querypb.LoadCollectionRequest{
				CollectionID: collection,
			}
			job := NewLoadCollectionJob(
				ctx,
				req,
				suite.dist,
				suite.meta,
				suite.targetMgr,
				suite.broker,
				suite.nodeMgr,
			)
			suite.scheduler.Add(job)
			err := job.Wait()
			suite.NoError(err)
			suite.EqualValues(1, suite.meta.GetReplicaNumber(collection))
			suite.True(suite.meta.Exist(collection))
			suite.NotNil(suite.meta.GetCollection(collection))
			suite.targetMgr.UpdateCollectionCurrentTarget(collection)
		} else {
			req := &querypb.LoadPartitionsRequest{
				CollectionID: collection,
				PartitionIDs: suite.partitions[collection],
			}
			job := NewLoadPartitionJob(
				ctx,
				req,
				suite.dist,
				suite.meta,
				suite.targetMgr,
				suite.broker,
				suite.nodeMgr,
			)
			suite.scheduler.Add(job)
			err := job.Wait()
			suite.NoError(err)
			suite.EqualValues(1, suite.meta.GetReplicaNumber(collection))
			suite.True(suite.meta.Exist(collection))
			suite.NotNil(suite.meta.GetPartitionsByCollection(collection))
			suite.targetMgr.UpdateCollectionCurrentTarget(collection)
		}
	}
}

func (suite *JobSuite) releaseAll() {
	ctx := context.Background()
	for _, collection := range suite.collections {
		req := &querypb.ReleaseCollectionRequest{
			CollectionID: collection,
		}
		job := NewReleaseCollectionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.targetMgr,
			suite.targetObserver,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.assertReleased(collection)
	}
}

func (suite *JobSuite) assertLoaded(collection int64) {
	suite.True(suite.meta.Exist(collection))
	for _, channel := range suite.channels[collection] {
		suite.NotNil(suite.targetMgr.GetDmChannel(collection, channel, meta.CurrentTarget))
	}
	for _, partitions := range suite.segments[collection] {
		for _, segment := range partitions {
			suite.NotNil(suite.targetMgr.GetHistoricalSegment(collection, segment, meta.CurrentTarget))
		}
	}
}

func (suite *JobSuite) assertReleased(collection int64) {
	suite.False(suite.meta.Exist(collection))
	for _, channel := range suite.channels[collection] {
		suite.Nil(suite.targetMgr.GetDmChannel(collection, channel, meta.CurrentTarget))
	}
	for _, partitions := range suite.segments[collection] {
		for _, segment := range partitions {
			suite.Nil(suite.targetMgr.GetHistoricalSegment(collection, segment, meta.CurrentTarget))
		}
	}
}

func TestJob(t *testing.T) {
	suite.Run(t, new(JobSuite))
}
