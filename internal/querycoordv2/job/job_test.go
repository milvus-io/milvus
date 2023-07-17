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
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
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
	kv                kv.MetaKv
	store             meta.Store
	dist              *meta.DistributionManager
	meta              *meta.Meta
	cluster           *session.MockCluster
	targetMgr         *meta.TargetManager
	targetObserver    *observers.TargetObserver
	broker            *meta.MockBroker
	nodeMgr           *session.NodeManager
	checkerController *checkers.CheckerController

	// Test objects
	scheduler *Scheduler
}

func (suite *JobSuite) SetupSuite() {
	Params.Init()

	suite.collections = []int64{1000, 1001}
	suite.partitions = map[int64][]int64{
		1000: {100, 101, 102},
		1001: {103, 104, 105},
	}
	suite.channels = map[int64][]string{
		1000: {"1000-dmc0", "1000-dmc1"},
		1001: {"1001-dmc0", "1001-dmc1"},
	}
	suite.segments = map[int64]map[int64][]int64{
		1000: {
			100: {1, 2},
			101: {3, 4},
			102: {5, 6},
		},
		1001: {
			103: {7, 8},
			104: {9, 10},
			105: {11, 12},
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

		segmentBinlogs := []*datapb.SegmentInfo{}
		for partition, segments := range partitions {
			for _, segment := range segments {
				segmentBinlogs = append(segmentBinlogs, &datapb.SegmentInfo{
					ID:            segment,
					PartitionID:   partition,
					InsertChannel: suite.channels[collection][segment%2],
				})
			}
		}
		suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collection).Return(vChannels, segmentBinlogs, nil)
	}

	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything).
		Return(nil, nil)
	suite.broker.EXPECT().DescribeIndex(mock.Anything, mock.Anything).
		Return(nil, nil)

	suite.cluster = session.NewMockCluster(suite.T())
	suite.cluster.EXPECT().
		LoadPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)
	suite.cluster.EXPECT().
		ReleasePartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)
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
	suite.targetObserver.Start(context.Background())
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

	suite.checkerController = &checkers.CheckerController{}
}

func (suite *JobSuite) TearDownTest() {
	suite.kv.Close()
	suite.scheduler.Stop()
	suite.targetObserver.Stop()
}

func (suite *JobSuite) BeforeTest(suiteName, testName string) {
	for collection, partitions := range suite.partitions {
		suite.broker.EXPECT().
			GetPartitions(mock.Anything, collection).
			Return(partitions, nil)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.EqualValues(1, suite.meta.GetReplicaNumber(collection))
		suite.targetMgr.UpdateCollectionCurrentTarget(collection)
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
		job := NewLoadCollectionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, merr.ErrParameterInvalid)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
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
		suite.broker,
		suite.cluster,
		suite.targetMgr,
		suite.targetObserver,
		suite.nodeMgr,
	)
	suite.scheduler.Add(job)
	err := job.Wait()
	suite.ErrorContains(err, meta.ErrNodeNotEnough.Error())

	// Load with 3 replica on 3 rg
	req = &querypb.LoadCollectionRequest{
		CollectionID:   1001,
		ReplicaNumber:  3,
		ResourceGroups: []string{"rg1", "rg2", "rg3"},
	}
	job = NewLoadCollectionJob(
		ctx,
		req,
		suite.dist,
		suite.meta,
		suite.broker,
		suite.cluster,
		suite.targetMgr,
		suite.targetObserver,
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.EqualValues(1, suite.meta.GetReplicaNumber(collection))
		suite.targetMgr.UpdateCollectionCurrentTarget(collection, suite.partitions[collection]...)
		suite.assertCollectionLoaded(collection)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, merr.ErrParameterInvalid)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.EqualValues(1, suite.meta.GetReplicaNumber(collection))
		suite.targetMgr.UpdateCollectionCurrentTarget(collection, suite.partitions[collection]...)
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
		job := NewLoadPartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, merr.ErrParameterInvalid)
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
		job := NewLoadPartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
	}

	suite.meta.ResourceManager.AddResourceGroup("rg1")
	suite.meta.ResourceManager.AddResourceGroup("rg2")
	suite.meta.ResourceManager.AddResourceGroup("rg3")

	// test load 3 replica in 1 rg, should pass rg check
	req := &querypb.LoadPartitionsRequest{
		CollectionID:   999,
		PartitionIDs:   []int64{888},
		ReplicaNumber:  3,
		ResourceGroups: []string{"rg1"},
	}
	job := NewLoadPartitionJob(
		ctx,
		req,
		suite.dist,
		suite.meta,
		suite.broker,
		suite.cluster,
		suite.targetMgr,
		suite.targetObserver,
		suite.nodeMgr,
	)
	suite.scheduler.Add(job)
	err := job.Wait()
	suite.Contains(err.Error(), meta.ErrNodeNotEnough.Error())

	// test load 3 replica in 3 rg, should pass rg check
	req = &querypb.LoadPartitionsRequest{
		CollectionID:   999,
		PartitionIDs:   []int64{888},
		ReplicaNumber:  3,
		ResourceGroups: []string{"rg1", "rg2", "rg3"},
	}
	job = NewLoadPartitionJob(
		ctx,
		req,
		suite.dist,
		suite.meta,
		suite.broker,
		suite.cluster,
		suite.targetMgr,
		suite.targetObserver,
		suite.nodeMgr,
	)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.Contains(err.Error(), meta.ErrNodeNotEnough.Error())
}

func (suite *JobSuite) TestDynamicLoad() {
	ctx := context.Background()

	collection := suite.collections[0]
	p0, p1, p2 := suite.partitions[collection][0], suite.partitions[collection][1], suite.partitions[collection][2]
	newLoadPartJob := func(partitions ...int64) *LoadPartitionJob {
		req := &querypb.LoadPartitionsRequest{
			CollectionID:  collection,
			PartitionIDs:  partitions,
			ReplicaNumber: 1,
		}
		job := NewLoadPartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		return job
	}
	newLoadColJob := func() *LoadCollectionJob {
		req := &querypb.LoadCollectionRequest{
			CollectionID:  collection,
			ReplicaNumber: 1,
		}
		job := NewLoadCollectionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		return job
	}

	// loaded: none
	// action: load p0, p1, p2
	// expect: p0, p1, p2 loaded
	job := newLoadPartJob(p0, p1, p2)
	suite.scheduler.Add(job)
	err := job.Wait()
	suite.NoError(err)
	suite.targetMgr.UpdateCollectionCurrentTarget(collection)
	suite.assertPartitionLoaded(collection, p0, p1, p2)

	// loaded: p0, p1, p2
	// action: load p0, p1, p2
	// expect: do nothing, p0, p1, p2 loaded
	job = newLoadPartJob(p0, p1, p2)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.NoError(err)
	suite.assertPartitionLoaded(collection)

	// loaded: p0, p1
	// action: load p2
	// expect: p0, p1, p2 loaded
	suite.releaseAll()
	job = newLoadPartJob(p0, p1)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.NoError(err)
	suite.targetMgr.UpdateCollectionCurrentTarget(collection)
	suite.assertPartitionLoaded(collection, p0, p1)
	job = newLoadPartJob(p2)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.NoError(err)
	suite.targetMgr.UpdateCollectionCurrentTarget(collection)
	suite.assertPartitionLoaded(collection, p2)

	// loaded: p0, p1
	// action: load p1, p2
	// expect: p0, p1, p2 loaded
	suite.releaseAll()
	job = newLoadPartJob(p0, p1)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.NoError(err)
	suite.targetMgr.UpdateCollectionCurrentTarget(collection)
	suite.assertPartitionLoaded(collection, p0, p1)
	job = newLoadPartJob(p1, p2)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.NoError(err)
	suite.targetMgr.UpdateCollectionCurrentTarget(collection)
	suite.assertPartitionLoaded(collection, p2)

	// loaded: p0, p1
	// action: load col
	// expect: col loaded
	suite.releaseAll()
	job = newLoadPartJob(p0, p1)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.NoError(err)
	suite.targetMgr.UpdateCollectionCurrentTarget(collection)
	suite.assertPartitionLoaded(collection, p0, p1)
	colJob := newLoadColJob()
	suite.scheduler.Add(colJob)
	err = colJob.Wait()
	suite.NoError(err)
	suite.targetMgr.UpdateCollectionCurrentTarget(collection)
	suite.assertPartitionLoaded(collection, p2)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.EqualValues(1, suite.meta.GetReplicaNumber(collection))
		suite.targetMgr.UpdateCollectionCurrentTarget(collection, suite.partitions[collection]...)
		suite.assertCollectionLoaded(collection)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, merr.ErrParameterInvalid)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.checkerController,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.assertCollectionReleased(collection)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.checkerController,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.assertCollectionReleased(collection)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.checkerController,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.assertPartitionReleased(collection, suite.partitions[collection]...)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.checkerController,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
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
		job := NewReleasePartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.checkerController,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.True(suite.meta.Exist(collection))
		partitions := suite.meta.GetPartitionsByCollection(collection)
		suite.Len(partitions, 1)
		suite.Equal(suite.partitions[collection][0], partitions[0].GetPartitionID())
		suite.assertPartitionReleased(collection, suite.partitions[collection][1:]...)
	}
}

func (suite *JobSuite) TestDynamicRelease() {
	ctx := context.Background()

	col0, col1 := suite.collections[0], suite.collections[1]
	p0, p1, p2 := suite.partitions[col0][0], suite.partitions[col0][1], suite.partitions[col0][2]
	p3, p4, p5 := suite.partitions[col1][0], suite.partitions[col1][1], suite.partitions[col1][2]
	newReleasePartJob := func(col int64, partitions ...int64) *ReleasePartitionJob {
		req := &querypb.ReleasePartitionsRequest{
			CollectionID: col,
			PartitionIDs: partitions,
		}
		job := NewReleasePartitionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.checkerController,
		)
		return job
	}
	newReleaseColJob := func(col int64) *ReleaseCollectionJob {
		req := &querypb.ReleaseCollectionRequest{
			CollectionID: col,
		}
		job := NewReleaseCollectionJob(
			ctx,
			req,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.checkerController,
		)
		return job
	}

	// loaded: p0, p1, p2
	// action: release p0
	// expect: p0 released, p1, p2 loaded
	suite.loadAll()
	job := newReleasePartJob(col0, p0)
	suite.scheduler.Add(job)
	err := job.Wait()
	suite.NoError(err)
	suite.assertPartitionReleased(col0, p0)
	suite.assertPartitionLoaded(col0, p1, p2)

	// loaded: p1, p2
	// action: release p0, p1
	// expect: p1 released, p2 loaded
	job = newReleasePartJob(col0, p0, p1)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.NoError(err)
	suite.assertPartitionReleased(col0, p0, p1)
	suite.assertPartitionLoaded(col0, p2)

	// loaded: p2
	// action: release p2
	// expect: loadType=col: col loaded, p2 released
	job = newReleasePartJob(col0, p2)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.NoError(err)
	suite.assertPartitionReleased(col0, p0, p1, p2)
	suite.True(suite.meta.Exist(col0))

	// loaded: p0, p1, p2
	// action: release col
	// expect: col released
	suite.releaseAll()
	suite.loadAll()
	releaseColJob := newReleaseColJob(col0)
	suite.scheduler.Add(releaseColJob)
	err = releaseColJob.Wait()
	suite.NoError(err)
	suite.assertCollectionReleased(col0)
	suite.assertPartitionReleased(col0, p0, p1, p2)

	// loaded: p3, p4, p5
	// action: release p3, p4, p5
	// expect: loadType=partition: col released
	suite.releaseAll()
	suite.loadAll()
	job = newReleasePartJob(col1, p3, p4, p5)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.NoError(err)
	suite.assertCollectionReleased(col1)
	suite.assertPartitionReleased(col1, p3, p4, p5)
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
		suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil)
		err := errors.New("failed to store collection")
		store.EXPECT().SaveReplica(mock.Anything).Return(nil)
		store.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(err)
		store.EXPECT().ReleaseReplicas(collection).Return(nil)

		req := &querypb.LoadCollectionRequest{
			CollectionID: collection,
		}
		job := NewLoadCollectionJob(
			context.Background(),
			req,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
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
		store.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(err)
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
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
		suite.broker.EXPECT().
			GetPartitions(mock.Anything, collection).
			Return(suite.partitions[collection], nil)
		req := &querypb.LoadCollectionRequest{
			CollectionID: collection,
		}
		job := NewLoadCollectionJob(
			context.Background(),
			req,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.ErrorIs(err, ErrFailedAllocateID)
	}
}

func (suite *JobSuite) TestCallLoadPartitionFailed() {
	// call LoadPartitions failed at get index info
	getIndexErr := fmt.Errorf("mock get index error")
	suite.broker.ExpectedCalls = lo.Filter(suite.broker.ExpectedCalls, func(call *mock.Call, _ int) bool {
		return call.Method != "DescribeIndex"
	})
	for _, collection := range suite.collections {
		suite.broker.EXPECT().DescribeIndex(mock.Anything, collection).Return(nil, getIndexErr)
		loadCollectionReq := &querypb.LoadCollectionRequest{
			CollectionID: collection,
		}
		loadCollectionJob := NewLoadCollectionJob(
			context.Background(),
			loadCollectionReq,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(loadCollectionJob)
		err := loadCollectionJob.Wait()
		suite.T().Logf("%s", err)
		suite.ErrorIs(err, getIndexErr)

		loadPartitionReq := &querypb.LoadPartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
		}
		loadPartitionJob := NewLoadPartitionJob(
			context.Background(),
			loadPartitionReq,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(loadPartitionJob)
		err = loadPartitionJob.Wait()
		suite.ErrorIs(err, getIndexErr)
	}

	// call LoadPartitions failed at get schema
	getSchemaErr := fmt.Errorf("mock get schema error")
	suite.broker.ExpectedCalls = lo.Filter(suite.broker.ExpectedCalls, func(call *mock.Call, _ int) bool {
		return call.Method != "GetCollectionSchema"
	})
	for _, collection := range suite.collections {
		suite.broker.EXPECT().GetCollectionSchema(mock.Anything, collection).Return(nil, getSchemaErr)
		loadCollectionReq := &querypb.LoadCollectionRequest{
			CollectionID: collection,
		}
		loadCollectionJob := NewLoadCollectionJob(
			context.Background(),
			loadCollectionReq,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(loadCollectionJob)
		err := loadCollectionJob.Wait()
		suite.ErrorIs(err, getSchemaErr)

		loadPartitionReq := &querypb.LoadPartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
		}
		loadPartitionJob := NewLoadPartitionJob(
			context.Background(),
			loadPartitionReq,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.nodeMgr,
		)
		suite.scheduler.Add(loadPartitionJob)
		err = loadPartitionJob.Wait()
		suite.ErrorIs(err, getSchemaErr)
	}

	suite.broker.ExpectedCalls = lo.Filter(suite.broker.ExpectedCalls, func(call *mock.Call, _ int) bool {
		return call.Method != "DescribeIndex" && call.Method != "GetCollectionSchema"
	})
	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything).Return(nil, nil)
	suite.broker.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(nil, nil)
}

func (suite *JobSuite) TestCallReleasePartitionFailed() {
	ctx := context.Background()
	suite.loadAll()

	releasePartitionErr := fmt.Errorf("mock release partitions error")
	suite.cluster.ExpectedCalls = lo.Filter(suite.cluster.ExpectedCalls, func(call *mock.Call, _ int) bool {
		return call.Method != "ReleasePartitions"
	})
	suite.cluster.EXPECT().ReleasePartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, releasePartitionErr)
	for _, collection := range suite.collections {
		releaseCollectionReq := &querypb.ReleaseCollectionRequest{
			CollectionID: collection,
		}
		releaseCollectionJob := NewReleaseCollectionJob(
			ctx,
			releaseCollectionReq,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.checkerController,
		)
		suite.scheduler.Add(releaseCollectionJob)
		err := releaseCollectionJob.Wait()
		suite.NoError(err)

		releasePartitionReq := &querypb.ReleasePartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
		}
		releasePartitionJob := NewReleasePartitionJob(
			ctx,
			releasePartitionReq,
			suite.dist,
			suite.meta,
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.checkerController,
		)
		suite.scheduler.Add(releasePartitionJob)
		err = releasePartitionJob.Wait()
		suite.NoError(err)
	}

	suite.cluster.ExpectedCalls = lo.Filter(suite.cluster.ExpectedCalls, func(call *mock.Call, _ int) bool {
		return call.Method != "ReleasePartitions"
	})
	suite.cluster.EXPECT().ReleasePartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)
}

func (suite *JobSuite) TestSyncNewCreatedPartition() {
	newPartition := int64(999)

	// test sync new created partition
	suite.loadAll()
	req := &querypb.SyncNewCreatedPartitionRequest{
		CollectionID: suite.collections[0],
		PartitionID:  newPartition,
	}
	job := NewSyncNewCreatedPartitionJob(
		context.Background(),
		req,
		suite.meta,
		suite.cluster,
		suite.broker,
	)
	suite.scheduler.Add(job)
	err := job.Wait()
	suite.NoError(err)
	partition := suite.meta.CollectionManager.GetPartition(newPartition)
	suite.NotNil(partition)
	suite.Equal(querypb.LoadStatus_Loaded, partition.GetStatus())

	// test collection not loaded
	req = &querypb.SyncNewCreatedPartitionRequest{
		CollectionID: int64(888),
		PartitionID:  newPartition,
	}
	job = NewSyncNewCreatedPartitionJob(
		context.Background(),
		req,
		suite.meta,
		suite.cluster,
		suite.broker,
	)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.NoError(err)

	// test collection loaded, but its loadType is loadPartition
	req = &querypb.SyncNewCreatedPartitionRequest{
		CollectionID: suite.collections[1],
		PartitionID:  newPartition,
	}
	job = NewSyncNewCreatedPartitionJob(
		context.Background(),
		req,
		suite.meta,
		suite.cluster,
		suite.broker,
	)
	suite.scheduler.Add(job)
	err = job.Wait()
	suite.NoError(err)
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
				suite.broker,
				suite.cluster,
				suite.targetMgr,
				suite.targetObserver,
				suite.nodeMgr,
			)
			suite.scheduler.Add(job)
			err := job.Wait()
			suite.NoError(err)
			suite.EqualValues(1, suite.meta.GetReplicaNumber(collection))
			suite.True(suite.meta.Exist(collection))
			suite.NotNil(suite.meta.GetCollection(collection))
			suite.NotNil(suite.meta.GetPartitionsByCollection(collection))
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
				suite.broker,
				suite.cluster,
				suite.targetMgr,
				suite.targetObserver,
				suite.nodeMgr,
			)
			suite.scheduler.Add(job)
			err := job.Wait()
			suite.NoError(err)
			suite.EqualValues(1, suite.meta.GetReplicaNumber(collection))
			suite.True(suite.meta.Exist(collection))
			suite.NotNil(suite.meta.GetCollection(collection))
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
			suite.broker,
			suite.cluster,
			suite.targetMgr,
			suite.targetObserver,
			suite.checkerController,
		)
		suite.scheduler.Add(job)
		err := job.Wait()
		suite.NoError(err)
		suite.assertCollectionReleased(collection)
	}
}

func (suite *JobSuite) assertCollectionLoaded(collection int64) {
	suite.True(suite.meta.Exist(collection))
	suite.NotEqual(0, len(suite.meta.ReplicaManager.GetByCollection(collection)))
	for _, channel := range suite.channels[collection] {
		suite.NotNil(suite.targetMgr.GetDmChannel(collection, channel, meta.CurrentTarget))
	}
	for _, segments := range suite.segments[collection] {
		for _, segment := range segments {
			suite.NotNil(suite.targetMgr.GetHistoricalSegment(collection, segment, meta.CurrentTarget))
		}
	}
}

func (suite *JobSuite) assertPartitionLoaded(collection int64, partitionIDs ...int64) {
	suite.True(suite.meta.Exist(collection))
	suite.NotEqual(0, len(suite.meta.ReplicaManager.GetByCollection(collection)))
	for _, channel := range suite.channels[collection] {
		suite.NotNil(suite.targetMgr.GetDmChannel(collection, channel, meta.CurrentTarget))
	}
	for partitionID, segments := range suite.segments[collection] {
		if !lo.Contains(partitionIDs, partitionID) {
			continue
		}
		suite.NotNil(suite.meta.GetPartition(partitionID))
		for _, segment := range segments {
			suite.NotNil(suite.targetMgr.GetHistoricalSegment(collection, segment, meta.CurrentTarget))
		}
	}
}

func (suite *JobSuite) assertCollectionReleased(collection int64) {
	suite.False(suite.meta.Exist(collection))
	suite.Equal(0, len(suite.meta.ReplicaManager.GetByCollection(collection)))
	for _, channel := range suite.channels[collection] {
		suite.Nil(suite.targetMgr.GetDmChannel(collection, channel, meta.CurrentTarget))
	}
	for _, partitions := range suite.segments[collection] {
		for _, segment := range partitions {
			suite.Nil(suite.targetMgr.GetHistoricalSegment(collection, segment, meta.CurrentTarget))
		}
	}
}

func (suite *JobSuite) assertPartitionReleased(collection int64, partitionIDs ...int64) {
	for _, partition := range partitionIDs {
		suite.Nil(suite.meta.GetPartition(partition))
		segments := suite.segments[collection][partition]
		for _, segment := range segments {
			suite.Nil(suite.targetMgr.GetHistoricalSegment(collection, segment, meta.CurrentTarget))
		}
	}
}

func TestJob(t *testing.T) {
	suite.Run(t, new(JobSuite))
}
