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

package observers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// CollectionObserverRGSuite drives CollectionObserver one tick at a time
// against real meta, target and distribution state, so that every assertion
// below is about what the observer did to that state rather than about which
// method a double saw called. The observer's own ticker is deliberately never
// started: Observe/observeTimeout/observeLoadStatus are invoked by hand so a
// tick happens exactly when the test says it does.
type CollectionObserverRGSuite struct {
	suite.Suite

	ctx context.Context

	kv    kv.MetaKv
	store metastore.QueryCoordCatalog

	broker       *meta.MockBroker
	cluster      *session.MockCluster
	proxyManager *proxyutil.MockProxyClientManager
	nodeMgr      *session.NodeManager

	dist              *meta.DistributionManager
	meta              *meta.Meta
	targetMgr         *meta.TargetManager
	targetObserver    *TargetObserver
	checkerController *checkers.CheckerController

	ob *CollectionObserver
}

func (s *CollectionObserverRGSuite) SetupSuite() {
	paramtable.Init()
}

func (s *CollectionObserverRGSuite) SetupTest() {
	s.ctx = context.Background()

	config := GenerateEtcdConfig()
	client, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	s.Require().NoError(err)
	s.kv = etcdkv.NewEtcdKV(client, Params.EtcdCfg.MetaRootPath.GetValue()+"-"+RandomMetaRootPath())
	s.store = querycoord.NewCatalog(s.kv)

	s.nodeMgr = session.NewNodeManager()
	s.dist = meta.NewDistributionManager(s.nodeMgr)
	s.meta = meta.NewMeta(RandomIncrementIDAllocator(), s.store, s.nodeMgr)
	s.broker = meta.NewMockBroker(s.T())
	s.targetMgr = meta.NewTargetManager(s.broker, s.meta)
	s.cluster = session.NewMockCluster(s.T())
	s.proxyManager = proxyutil.NewMockProxyClientManager(s.T())
	s.proxyManager.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.broker.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	s.broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	s.cluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()

	s.targetObserver = NewTargetObserver(s.meta, s.targetMgr, s.dist, s.broker, s.cluster, s.nodeMgr)
	s.checkerController = &checkers.CheckerController{}

	// Built before any collection is registered, so the recovery loop inside
	// the constructor registers no task and every task in these tests is one
	// the test itself asked for.
	s.ob = NewCollectionObserver(s.dist, s.meta, s.targetMgr, s.targetObserver, s.checkerController, s.proxyManager)
	s.Require().Zero(s.ob.loadTasks.Len())

	s.targetObserver.Start()

	// Long enough that nothing in these tests times out by wall clock; every
	// timeout below is produced by explicitly backdating a timestamp.
	paramtable.Get().Save(Params.QueryCoordCfg.LoadTimeoutSeconds.Key, "600")
}

func (s *CollectionObserverRGSuite) TearDownTest() {
	paramtable.Get().Reset(Params.QueryCoordCfg.LoadTimeoutSeconds.Key)
	s.targetObserver.Stop()
	s.kv.Close()
}

const (
	rgA = "rg-a"
	rgB = "rg-b"
)

// registerLoadingCollection puts collectionID into meta as a LoadCollection in
// progress with a single partition, and gives it a next target made of one
// channel plus segmentIDs, all on that channel.
func (s *CollectionObserverRGSuite) registerLoadingCollection(collectionID, partitionID int64, channel string, replicaNumber int32, segmentIDs ...int64) {
	s.Require().NoError(s.meta.PutCollection(s.ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: replicaNumber,
			Status:        querypb.LoadStatus_Loading,
			LoadType:      querypb.LoadType_LoadCollection,
		},
		LoadPercentage: 0,
		CreatedAt:      time.Now(),
	}))
	s.Require().NoError(s.meta.PutPartition(s.ctx, &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			ReplicaNumber: replicaNumber,
			Status:        querypb.LoadStatus_Loading,
		},
		LoadPercentage: 0,
		CreatedAt:      time.Now(),
	}))

	segments := make([]*datapb.SegmentInfo, 0, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		segments = append(segments, &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channel,
		})
	}
	channels := []*datapb.VchannelInfo{{CollectionID: collectionID, ChannelName: channel}}

	s.broker.EXPECT().GetPartitions(mock.Anything, collectionID).Return([]int64{partitionID}, nil).Maybe()
	s.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(channels, segments, nil).Maybe()
	s.Require().NoError(s.targetMgr.UpdateCollectionNextTarget(s.ctx, collectionID))
}

// putReplica adds a replica of collectionID in rgName owning exactly nodeID.
//
// The resource group is registered first: the per-resource-group load
// percentage refuses a group the resource manager does not know, so a replica
// placed in an unregistered group would read as -1 rather than as progress.
func (s *CollectionObserverRGSuite) putReplica(collectionID, replicaID, nodeID int64, rgName string) {
	if rgName != "" && !s.meta.ContainResourceGroup(s.ctx, rgName) {
		_, err := s.meta.AddResourceGroup(s.ctx, rgName, &rgpb.ResourceGroupConfig{
			Requests: &rgpb.ResourceGroupLimit{NodeNum: 0},
			Limits:   &rgpb.ResourceGroupLimit{NodeNum: 100},
		})
		s.Require().NoError(err)
	}
	s.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: nodeID}))
	s.Require().NoError(s.meta.Put(s.ctx, meta.NewReplica(&querypb.Replica{
		ID:            replicaID,
		CollectionID:  collectionID,
		ResourceGroup: rgName,
		Nodes:         []int64{nodeID},
	})))
}

// putDelegator makes nodeID the delegator of channel, holding segmentIDs.
func (s *CollectionObserverRGSuite) putDelegator(collectionID, nodeID int64, channel string, segmentIDs ...int64) {
	segments := make(map[int64]*querypb.SegmentDist, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		segments[segmentID] = &querypb.SegmentDist{NodeID: nodeID, Version: 1}
	}
	s.dist.ChannelDistManager.Update(nodeID, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: channel},
		Node:         nodeID,
		View: &meta.LeaderView{
			ID:           nodeID,
			CollectionID: collectionID,
			Channel:      channel,
			Segments:     segments,
		},
	})
}

// backdateCollectionUpdatedAt simulates a collection whose load was registered
// long ago: exactly the state the shared collection.UpdatedAt is left in once a
// first resource group has finished loading.
func (s *CollectionObserverRGSuite) backdateCollectionUpdatedAt(collectionID int64, age time.Duration) {
	collection := s.meta.GetCollection(s.ctx, collectionID)
	s.Require().NotNil(collection)
	collection.UpdatedAt = time.Now().Add(-age)
}

// backdateTaskWatermark simulates a task whose own progress last moved age ago,
// while sitting at the given percentage.
func (s *CollectionObserverRGSuite) backdateTaskWatermark(key string, percentage int32, age time.Duration) {
	task, ok := s.ob.loadTasks.Get(key)
	s.Require().True(ok)
	task.LastProgress = percentage
	task.LastProgressAt = time.Now().Add(-age)
	s.ob.loadTasks.Insert(key, task)
}

// taskKey finds the key the observer filed the task for (collectionID, rgName)
// under, without the test having to reproduce the key format.
func (s *CollectionObserverRGSuite) taskKey(collectionID int64, rgName string) string {
	found := ""
	s.ob.loadTasks.Range(func(key string, task LoadTask) bool {
		if task.CollectionID == collectionID && task.ResourceGroup == rgName {
			found = key
			return false
		}
		return true
	})
	s.Require().NotEmpty(found)
	return found
}

func (s *CollectionObserverRGSuite) replicaIDsInRG(collectionID int64, rgName string) []int64 {
	ids := make([]int64, 0)
	for _, replica := range s.meta.GetByCollection(s.ctx, collectionID) {
		if replica.GetResourceGroup() == rgName {
			ids = append(ids, replica.GetID())
		}
	}
	return ids
}

// TestUnscopedTaskKeepsCollectionWideTimeout is the equivalence assertion that
// protects upstream on the timeout side. The collection here is fully loaded as
// far as its one resource group is concerned, so the per-resource-group
// watermark would refresh forever and the task would never expire. The task is
// registered with an empty resource group, and must therefore still expire on
// the stale collection.UpdatedAt exactly as it did before resource groups were
// plumbed in.
//
// It also asserts the per-tick resource-group scan produced nothing at all: an
// unscoped task must not cause a single target or distribution read.
//
// Deleting the `if task.ResourceGroup != ""` guard in observeTimeout (i.e.
// sending every task down the watermark branch) leaves the collection loaded and
// fails this test. Deleting the `if task.ResourceGroup == "" { return true }`
// short-circuit in observeResourceGroupProgress fails the nil-map assertion.
func (s *CollectionObserverRGSuite) TestUnscopedTaskKeepsCollectionWideTimeout() {
	s.registerLoadingCollection(100, 10, "100-dmc0", 1, 1, 2)
	s.putReplica(100, 1000, 1, rgA)
	s.putDelegator(100, 1, "100-dmc0", 1, 2) // rg-a carries every target: 100%
	s.backdateCollectionUpdatedAt(100, time.Hour)

	s.ob.LoadCollection(s.ctx, 100, "")
	key := s.taskKey(100, "")

	progress := s.ob.observeResourceGroupProgress(s.ctx)
	s.Nil(progress, "an unscoped task must not trigger any per-resource-group progress read")

	s.ob.observeTimeout(s.ctx, progress)

	s.Nil(s.meta.GetCollection(s.ctx, 100), "unscoped task must still time out on the stale collection.UpdatedAt")
	s.Empty(s.meta.GetByCollection(s.ctx, 100))
	s.False(s.ob.loadTasks.Contain(key))
}

// TestUnscopedTaskCompletionIgnoresResourceGroupProgress is the equivalence
// assertion on the observeLoadStatus side. An unscoped task must decide it is
// finished purely from the partitions' recorded load percentage, and must not
// consult the per-resource-group progress even when an entry for its key is
// present. Both directions are asserted, so the test cannot be satisfied by a
// path that ignores the partition state either.
//
// Deleting the `if task.ResourceGroup != ""` guard around
// `loaded = progress[traceID] >= 100` flips both halves at once.
func (s *CollectionObserverRGSuite) TestUnscopedTaskCompletionIgnoresResourceGroupProgress() {
	s.registerLoadingCollection(101, 11, "101-dmc0", 1, 1, 2)
	s.putReplica(101, 1010, 1, rgA)
	// No delegator: the partition is at 0%.

	s.ob.LoadCollection(s.ctx, 101, "")
	key := s.taskKey(101, "")

	s.ob.observeLoadStatus(s.ctx, map[string]int32{key: 100})
	s.True(s.ob.loadTasks.Contain(key),
		"unscoped task must stay open while its partition is unloaded, whatever the resource group figure says")

	s.Require().NoError(s.meta.UpdatePartitionLoadPercent(s.ctx, 11, 100))

	s.ob.observeLoadStatus(s.ctx, map[string]int32{key: 0})
	s.False(s.ob.loadTasks.Contain(key),
		"unscoped task must finish once its partition reports 100, whatever the resource group figure says")
}

// TestLoadCollectionKeysTasksPerResourceGroup pins the two properties the task
// key has to have: an unscoped registration keeps byte-for-byte the key it had
// before resource groups existed, and two registrations for two resource groups
// on the same collection are two tasks rather than one overwriting the other.
//
// Deleting qualifyTaskKeyByResourceGroup collapses the second half to a single
// task; making it qualify unconditionally breaks the first half.
func (s *CollectionObserverRGSuite) TestLoadCollectionKeysTasksPerResourceGroup() {
	s.ob.LoadCollection(s.ctx, 102, "")
	s.True(s.ob.loadTasks.Contain("LoadCollection_102"), "unscoped task key must be unchanged")

	s.ob.LoadCollection(s.ctx, 103, rgA)
	s.ob.LoadCollection(s.ctx, 103, rgB)
	s.Equal(3, s.ob.loadTasks.Len(), "two resource groups on one collection must be two tasks")

	taskA, ok := s.ob.loadTasks.Get(s.taskKey(103, rgA))
	s.Require().True(ok)
	s.EqualValues(-1, taskA.LastProgress, "a fresh task must start below any real percentage")
	s.False(taskA.LastProgressAt.IsZero(), "a fresh task must start its own clock at registration")
}

// TestFreshResourceGroupTaskSurvivesStaleCollectionUpdatedAt reproduces the
// production incident. Collection 201 was loaded into rg-a long ago, so its
// collection.UpdatedAt is an hour in the past; rg-b is then asked to load the
// same collection and its task is registered. On the very first observer tick
// after that registration, rg-b's brand new replica must still be there.
//
// Collection 200 is the control: identical world state -- same age, same two
// replicas, same absence of progress in rg-b -- with the only difference being
// that its task carries no resource group. It is torn down on that same first
// tick, which is what makes this a real reproduction rather than a test that
// would pass on an observer that never times anything out.
func (s *CollectionObserverRGSuite) TestFreshResourceGroupTaskSurvivesStaleCollectionUpdatedAt() {
	for _, collectionID := range []int64{200, 201} {
		partitionID := collectionID - 180
		channel := "dmc0"
		s.registerLoadingCollection(collectionID, partitionID, channel, 2, collectionID*10+1, collectionID*10+2)
		// rg-a loaded long ago and carries everything.
		s.putReplica(collectionID, collectionID*100+1, collectionID, rgA)
		s.putDelegator(collectionID, collectionID, channel, collectionID*10+1, collectionID*10+2)
		// rg-b was just added and carries nothing yet.
		s.putReplica(collectionID, collectionID*100+2, collectionID+1, rgB)
		s.backdateCollectionUpdatedAt(collectionID, time.Hour)
	}

	s.ob.LoadCollection(s.ctx, 200, "") // control: pre-change behavior
	s.ob.LoadCollection(s.ctx, 201, rgB)
	scopedKey := s.taskKey(201, rgB)

	s.ob.Observe(s.ctx)

	// Control: the stale collection.UpdatedAt does expire an unscoped task on
	// the first tick, so the scenario really is a timeout trap.
	s.Nil(s.meta.GetCollection(s.ctx, 200), "control: unscoped task must be expired by the stale UpdatedAt")
	s.Empty(s.replicaIDsInRG(200, rgB), "control: rg-b's replica is collateral damage of the collection-wide timeout")

	// The fix: rg-b's task judges itself on its own watermark, which was seeded
	// at registration, so nothing is torn down.
	s.NotNil(s.meta.GetCollection(s.ctx, 201), "resource-group-scoped task must not expire on a sibling's UpdatedAt")
	s.Len(s.replicaIDsInRG(201, rgB), 1, "the replica just spawned for rg-b must survive its first tick")
	s.Len(s.replicaIDsInRG(201, rgA), 1, "rg-a must be untouched")
	s.True(s.ob.loadTasks.Contain(scopedKey), "rg-b's task must still be running")

	// And it survives further ticks on which it still has not moved: loads take
	// many ticks, so "no progress since the last tick" is not a timeout. Only
	// LoadTimeoutSeconds without progress is, which is asserted separately by
	// TestStalledResourceGroupTimesOutWithoutTouchingSiblings.
	for i := 0; i < 3; i++ {
		s.ob.Observe(s.ctx)
	}
	s.Len(s.replicaIDsInRG(201, rgB), 1, "a task well inside the load timeout must survive ticks without progress")
	s.True(s.ob.loadTasks.Contain(scopedKey))
}

// TestFullyLoadedResourceGroupNeverTimesOut asserts that a resource group at
// 100 is never torn down however long its task sits there. Each iteration
// backdates the watermark by an hour against a ten minute timeout, so a task
// that did not refresh at 100 would be expired on the first pass.
//
// Deleting the `|| percentage >= 100` disjunct in observeResourceGroupTimeout
// makes the first iteration release a replica that is fully loaded and serving.
func (s *CollectionObserverRGSuite) TestFullyLoadedResourceGroupNeverTimesOut() {
	s.registerLoadingCollection(300, 30, "300-dmc0", 1, 1, 2)
	s.putReplica(300, 3000, 1, rgA)
	s.putDelegator(300, 1, "300-dmc0", 1, 2)

	s.ob.LoadCollection(s.ctx, 300, rgA)
	key := s.taskKey(300, rgA)

	progress := s.ob.observeResourceGroupProgress(s.ctx)
	s.Require().EqualValues(100, progress[key])

	for i := 0; i < 3; i++ {
		s.backdateTaskWatermark(key, 100, time.Hour)
		s.ob.observeTimeout(s.ctx, progress)

		s.Len(s.replicaIDsInRG(300, rgA), 1, "a fully loaded resource group must never be released by load timeout")
		s.NotNil(s.meta.GetCollection(s.ctx, 300))
		s.Require().True(s.ob.loadTasks.Contain(key))

		task, _ := s.ob.loadTasks.Get(key)
		s.WithinDuration(time.Now(), task.LastProgressAt, time.Minute,
			"a fully loaded resource group must keep refreshing its watermark")
	}
}

// TestStalledResourceGroupTimesOutWithoutTouchingSiblings asserts the other
// side of the watermark: a resource group whose progress has genuinely not
// moved for longer than the load timeout is released, and only it is. rg-a on
// the same collection is fully loaded and must come through untouched, as must
// the collection itself.
//
// Deleting the releaseResourceGroupOnTimeout call leaves rg-b's replica in
// place; dropping the resource group filter inside it takes rg-a's replica down
// too; deleting the `now.Before(...)` guard would expire the healthy cases
// covered by the two tests above.
func (s *CollectionObserverRGSuite) TestStalledResourceGroupTimesOutWithoutTouchingSiblings() {
	s.registerLoadingCollection(400, 40, "400-dmc0", 2, 1, 2)
	s.putReplica(400, 4001, 1, rgA)
	s.putDelegator(400, 1, "400-dmc0", 1, 2)
	s.putReplica(400, 4002, 2, rgB) // rg-b carries nothing and never will

	s.ob.LoadCollection(s.ctx, 400, rgB)
	key := s.taskKey(400, rgB)

	progress := s.ob.observeResourceGroupProgress(s.ctx)
	s.Require().EqualValues(0, progress[key])
	s.backdateTaskWatermark(key, 0, time.Hour)

	s.ob.observeTimeout(s.ctx, progress)

	s.Empty(s.replicaIDsInRG(400, rgB), "a stalled resource group must be released")
	s.Len(s.replicaIDsInRG(400, rgA), 1, "a sibling resource group must survive its neighbour's timeout")
	s.NotNil(s.meta.GetCollection(s.ctx, 400), "the collection must survive while any resource group holds it")
	s.EqualValues(1, s.meta.GetCollection(s.ctx, 400).GetReplicaNumber(),
		"releasing the timed-out resource group's replicas must write ReplicaNumber back down, "+
			"or the collection-wide load percentage keeps a denominator counting replicas that no longer exist")
	s.False(s.ob.loadTasks.Contain(key))
}

// TestLastResourceGroupTimeoutReleasesCollection asserts the symmetric half of
// the teardown: when the timed-out resource group was the only one left, the
// collection meta goes away too, so a failed load does not leave a collection
// pinned in Loading forever.
//
// Deleting the `if remaining := ...; len(remaining) == 0` block leaves
// collection 500 in meta with no replica behind it.
func (s *CollectionObserverRGSuite) TestLastResourceGroupTimeoutReleasesCollection() {
	s.registerLoadingCollection(500, 50, "500-dmc0", 1, 1, 2)
	s.putReplica(500, 5001, 1, rgB)
	// A delegator of this collection reports from a node that is not in rg-b's
	// replica, so the distribution has something to say about the collection
	// while rg-b still carries none of its targets: a real 0, which is what
	// makes this a stall rather than the unknown of a coordinator that has just
	// restarted.
	s.putDelegator(500, 2, "500-dmc0", 1, 2)

	s.ob.LoadCollection(s.ctx, 500, rgB)
	key := s.taskKey(500, rgB)
	s.backdateTaskWatermark(key, 0, time.Hour)

	s.ob.observeTimeout(s.ctx, s.ob.observeResourceGroupProgress(s.ctx))

	s.Empty(s.meta.GetByCollection(s.ctx, 500))
	s.Nil(s.meta.GetCollection(s.ctx, 500), "the last resource group's timeout must release the collection")
	s.False(s.ob.loadTasks.Contain(key))
}

// TestLoadTimeoutNeverUnloadsAServingCollection pins the hard limit on the
// teardown. The resource group here really is stalled -- its delegator reports
// the channel and never picks the segments up, so the percentage is a genuine,
// constant 33 -- and it is the collection's only group. Releasing it would take
// the collection's last replicas and its load meta with them, and every query
// would fail until an operator noticed and reloaded.
//
// A load timeout may shrink an expansion that never came up. It may not unload
// a collection that is serving, however the percentage was arrived at. The task
// is dropped instead, and the collection keeps the replicas it has.
func (s *CollectionObserverRGSuite) TestLoadTimeoutNeverUnloadsAServingCollection() {
	s.registerLoadingCollection(1100, 1101, "1100-dmc0", 1, 11001, 11002)
	s.putReplica(1100, 110001, 21, rgA)
	s.putDelegator(1100, 21, "1100-dmc0")
	s.markCollectionLoaded(1100, 1101)

	s.ob.LoadCollection(s.ctx, 1100, rgA)
	key := s.taskKey(1100, rgA)
	s.Require().EqualValues(33, s.ob.observeResourceGroupProgress(s.ctx)[key],
		"the group must be reporting a real, stalled percentage for this to be a timeout at all")
	s.backdateTaskWatermark(key, 33, time.Hour)

	s.ob.Observe(s.ctx)

	s.Len(s.replicaIDsInRG(1100, rgA), 1,
		"a load timeout must never take the last replicas of a collection that is serving")
	s.NotNil(s.meta.GetCollection(s.ctx, 1100), "nor its load meta")
	s.Equal(querypb.LoadStatus_Loaded, s.meta.GetCollection(s.ctx, 1100).GetStatus())
	s.False(s.ob.loadTasks.Contain(key), "the task is dropped instead, so the teardown is not retried forever")
}

// TestResourceGroupWatermarkTracksRealProgress asserts the watermark is written
// back to the task and follows the resource group's actual progress, which is
// what makes the timeout in TestStalledResourceGroupTimesOutWithoutTouchingSiblings
// mean "stalled" rather than "old". A task registered at -1 records 50 once its
// replica has picked up half of the targets, then 100 once it has the rest.
//
// Deleting the `ob.loadTasks.Insert(key, task)` write-back leaves LastProgress
// at its seeded -1 forever.
func (s *CollectionObserverRGSuite) TestResourceGroupWatermarkTracksRealProgress() {
	s.registerLoadingCollection(600, 60, "600-dmc0", 1, 1, 2, 3)
	s.putReplica(600, 6001, 1, rgA)
	s.putDelegator(600, 1, "600-dmc0", 1) // channel + 1 of 3 segments = 2 of 4

	s.ob.LoadCollection(s.ctx, 600, rgA)
	key := s.taskKey(600, rgA)

	s.ob.observeTimeout(s.ctx, s.ob.observeResourceGroupProgress(s.ctx))
	task, ok := s.ob.loadTasks.Get(key)
	s.Require().True(ok)
	s.EqualValues(50, task.LastProgress, "the watermark must record the resource group's real progress")
	halfwayAt := task.LastProgressAt

	// The replica picks up the rest; backdate first so a refreshed watermark is
	// distinguishable from an untouched one.
	s.backdateTaskWatermark(key, 50, time.Hour)
	s.putDelegator(600, 1, "600-dmc0", 1, 2, 3)

	s.ob.observeTimeout(s.ctx, s.ob.observeResourceGroupProgress(s.ctx))
	task, ok = s.ob.loadTasks.Get(key)
	s.Require().True(ok)
	s.EqualValues(100, task.LastProgress, "the watermark must follow the resource group forward")
	s.True(task.LastProgressAt.After(halfwayAt.Add(-time.Hour)), "advancing progress must refresh the watermark")
}

// TestScopedTaskFinishesOnItsOwnResourceGroup asserts the completion side of
// resource-group scoping, in both directions at once. rg-a carries every target
// while rg-b carries none, so the partition's recorded percentage -- which sums
// over both replicas -- sits at 50 and would, under the rule it replaces, hold
// rg-a's task open until rg-b caught up. rg-a's task must finish anyway, and
// rg-b's must stay open even though a sibling is done.
//
// Deleting the `loaded = progress[traceID] >= 100 && !targetNotReady`
// assignment leaves rg-a's task open, which is the interference this change
// exists to remove.
func (s *CollectionObserverRGSuite) TestScopedTaskFinishesOnItsOwnResourceGroup() {
	s.registerLoadingCollection(700, 70, "700-dmc0", 2, 1, 2)
	s.putReplica(700, 7001, 1, rgA)
	s.putDelegator(700, 1, "700-dmc0", 1, 2)
	s.putReplica(700, 7002, 2, rgB)

	s.ob.LoadCollection(s.ctx, 700, rgA)
	s.ob.LoadCollection(s.ctx, 700, rgB)
	keyA := s.taskKey(700, rgA)
	keyB := s.taskKey(700, rgB)

	progress := s.ob.observeResourceGroupProgress(s.ctx)
	s.Require().EqualValues(100, progress[keyA])
	s.Require().EqualValues(0, progress[keyB])

	s.ob.observeLoadStatus(s.ctx, progress)

	s.EqualValues(50, s.meta.GetPartitionLoadPercentage(s.ctx, 70),
		"the collection-wide figure really is below 100, so this is not a trivially satisfied case")
	s.False(s.ob.loadTasks.Contain(keyA), "a loaded resource group must not wait for a lagging sibling")
	s.True(s.ob.loadTasks.Contain(keyB), "a lagging resource group must not be finished by a loaded sibling")
}

// TestScopedTaskWaitsForCurrentTargetPromotion asserts that finishing on the
// per-resource-group percentage did not lose the guarantee the replaced rule
// gave for free. That percentage is measured against the next target, so it
// reaches 100 before targetObserver.Check has promoted the current target. A
// task that finished at that moment would leave the collection below Loaded
// with nothing left to drive the promotion.
//
// Deleting the `&& !targetNotReady` term makes the first half finish the task
// while the current target is still absent.
func (s *CollectionObserverRGSuite) TestScopedTaskWaitsForCurrentTargetPromotion() {
	s.registerLoadingCollection(800, 80, "800-dmc0", 1, 1, 2)
	s.putReplica(800, 8001, 1, rgA)
	s.putDelegator(800, 1, "800-dmc0", 1, 2)

	s.ob.LoadCollection(s.ctx, 800, rgA)
	key := s.taskKey(800, rgA)
	s.Require().EqualValues(100, s.ob.observeResourceGroupProgress(s.ctx)[key])
	s.Require().False(s.targetMgr.IsCurrentTargetExist(s.ctx, 800, 80))

	s.ob.observeLoadStatus(s.ctx, s.ob.observeResourceGroupProgress(s.ctx))
	s.True(s.ob.loadTasks.Contain(key),
		"a scoped task must not finish while the current target promotion is still pending")

	// Promote the current target, then refill the next target, which is the
	// order TargetObserver drives these two in.
	s.targetMgr.UpdateCollectionCurrentTarget(s.ctx, 800)
	s.Require().NoError(s.targetMgr.UpdateCollectionNextTarget(s.ctx, 800))
	s.Require().True(s.targetMgr.IsCurrentTargetExist(s.ctx, 800, 80))

	s.ob.observeLoadStatus(s.ctx, s.ob.observeResourceGroupProgress(s.ctx))
	s.False(s.ob.loadTasks.Contain(key), "a scoped task must finish once its resource group is loaded and promoted")
	s.EqualValues(100, s.meta.GetPartitionLoadPercentage(s.ctx, 80))
}

func TestCollectionObserverRG(t *testing.T) {
	suite.Run(t, new(CollectionObserverRGSuite))
}

// TestResourceGroupWatermarkRefreshesOnRegression asserts that a load which
// goes backwards and then forwards again is not torn down. A resource group
// can legitimately regress -- a delegator restarts, or a freshly flushed
// segment enters the next target -- and the percentage then climbs back
// through values it has already visited. A watermark that only ever ratchets
// up would stop refreshing the moment the load dropped below its old peak, and
// the load timeout would release a resource group that is making progress the
// whole time.
func (s *CollectionObserverRGSuite) TestResourceGroupWatermarkRefreshesOnRegression() {
	s.registerLoadingCollection(900, 90, "900-dmc0", 1, 1, 2)
	s.putReplica(900, 9001, 1, rgA)

	s.ob.LoadCollection(s.ctx, 900, rgA)
	key := s.taskKey(900, rgA)

	// The group peaked at 94, then dropped to 70: the drop is progress moving,
	// so the clock restarts on it.
	s.backdateTaskWatermark(key, 94, time.Hour)
	task, ok := s.ob.loadTasks.Get(key)
	s.Require().True(ok)
	s.ob.observeResourceGroupTimeout(s.ctx, key, task, 70)

	task, ok = s.ob.loadTasks.Get(key)
	s.Require().True(ok)
	s.EqualValues(70, task.LastProgress, "a regression must be recorded, not ignored")
	s.WithinDuration(time.Now(), task.LastProgressAt, time.Minute,
		"a regression is the load moving, so it must refresh the watermark")
	s.Len(s.replicaIDsInRG(900, rgA), 1)

	// Climbing back to 80 -- still below the old peak of 94 -- refreshes it too.
	s.backdateTaskWatermark(key, 70, time.Hour)
	task, ok = s.ob.loadTasks.Get(key)
	s.Require().True(ok)
	s.ob.observeResourceGroupTimeout(s.ctx, key, task, 80)

	task, ok = s.ob.loadTasks.Get(key)
	s.Require().True(ok)
	s.EqualValues(80, task.LastProgress)
	s.WithinDuration(time.Now(), task.LastProgressAt, time.Minute,
		"a rise below the old peak is still the load moving")
	s.Len(s.replicaIDsInRG(900, rgA), 1,
		"a resource group that keeps moving must never be released by the load timeout")
	s.True(s.ob.loadTasks.Contain(key))
}

// TestUnreadableResourceGroupIsNeverTornDown drives the whole tick -- the scan
// that publishes the percentage and the timeout that consumes it -- against a
// resource group whose percentage cannot be read at all. Every tick backdates
// the watermark by an hour against a ten minute timeout, so a task that let the
// clock run would be released on the first pass.
//
// The group here is not registered with the resource manager, which is one of
// the ways the read fails outright; the collection itself has reported, so this
// is the read-failure half of "unknown" rather than the nothing-reported-yet
// half.
func (s *CollectionObserverRGSuite) TestUnreadableResourceGroupIsNeverTornDown() {
	s.registerLoadingCollection(901, 91, "901-dmc0", 1, 911, 912)
	s.putReplica(901, 9010, 11, rgA)
	s.putDelegator(901, 11, "901-dmc0", 911, 912)

	s.ob.LoadCollection(s.ctx, 901, "no-such-rg")
	key := s.taskKey(901, "no-such-rg")
	s.EqualValues(-1, s.ob.observeResourceGroupProgress(s.ctx)[key],
		"a percentage that could not be read must be published as unknown, not as a figure")

	for i := 0; i < 3; i++ {
		s.backdateTaskWatermark(key, 60, time.Hour)

		s.ob.Observe(s.ctx)

		s.Require().True(s.ob.loadTasks.Contain(key),
			"a resource group nobody could read must not be released by the load timeout")
		task, ok := s.ob.loadTasks.Get(key)
		s.Require().True(ok)
		s.EqualValues(60, task.LastProgress, "an unreadable tick must not overwrite the last known percentage")
		s.WithinDuration(time.Now(), task.LastProgressAt, time.Minute,
			"an unreadable tick pauses the clock rather than letting it run")
		s.Len(s.replicaIDsInRG(901, rgA), 1)
		s.NotNil(s.meta.GetCollection(s.ctx, 901))
	}
}

// TestNothingReportedYetIsUnknown covers the other half of "unknown": a
// collection no QueryNode has reported on since this coordinator started. Every
// resource group of it would read 0 -- "carries none of its targets" -- and 0
// there is a guess, not a measurement. Once a delegator does report, the figure
// is real and the clock starts from that tick.
func (s *CollectionObserverRGSuite) TestNothingReportedYetIsUnknown() {
	s.registerLoadingCollection(902, 92, "902-dmc0", 1, 921, 922)
	s.putReplica(902, 9020, 12, rgA)
	// No delegator anywhere: the distribution says nothing about collection 902.

	s.ob.LoadCollection(s.ctx, 902, rgA)
	key := s.taskKey(902, rgA)
	s.EqualValues(-1, s.ob.observeResourceGroupProgress(s.ctx)[key],
		"with nothing reported for the collection, the percentage is unknown rather than 0")

	s.backdateTaskWatermark(key, 0, time.Hour)
	s.ob.Observe(s.ctx)
	s.Require().True(s.ob.loadTasks.Contain(key),
		"a resource group nothing has reported on must not be released by the load timeout")
	s.Len(s.replicaIDsInRG(902, rgA), 1)

	// One delegator reports, carrying the channel and one of the two segments.
	s.putDelegator(902, 12, "902-dmc0", 921)

	s.ob.Observe(s.ctx)

	task, ok := s.ob.loadTasks.Get(key)
	s.Require().True(ok)
	s.EqualValues(66, task.LastProgress, "the first informative observation is recorded as it is")
	s.WithinDuration(time.Now(), task.LastProgressAt, time.Minute,
		"the timeout is measured from the first tick that learned something")
}
