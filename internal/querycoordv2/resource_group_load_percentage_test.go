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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// rgLoadPercentageFixture wires up the same trio of read-only stores
// GetLoadPercentageByResourceGroup composes over -- ReplicaManager,
// TargetManager, and DistributionManager -- without touching etcd or any
// other real backend, mirroring the in-memory-only style already used by
// TargetManagerSuite and ServiceSuite in this package family.
type rgLoadPercentageFixture struct {
	meta      *meta.Meta
	targetMgr *meta.TargetManager
	dist      *meta.DistributionManager
	broker    *meta.MockBroker
}

func newRGLoadPercentageFixture(t *testing.T) *rgLoadPercentageFixture {
	paramtable.Init() // AddResourceGroup reads the resource-group quota
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.On("SaveReplica", mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.On("SaveReplica", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.On("SaveResourceGroup", mock.Anything, mock.Anything).Return(nil).Maybe()

	nodeMgr := session.NewNodeManager()
	m := &meta.Meta{
		CollectionManager: meta.NewCollectionManager(catalog),
		ReplicaManager:    meta.NewReplicaManager(params.RandomIncrementIDAllocator(), catalog),
		// The surfaces validate rgName against the ResourceManager before
		// anything else, so the fixture carries one; putReplica registers
		// the group it homes a replica in, and putResourceGroup registers a
		// group that deliberately holds nothing.
		ResourceManager: meta.NewResourceManager(catalog, nodeMgr),
	}
	broker := meta.NewMockBroker(t)

	return &rgLoadPercentageFixture{
		meta:      m,
		targetMgr: meta.NewTargetManager(broker, m),
		dist:      meta.NewDistributionManager(nodeMgr),
		broker:    broker,
	}
}

// server builds a *Server wired to this fixture's stores, ready to call
// GetLoadPercentageByResourceGroup on.
// server builds a *Server wired to this fixture's stores. The status has to
// be set explicitly: GetLoadPercentageByResourceGroup gates on
// merr.CheckHealthy(s.State()), and a zero Server reports
// StateCode_Initializing.
func (f *rgLoadPercentageFixture) server() *Server {
	s := &Server{meta: f.meta, targetMgr: f.targetMgr, dist: f.dist}
	s.status.Store(int32(commonpb.StateCode_Healthy))
	return s
}

// freeFn calls utils.LoadPercentageByResourceGroup with nothing but the three
// stores CollectionObserver already holds -- no *Server anywhere. The tests
// that use it therefore double as a compile-time statement of the point of
// the split: this figure is reachable from the observer's dependency set.
func (f *rgLoadPercentageFixture) freeFn(ctx context.Context, collectionID int64, rgName string) (int32, error) {
	return utils.LoadPercentageByResourceGroup(ctx, f.meta, f.targetMgr, f.dist, collectionID, rgName)
}

// putTarget registers collectionID as loaded with a single partition, and
// gives it a next target containing one channel and the given segments, all
// on that channel. This is what GetSealedSegmentsByCollection and
// GetDmChannelsByCollection (both read with meta.NextTargetFirst) will
// report. NextTargetFirst, not NextTarget: replicaLoadPercentage reads it
// that way on purpose, and TestGetLoadPercentageByResourceGroup_SurvivesTargetPromotion
// exists to forbid changing it back.
func (f *rgLoadPercentageFixture) putTarget(t *testing.T, collectionID, partitionID int64, channelName string, segmentIDs ...int64) {
	ctx := context.Background()
	require.NoError(t, f.meta.PutCollectionWithoutSave(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: collectionID},
	}))
	require.NoError(t, f.meta.PutPartitionWithoutSave(ctx, &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{CollectionID: collectionID, PartitionID: partitionID},
	}))

	segmentInfos := make([]*datapb.SegmentInfo, 0, len(segmentIDs))
	for _, segID := range segmentIDs {
		segmentInfos = append(segmentInfos, &datapb.SegmentInfo{
			ID:            segID,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channelName,
		})
	}
	vChannel := &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: channelName}

	f.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return([]*datapb.VchannelInfo{vChannel}, segmentInfos, nil).Once()
	require.NoError(t, f.targetMgr.UpdateCollectionNextTarget(ctx, collectionID))
}

// putReplica registers a replica of collectionID in rgName, homed on nodeID
// (i.e. nodeID is one of the replica's own nodes). nodeID also serves as the
// replica's ID, which is fine here since every replica in a single test uses
// a distinct node; ReplicaManager indexes replicas by ID, so reusing the
// zero value across more than one replica in the same test would make the
// second Put silently clobber the first.
func (f *rgLoadPercentageFixture) putReplica(t *testing.T, collectionID, nodeID int64, rgName string) {
	f.putResourceGroup(t, rgName)
	replica := meta.NewReplica(&querypb.Replica{
		ID:            nodeID,
		CollectionID:  collectionID,
		ResourceGroup: rgName,
		Nodes:         []int64{nodeID},
	})
	require.NoError(t, f.meta.Put(context.Background(), replica))
}

// putResourceGroup registers rgName with the ResourceManager, idempotently,
// so that the surfaces' existence check passes for it. A group registered
// here and never given a replica is the "exists but holds nothing" state
// that must keep answering -1 rather than ErrResourceGroupNotFound.
func (f *rgLoadPercentageFixture) putResourceGroup(t *testing.T, rgName string) {
	t.Helper()
	putResourceGroup(t, f.meta, rgName)
}

// putResourceGroup is the shared body of the two fixtures' helpers.
func putResourceGroup(t *testing.T, m *meta.Meta, rgName string) {
	t.Helper()
	if m.ContainResourceGroup(context.Background(), rgName) {
		return
	}
	_, err := m.AddResourceGroup(context.Background(), rgName, &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 0},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 0},
	})
	require.NoError(t, err)
}

// putDelegator records nodeID as the delegator for channelName, holding
// segmentIDs. Passing no segmentIDs still marks the channel itself as
// watched by nodeID, without any segment held yet.
func (f *rgLoadPercentageFixture) putDelegator(collectionID, nodeID int64, channelName string, segmentIDs ...int64) {
	segments := make(map[int64]*querypb.SegmentDist, len(segmentIDs))
	for _, segID := range segmentIDs {
		segments[segID] = &querypb.SegmentDist{NodeID: nodeID, Version: 1}
	}
	channel := &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: channelName},
		View: &meta.LeaderView{
			ID:           nodeID,
			CollectionID: collectionID,
			Channel:      channelName,
			Segments:     segments,
		},
	}
	f.dist.ChannelDistManager.Update(nodeID, channel)
}

// TestGetLoadPercentageByResourceGroup_NoReplicaOnRG asserts that a
// collection with no replica in the requested resource group reports -1,
// even though the collection is fully loaded elsewhere. Deleting the
// "if len(replicas) == 0 { return -1, nil }" guard in
// GetLoadPercentageByResourceGroup would make this test fall through to the
// percentage computation with an empty replica set instead.
func TestGetLoadPercentageByResourceGroup_NoReplicaOnRG(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 100, 1000, "100-dmc0", 1, 2)
	f.putReplica(t, 100, 10, "rg-other")
	f.putDelegator(100, 10, "100-dmc0", 1, 2)
	f.putResourceGroup(t, "rg-target") // exists, but nothing of collection 100 lives in it

	percentage, err := f.server().GetLoadPercentageByResourceGroup(context.Background(), 100, "rg-target")

	assert.NoError(t, err)
	assert.EqualValues(t, -1, percentage)
}

// TestGetLoadPercentageByResourceGroup_TwoResourceGroupsDiffer is the crux of
// this rework: two resource groups on the same collection, one whose replica
// carries every current target and one whose replica carries none, must be
// reported at their own real progress -- 100 and 0 -- not the collection-wide
// average (50) the old, CalculateLoadPercentage-based implementation would
// have given to both. Deleting the per-replica restriction inside
// replicaLoadPercentage (i.e. checking "replica.Contains(delegator.Node)"
// instead of aggregating over every replica) collapses both results back to
// the same number.
func TestGetLoadPercentageByResourceGroup_TwoResourceGroupsDiffer(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 200, 2000, "200-dmc0", 1, 2)

	// rg-full's replica carries the channel and both segments.
	f.putReplica(t, 200, 20, "rg-full")
	f.putDelegator(200, 20, "200-dmc0", 1, 2)

	// rg-empty's replica exists but its node carries nothing at all.
	f.putReplica(t, 200, 21, "rg-empty")

	full, err := f.server().GetLoadPercentageByResourceGroup(context.Background(), 200, "rg-full")
	assert.NoError(t, err)
	assert.EqualValues(t, 100, full)

	empty, err := f.server().GetLoadPercentageByResourceGroup(context.Background(), 200, "rg-empty")
	assert.NoError(t, err)
	assert.EqualValues(t, 0, empty)

	assert.NotEqual(t, full, empty)
}

// TestGetLoadPercentageByResourceGroup_ZeroVsAbsent asserts the two outcomes
// the doc comment calls out as needing to stay distinguishable: a resource
// group that holds a replica but no segments yet (0) versus a resource group
// that is not on the collection at all (-1). Deleting the
// "if len(replicas) == 0 { return -1, nil }" guard would make the second
// case fall through to the percentage computation with an empty replica set,
// where the loop never runs and the percentage keeps its initial value: the
// function would answer 100 -- "fully loaded" for a resource group that
// holds nothing -- which is worse than a panic because nothing reports it;
// deleting the
// "if targetNum == 0 { return 0 }" guard is exercised by a separate test
// below, since this test's target is non-empty.
func TestGetLoadPercentageByResourceGroup_ZeroVsAbsent(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 300, 3000, "300-dmc0", 1, 2)
	f.putReplica(t, 300, 30, "rg-present")
	f.putResourceGroup(t, "rg-absent") // exists, holds nothing: the -1 row, not the 300 one
	// No putDelegator call: rg-present's replica has not picked up
	// anything from the target yet.

	present, err := f.server().GetLoadPercentageByResourceGroup(context.Background(), 300, "rg-present")
	assert.NoError(t, err)
	assert.EqualValues(t, 0, present)

	absent, err := f.server().GetLoadPercentageByResourceGroup(context.Background(), 300, "rg-absent")
	assert.NoError(t, err)
	assert.EqualValues(t, -1, absent)

	assert.NotEqual(t, present, absent)
}

// TestGetLoadPercentageByResourceGroup_ChannelWatchedNoSegments asserts that
// channel targets are counted alongside segment targets, matching what
// CollectionObserver treats as the load target. The replica's node watches
// the one channel target but holds none of the one segment target, so the
// expected figure is 50 (1 of 2 targets). Deleting either the channel-loop's
// "loadedCount++" or the "channelTargets" term inside "targetNum := ..."
// changes this to 0 or 100 respectively.
func TestGetLoadPercentageByResourceGroup_ChannelWatchedNoSegments(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 400, 4000, "400-dmc0", 1)
	f.putReplica(t, 400, 40, "rg-target")
	f.putDelegator(400, 40, "400-dmc0") // watching the channel, no segments

	percentage, err := f.server().GetLoadPercentageByResourceGroup(context.Background(), 400, "rg-target")

	assert.NoError(t, err)
	assert.EqualValues(t, 50, percentage)
}

// TestGetLoadPercentageByResourceGroup_NoTargetYet asserts that a replica in
// the requested resource group, on a collection whose next target has not
// been computed yet (targetNum == 0), reports 0 without panicking. Deleting
// the "if targetNum == 0 { return 0 }" guard in replicaLoadPercentage
// turns this into an integer-divide-by-zero panic.
func TestGetLoadPercentageByResourceGroup_NoTargetYet(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	ctx := context.Background()
	require.NoError(t, f.meta.PutCollectionWithoutSave(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 500},
	}))
	// The partition record is what makes the collection count as registered:
	// the registration test is CalculateLoadPercentage, which needs a
	// non-empty partition set. Without it this fixture would land in the
	// not-loaded branch and test nothing about the divide-by-zero guard.
	require.NoError(t, f.meta.PutPartitionWithoutSave(ctx, &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{CollectionID: 500, PartitionID: 5000},
	}))
	f.putReplica(t, 500, 50, "rg-target")

	assert.NotPanics(t, func() {
		percentage, err := f.server().GetLoadPercentageByResourceGroup(ctx, 500, "rg-target")
		assert.NoError(t, err)
		assert.EqualValues(t, 0, percentage)
	})
}

// TestGetLoadPercentageByResourceGroup_MultipleReplicasSameRG asserts that
// when a resource group holds more than one replica of the collection (Spawn
// allows replicaNumInRG[rg] > 1), the reported figure is the minimum across
// those replicas, not the first one found or their average. Deleting the
// "if p := ...; p < percentage" comparison (replacing it with, say, always
// taking the last replica's figure) would report 100 here instead of 0,
// because the fully-loaded replica is inserted before the empty one.
func TestGetLoadPercentageByResourceGroup_MultipleReplicasSameRG(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 600, 6000, "600-dmc0", 1, 2)

	f.putReplica(t, 600, 60, "rg-shared")
	f.putDelegator(600, 60, "600-dmc0", 1, 2) // this replica: fully loaded

	f.putReplica(t, 600, 61, "rg-shared")
	// this replica: nothing loaded

	percentage, err := f.server().GetLoadPercentageByResourceGroup(context.Background(), 600, "rg-shared")

	assert.NoError(t, err)
	assert.EqualValues(t, 0, percentage)
}

// seedFailedLoadCache installs a fresh GlobalFailedLoadCache holding err for
// collectionID, and restores the previous cache when the test ends so the
// package-global state does not leak into other tests in this package.
func seedFailedLoadCache(t *testing.T, collectionID int64, err error) {
	t.Helper()
	prev := meta.GlobalFailedLoadCache
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	meta.GlobalFailedLoadCache.Put(collectionID, err)
	t.Cleanup(func() { meta.GlobalFailedLoadCache = prev })
}

// nilFailedLoadCache sets the package-global GlobalFailedLoadCache to nil for
// the duration of the test, restoring the previous cache on cleanup. This is
// the state initQueryCoord leaves the global in AFTER wiring meta/dist/
// targetMgr and BEFORE assigning the cache, so a "partially wired Server"
// tolerance that covers the stores but not the cache misses exactly this
// window.
func nilFailedLoadCache(t *testing.T) {
	t.Helper()
	prev := meta.GlobalFailedLoadCache
	meta.GlobalFailedLoadCache = nil
	t.Cleanup(func() { meta.GlobalFailedLoadCache = prev })
}

// TestGetLoadPercentageByResourceGroup_NilFailedLoadCache asserts the
// partially-wired-Server tolerance covers the one dependency that is wired
// LAST during init: GlobalFailedLoadCache is assigned in initQueryCoord after
// initMeta has already wired meta, dist and targetMgr, and FailedLoadCache.Get
// dereferences a nil receiver. Reading the cache without a nil check panics in
// that window on any collection that is not registered.
func TestGetLoadPercentageByResourceGroup_NilFailedLoadCache(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	f.putResourceGroup(t, "rg-target")
	nilFailedLoadCache(t)

	assert.NotPanics(t, func() {
		percentage, err := f.freeFn(context.Background(), 1300, "rg-target")
		assert.NoError(t, err)
		assert.EqualValues(t, -1, percentage)
	})
}

// TestGetLoadPercentageByResourceGroup_FailedLoad asserts that when the
// collection has a replica in the requested resource group but the
// collection itself is not currently registered as loaded, a recorded
// GlobalFailedLoadCache error is surfaced to the caller instead of a bare
// -1. Deleting the GlobalFailedLoadCache.Get block makes this test observe a
// nil error where it expects the seeded failure.
//
// The error is normalized to ErrCollectionNotLoaded rather than returned
// verbatim, so the assertion is on the code plus the preserved cause text,
// not on errors.Is against the seeded error -- see
// TestGetLoadPercentageByResourceGroup_FailedLoadIsNotRetriable for why the
// normalization is load-bearing.
func TestGetLoadPercentageByResourceGroup_FailedLoad(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	// Collection 700 is never registered via putTarget/PutCollectionWithoutSave,
	// so its load percentage reads negative even though a replica record exists.
	f.putReplica(t, 700, 70, "rg-target")

	loadErr := errors.New("mocked load failure")
	seedFailedLoadCache(t, 700, loadErr)

	percentage, err := f.server().GetLoadPercentageByResourceGroup(context.Background(), 700, "rg-target")

	assert.EqualValues(t, -1, percentage)
	assert.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
	assert.Contains(t, err.Error(), "mocked load failure",
		"normalizing the code must not throw away the recorded cause")
}

// TestGetLoadPercentageByResourceGroup_FailedLoadIsNotRetriable is the reason
// the recorded failure is normalized rather than passed through.
// FailedLoadCache stores whatever error the failing load task recorded, and a
// load genuinely fails with retriable sentinels -- ErrServiceNotReady is what
// LoadSegments returns when the target query node is restarting, and the
// scheduler's exclusion list does not filter it out before
// recordSegmentTaskError stores it.
//
// ErrServiceNotReady is also this function's OWN answer for the init window,
// where it means "retry, this fixes itself in a moment". Returning the cached
// error verbatim would make a load that is never coming back
// indistinguishable from one that is, and a caller written to the documented
// contract would retry until the cache entry expires 24h later. The seeded
// error here is deliberately a retriable sentinel; the previous test's
// errors.New could not catch this.
func TestGetLoadPercentageByResourceGroup_FailedLoadIsNotRetriable(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	f.putReplica(t, 720, 72, "rg-target")

	seedFailedLoadCache(t, 720, merr.WrapErrServiceNotReady("querynode", 72, "restarting"))

	percentage, err := f.server().GetLoadPercentageByResourceGroup(context.Background(), 720, "rg-target")

	assert.EqualValues(t, -1, percentage)
	assert.ErrorIs(t, err, merr.ErrCollectionNotLoaded,
		"a terminally failed load must be reported with the terminal code")
	assert.NotErrorIs(t, err, merr.ErrServiceNotReady,
		"a failed load must not borrow the code this function uses for the self-healing init window")
	assert.False(t, merr.IsRetryableErr(err),
		"the caller must not be told to wait for a load that is never coming back")
}

// TestGetLoadPercentageByResourceGroup_FailedLoadSurvivesReplicaCleanup pins
// the terminal failed-load state, which is also the common one:
// CollectionObserver.observeTimeout removes BOTH the collection registration
// and every replica record, leaving only the GlobalFailedLoadCache entry
// behind. The recorded failure must still reach the caller from that state;
// checking replicas before consulting the cache would swallow it into a bare
// (-1, nil), which reads as "nothing here" rather than "the load failed".
func TestGetLoadPercentageByResourceGroup_FailedLoadSurvivesReplicaCleanup(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	// No putTarget, no putReplica: collection 1200 has been fully cleaned up
	// after its load timed out; only the failure record remains.
	loadErr := errors.New("mocked load failure")
	seedFailedLoadCache(t, 1200, loadErr)

	percentage, err := f.freeFn(context.Background(), 1200, "")

	assert.EqualValues(t, -1, percentage)
	assert.ErrorIs(t, err, merr.ErrCollectionNotLoaded,
		"the recorded load failure must survive the removal of the replica records")
	assert.Contains(t, err.Error(), loadErr.Error())
}

// TestGetLoadPercentageByResourceGroup_NotHealthy asserts what actually
// protects this entry point from a Server that is still coming up: the
// merr.CheckHealthy(s.State()) gate, the same one the rest of Server's
// surface takes. The Server here is fully wired -- only its status says it is
// not serving yet -- so the nil checks downstream would let the read through;
// the gate is what stops it.
//
// The gate, not those nil checks, is also the only one of the two that can
// order the read against Init: s.meta/s.dist/s.targetMgr are plain fields
// with no synchronization, while the status is an atomic stored after
// everything is wired.
//
// The answer is -1 WITH ErrServiceNotReady, not a bare -1: the fixture has a
// replica of the collection in rg-target, so a bare -1 would state the
// opposite of the truth ("no replica in this resource group") for a state in
// which nothing is known at all.
func TestGetLoadPercentageByResourceGroup_NotHealthy(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 1100, 11000, "1100-dmc0", 1)
	f.putReplica(t, 1100, 110, "rg-target")

	s := &Server{meta: f.meta, targetMgr: f.targetMgr, dist: f.dist} // wired, but not serving
	require.Equal(t, commonpb.StateCode_Initializing, s.State(),
		"a zero Server must report Initializing, which is the state this test is about")

	percentage, err := s.GetLoadPercentageByResourceGroup(context.Background(), 1100, "rg-target")
	assert.ErrorIs(t, err, merr.ErrServiceNotReady,
		"a coordinator that is not serving yet must say so, not report the resource group as empty")
	assert.EqualValues(t, -1, percentage)
}

// TestLoadPercentageByResourceGroup_NilStores covers the utils-level nil
// guard on its own terms. It is defense in depth for a direct caller -- the
// observers hold these stores and call the free function, bypassing Server
// and its health gate -- and it is deliberately NOT reached through Server,
// because the gate above would answer first.
//
// Deleting the guard turns each of these into a nil-pointer dereference.
func TestLoadPercentageByResourceGroup_NilStores(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	ctx := context.Background()

	for name, call := range map[string]func() (int32, error){
		"nil meta": func() (int32, error) {
			return utils.LoadPercentageByResourceGroup(ctx, nil, f.targetMgr, f.dist, 1, "rg")
		},
		"nil targetMgr": func() (int32, error) { return utils.LoadPercentageByResourceGroup(ctx, f.meta, nil, f.dist, 1, "rg") },
		"nil dist": func() (int32, error) {
			return utils.LoadPercentageByResourceGroup(ctx, f.meta, f.targetMgr, nil, 1, "rg")
		},
		"nil resource manager": func() (int32, error) {
			partial := &meta.Meta{CollectionManager: f.meta.CollectionManager, ReplicaManager: f.meta.ReplicaManager}
			return utils.LoadPercentageByResourceGroup(ctx, partial, f.targetMgr, f.dist, 1, "rg")
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				percentage, err := call()
				assert.ErrorIs(t, err, merr.ErrServiceNotReady)
				assert.EqualValues(t, -1, percentage)
			})
		})
	}
}

// TestLoadPercentageByResourceGroup_EmptyRGCoversEveryResourceGroup asserts
// that an empty rgName is the absence of a filter, not a filter that matches
// nothing: it selects every replica of the collection regardless of which
// resource group each one lives in, and reports the laggard among them.
//
// The setup deliberately makes the three plausible wrong answers all
// distinguishable from the right one (50):
//   - dropping the `rgName == ""` disjunct from the replica selection makes
//     the replica set empty and the answer -1;
//   - keeping only the first selected replica reports 100;
//   - averaging across the selected replicas instead of taking the minimum
//     reports 75.
func TestLoadPercentageByResourceGroup_EmptyRGCoversEveryResourceGroup(t *testing.T) {
	ctx := context.Background()
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 800, 8000, "800-dmc0", 1) // 2 targets: 1 channel + 1 segment

	// rg-full's replica carries the channel and the segment: 100.
	f.putReplica(t, 800, 80, "rg-full")
	f.putDelegator(800, 80, "800-dmc0", 1)

	// rg-half's replica watches the channel but holds no segment: 50.
	f.putReplica(t, 800, 81, "rg-half")
	f.putDelegator(800, 81, "800-dmc0")

	full, err := f.freeFn(ctx, 800, "rg-full")
	require.NoError(t, err)
	require.EqualValues(t, 100, full)

	half, err := f.freeFn(ctx, 800, "rg-half")
	require.NoError(t, err)
	require.EqualValues(t, 50, half)

	all, err := f.freeFn(ctx, 800, "")
	assert.NoError(t, err)
	assert.EqualValues(t, 50, all, "empty resource group must span every replica and report the laggard")
	assert.EqualValues(t, half, all, "empty resource group must agree with the furthest-behind resource group")
}

// TestLoadPercentageByResourceGroup_EmptyRGMatchesCollectionWideFigure is the
// equivalence assertion the resource-group concept has to earn, on the ONE
// shape where the two figures coincide: a single-partition collection with
// one replica (replicaNum 1). There an empty rgName must reproduce the
// collection-wide percentage CollectionObserver.observePartitionLoadStatus
// computes, loadedCount * 100 / (targetNum * replicaNum), wherever progress
// is at or above 1% -- below that this figure rounds up to 1 where the
// observer truncates to 0 (TestGetLoadPercentageByResourceGroup_AnyProgressReadsAtLeastOne),
// so "exactly" holds for -1, 100, and the whole range from 1% up, not for
// the sub-1% band. With more partitions the two weight intermediate progress
// differently (see TestGetLoadPercentageByResourceGroup_PoolsTargetsAcrossPartitions)
// while still agreeing at -1 and at 100.
//
// Here targetNum is 4 (one channel plus three segments), the single replica
// carries the channel and one of the three segments so loadedCount is 2, and
// replicaNum is 1: 2 * 100 / (4 * 1) == 50. The empty-rgName answer is
// asserted against that hand-evaluated collection-wide formula, and against
// the answer for the one resource group that actually holds the replica --
// "no filter" and "the only filter" cannot disagree.
//
// Any change that makes an empty rgName mean something other than "the whole
// collection" -- returning -1, returning 0, or restricting to some default
// resource group -- breaks this.
func TestLoadPercentageByResourceGroup_EmptyRGMatchesCollectionWideFigure(t *testing.T) {
	ctx := context.Background()
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 900, 9000, "900-dmc0", 1, 2, 3) // 4 targets: 1 channel + 3 segments

	f.putReplica(t, 900, 90, "rg-only")
	f.putDelegator(900, 90, "900-dmc0", 1) // channel + segment 1 loaded: 2 of 4

	const loadedCount, targetNum, replicaNum = 2, 4, 1
	expected := int32(loadedCount * 100 / (targetNum * replicaNum))

	all, err := f.freeFn(ctx, 900, "")
	assert.NoError(t, err)
	assert.EqualValues(t, expected, all, "empty resource group must reproduce the collection-wide load percentage")

	only, err := f.freeFn(ctx, 900, "rg-only")
	assert.NoError(t, err)
	assert.EqualValues(t, all, only, "with a single resource group, filtering by it must equal not filtering at all")
}

// TestLoadPercentageByResourceGroup_EmptyRGStillReportsNoReplica asserts that
// widening an empty rgName to "every replica" did not also swallow the
// no-replica outcome: a collection that has a load target but no replica at
// all must still report -1, not 0. Moving the `rgName == ""` handling ahead of
// the `len(replicas) == 0` guard -- for instance by short-circuiting an empty
// rgName straight into the percentage computation -- turns this into 0, which
// callers read as "loading, but no progress" rather than "nothing here".
func TestLoadPercentageByResourceGroup_EmptyRGStillReportsNoReplica(t *testing.T) {
	ctx := context.Background()
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 1000, 10000, "1000-dmc0", 1, 2)
	// No putReplica call: the collection is registered and has targets, but
	// no replica of it exists anywhere.

	percentage, err := f.freeFn(ctx, 1000, "")

	assert.NoError(t, err)
	assert.EqualValues(t, -1, percentage, "no replica anywhere must stay distinguishable from a replica at zero progress")
}

// putTargetTwoPartitions registers collectionID as loaded with TWO partitions
// and gives it a next target with one channel, segsA on partition A and segsB
// on partition B, all on that channel.
func (f *rgLoadPercentageFixture) putTargetTwoPartitions(t *testing.T, collectionID, partitionA, partitionB int64, channelName string, segsA, segsB []int64) {
	ctx := context.Background()
	require.NoError(t, f.meta.PutCollectionWithoutSave(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: collectionID},
	}))
	for _, partitionID := range []int64{partitionA, partitionB} {
		require.NoError(t, f.meta.PutPartitionWithoutSave(ctx, &meta.Partition{
			PartitionLoadInfo: &querypb.PartitionLoadInfo{CollectionID: collectionID, PartitionID: partitionID},
		}))
	}

	segmentInfos := make([]*datapb.SegmentInfo, 0, len(segsA)+len(segsB))
	for _, segID := range segsA {
		segmentInfos = append(segmentInfos, &datapb.SegmentInfo{
			ID: segID, CollectionID: collectionID, PartitionID: partitionA, InsertChannel: channelName,
		})
	}
	for _, segID := range segsB {
		segmentInfos = append(segmentInfos, &datapb.SegmentInfo{
			ID: segID, CollectionID: collectionID, PartitionID: partitionB, InsertChannel: channelName,
		})
	}
	vChannel := &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: channelName}

	f.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return([]*datapb.VchannelInfo{vChannel}, segmentInfos, nil).Once()
	require.NoError(t, f.targetMgr.UpdateCollectionNextTarget(ctx, collectionID))
}

// TestGetLoadPercentageByResourceGroup_PoolsTargetsAcrossPartitions pins the
// deliberate divergence from the collection-wide figure on multi-partition
// collections: this figure pools every target (each target counts once),
// while the observer computes each partition separately -- its own segments
// plus the channel targets -- and CalculateLoadPercentage averages the
// partitions, weighting a 1-segment partition as much as a 3-segment one.
//
// Here the work set is 1 channel + 1 segment on partition A + 3 segments on
// partition B, and the replica carries the channel and A's segment:
//   - pooled (this function): 2 of 5 targets = 40;
//   - observer-style: partition A = 2/2 = 100, partition B = 1/4 = 25,
//     average = 62.
//
// Both agree at 0 and at 100; only the intermediate weighting differs. If
// this test starts failing at 62, the function has silently switched to
// per-partition averaging and its doc comment is now wrong.
func TestGetLoadPercentageByResourceGroup_PoolsTargetsAcrossPartitions(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	f.putTargetTwoPartitions(t, 1400, 14000, 14001, "1400-dmc0", []int64{1}, []int64{2, 3, 4})
	f.putReplica(t, 1400, 140, "rg-target")
	f.putDelegator(1400, 140, "1400-dmc0", 1) // channel + partition A's segment

	percentage, err := f.server().GetLoadPercentageByResourceGroup(context.Background(), 1400, "rg-target")

	assert.NoError(t, err)
	assert.EqualValues(t, 40, percentage,
		"the figure pools targets across partitions; the observer's per-partition average would say 62 here")
}

// TestGetLoadPercentageByResourceGroup_NewSegmentReArmsTheFigure pins the
// second deliberate divergence: this is a live coverage figure with no
// persistence, so when new work lands in the target -- a freshly flushed
// segment, a compaction output -- it drops back below 100 until the replica
// picks the new segment up. The observer's persisted number never regresses
// from 100 in the same state, so ShowLoadCollections would keep saying 100.
// This is what makes the figure progress rather than a gate: on a collection
// under ingestion == 100 is a transient, so a caller must read it as "how far
// along" and leave "may I route" to readiness, never AND the two.
func TestGetLoadPercentageByResourceGroup_NewSegmentReArmsTheFigure(t *testing.T) {
	ctx := context.Background()
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 1500, 15000, "1500-dmc0", 1)
	f.putReplica(t, 1500, 150, "rg-target")
	f.putDelegator(1500, 150, "1500-dmc0", 1)

	before, err := f.server().GetLoadPercentageByResourceGroup(ctx, 1500, "rg-target")
	require.NoError(t, err)
	require.EqualValues(t, 100, before, "the fixture must start fully loaded")

	// A new segment lands in the next target, as after a flush or compaction.
	f.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1500)).Return(
		[]*datapb.VchannelInfo{{CollectionID: 1500, ChannelName: "1500-dmc0"}},
		[]*datapb.SegmentInfo{
			{ID: 1, CollectionID: 1500, PartitionID: 15000, InsertChannel: "1500-dmc0"},
			{ID: 2, CollectionID: 1500, PartitionID: 15000, InsertChannel: "1500-dmc0"},
		}, nil).Once()
	require.NoError(t, f.targetMgr.UpdateCollectionNextTarget(ctx, 1500))

	after, err := f.server().GetLoadPercentageByResourceGroup(ctx, 1500, "rg-target")

	assert.NoError(t, err)
	assert.EqualValues(t, 66, after,
		"new work in the target must re-arm the figure below 100 (2 of 3 targets carried) until it is picked up")
}

// TestGetLoadPercentageByResourceGroup_SurvivesTargetPromotion asserts the
// figure is read NextTargetFirst: promoting the next target to current clears
// the next target until the observer re-pulls it ~10s later, and a plain
// NextTarget read in that window reports 0 - a fully loaded, serving resource
// group flapping 100/0 on every promotion. Changing NextTargetFirst back to
// NextTarget in replicaLoadPercentage fails this at 0.
func TestGetLoadPercentageByResourceGroup_SurvivesTargetPromotion(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 900, 9000, "900-dmc0")
	f.putReplica(t, 900, 90, "rg-promoted")
	f.putDelegator(900, 90, "900-dmc0")

	require.True(t, f.targetMgr.UpdateCollectionCurrentTarget(context.Background(), 900),
		"promotion moves next to current and clears next")

	percentage, err := f.server().GetLoadPercentageByResourceGroup(context.Background(), 900, "rg-promoted")
	assert.NoError(t, err)
	assert.EqualValues(t, 100, percentage,
		"a promoted target must keep reporting the loaded figure, not flap to 0")
}

// countingTargetManager wraps a real TargetManager and counts the two reads
// LoadPercentageByResourceGroup makes, so a test can pin that they happen
// once per CALL rather than once per replica. Everything else delegates.
type countingTargetManager struct {
	meta.TargetManagerInterface
	channelReads int
	segmentReads int
}

func (c *countingTargetManager) GetDmChannelsByCollection(ctx context.Context, collectionID int64, scope meta.TargetScope) map[string]*meta.DmChannel {
	c.channelReads++
	return c.TargetManagerInterface.GetDmChannelsByCollection(ctx, collectionID, scope)
}

func (c *countingTargetManager) GetSealedSegmentsByCollection(ctx context.Context, collectionID int64, scope meta.TargetScope) map[int64]*datapb.SegmentInfo {
	c.segmentReads++
	return c.TargetManagerInterface.GetSealedSegmentsByCollection(ctx, collectionID, scope)
}

// TestLoadPercentageByResourceGroup_ReadsTargetsOncePerCall pins the
// correctness half of the target hoist. Each replica used to take its own
// snapshot of the channel and segment targets, so a promotion landing
// mid-loop left the min-across-replicas comparison below weighing two
// different denominators against each other.
//
// Counting the reads is what makes that falsifiable: the returned percentage
// is identical either way on a static fixture, so only the call count can
// tell a shared snapshot from a per-replica one. Moving either read back
// inside replicaLoadPercentage turns these into 3.
func TestLoadPercentageByResourceGroup_ReadsTargetsOncePerCall(t *testing.T) {
	ctx := context.Background()
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 1600, 16000, "1600-dmc0", 1, 2)
	// Three replicas in one resource group: a per-replica read would be 3.
	f.putReplica(t, 1600, 160, "rg-target")
	f.putReplica(t, 1600, 161, "rg-target")
	f.putReplica(t, 1600, 162, "rg-target")
	f.putDelegator(1600, 160, "1600-dmc0", 1, 2)

	counting := &countingTargetManager{TargetManagerInterface: f.targetMgr}
	percentage, err := utils.LoadPercentageByResourceGroup(ctx, f.meta, counting, f.dist, 1600, "rg-target")

	require.NoError(t, err)
	assert.EqualValues(t, 0, percentage,
		"the laggards carry nothing, so the min is 0 -- the figure itself is unchanged by the hoist")
	assert.Equal(t, 1, counting.channelReads,
		"the channel targets must be read once per call, not once per replica")
	assert.Equal(t, 1, counting.segmentReads,
		"and so must the segment targets")
}

// TestGetLoadPercentageByResourceGroup_UnknownResourceGroupIsInputError pins
// that a resource group which does not exist at all is refused as the
// request's own mistake -- ErrResourceGroupNotFound, an InputError -- rather
// than reported as the terminal bare -1 an existing-but-empty group gets.
// The two look identical to the replica scan; only the ResourceManager can
// tell them apart, and a caller told "-1, stop" for a typo has no way to
// learn what it actually did wrong.
func TestGetLoadPercentageByResourceGroup_UnknownResourceGroupIsInputError(t *testing.T) {
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 1600, 16000, "1600-dmc0", 1)
	f.putReplica(t, 1600, 160, "rg-real")

	percentage, err := f.server().GetLoadPercentageByResourceGroup(context.Background(), 1600, "rg-typo")

	assert.ErrorIs(t, err, merr.ErrResourceGroupNotFound)
	assert.Equal(t, merr.InputError, merr.GetErrorType(err), "a misspelled group is the request's fault")
	assert.False(t, merr.IsRetryableErr(err))
	assert.EqualValues(t, -1, percentage)

	// The check is about the name, not the collection: an unknown group is
	// refused the same way for a collection that is not loaded at all.
	_, err = f.server().GetLoadPercentageByResourceGroup(context.Background(), 999999, "rg-typo")
	assert.ErrorIs(t, err, merr.ErrResourceGroupNotFound)

	// And the empty name is the absence of a filter, never a name to validate.
	all, err := f.server().GetLoadPercentageByResourceGroup(context.Background(), 1600, "")
	assert.NoError(t, err)
	assert.EqualValues(t, 0, all)
}

// TestGetLoadPercentageByResourceGroup_AnyProgressReadsAtLeastOne pins that 0
// means exactly "carries none of the targets yet". Integer division would
// truncate 1 of 200 to 0, which the contract reserves for no progress at all;
// the value is rounded up so a caller reading 0 can rely on that meaning.
// The top end is asserted alongside: 199 of 200 must still read 99, since
// 100 is reached only when every target is carried.
func TestGetLoadPercentageByResourceGroup_AnyProgressReadsAtLeastOne(t *testing.T) {
	ctx := context.Background()
	f := newRGLoadPercentageFixture(t)
	segments := make([]int64, 0, 199)
	for i := int64(1); i <= 199; i++ {
		segments = append(segments, i)
	}
	f.putTarget(t, 1700, 17000, "1700-dmc0", segments...) // 1 channel + 199 segments = 200 targets
	f.putReplica(t, 1700, 170, "rg-target")

	f.putDelegator(1700, 170, "1700-dmc0") // the channel only: 1 of 200
	low, err := f.server().GetLoadPercentageByResourceGroup(ctx, 1700, "rg-target")
	require.NoError(t, err)
	assert.EqualValues(t, 1, low, "1 of 200 is progress, and progress must not read as 0")

	f.putDelegator(1700, 170, "1700-dmc0", segments[:198]...) // channel + 198 segments: 199 of 200
	high, err := f.server().GetLoadPercentageByResourceGroup(ctx, 1700, "rg-target")
	require.NoError(t, err)
	assert.EqualValues(t, 99, high, "199 of 200 must not round up to 100")
}

// TestLoadPercentageByResourceGroup_InvisibleReplicaCounts pins the three-way
// state the division-of-labor note on LoadPercentageByResourceGroup
// describes: a group whose only replica is still query-invisible reads 100
// here (the progress figure counts it -- it is exactly what the load-config
// path is waiting on), while readiness says Ready=false with
// ShardsWithoutLeader (routing can never reach it), and the group is absent
// from the GetShardLeaders answer (TestShardLeaderResourceGroupTagIsNotAServabilityVerdict).
// This is why the percentage is progress and readiness is the gate.
func TestLoadPercentageByResourceGroup_InvisibleReplicaCounts(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 1800, 18000, "1800-dmc0")
	f.putInvisibleReplica(t, 1800, "rg-new", 180)
	f.putLeader(1800, 180, "1800-dmc0", true)

	percentage, err := utils.LoadPercentageByResourceGroup(ctx, f.meta, f.targetMgr, f.dist, 1800, "rg-new")
	require.NoError(t, err)
	assert.EqualValues(t, 100, percentage, "the progress figure counts the invisible replica")

	readiness := f.readiness(t, 1800, "rg-new")
	assert.False(t, readiness.Ready, "100 is not a servability verdict")
	assert.Equal(t, utils.ShardLeadersReasonShardsWithoutLeader, readiness.Reason,
		"the group holds a replica that is coming up, so waiting helps")
}

// TestLoadPercentageByResourceGroup_UnscopedNeedsNoResourceManager pins the
// inertness promise on the resource-group concept: a caller that never names
// a group must not be able to fail on one. Only the scoped form consults the
// ResourceManager, so only the scoped form requires it; a Meta assembled
// without one -- what an observer-level caller or a hand-built fixture may
// hold -- still answers the unscoped question, and reports ErrServiceNotReady
// only for a named group.
func TestLoadPercentageByResourceGroup_UnscopedNeedsNoResourceManager(t *testing.T) {
	ctx := context.Background()
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 1900, 19000, "1900-dmc0", 1)
	f.putReplica(t, 1900, 190, "rg-target")
	f.putDelegator(1900, 190, "1900-dmc0", 1)
	partial := &meta.Meta{CollectionManager: f.meta.CollectionManager, ReplicaManager: f.meta.ReplicaManager}

	all, err := utils.LoadPercentageByResourceGroup(ctx, partial, f.targetMgr, f.dist, 1900, "")
	assert.NoError(t, err, "the unscoped form never consults the resource manager")
	assert.EqualValues(t, 100, all)

	scoped, err := utils.LoadPercentageByResourceGroup(ctx, partial, f.targetMgr, f.dist, 1900, "rg-target")
	assert.ErrorIs(t, err, merr.ErrServiceNotReady, "the scoped form does, and says so with the retriable sentinel")
	assert.EqualValues(t, -1, scoped)
}
