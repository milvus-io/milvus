package balancer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/resourcewatermark"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestCapacityWatermarksChooseRelevantDimension(t *testing.T) {
	for _, tc := range []struct {
		reason viewpb.SegmentPerparingFailure_Reason
		want   int64
	}{
		{viewpb.SegmentPerparingFailure_MemoryLimitExceeded, 2},
		{viewpb.SegmentPerparingFailure_DiskLimitExceeded, 3},
		{viewpb.SegmentPerparingFailure_ResourceReservationFailed, 3},
	} {
		t.Run(tc.reason.String(), func(t *testing.T) {
			c := replicaCache(1, 3)
			id := cacheShard(1, 10)
			key := coordview.FailureKey(1, 1000, 0, 0, c.GetCollection(1).ConfigVersion(), qviews.DataVersion{StreamingVersion: 1})
			c.PublishShard(id, &coordview.ShardStats{FailedResources: map[int64]map[coordview.FailedResourceKey]coordview.ResourceFailure{1: {key: {Reason: tc.reason, ObservedAt: time.Now(), Version: *ver(1, 0, 1)}}}})
			store := resourcewatermark.New()
			store.Start(1)(&viewpb.NodeResourceWatermarks{Memory: 95, Disk: 95})
			store.Start(2)(&viewpb.NodeResourceWatermarks{Memory: 10, Disk: 90})
			store.Start(3)(&viewpb.NodeResourceWatermarks{Memory: 70, Disk: 20})
			plan := NewDefaultBalancePolicy(WithResourceWatermarks(store)).Plan(c, []qviews.ShardID{id})
			require.Contains(t, plan.Prepares, id)
			require.Equal(t, tc.want, assignmentsFromBuilder(plan.Prepares[id])[1000])
			require.Empty(t, plan.Releases)
		})
	}
}

func TestCapacityWatermarksOptionalRecoveryAndHysteresis(t *testing.T) {
	c, id := optionalBalanceFixture()
	config := *c.GetBalanceConfig()
	config.OptimizationEnabled = true
	c.UpdateBalanceConfig(&config)
	stats := upStats(qviews.DataVersion{StreamingVersion: 1}, placement(1000, 1, 1, coordview.SegmentStateUp))
	key := coordview.FailureKey(1, 1000, 0, 0, c.GetCollection(1).ConfigVersion(), qviews.DataVersion{StreamingVersion: 1})
	store := resourcewatermark.New()
	update := store.Start(2)
	update(&viewpb.NodeResourceWatermarks{Memory: 80, Disk: 90})
	baseline, _ := store.Read(2, time.Now())
	stats.FailedResources = map[int64]map[coordview.FailedResourceKey]coordview.ResourceFailure{2: {key: {Reason: viewpb.SegmentPerparingFailure_MemoryLimitExceeded, ObservedAt: time.Now(), Watermarks: baseline, Version: *ver(1, 0, 2)}}}
	c.PublishShard(id, stats)
	policy := NewDefaultBalancePolicy(WithResourceWatermarks(store))
	for _, level := range []float64{80, 79, 81, 75.01} {
		update(&viewpb.NodeResourceWatermarks{Memory: level, Disk: 0})
		plan := policy.Plan(c, []qviews.ShardID{id})
		require.Empty(t, plan.Prepares, "small changes and unrelated disk recovery must not repeat failed migration")
		require.Empty(t, plan.Releases)
	}
	update(&viewpb.NodeResourceWatermarks{Memory: 75, Disk: 100})
	plan := policy.Plan(c, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
	require.Equal(t, int64(2), assignmentsFromBuilder(plan.Prepares[id])[1000], "memory recovery can enable migration even while disk is full")
	require.NotNil(t, c.GetCollection(1).GetShard(id).Stats().UpVersion, "planning retains serving coverage")
	// Unknown/unchanged capacity can eventually be probed, but never on every scan.
	update(nil)
	failure := stats.FailedResources[2][key]
	failure.ObservedAt = time.Now().Add(-capacityProbeInterval)
	stats.FailedResources[2][key] = failure
	c.PublishShard(id, stats)
	plan = policy.Plan(c, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
}

func TestCapacityWatermarksAllSaturatedStillRetry(t *testing.T) {
	c := replicaCache(1, 2)
	id := cacheShard(1, 10)
	key := coordview.FailureKey(1, 1000, 0, 0, 1, qviews.DataVersion{StreamingVersion: 1})
	stats := &coordview.ShardStats{FailedResources: map[int64]map[coordview.FailedResourceKey]coordview.ResourceFailure{
		1: {key: {Reason: viewpb.SegmentPerparingFailure_DiskLimitExceeded, ObservedAt: time.Now(), Version: *ver(1, 0, 2)}},
		2: {key: {Reason: viewpb.SegmentPerparingFailure_DiskLimitExceeded, ObservedAt: time.Now(), Version: *ver(1, 0, 1)}},
	}}
	c.PublishShard(id, stats)
	store := resourcewatermark.New()
	for _, node := range []int64{1, 2} {
		store.Start(node)(&viewpb.NodeResourceWatermarks{Disk: 100})
	}
	plan := NewDefaultBalancePolicy(WithResourceWatermarks(store)).Plan(c, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
	require.Equal(t, int64(2), assignmentsFromBuilder(plan.Prepares[id])[1000])
	require.Empty(t, plan.Releases)
}

func TestCapacityWatermarksHealthyPathDoesNotReadOrAllocate(t *testing.T) {
	c := replicaCache(1, 2)
	id := cacheShard(1, 10)
	p := newPlanningContext(c)
	p.resourceWatermarks = resourcewatermark.New()
	require.Zero(t, testing.AllocsPerRun(100, func() {
		require.Nil(t, newCapacityPlacement(p, id, nil, nil))
	}))
	require.Nil(t, p.resourceSamples)
	require.Equal(t, pressureCost(71), pressureCost(74))
	require.Greater(t, pressureCost(95)-pressureCost(90), pressureCost(40)-pressureCost(35))
}

func TestCapacityWatermarksExpiredHistoryAllocatesNothing(t *testing.T) {
	c := replicaCache(1, 2)
	id := cacheShard(1, 10)
	p := newPlanningContext(c)
	key := coordview.FailureKey(1, 1000, 0, 0, 1, qviews.DataVersion{StreamingVersion: 1})
	failures := map[int64]map[coordview.FailedResourceKey]coordview.ResourceFailure{1: {key: {ObservedAt: time.Now().Add(-11 * time.Minute)}}}
	segments := map[int64]*SegmentDataView{1000: {SegmentID: 1000, PartitionID: 1}}
	require.Zero(t, testing.AllocsPerRun(100, func() { require.Nil(t, newCapacityPlacement(p, id, segments, failures)) }))
	require.Nil(t, p.resourceSamples)
}
