package balancer

import (
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/viewfeedback"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestPreparationPriorityScenarios(t *testing.T) {
	now := time.Unix(1800000000, 0)
	for _, tc := range []struct {
		name            string
		age             time.Duration
		known, up, auto bool
		rate, total     float64
		low             int32
	}{
		{"hot-recovery", time.Minute, true, false, false, 100, 100, 18},
		{"hot-recovery-before-auto", time.Minute, true, false, true, 100, 100, 18},
		{"hot-recovery-zero-rate", time.Minute, true, false, false, 0, 100, 10},
		{"hot-recovery-zero-total", time.Minute, true, false, false, 100, 0, 10},
		{"hot-recovery-clamped-share", time.Minute, true, false, false, 200, 100, 18},
		{"one-percent", time.Minute, true, false, false, 1, 100, 11},
		{"five-percent", time.Minute, true, false, false, 5, 100, 13},
		{"ten-percent", time.Minute, true, false, false, 10, 100, 14},
		{"half", time.Minute, true, false, false, 50, 100, 17},
		{"same-share", time.Minute, true, false, false, 500, 1000, 17},
		{"auto-unknown", 0, false, false, true, 0, 0, 10},
		{"auto-cold", time.Hour, true, false, true, 0, 0, 10},
		{"auto-cold-handoff", time.Hour, true, true, true, 0, 0, -10},
		{"auto-active-handoff", time.Minute, true, true, true, 100, 100, 2},
		{"auto-unknown-handoff", 0, false, true, true, 0, 0, -10},
		{"manual-unknown", 0, false, false, false, 0, 0, 0},
		{"cold-recovery", time.Hour, true, false, false, 100, 100, -5},
		{"cold-handoff", time.Hour, true, true, false, 100, 100, -10},
		{"silent-up-handoff", 0, false, true, false, 0, 0, -10},
		{"active-handoff", time.Minute, true, true, false, 100, 100, 2},
		{"active-handoff-zero-rate", time.Minute, true, true, false, 0, 100, 0},
		{"recent-boundary", 2 * time.Minute, true, false, false, 0, 0, 10},
		{"after-boundary", 2*time.Minute + time.Nanosecond, true, false, false, 0, 0, -5},
		{"handoff-at-boundary", 2 * time.Minute, true, true, false, 100, 100, 2},
		{"handoff-after-boundary", 2*time.Minute + time.Nanosecond, true, true, false, 100, 100, -10},
		{"autoload-after-boundary", 2*time.Minute + time.Nanosecond, true, false, true, 100, 100, 10},
		{"future-access", -time.Minute, true, false, false, 0, 0, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sample *viewfeedback.Snapshot
			if tc.known {
				sample = &viewfeedback.Snapshot{LastAccessUnixNanos: now.Add(-tc.age).UnixNano(), RequestRates: viewfeedback.RequestRates{Last6Windows: tc.rate, Last2Windows: 1e9, Last24Windows: 1e9}}
			}
			low := preparationPriority(commonpb.LoadPriority_LOW, sample, tc.total, tc.up, tc.auto, now)
			high := preparationPriority(commonpb.LoadPriority_HIGH, sample, tc.total, tc.up, tc.auto, now)
			require.Equal(t, tc.low, low)
			require.Equal(t, low+2, high)
			require.GreaterOrEqual(t, low, minPreparationPriority)
			require.LessOrEqual(t, high, maxPreparationPriority)
		})
	}
	require.Zero(t, preparationPriority(commonpb.LoadPriority(99), &viewfeedback.Snapshot{}, 0, false, false, now))
	// Recovery shares the AutoLoad baseline; traffic can only raise its priority.
	auto := preparationPriority(commonpb.LoadPriority_LOW, nil, 0, false, true, now)
	previous := int32(0)
	for rate := 0.0; rate <= 100; rate++ {
		sample := &viewfeedback.Snapshot{LastAccessUnixNanos: now.UnixNano(), RequestRates: viewfeedback.RequestRates{Last6Windows: rate}}
		score := preparationPriority(commonpb.LoadPriority_LOW, sample, 100, false, false, now)
		require.GreaterOrEqual(t, score, previous)
		require.GreaterOrEqual(t, score, auto)
		previous = score
	}
}

func TestPreparationPriorityCapturesRGReplicaRateShares(t *testing.T) {
	mockey.PatchConvey("same-RG replicas across collections share one consistent denominator", t, func() {
		cfg := cfgFor(10, 1, []int64{1}, nil)
		cfg.Replicas[0].Priority = commonpb.LoadPriority_LOW
		cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: 2, ResourceGroup: "rg1"})
		cache := newTestCache(cfg)
		cache.PublishLoadConfig(20, cfgFor(20, 3, nil, nil), 1)
		other := cfgFor(30, 4, nil, nil)
		other.Replicas[0].ResourceGroup = "rg2"
		cache.PublishLoadConfig(30, other, 1)
		cache.PublishLoadConfig(40, cfgFor(40, 5, nil, nil), 1)
		publishTestData(cache, 50, qviews.DataVersion{StreamingVersion: 1}, nil, shardDataView("p_50v0", 1))
		now := time.Now().UnixNano()
		samples := map[int64]*viewfeedback.Snapshot{}
		for id, rate := range map[int64]float64{1: 20, 2: 30, 3: 50, 4: 1e9} {
			samples[id] = &viewfeedback.Snapshot{LastAccessUnixNanos: now, RequestRates: viewfeedback.RequestRates{Last6Windows: rate}}
		}
		calls := 0
		mockey.Mock((*viewfeedback.ReplicaFeedbackStore).GetViewFeedback).To(func(_ *viewfeedback.ReplicaFeedbackStore, id int64) (*viewfeedback.Snapshot, bool) {
			calls++
			sample, ok := samples[id]
			return sample, ok
		}).Build()
		b := NewDefaultBalancer(cache, nil, NewDefaultBalancePolicy(WithViewFeedbackReader(&viewfeedback.ReplicaFeedbackStore{})), nil)
		p := b.preparationPriorities()
		id := qviews.ShardID{ReplicaID: 1, VChannel: "p_10v0"}
		require.EqualValues(t, 15, p.preparationPriority(id))
		require.EqualValues(t, 18, p.preparationPriority(qviews.ShardID{ReplicaID: 2, VChannel: "p_10v1"}))
		require.EqualValues(t, 19, p.preparationPriority(qviews.ShardID{ReplicaID: 3, VChannel: "p_20v0"}))
		require.EqualValues(t, 20, p.preparationPriority(qviews.ShardID{ReplicaID: 4, VChannel: "p_30v0"}))
		require.EqualValues(t, 2, p.preparationPriority(qviews.ShardID{ReplicaID: 5, VChannel: "p_40v0"}))
		require.Equal(t, 5, calls, "read each replica once, without walking its shards")
		require.Equal(t, 100.0, p.preparationFeedback.groupRates["rg1"])
		samples[1] = &viewfeedback.Snapshot{LastAccessUnixNanos: now, RequestRates: viewfeedback.RequestRates{Last6Windows: 200}}
		require.EqualValues(t, 15, p.preparationPriority(id), "the same operation retains old numerator and denominator")
		require.EqualValues(t, 17, b.preparationPriorities().preparationPriority(id), "the next operation captures new feedback")
		require.Zero(t, p.preparationPriority(qviews.ShardID{ReplicaID: 999, VChannel: "p_99v0"}))
		b = NewDefaultBalancer(cache, nil, NewDefaultBalancePolicy(), nil)
		require.EqualValues(t, 0, b.preparationPriorities().preparationPriority(id), "missing reader keeps the config offset")
	})
}

func TestPreparationPriorityUpViewHealthAndAutoLoad(t *testing.T) {
	mockey.PatchConvey("health is per shard; stopping still serves; persisted AutoLoad boosts only shards without healthy Up views", t, func() {
		cfg := cfgFor(10, 1, nil, nil)
		cache := newTestCache(cfg)
		id := qviews.ShardID{ReplicaID: 1, VChannel: "p_10v0"}
		other := qviews.ShardID{ReplicaID: 1, VChannel: "p_10v1"}
		cache.PublishShard(id, testShardStats(ver(1, 0, 1), 1, placement(11, 1, 1, coordview.SegmentStateUp)))
		cache.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
		sample := &viewfeedback.Snapshot{LastAccessUnixNanos: time.Now().UnixNano(), RequestRates: viewfeedback.RequestRates{Last6Windows: 100}}
		mockey.Mock((*viewfeedback.ReplicaFeedbackStore).GetViewFeedback).To(func(_ *viewfeedback.ReplicaFeedbackStore, _ int64) (*viewfeedback.Snapshot, bool) {
			return sample, sample != nil
		}).Build()
		b := NewDefaultBalancer(cache, nil, NewDefaultBalancePolicy(WithViewFeedbackReader(&viewfeedback.ReplicaFeedbackStore{})), nil)
		p := b.preparationPriorities()
		require.EqualValues(t, 4, p.preparationPriority(id))
		require.EqualValues(t, 20, p.preparationPriority(other), "another shard's Up does not cover this shard")
		cache.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, Stopping: true, ResourceGroup: "rg1"})
		require.EqualValues(t, 4, b.preparationPriorities().preparationPriority(id))
		cache.PublishNode(1, &NodeInfo{NodeID: 1, Alive: false})
		require.EqualValues(t, 20, b.preparationPriorities().preparationPriority(id), "Up metadata survives NodeDown")
		require.EqualValues(t, 4, p.preparationPriority(id), "an existing planning operation retains node facts")
		cache.PublishNode(1, nil)
		require.EqualValues(t, 20, b.preparationPriorities().preparationPriority(id))
		sample = &viewfeedback.Snapshot{LastAccessUnixNanos: time.Now().Add(-time.Hour).UnixNano()}
		require.EqualValues(t, -3, b.preparationPriorities().preparationPriority(id))
		cfg = cfg.Clone() // The cache retains published configs as immutable snapshots.
		cfg.AutoLoad = true
		cache.PublishLoadConfig(10, cfg, 2)
		auto := b.preparationPriorities()
		require.EqualValues(t, 12, auto.preparationPriority(id), "AutoLoad with no healthy Up overrides cold history")
		cache.PublishNode(1, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
		require.EqualValues(t, -8, b.preparationPriorities().preparationPriority(id), "AutoLoad does not boost a cold handoff")
		cache.PublishNode(1, nil)
		cfg = cfg.Clone() // The cache retains published configs as immutable snapshots.
		cfg.AutoLoad = false
		cache.PublishLoadConfig(10, cfg, 3)
		require.EqualValues(t, 12, auto.preparationPriority(id), "captured load config remains immutable")
		require.EqualValues(t, -3, b.preparationPriorities().preparationPriority(id))
		sample = nil
		require.EqualValues(t, 2, b.preparationPriorities().preparationPriority(id))
		cfg = cfg.Clone() // The cache retains published configs as immutable snapshots.
		cfg.AutoLoad = true
		cache.PublishLoadConfig(10, cfg, 4)
		b = NewDefaultBalancer(cache, nil, NewDefaultBalancePolicy(), nil)
		require.EqualValues(t, 12, b.preparationPriorities().preparationPriority(id), "AutoLoad does not require feedback")
	})
}

func TestPreparationPriorityPlanPublishesImmutableControl(t *testing.T) {
	cfg := cfgFor(10, 1, []int64{1}, nil)
	cfg.AutoLoad = true
	cache := newTestCache(cfg)
	id := qviews.ShardID{ReplicaID: 1, VChannel: "p_10v0"}
	publishTestData(cache, 10, qviews.DataVersion{StreamingVersion: 1}, map[int64]*SegmentDataView{
		11: {SegmentID: 11, PartitionID: 1, LogicalSize: 100},
	}, shardDataView(id.VChannel, 1, 11))
	publishTestNodes(cache, &NodeInfo{NodeID: 1, Alive: true, ResourceGroup: "rg1"})
	policy := NewDefaultBalancePolicy()
	plan := policy.Plan(cache, []qviews.ShardID{id})
	require.Contains(t, plan.Prepares, id)
	view := plan.Prepares[id].Build()
	require.EqualValues(t, 12, view.GetMeta().GetControl().GetPreparationPriority())
	cfg = cfg.Clone()
	cfg.AutoLoad = false
	cache.PublishLoadConfig(10, cfg, 2)
	require.EqualValues(t, 12, view.GetMeta().GetControl().GetPreparationPriority())
	next := policy.Plan(cache, []qviews.ShardID{id})
	require.Contains(t, next.Prepares, id)
	require.EqualValues(t, 2, next.Prepares[id].Build().GetMeta().GetControl().GetPreparationPriority())
}
