package snview

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func useLinearHandoff(t *testing.T) {
	t.Helper()
	paramtable.Init()
	for key, value := range map[string]string{"queryView.handoffPolicy.type": string(paramtable.HandoffPolicyLinear), "queryView.handoffPolicy.linear.duration": "30s", "queryView.handoffPolicy.linear.initialRatio": "0.1"} {
		require.NoError(t, paramtable.Get().Save(key, value))
		t.Cleanup(func() { paramtable.Get().Reset(key) })
	}
}

func addHandoffView(t *testing.T, h *SNQueryViewHandler, version int64, schema int32, load uint64) *snViewEntry {
	t.Helper()
	meta := buildHandlerTestMeta(version)
	meta.SchemaVersion, meta.LoadInfoVersion = schema, load
	view := qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
	h.ApplyViews([]handler.ApplyView{{View: view}})
	acquired, ok := h.resMgr.(*mockResourceManager).getAcquired(view.QueryViewKey())
	require.True(t, ok)
	acquired.OnReady()
	up := view.IntoProto()
	up.Meta.State = viewpb.QueryViewState_QueryViewStateUp
	h.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtWorkNodeFromProto(up)}})
	return h.shards[view.ShardID()].views[view.QueryViewKey().QueryViewVersion]
}

func TestHandoffLinearWeight(t *testing.T) {
	w := linearHandoff{started: time.Now(), duration: 30 * time.Second, initial: .1}
	for offset, want := range map[time.Duration]float64{-time.Second: .1, 0: .1, 15 * time.Second: .55, 30 * time.Second: 1, time.Hour: 1} {
		require.InDelta(t, want, w.targetWeight(w.started.Add(offset)), 1e-12)
	}
}

func TestHandoffRetrySelectsTargetWithinEligibility(t *testing.T) {
	useLinearHandoff(t)
	h, shard, old, _, _ := leasedUpView(t, time.Minute)
	schema, load := old.sm.meta.GetSchemaVersion(), old.sm.meta.GetLoadInfoVersion()
	target := addHandoffView(t, h, 2, schema, load)
	patch := mockey.Mock(rand.Float64).Return(.99).Build()
	defer patch.UnPatch()
	for _, bypass := range []bool{false, true, false} {
		lease, err := h.AcquireUpViewForRouting(t.Context(), shard.shardID.VChannel, &schema, &viewpb.QueryRoutingConstraints{BypassHandoff: bypass})
		require.NoError(t, err)
		want := old.View.QueryViewKey().QueryViewVersion
		if bypass {
			want = target.View.QueryViewKey().QueryViewVersion
		}
		require.Equal(t, want, lease.Version)
		lease.Release()
		require.NotNil(t, target.handoff, "retry must not change the policy for other requests")
	}
	addHandoffView(t, h, 3, schema+1, load)
	lease, err := h.AcquireUpViewForRouting(t.Context(), shard.shardID.VChannel, &schema, &viewpb.QueryRoutingConstraints{BypassHandoff: true})
	require.NoError(t, err)
	require.EqualValues(t, 2, lease.Version.QueryVersion, "retry preserves exact schema")
	lease.Release()
	addHandoffView(t, h, 4, schema, load+1)
	lease, err = h.AcquireUpViewForRouting(t.Context(), shard.shardID.VChannel, &schema, &viewpb.QueryRoutingConstraints{BypassHandoff: true})
	require.NoError(t, err)
	require.EqualValues(t, 4, lease.Version.QueryVersion, "retry respects the greatest Up LoadInfoVersion")
	lease.Release()
}

func TestHandoffPlanningAndExplicitVersions(t *testing.T) {
	useLinearHandoff(t)
	h, shard, old, _, reports := leasedUpView(t, time.Minute)
	oldVersion := old.View.QueryViewKey().QueryViewVersion
	lease, err := h.AcquireUpView(t.Context(), shard.shardID, oldVersion)
	require.NoError(t, err)
	lease.Release()
	target := addHandoffView(t, h, 2, old.sm.Meta().GetSchemaVersion(), old.sm.Meta().GetLoadInfoVersion())
	require.NotNil(t, target.handoff)
	started := target.handoff.started
	h.ApplyViews([]handler.ApplyView{{View: target.View}})
	require.Equal(t, started, target.handoff.started, "duplicate Up cannot reset the window")
	pushLeaseDown(h, reports)
	var random float64
	patch := mockey.Mock(rand.Float64).To(func() float64 { return random }).Build()
	defer patch.UnPatch()
	for _, test := range []struct {
		random  float64
		version int64
	}{{0, 2}, {.99, 1}} {
		random = test.random
		planned, err := h.AcquireUpViewAtSchema(t.Context(), shard.shardID, old.sm.Meta().GetSchemaVersion())
		require.NoError(t, err)
		require.Equal(t, test.version, planned.Version.QueryVersion)
		planned.Release()
	}
	explicit, err := h.AcquireUpView(t.Context(), shard.shardID, target.View.QueryViewKey().QueryViewVersion)
	require.NoError(t, err)
	require.Equal(t, int64(2), explicit.Version.QueryVersion, "Phase 2 bypasses sampling")
	explicit.Release()
	old.leaseExpireAt = time.Now().Add(-time.Second)
	planned, err := h.AcquireLatestUpView(t.Context(), shard.shardID)
	require.NoError(t, err)
	require.Equal(t, int64(2), planned.Version.QueryVersion, "expired source ends handoff immediately")
	planned.Release()
	require.Nil(t, target.handoff)
	require.Equal(t, qviews.QueryViewStateDown, old.sm.State())
}

func TestHandoffBoundariesAndGenerations(t *testing.T) {
	useLinearHandoff(t)
	h, shard, old, _, _ := leasedUpView(t, time.Minute)
	schema, load := old.sm.Meta().GetSchemaVersion(), old.sm.Meta().GetLoadInfoVersion()
	b := addHandoffView(t, h, 2, schema, load)
	require.NotNil(t, b.handoff)
	c := addHandoffView(t, h, 3, schema, load)
	require.NotNil(t, c.handoff, "third generation restarts linear handoff")
	require.Equal(t, old.View.QueryViewKey().QueryViewVersion, c.handoff.source)
	require.Nil(t, b.handoff)
	d := addHandoffView(t, h, 4, schema, load+1)
	require.Nil(t, d.handoff, "never mix load configurations")
	// A late historical-schema replacement has a larger QV but older LoadInfo.
	e := addHandoffView(t, h, 5, schema, load)
	require.Nil(t, e.handoff)
	planned, err := h.AcquireUpViewAtSchema(t.Context(), shard.shardID, schema)
	require.NoError(t, err)
	require.Equal(t, int64(4), planned.Version.QueryVersion)
	planned.Release()
	f := addHandoffView(t, h, 6, schema+1, load+1)
	require.Nil(t, f.handoff, "never mix schemas")
	planned, err = h.AcquireLatestUpView(t.Context(), shard.shardID)
	require.NoError(t, err)
	require.Equal(t, int64(6), planned.Version.QueryVersion)
	planned.Release()
}

func TestHandoffTermination(t *testing.T) {
	for _, reason := range []string{"elapsed", "policy_changed", "source missing", "source preempted", "source down"} {
		t.Run(reason, func(t *testing.T) {
			useLinearHandoff(t)
			h, shard, old, _, _ := leasedUpView(t, time.Minute)
			target := addHandoffView(t, h, 2, old.sm.Meta().GetSchemaVersion(), old.sm.Meta().GetLoadInfoVersion())
			require.NotNil(t, target.handoff)
			switch reason {
			case "elapsed":
				target.handoff.started = time.Now().Add(-time.Minute)
			case "policy_changed":
				require.NoError(t, paramtable.Get().Save("queryView.handoffPolicy.type", string(paramtable.HandoffPolicyImmediate)))
			case "source missing":
				delete(shard.views, old.View.QueryViewKey().QueryViewVersion)
			case "source preempted":
				old.disableServingLease()
			case "source down":
				old.sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
			}
			version, selected := shard.selectHandoffLocked(target.View.QueryViewKey().QueryViewVersion, target)
			require.Same(t, target, selected)
			require.Equal(t, int64(2), version.QueryVersion)
			require.Nil(t, target.handoff)
		})
	}
}

func TestHandoffDisabledAndInvalidConfiguration(t *testing.T) {
	for _, config := range []struct{ key, value string }{{"type", "immediate"}, {"type", "unknown"}, {"linear.duration", "0s"}, {"linear.initialRatio", "1"}, {"linear.initialRatio", "-0.1"}, {"linear.initialRatio", "NaN"}} {
		t.Run(config.key+config.value, func(t *testing.T) {
			useLinearHandoff(t)
			require.NoError(t, paramtable.Get().Save("queryView.handoffPolicy."+config.key, config.value))
			h, _, old, _, _ := leasedUpView(t, 0)
			target := addHandoffView(t, h, 2, old.sm.Meta().GetSchemaVersion(), old.sm.Meta().GetLoadInfoVersion())
			require.Nil(t, target.handoff)
		})
	}
}

func TestHandoffRecoverySkipsWindow(t *testing.T) {
	useLinearHandoff(t)
	resources := newMockResourceManager()
	var views []*viewpb.QueryViewOfShard
	for _, version := range []int64{1, 2} {
		meta := buildHandlerTestMeta(version)
		meta.State = viewpb.QueryViewState_QueryViewStateUp
		views = append(views, &viewpb.QueryViewOfShard{Meta: meta, StreamingNode: &viewpb.QueryViewOfStreamingNode{}})
	}
	h := recoverSNQueryViewHandler(t.Context(), testPChannel, newMockCatalog(), resources, views)
	shard := h.shards[qviews.NewShardIDFromQVMeta(views[0].Meta)]
	for _, entry := range shard.views {
		acquired, ok := resources.getAcquired(entry.View.QueryViewKey())
		require.True(t, ok)
		acquired.OnReady()
		require.Nil(t, entry.handoff)
	}
	lease, err := h.AcquireLatestUpView(t.Context(), shard.shardID)
	require.NoError(t, err)
	require.Equal(t, int64(2), lease.Version.QueryVersion)
	lease.Release()
}

func TestHandoffUnavailableTargetAndExpiredPreviousWindow(t *testing.T) {
	useLinearHandoff(t)
	h, shard, old, _, _ := leasedUpView(t, 0)
	target := addHandoffView(t, h, 2, old.sm.Meta().GetSchemaVersion(), old.sm.Meta().GetLoadInfoVersion())
	target.handoff.started = time.Now().Add(-time.Minute)
	next := addHandoffView(t, h, 3, old.sm.Meta().GetSchemaVersion(), old.sm.Meta().GetLoadInfoVersion())
	require.NotNil(t, next.handoff, "an expired window does not suppress the next handoff")
	require.Equal(t, target.View.QueryViewKey().QueryViewVersion, next.handoff.source)
	next.disableServingLease()
	lease, err := h.AcquireLatestUpView(t.Context(), shard.shardID)
	require.NoError(t, err)
	require.Equal(t, int64(2), lease.Version.QueryVersion)
	lease.Release()
	require.Nil(t, target.handoff)
}

func TestHandoffRecoveryNeverFallsBackToOlderLoadInfo(t *testing.T) {
	useLinearHandoff(t)
	resources := newMockResourceManager()
	var views []*viewpb.QueryViewOfShard
	for _, version := range []int64{1, 2} {
		meta := buildHandlerTestMeta(version)
		meta.LoadInfoVersion = uint64(version)
		meta.State = viewpb.QueryViewState_QueryViewStateUp
		views = append(views, &viewpb.QueryViewOfShard{Meta: meta, StreamingNode: &viewpb.QueryViewOfStreamingNode{}})
	}
	h := recoverSNQueryViewHandler(t.Context(), testPChannel, newMockCatalog(), resources, views)
	shard := h.shards[qviews.NewShardIDFromQVMeta(views[0].Meta)]
	old, ok := resources.getAcquired(qviews.NewQueryViewAtWorkNodeFromProto(views[0]).QueryViewKey())
	require.True(t, ok)
	old.OnReady()
	lease, changed, err := shard.tryAcquireUpViewForSchema(t.Context(), nil)
	require.NoError(t, err)
	require.Nil(t, lease)
	require.NotNil(t, changed, "newer load config recovery must block older admission")
	newest, ok := resources.getAcquired(qviews.NewQueryViewAtWorkNodeFromProto(views[1]).QueryViewKey())
	require.True(t, ok)
	newest.OnUnrecoverable()
	lease, changed, err = shard.tryAcquireUpViewForSchema(t.Context(), nil)
	require.Error(t, err)
	require.Nil(t, lease)
	require.Nil(t, changed, "failed recovery cannot roll back the load boundary")
}

// Tests that specifically require the latest version use immediate explicitly.
func useImmediateHandoff(t *testing.T) {
	t.Helper()
	paramtable.Init()
	key := paramtable.Get().QueryViewCfg.HandoffPolicy.Key
	require.NoError(t, paramtable.Get().Save(key, string(paramtable.HandoffPolicyImmediate)))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

func TestDefaultLinearHandoff(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().QueryViewCfg
	require.Equal(t, paramtable.HandoffPolicyLinear, cfg.GetHandoffPolicy())
	h, _, old, _, _ := leasedUpView(t, time.Minute)
	target := addHandoffView(t, h, 2, old.sm.Meta().GetSchemaVersion(), old.sm.Meta().GetLoadInfoVersion())
	require.NotNil(t, target.handoff)
	require.Equal(t, 30*time.Second, target.handoff.duration)
	require.Equal(t, .1, target.handoff.initial)
}

func TestLinearHandoffReplacesTargetAndRetainsSource(t *testing.T) {
	useLinearHandoff(t)
	h, shard, a, _, reports := leasedUpView(t, time.Minute)
	versionA := a.View.QueryViewKey().QueryViewVersion
	lease, err := h.AcquireUpView(t.Context(), shard.shardID, versionA)
	require.NoError(t, err)
	lease.Release()
	pushLeaseDown(h, reports)
	require.True(t, a.pendingDown, "existing serving lease keeps the source eligible")
	schema, load := a.sm.Meta().GetSchemaVersion(), a.sm.Meta().GetLoadInfoVersion()
	previous := addHandoffView(t, h, 2, schema, load)
	var random float64
	patch := mockey.Mock(rand.Float64).To(func() float64 { return random }).Build()
	defer patch.UnPatch()
	for _, version := range []int64{3, 4, 5} {
		previous.handoff.started = time.Now().Add(-15 * time.Second)
		before := time.Now()
		target := addHandoffView(t, h, version, schema, load)
		require.Nil(t, previous.handoff, "superseded target cannot keep an active window")
		require.NotNil(t, target.handoff)
		require.Equal(t, versionA, target.handoff.source)
		require.False(t, target.handoff.started.Before(before))
		require.InDelta(t, .1, target.handoff.targetWeight(target.handoff.started), 1e-12)
		for _, sample := range []struct {
			random float64
			want   int64
		}{{.5, 1}, {0, version}} {
			random = sample.random
			planned, err := h.AcquireUpViewAtSchema(t.Context(), shard.shardID, schema)
			require.NoError(t, err)
			require.Equal(t, sample.want, planned.Version.QueryVersion)
			planned.Release()
		}
		explicit, err := h.AcquireUpView(t.Context(), shard.shardID, previous.View.QueryViewKey().QueryViewVersion)
		require.NoError(t, err, "existing plans may still execute on the superseded target")
		explicit.Release()
		previous = target
	}
}

func TestLinearHandoffReplacesUnavailableSource(t *testing.T) {
	for _, state := range []string{"missing", "disabled", "down", "expired"} {
		t.Run(state, func(t *testing.T) {
			useLinearHandoff(t)
			h, shard, a, _, reports := leasedUpView(t, time.Minute)
			schema, load := a.sm.Meta().GetSchemaVersion(), a.sm.Meta().GetLoadInfoVersion()
			b := addHandoffView(t, h, 2, schema, load)
			switch state {
			case "missing":
				delete(shard.views, a.View.QueryViewKey().QueryViewVersion)
			case "disabled":
				a.disableServingLease()
			case "down":
				a.sm.OnCoordStateDelivered(qviews.QueryViewStateDown)
			case "expired":
				lease, err := h.AcquireUpView(t.Context(), shard.shardID, a.View.QueryViewKey().QueryViewVersion)
				require.NoError(t, err)
				lease.Release()
				pushLeaseDown(h, reports)
				a.leaseExpireAt = time.Now().Add(-time.Second)
			}
			c := addHandoffView(t, h, 3, schema, load)
			require.Nil(t, b.handoff)
			require.NotNil(t, c.handoff)
			require.Equal(t, b.View.QueryViewKey().QueryViewVersion, c.handoff.source)
			require.InDelta(t, .1, c.handoff.targetWeight(c.handoff.started), 1e-12)
			if state == "expired" {
				require.Equal(t, qviews.QueryViewStateDown, a.sm.State(), "expired pending Down source is not revived")
			}
		})
	}
}

func TestLinearHandoffDoesNotRetainSupersededTargets(t *testing.T) {
	useLinearHandoff(t)
	h, shard, a, _, _ := leasedUpView(t, time.Minute)
	schema, load := a.sm.Meta().GetSchemaVersion(), a.sm.Meta().GetLoadInfoVersion()
	var target *snViewEntry
	for _, version := range []int64{2, 3, 4, 5} {
		target = addHandoffView(t, h, version, schema, load)
	}
	require.Equal(t, a.View.QueryViewKey().QueryViewVersion, target.handoff.source)
	resources := h.resMgr.(*mockResourceManager)
	for _, version := range []int64{2, 3, 4} {
		key := newPreparingSNView(version).QueryViewKey()
		entry := shard.views[key.QueryViewVersion]
		require.Nil(t, entry.handoff)
		require.Zero(t, entry.queryRefs, "handoff metadata does not pin superseded targets")
		lease, err := h.AcquireUpView(t.Context(), shard.shardID, key.QueryViewVersion)
		require.NoError(t, err)
		entry.leaseExpireAt = time.Now().Add(-time.Second)
		h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(version, viewpb.QueryViewState_QueryViewStateDown)}})
		require.Equal(t, qviews.QueryViewStateUp, entry.sm.State(), "in-flight execution still protects Down")
		lease.Release()
		require.Equal(t, qviews.QueryViewStateDown, entry.sm.State(), "latest handoff window does not block Down")
		h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(version, viewpb.QueryViewState_QueryViewStateDropped)}})
		require.True(t, entry.releaseStarted)
		resources.invokeReleaseCallback(key)
		require.Equal(t, qviews.QueryViewStateDropped, entry.sm.State())
		require.NotContains(t, shard.views, key.QueryViewVersion)
		require.NotNil(t, target.handoff)
		require.Less(t, target.handoff.targetWeight(time.Now()), 1.0)
	}
	require.Equal(t, 3, resources.releasedCount())
	require.Equal(t, qviews.QueryViewStateUp, a.sm.State())
	require.Equal(t, qviews.QueryViewStateUp, target.sm.State())
}

func TestHandoffPolicyHotUpdate(t *testing.T) {
	useLinearHandoff(t)
	params := paramtable.Get()
	cfg := &params.QueryViewCfg
	h, shard, a, _, _ := leasedUpView(t, time.Minute)
	schema, load := a.sm.Meta().GetSchemaVersion(), a.sm.Meta().GetLoadInfoVersion()
	b := addHandoffView(t, h, 2, schema, load)
	require.NotNil(t, b.handoff)
	patch := mockey.Mock(rand.Float64).Return(.9).Build()
	defer patch.UnPatch()
	planned, err := h.AcquireUpViewAtSchema(t.Context(), shard.shardID, schema)
	require.NoError(t, err)
	require.Equal(t, int64(1), planned.Version.QueryVersion)
	planned.Release()

	require.NoError(t, params.Save(cfg.LinearHandoffDuration.Key, "60s"))
	require.NoError(t, params.Save(cfg.LinearHandoffInitialRatio.Key, "0.25"))
	require.Equal(t, 30*time.Second, b.handoff.duration, "active windows retain sampled parameters")
	require.Equal(t, .1, b.handoff.initial)
	require.NoError(t, params.Save(cfg.HandoffPolicy.Key, string(paramtable.HandoffPolicyImmediate)))
	planned, err = h.AcquireUpViewAtSchema(t.Context(), shard.shardID, schema)
	require.NoError(t, err)
	require.Equal(t, int64(2), planned.Version.QueryVersion)
	planned.Release()
	require.Nil(t, b.handoff)

	require.NoError(t, params.Save(cfg.HandoffPolicy.Key, string(paramtable.HandoffPolicyLinear)))
	planned, err = h.AcquireUpViewAtSchema(t.Context(), shard.shardID, schema)
	require.NoError(t, err)
	require.Equal(t, int64(2), planned.Version.QueryVersion, "switching back does not retroactively start a window")
	planned.Release()
	c := addHandoffView(t, h, 3, schema, load)
	require.NotNil(t, c.handoff)
	require.Equal(t, b.View.QueryViewKey().QueryViewVersion, c.handoff.source)
	require.Equal(t, 60*time.Second, c.handoff.duration)
	require.Equal(t, .25, c.handoff.initial)
	planned, err = h.AcquireUpViewAtSchema(t.Context(), shard.shardID, schema)
	require.NoError(t, err)
	require.Equal(t, int64(2), planned.Version.QueryVersion)
	planned.Release()
}
