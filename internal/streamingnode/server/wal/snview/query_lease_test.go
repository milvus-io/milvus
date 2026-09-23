package snview

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func leasedUpView(t *testing.T, duration time.Duration) (*SNQueryViewHandler, *snShardView, *snViewEntry, *mockCatalog, *reportCollector) {
	t.Helper()
	cat, mgr, reports := newMockCatalog(), newMockResourceManager(), &reportCollector{}
	h := recoverSNQueryViewHandler(context.Background(), testPChannel, cat, mgr, nil)
	h.leaseDuration = duration
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{{View: view, OnReport: reports.onReport}})
	resource, ok := mgr.getAcquired(view.QueryViewKey())
	require.True(t, ok)
	resource.OnReady()
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: reports.onReport}})
	shard := h.shards[view.ShardID()]
	entry := shard.views[view.QueryViewKey().QueryViewVersion]
	t.Cleanup(func() {
		shard.mu.Lock()
		defer shard.mu.Unlock()
		for _, entry := range shard.views {
			entry.cancelPendingDown()
		}
	})
	return h, shard, entry, cat, reports
}

func pushLeaseDown(h *SNQueryViewHandler, reports *reportCollector) {
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDown), OnReport: reports.onReport}})
}

func TestServingLeaseRenewalAndDownCallbackReplacement(t *testing.T) {
	h, shard, entry, cat, reports := leasedUpView(t, time.Minute)
	// Deliver timer callbacks deterministically, including an early callback
	// left over from the deadline preceding renewal.
	var callbacks []func()
	patch := mockey.Mock(time.AfterFunc).IncludeCurrentGoRoutine().To(func(_ time.Duration, fn func()) *time.Timer {
		callbacks = append(callbacks, fn)
		timer := time.NewTimer(time.Hour)
		t.Cleanup(func() { timer.Stop() })
		return timer
	}).Build()
	defer patch.UnPatch()
	lease, err := h.AcquireLatestUpView(context.Background(), shard.shardID)
	require.NoError(t, err)
	lease.Release()
	deadline := entry.leaseExpireAt
	pushLeaseDown(h, reports)
	require.Equal(t, qviews.QueryViewStateUp, entry.sm.State())
	require.Equal(t, 1, cat.savedCount())
	require.Len(t, callbacks, 1)
	newReports := &reportCollector{}
	pushLeaseDown(h, newReports)
	require.Equal(t, deadline, entry.leaseExpireAt)
	require.Zero(t, newReports.count(), "duplicate Down must not cause an Up/Down sync loop")
	require.Len(t, callbacks, 1)
	phase2, err := h.AcquireUpView(context.Background(), shard.shardID, lease.Version)
	require.NoError(t, err)
	require.True(t, entry.leaseExpireAt.After(deadline))
	require.Equal(t, viewpb.QueryViewState_QueryViewStateUp, phase2.Meta.State)
	require.Equal(t, viewpb.QueryViewState_QueryViewStateUp, phase2.View.Meta.State)
	phase2.Release()
	callbacks[0]() // Original deadline: renewal must keep the view Up.
	require.Equal(t, qviews.QueryViewStateUp, entry.sm.State())
	require.Len(t, callbacks, 2)
	entry.leaseExpireAt = time.Now().Add(-time.Second)
	callbacks[1]()
	require.Equal(t, qviews.QueryViewStateDown, entry.sm.State())
	require.Zero(t, cat.savedCount())
	require.Equal(t, qviews.QueryViewStateDown, newReports.last().State())
	require.Equal(t, qviews.QueryViewStateUp, reports.last().State())
	_, err = h.AcquireUpView(context.Background(), shard.shardID, lease.Version)
	require.Error(t, err)
}

func TestServingLeaseExpiryWithoutAnotherRequest(t *testing.T) {
	h, shard, _, cat, reports := leasedUpView(t, 40*time.Millisecond)
	lease, err := h.AcquireLatestUpView(context.Background(), shard.shardID)
	require.NoError(t, err)
	lease.Release()
	pushLeaseDown(h, reports)
	require.Eventually(t, func() bool { return reports.last().State() == qviews.QueryViewStateDown }, 3*time.Second, time.Millisecond)
	require.Zero(t, cat.savedCount())
}

func TestServingLeaseActivePlanningRenewsBeforeReturn(t *testing.T) {
	h, shard, entry, _, reports := leasedUpView(t, time.Minute)
	lease, err := h.AcquireLatestUpView(context.Background(), shard.shardID)
	require.NoError(t, err)
	entry.leaseExpireAt = time.Now().Add(-time.Second) // Simulate a slow Phase 1.
	pushLeaseDown(h, reports)
	require.Equal(t, qviews.QueryViewStateUp, entry.sm.State())
	require.Nil(t, entry.downTimer, "active call release will recheck the deadline")
	lease.Renew()
	require.Greater(t, time.Until(entry.leaseExpireAt), 59*time.Second)
	lease.Release()
	require.NotNil(t, entry.downTimer)
	deadline := entry.leaseExpireAt
	lease.Release()
	lease.Renew() // Released references cannot keep renewing the view.
	require.Equal(t, deadline, entry.leaseExpireAt)
	require.Zero(t, entry.queryRefs)
}

func TestServingLeaseExpiredPendingDownCannotBeRevived(t *testing.T) {
	for _, latest := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit version", true: "latest version"}[latest], func(t *testing.T) {
			h, shard, entry, _, reports := leasedUpView(t, time.Minute)
			lease, err := h.AcquireLatestUpView(context.Background(), shard.shardID)
			require.NoError(t, err)
			lease.Release()
			pushLeaseDown(h, reports)
			shard.mu.Lock()
			entry.downTimer.Stop() // Simulate a delayed callback.
			entry.leaseExpireAt = time.Now().Add(-time.Second)
			shard.mu.Unlock()
			if latest {
				_, err = h.AcquireLatestUpView(context.Background(), shard.shardID)
			} else {
				_, err = h.AcquireUpView(context.Background(), shard.shardID, lease.Version)
			}
			require.Error(t, err)
			require.Equal(t, qviews.QueryViewStateDown, reports.last().State())
		})
	}
}

func TestServingLeaseNoAccessAndDisabledDuration(t *testing.T) {
	h, _, entry, _, reports := leasedUpView(t, time.Minute)
	pushLeaseDown(h, reports)
	require.Equal(t, qviews.QueryViewStateDown, entry.sm.State(), "unused Up views have no lease")
	h, shard, entry, _, reports := leasedUpView(t, 0)
	lease, err := h.AcquireLatestUpView(context.Background(), shard.shardID)
	require.NoError(t, err)
	pushLeaseDown(h, reports)
	require.Equal(t, qviews.QueryViewStateUp, entry.sm.State(), "an active call still protects the view")
	lease.Release()
	require.Equal(t, qviews.QueryViewStateDown, entry.sm.State())
}

func TestServingLeaseForcedDropCancelsTimer(t *testing.T) {
	h, shard, entry, _, reports := leasedUpView(t, time.Minute)
	lease, err := h.AcquireLatestUpView(context.Background(), shard.shardID)
	require.NoError(t, err)
	lease.Release()
	pushLeaseDown(h, reports)
	require.NotNil(t, entry.downTimer)
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: reports.onReport}})
	require.Nil(t, entry.downTimer)
	require.False(t, entry.pendingDown)
	require.Equal(t, qviews.QueryViewStateDropping, entry.sm.State())
}

func TestServingLeaseConfigurationIsCaptured(t *testing.T) {
	params := paramtable.Get()
	key := params.QueryViewCfg.LeaseDuration.Key
	require.Equal(t, "60s", params.QueryViewCfg.LeaseDuration.DefaultValue)
	original := params.QueryViewCfg.LeaseDuration.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save(key, original)) })
	require.NoError(t, params.Save(key, "3s"))
	h := recoverSNQueryViewHandler(context.Background(), testPChannel, newMockCatalog(), newMockResourceManager(), nil)
	require.Equal(t, 3*time.Second, h.leaseDuration)
	require.NoError(t, params.Save(key, "4s"))
	shard := h.getOrCreateShard(newPreparingSNView(1).ShardID())
	require.Equal(t, 3*time.Second, shard.leaseDuration)
}

func TestServingLeaseCancellationReleasesMVCCWait(t *testing.T) {
	for _, search := range []bool{false, true} {
		t.Run(map[bool]string{false: "query", true: "search"}[search], func(t *testing.T) {
			h, shard, entry, _, reports := leasedUpView(t, time.Minute)
			h.resMgr.(*mockResourceManager).runtime = &mockQueryRuntime{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			patch := mockey.Mock((*mockQueryRuntime).WaitMVCCVisible).To(func(_ *mockQueryRuntime, ctx context.Context, _, _ uint64) error {
				shard.mu.Lock()
				entry.leaseExpireAt = time.Now().Add(-time.Second)
				shard.mu.Unlock()
				pushLeaseDown(h, reports)
				require.Equal(t, qviews.QueryViewStateUp, entry.sm.State())
				cancel()
				return ctx.Err()
			}).Build()
			defer patch.UnPatch()
			version := newPreparingSNView(1).QueryViewKey().QueryViewVersion
			var err error
			if search {
				_, err = h.AcquireSearchSegmentTasks(ctx, shard.shardID, version, &viewpb.QueryPlanMVCC{}, &internalpb.SearchRequest{})
			} else {
				_, err = h.AcquireQuerySegmentTasks(ctx, shard.shardID, version, &viewpb.QueryPlanMVCC{}, &internalpb.RetrieveRequest{})
			}
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, entry.queryRefs)
			require.Equal(t, qviews.QueryViewStateDown, reports.last().State())
		})
	}
}

func TestServingLeaseLatestViewAndOldVersionAccess(t *testing.T) {
	h, shard, entry, _, reports := leasedUpView(t, time.Minute)
	old, err := h.AcquireLatestUpView(context.Background(), shard.shardID)
	require.NoError(t, err)
	old.Release()
	pushLeaseDown(h, reports)
	view := newPreparingSNView(2)
	h.ApplyViews([]handler.ApplyView{{View: view}})
	resource, _ := h.resMgr.(*mockResourceManager).getAcquired(view.QueryViewKey())
	resource.OnReady()
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(2, viewpb.QueryViewState_QueryViewStateUp)}})
	latest, err := h.AcquireLatestUpView(context.Background(), shard.shardID)
	require.NoError(t, err)
	require.Equal(t, view.QueryViewKey().QueryViewVersion, latest.Version)
	latest.Release()
	oldAgain, err := h.AcquireUpView(context.Background(), shard.shardID, old.Version)
	require.NoError(t, err)
	oldAgain.Release()
	require.Equal(t, qviews.QueryViewStateUp, entry.sm.State())
}

func TestServingLeaseHandoffCancelsTimerAndRecoveryDoesNotInheritLease(t *testing.T) {
	h, shard, entry, cat, reports := leasedUpView(t, time.Minute)
	var expire func()
	timerPatch := mockey.Mock(time.AfterFunc).IncludeCurrentGoRoutine().To(func(_ time.Duration, fn func()) *time.Timer {
		expire = fn
		return time.NewTimer(time.Hour)
	}).Build()
	defer timerPatch.UnPatch()
	lease, err := h.AcquireLatestUpView(context.Background(), shard.shardID)
	require.NoError(t, err)
	lease.Release()
	pushLeaseDown(h, reports)
	saved, err := cat.ListQueryViews(context.Background(), testPChannel)
	require.NoError(t, err)
	require.Len(t, saved, 1)
	mgr := h.resMgr.(*mockResourceManager)
	patch := mockey.Mock((*mockResourceManager).Release).To(func(_ *mockResourceManager, req ReleaseResource) {
		go req.OnDropped()
	}).Build()
	h.CloseForHandoff()
	patch.UnPatch()
	expire() // A callback already queued before cancellation must be harmless.
	require.Nil(t, entry.downTimer)
	require.False(t, entry.pendingDown)
	lease.Renew()
	h = recoverSNQueryViewHandler(context.Background(), testPChannel, cat, mgr, saved)
	resource, _ := mgr.getAcquired(newPreparingSNView(1).QueryViewKey())
	resource.OnReady()
	pushLeaseDown(h, reports)
	require.Equal(t, qviews.QueryViewStateDown, reports.last().State())
}

func TestServingLeaseCancelledAndMissingAcquisitionDoesNotRenew(t *testing.T) {
	h, shard, entry, _, _ := leasedUpView(t, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	version := newPreparingSNView(1).QueryViewKey().QueryViewVersion
	_, err := h.AcquireUpView(ctx, shard.shardID, version)
	require.ErrorIs(t, err, context.Canceled)
	_, err = shard.acquireUpView(ctx, version)
	require.ErrorIs(t, err, context.Canceled)
	_, err = shard.acquireLatestUpView(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = h.AcquireUpView(context.Background(), qviews.ShardID{}, version)
	require.Error(t, err)
	_, err = h.AcquireUpView(context.Background(), shard.shardID, qviews.QueryViewVersion{})
	require.Error(t, err)
	require.True(t, entry.leaseExpireAt.IsZero())
	require.Zero(t, entry.queryRefs)
}
