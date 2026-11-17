package queryclient

import (
	"context"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func breakerTestKey(channel string, replica, data, version int64) qviews.QueryViewKey {
	return qviews.QueryViewKey{ShardID: qviews.ShardID{VChannel: channel, ReplicaID: replica}, QueryViewVersion: qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: data}, QueryVersion: version}}
}

func beginBreakerTest(t *testing.T, b *QueryViewCircuitBreaker, channel string) (context.Context, *routingAttempt) {
	t.Helper()
	return b.begin(t.Context(), channel, make(map[int64]time.Time), make(map[qviews.QueryViewKey]time.Time))
}

func breakerTestPlan(key qviews.QueryViewKey, nodes ...int64) *viewpb.QueryPlan {
	plan := &viewpb.QueryPlan{ShardId: key.ShardID.IntoProto(), Version: key.QueryViewVersion.IntoProto()}
	for _, id := range nodes {
		plan.WorkNodes = append(plan.WorkNodes, &viewpb.QueryPlanWorkNode{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: id}}})
	}
	return plan
}

func TestViewExclusionIsExactScopedAndExpiresWithoutProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{ViewExclusionTTL: 10 * time.Second})
		key := breakerTestKey("a", 1, 2, 3)
		node := qviews.NewQueryNode(11)
		ctx, first := beginBreakerTest(t, b, "a")
		token := b.observeStart(ctx, node)
		b.observe(ctx, node, key, token, viewerror.NewViewNotFound("missing"))
		_, next := beginBreakerTest(t, b, "a")
		exclusions := qviews.NewQueryRoutingExclusions("a", next.constraints)
		require.True(t, exclusions.ExcludesView(key))
		require.False(t, exclusions.ExcludesView(breakerTestKey("a", 2, 2, 3)))
		require.False(t, exclusions.ExcludesView(breakerTestKey("a", 1, 3, 3)))
		require.Empty(t, next.constraints.ExcludedQueryNodeIds)
		_, other := beginBreakerTest(t, b, "b")
		require.Empty(t, other.constraints.ExcludedQueryViews)
		// Success and repeated callbacks do not undo or refresh a View tombstone.
		time.Sleep(9 * time.Second)
		b.observe(ctx, node, key, token, nil)
		b.observe(ctx, node, key, token, viewerror.NewViewInvalidated("dropped"))
		time.Sleep(time.Second)
		_, expired := beginBreakerTest(t, b, "a")
		require.Empty(t, expired.constraints.ExcludedQueryViews)
		b.observe(ctx, node, key, token, viewerror.NewViewNotFound("late"))
		require.Empty(t, b.views)
		first.release()
	})
}

func TestNodeConnectionProbeIsSharedAndFencesLateResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{NodeFailureThreshold: 2})
		key := breakerTestKey("a", 1, 1, 1)
		node := qviews.NewQueryNode(11)
		ctx, _ := beginBreakerTest(t, b, "a")
		old := b.observeStart(ctx, node)
		b.observe(ctx, node, key, old, io.EOF)
		b.observe(ctx, node, key, b.observeStart(ctx, node), status.Error(codes.Unavailable, "connection lost"))
		_, blocked := beginBreakerTest(t, b, "other-channel")
		require.Equal(t, []int64{11}, blocked.constraints.ExcludedQueryNodeIds)
		b.observe(ctx, node, key, old, nil)
		require.False(t, b.nodes[11].retryAt.IsZero(), "late success must not close a newer circuit")
		// Retained execution may still reach the node during cooldown, but it
		// does not own the probe needed to close this generation.
		passive := b.observeStart(t.Context(), node)
		b.observe(t.Context(), node, key, passive, nil)
		require.False(t, b.nodes[11].retryAt.IsZero())
		time.Sleep(1300 * time.Millisecond)
		probeCtx, probe := beginBreakerTest(t, b, "a")
		require.Empty(t, probe.constraints.ExcludedQueryNodeIds)
		require.Empty(t, probe.probes, "planning must not reserve probes")
		require.True(t, probe.acquireProbes(breakerTestPlan(key, 11)))
		require.Len(t, probe.probes, 1)
		_, concurrent := beginBreakerTest(t, b, "other-channel")
		require.Equal(t, []int64{11}, concurrent.constraints.ExcludedQueryNodeIds)
		probeToken := b.observeStart(probeCtx, node)
		b.observe(probeCtx, node, key, probeToken, viewerror.NewViewNotFound("view is gone, node answered"))
		require.True(t, b.nodes[11].retryAt.IsZero())
		b.observe(probeCtx, node, key, probeToken, io.EOF)
		require.Zero(t, b.nodes[11].failures, "late callback cannot reopen recovered generation")
		probe.release()
		_, recovered := beginBreakerTest(t, b, "a")
		require.Empty(t, recovered.constraints.ExcludedQueryNodeIds)
		require.Len(t, recovered.constraints.ExcludedQueryViews, 1)
	})
}

func TestNodeProbeFailureBackoffAndUnusedPermitRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{NodeFailureThreshold: 1})
		key := breakerTestKey("a", 1, 1, 1)
		node := qviews.NewQueryNode(11)
		ctx, _ := beginBreakerTest(t, b, "a")
		b.observe(ctx, node, key, b.observeStart(ctx, node), io.EOF)
		time.Sleep(1300 * time.Millisecond)
		probeCtx, probe := beginBreakerTest(t, b, "a")
		require.True(t, probe.acquireProbes(breakerTestPlan(key, 11)))
		require.Same(t, probe, b.nodes[11].probe)
		b.observe(probeCtx, node, key, b.observeStart(probeCtx, node), io.EOF)
		require.Equal(t, 2*time.Second, b.nodes[11].backoff)
		time.Sleep(2500 * time.Millisecond)
		_, unused := beginBreakerTest(t, b, "a")
		require.True(t, unused.acquireProbes(breakerTestPlan(key, 11)))
		require.Len(t, unused.probes, 1)
		unused.release()
		require.Nil(t, b.nodes[11].probe)
		require.Equal(t, 2*time.Second, b.nodes[11].backoff)
	})
}

func TestSessionGoneIsNotPickerAbsenceAndNeverProbed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{SessionGoneTTL: 10 * time.Second})
		key := breakerTestKey("a", 1, 1, 1)
		node := qviews.NewQueryNode(11)
		ctx, _ := beginBreakerTest(t, b, "a")
		b.observe(ctx, node, key, b.observeStart(ctx, node), &viewerror.NodeSessionUnavailable{Cause: picker.ErrSubConnNotExist})
		require.False(t, b.nodes[11].gone)
		b.observe(ctx, node, key, b.observeStart(ctx, node), &viewerror.NodeSessionUnavailable{Cause: picker.ErrSubConnNotExist, Gone: true})
		time.Sleep(5 * time.Second)
		b.observe(ctx, node, key, b.observeStart(ctx, node), nil)
		_, snapshot := beginBreakerTest(t, b, "b")
		require.Equal(t, []int64{11}, snapshot.constraints.ExcludedQueryNodeIds)
		require.Empty(t, snapshot.probes)
		time.Sleep(5 * time.Second)
		_, snapshot = beginBreakerTest(t, b, "a")
		require.Empty(t, snapshot.constraints.ExcludedQueryNodeIds)
	})
}

func TestExpiredSessionGoneCallbackCannotRepopulateState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{SessionGoneTTL: time.Second})
		ctx, _ := beginBreakerTest(t, b, "a")
		node := qviews.NewQueryNode(11)
		token := b.observeStart(ctx, node)
		time.Sleep(time.Second)
		b.observe(ctx, node, breakerTestKey("a", 1, 1, 1), token, &viewerror.NodeSessionUnavailable{Cause: picker.ErrSubConnNotExist, Gone: true})
		require.Empty(t, b.nodes)
	})
}

func TestCircuitBreakerIgnoresNonAttributedFailuresAndBoundsMemory(t *testing.T) {
	b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{MaxNodes: 1, MaxViews: 1})
	key := breakerTestKey("a", 1, 1, 1)
	ctx, _ := beginBreakerTest(t, b, "a")
	node := qviews.NewQueryNode(11)
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, status.Error(codes.DeadlineExceeded, "timeout"), status.Error(codes.Canceled, "cancel"), merr.WrapErrParameterInvalidMsg("input"), viewerror.NewOnShutdownError("local client closing"), merr.WrapErrServiceInternalMsg("shared failure")} {
		b.observe(ctx, node, key, b.observeStart(ctx, node), err)
	}
	require.Empty(t, b.nodes)
	require.Empty(t, b.views)
	for _, id := range []int64{11, 12} {
		n := qviews.NewQueryNode(id)
		b.observe(ctx, n, key, b.observeStart(ctx, n), io.EOF)
	}
	require.Len(t, b.nodes, 1)
	b.observe(ctx, node, key, b.observeStart(ctx, node), viewerror.NewViewNotFound("one"))
	other := breakerTestKey("b", 1, 1, 1)
	b.observe(ctx, node, other, b.observeStart(ctx, node), viewerror.NewViewInvalidated("two"))
	require.Equal(t, 1, b.viewTTL.Len())
	require.NotContains(t, b.views, "a")
}

func TestCircuitBreakerWaitCancellation(t *testing.T) {
	b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{})
	_, attempt := beginBreakerTest(t, b, "a")
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, attempt.wait(canceled), context.Canceled)
}

func TestConcurrentPlanAdmissionReservesOnlyOneProbe(t *testing.T) {
	b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{})
	b.nodes[11] = &nodeCircuit{retryAt: time.Now().Add(-time.Second), expires: time.Now().Add(time.Minute), generation: 1}
	var wg sync.WaitGroup
	attempts := make([]*routingAttempt, 32)
	admitted := make([]bool, len(attempts))
	plan := breakerTestPlan(breakerTestKey("a", 1, 1, 1), 11)
	for i := range attempts {
		_, attempts[i] = beginBreakerTest(t, b, "a")
		require.Empty(t, attempts[i].constraints.ExcludedQueryNodeIds)
	}
	for i := range attempts {
		wg.Go(func() {
			admitted[i] = attempts[i].acquireProbes(plan)
		})
	}
	wg.Wait()
	probes := 0
	for i, a := range attempts {
		probes += len(a.probes)
		require.Equal(t, len(a.probes) == 1, admitted[i])
		a.release()
	}
	require.Equal(t, 1, probes)
}

func TestPlanAdmissionRechecksExclusionsWithoutPartialReservations(t *testing.T) {
	for _, reason := range []string{"cooldown", "gone", "probe", "view"} {
		t.Run(reason, func(t *testing.T) {
			b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{})
			key := breakerTestKey("a", 1, 1, 1)
			b.nodes[11] = &nodeCircuit{retryAt: time.Now().Add(-time.Second), expires: time.Now().Add(time.Minute)}
			_, attempt := beginBreakerTest(t, b, "a")
			entry := &nodeCircuit{expires: time.Now().Add(time.Minute)}
			switch reason {
			case "cooldown":
				entry.retryAt = time.Now().Add(time.Second)
			case "gone":
				entry.gone = true
			case "probe":
				entry.probe = &routingAttempt{}
			case "view":
				b.excludeViewLocked(key, time.Now(), time.Now())
			}
			b.nodes[12] = entry
			require.False(t, attempt.acquireProbes(breakerTestPlan(key, 11, 12)))
			require.Empty(t, attempt.probes)
			require.Nil(t, b.nodes[11].probe)
			// The healthy sibling's plan is independent of the rejected plan.
			other := breakerTestKey("b", 1, 1, 1)
			_, sibling := beginBreakerTest(t, b, "b")
			require.True(t, sibling.acquireProbes(breakerTestPlan(other, 11)))
			sibling.release()
		})
	}
}

func TestProbeReleaseWakesWaitersAndPreservesBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{})
		key := breakerTestKey("a", 1, 1, 1)
		b.nodes[11] = &nodeCircuit{retryAt: time.Now().Add(-time.Second), expires: time.Now().Add(time.Minute), backoff: 2 * time.Second}
		_, owner := beginBreakerTest(t, b, "a")
		require.True(t, owner.acquireProbes(breakerTestPlan(key, 11)))
		_, waiter := beginBreakerTest(t, b, "a")
		done := make(chan error, 1)
		started := time.Now()
		go func() { done <- waiter.wait(t.Context()) }()
		synctest.Wait()
		owner.release()
		require.NoError(t, <-done)
		require.Equal(t, started, time.Now(), "release must wake without the one-second poll")
		require.Equal(t, 2*time.Second, b.nodes[11].backoff)
		require.Nil(t, b.nodes[11].probe)
		// Notification must also survive a release between planning and wait.
		require.NoError(t, waiter.wait(t.Context()))
		require.Equal(t, started, time.Now())
		time.Sleep(shardRetryInterval)
		_, next := beginBreakerTest(t, b, "a")
		require.True(t, next.acquireProbes(breakerTestPlan(key, 11)))
		next.release()
	})
}

func TestAdmissionReservesOnlySelectedCoolingNodes(t *testing.T) {
	b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{})
	for _, id := range []int64{11, 12} {
		b.nodes[id] = &nodeCircuit{retryAt: time.Now().Add(-time.Second), expires: time.Now().Add(time.Minute)}
	}
	b.nodes[13] = &nodeCircuit{expires: time.Now().Add(time.Minute)}
	_, attempt := beginBreakerTest(t, b, "a")
	plan := breakerTestPlan(breakerTestKey("a", 1, 1, 1), 11, 11, 13, 14)
	plan.WorkNodes = append(plan.WorkNodes, &viewpb.QueryPlanWorkNode{Node: &viewpb.QueryPlanWorkNode_StreamingNode{StreamingNode: &viewpb.StreamingWorkNode{}}})
	require.True(t, attempt.acquireProbes(plan))
	require.Len(t, attempt.probes, 1)
	require.Same(t, attempt, b.nodes[11].probe)
	require.Nil(t, b.nodes[12].probe)
	require.Nil(t, b.nodes[13].probe)
	require.NotContains(t, b.nodes, int64(14))
	attempt.release()
}

func TestNodeFailureWindowAndHealthyResponseReset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{NodeFailureThreshold: 2, FailureWindow: time.Second})
		ctx, _ := beginBreakerTest(t, b, "a")
		node := qviews.NewQueryNode(11)
		key := breakerTestKey("a", 1, 1, 1)
		b.observe(ctx, node, key, b.observeStart(ctx, node), io.EOF)
		time.Sleep(time.Second)
		b.observe(ctx, node, key, b.observeStart(ctx, node), io.EOF)
		require.Equal(t, 1, b.nodes[11].failures)
		require.True(t, b.nodes[11].retryAt.IsZero())
		b.observe(ctx, node, key, b.observeStart(ctx, node), nil)
		require.Zero(t, b.nodes[11].failures)
	})
}

func BenchmarkQueryViewCircuitBreakerHealthy(b *testing.B) {
	breaker := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, permit := breaker.begin(b.Context(), "healthy", make(map[int64]time.Time), make(map[qviews.QueryViewKey]time.Time))
			permit.release()
		}
	})
}
