package queryclient

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func breakerTestKey(channel string, replica, data, version int64) qviews.QueryViewKey {
	return qviews.QueryViewKey{ShardID: qviews.ShardID{VChannel: channel, ReplicaID: replica}, QueryViewVersion: qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: data}, QueryVersion: version}}
}

func beginBreakerTest(t *testing.T, b *QueryViewCircuitBreaker, channel string) (context.Context, *routingAttempt) {
	t.Helper()
	return b.begin(t.Context(), channel, make(map[int64]localNodeExclusion), make(map[qviews.QueryViewKey]time.Time), nil)
}

func TestViewExclusionIsExactScopedAndExpiresWithoutProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{ViewExclusionTTL: 10 * time.Second})
		key := breakerTestKey("a", 1, 2, 3)
		node := qviews.NewQueryNode(11)
		ctx, _ := beginBreakerTest(t, b, "a")
		token := time.Now()
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
	})
}

func TestSessionGoneIsNotPickerAbsenceAndNeverProbed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{SessionGoneTTL: 10 * time.Second})
		key := breakerTestKey("a", 1, 1, 1)
		node := qviews.NewQueryNode(11)
		ctx, _ := beginBreakerTest(t, b, "a")
		b.observe(ctx, node, key, time.Now(), &viewerror.NodeSessionUnavailable{Cause: picker.ErrSubConnNotExist})
		require.Empty(t, b.nodes)
		b.observe(ctx, node, key, time.Now(), &viewerror.NodeSessionUnavailable{Cause: picker.ErrSubConnNotExist, Gone: true})
		time.Sleep(5 * time.Second)
		b.observe(ctx, node, key, time.Now(), nil)
		_, snapshot := beginBreakerTest(t, b, "b")
		require.Equal(t, []int64{11}, snapshot.constraints.ExcludedQueryNodeIds)
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
		token := time.Now()
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
		b.observe(ctx, node, key, time.Now(), err)
	}
	require.Empty(t, b.nodes)
	require.Empty(t, b.views)
	for _, id := range []int64{11, 12} {
		n := qviews.NewQueryNode(id)
		b.observe(ctx, n, key, time.Now(), &viewerror.NodeSessionUnavailable{Cause: picker.ErrSubConnNotExist, Gone: true})
	}
	require.Len(t, b.nodes, 1)
	b.observe(ctx, node, key, time.Now(), viewerror.NewViewNotFound("one"))
	other := breakerTestKey("b", 1, 1, 1)
	b.observe(ctx, node, other, time.Now(), viewerror.NewViewInvalidated("two"))
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

func BenchmarkQueryViewCircuitBreakerHealthy(b *testing.B) {
	breaker := NewQueryViewCircuitBreaker(QueryViewCircuitBreakerConfig{})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			breaker.begin(b.Context(), "healthy", make(map[int64]localNodeExclusion), make(map[qviews.QueryViewKey]time.Time), nil)
		}
	})
}
