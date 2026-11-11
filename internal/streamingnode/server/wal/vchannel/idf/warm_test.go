package idf

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func mockSealedResources(t *testing.T, fn func(context.Context, qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error)) {
	t.Helper()
	patch := mockey.Mock((*Provider).getSealedBM25Resources).To(func(_ *Provider, ctx context.Context, _ int64, _ string, version qviews.DataVersion, _ []int64, _ uint64) ([]*datapb.StreamingNodeBM25Resource, error) {
		return fn(ctx, version)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

func startPrewarm(ctx context.Context, r *oracleRuntime, target qviews.DataVersion) <-chan error {
	done := make(chan error, 1)
	go func() { _, err := r.awaitMaterializationAt(ctx, &target); done <- err }()
	return done
}

func TestPrewarmMaterializesLazyAggregateOnce(t *testing.T) {
	var calls atomic.Int32
	mockSealedResources(t, func(context.Context, qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error) {
		calls.Add(1)
		return nil, nil
	})
	r := newLazyOracle(t)
	require.NoError(t, <-startPrewarm(t.Context(), r, r.currentVersion))
	require.NoError(t, <-startPrewarm(t.Context(), r, r.currentVersion))
	require.NoError(t, r.ensureMaterialized(t.Context()))
	require.EqualValues(t, 1, calls.Load())
}

func TestPrewarmFailureRequiresAnotherExplicitRequest(t *testing.T) {
	var calls atomic.Int32
	mockSealedResources(t, func(context.Context, qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error) {
		if calls.Add(1) == 1 {
			return nil, context.DeadlineExceeded
		}
		return nil, nil
	})
	r := newLazyOracle(t)
	target := r.currentVersion
	require.ErrorIs(t, <-startPrewarm(t.Context(), r, target), context.DeadlineExceeded)
	require.Never(t, func() bool { return calls.Load() > 1 }, 100*time.Millisecond, time.Millisecond)
	require.NoError(t, <-startPrewarm(t.Context(), r, target))
	require.EqualValues(t, 2, calls.Load())
}

func TestPrewarmFinishesOldTargetWithoutFollowingNewView(t *testing.T) {
	started := make(chan context.Context, 1)
	finish := make(chan struct{})
	var calls atomic.Int32
	mockSealedResources(t, func(ctx context.Context, version qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error) {
		calls.Add(1)
		if version.StreamingVersion == 10 {
			started <- ctx
			select {
			case <-finish:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, nil
	})
	r := newLazyOracle(t)
	done := startPrewarm(t.Context(), r, r.currentVersion)
	ctx := <-started
	require.NoError(t, r.PrepareDataVersion(t.Context(), qviews.DataVersion{StreamingVersion: 11}))
	require.NoError(t, ctx.Err(), "changing the View must preserve the acquired reads")
	close(finish)
	require.NoError(t, <-done)
	require.EqualValues(t, 1, calls.Load())
	r.mu.RLock()
	stats := r.currentStats
	r.mu.RUnlock()
	require.Nil(t, stats, "old aggregate cannot be published for the new target")
	require.NoError(t, <-startPrewarm(t.Context(), r, qviews.DataVersion{StreamingVersion: 11}))
	require.EqualValues(t, 2, calls.Load())
}

func TestPrewarmCancellationPreservesSharingQuery(t *testing.T) {
	started := make(chan context.Context, 1)
	finish := make(chan struct{})
	mockSealedResources(t, func(ctx context.Context, _ qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error) {
		started <- ctx
		select {
		case <-finish:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	r := newLazyOracle(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := startPrewarm(ctx, r, r.currentVersion)
	loadCtx := <-started
	query := make(chan error, 1)
	go func() { query <- r.ensureMaterialized(t.Context()) }()
	require.Eventually(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.materialization != nil && r.materialization.waiters == 2
	}, 5*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, loadCtx.Err())
	close(finish)
	require.NoError(t, <-query)
}

func TestPrewarmCancellationAndShutdownDiscardUnsharedWork(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "request", true: "shutdown"}[shutdown], func(t *testing.T) {
			started := make(chan context.Context, 1)
			mockSealedResources(t, func(ctx context.Context, _ qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error) {
				started <- ctx
				<-ctx.Done()
				return nil, ctx.Err()
			})
			r := newLazyOracle(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := startPrewarm(ctx, r, r.currentVersion)
			loadCtx := <-started
			if shutdown {
				r.Close()
			} else {
				cancel()
			}
			require.ErrorIs(t, <-done, context.Canceled)
			require.Eventually(t, func() bool { return loadCtx.Err() != nil }, time.Second, time.Millisecond)
		})
	}
}

func TestPrewarmShutdownDrainsDetachedFlight(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	mockSealedResources(t, func(context.Context, qviews.DataVersion) ([]*datapb.StreamingNodeBM25Resource, error) {
		close(started)
		<-finish // Simulate a storage call that cannot immediately honor cancellation.
		return nil, nil
	})
	r := newLazyOracle(t)
	done := startPrewarm(t.Context(), r, r.currentVersion)
	<-started
	require.NoError(t, r.PrepareDataVersion(t.Context(), qviews.DataVersion{StreamingVersion: 11}))
	closed := make(chan struct{})
	go func() { r.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("closed resources before the detached materialization returned")
	case <-time.After(20 * time.Millisecond):
	}
	close(finish)
	require.ErrorIs(t, <-done, context.Canceled)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not drain prewarm")
	}
}
