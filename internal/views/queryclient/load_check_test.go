package queryclient

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type loadCheckTestClient struct{ LoadInfoClient }

func (*loadCheckTestClient) GetQueryViewLoadInfo(context.Context, *querypb.GetQueryViewLoadInfoRequest, ...grpc.CallOption) (*querypb.GetQueryViewLoadInfoResponse, error) {
	panic("mockey")
}

func patchLoadCheck(t *testing.T, call func(context.Context, *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error)) *collectionLoadChecks {
	patch := mockey.Mock((*loadCheckTestClient).GetQueryViewLoadInfo).To(func(_ *loadCheckTestClient, ctx context.Context, req *querypb.GetQueryViewLoadInfoRequest, _ ...grpc.CallOption) (*querypb.GetQueryViewLoadInfoResponse, error) {
		return call(ctx, req)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return newCollectionLoadChecks(&loadCheckTestClient{})
}

func publishTestRoute(r *replicaRoute, snapshot qviews.ReplicaSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshot = snapshot
	close(r.changed)
	r.changed = make(chan struct{})
}

func TestReplicaLoadCheckWaitsForInitialization(t *testing.T) {
	route := &replicaRoute{changed: make(chan struct{})}
	var calls atomic.Int64
	checks := patchLoadCheck(t, func(context.Context, *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
		calls.Add(1)
		return nil, merr.WrapErrCollectionNotLoaded(100)
	})
	check := checks.acquire(100)
	defer check.release()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, _, err := route.nextReplica(ctx, nil, nil, check.check)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, calls.Load(), "uninitialized snapshots must not probe Coordinator")
	publishTestRoute(route, qviews.ReplicaSnapshot{Valid: true})
	_, _, err = route.nextReplica(t.Context(), nil, nil, check.check)
	require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
	require.EqualValues(t, 1, calls.Load())
	// Reconnection invalidates initialization even after a previous valid result.
	publishTestRoute(route, qviews.ReplicaSnapshot{})
	ctx2, cancel2 := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel2()
	_, _, err = route.nextReplica(ctx2, nil, nil, check.check)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.EqualValues(t, 1, calls.Load())
}

func TestReplicaLoadCheckRepeatsWithoutWatchUpdate(t *testing.T) {
	route := &replicaRoute{changed: make(chan struct{}), snapshot: qviews.ReplicaSnapshot{Valid: true}}
	calls := 0
	checks := patchLoadCheck(t, func(_ context.Context, req *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
		require.EqualValues(t, 100, req.GetCollectionID())
		require.Zero(t, req.GetVersion(), "load-state checks must not query history")
		calls++
		if calls == 1 {
			return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success()}, nil
		}
		return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Status(merr.WrapErrCollectionNotLoaded(100))}, nil
	})
	check := checks.acquire(100)
	defer check.release()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, _, err := route.nextReplica(ctx, nil, nil, check.check)
	require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
	require.Equal(t, 2, calls, "release with an unchanged empty directory must be detected")
}

func TestReplicaLoadCheckDoesNotOverrideNewReplica(t *testing.T) {
	route := &replicaRoute{changed: make(chan struct{}), snapshot: qviews.ReplicaSnapshot{Valid: true}}
	checks := patchLoadCheck(t, func(context.Context, *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
		publishTestRoute(route, qviews.ReplicaSnapshot{Valid: true, Replicas: []qviews.Replica{{ReplicaID: 7}}})
		return nil, merr.WrapErrCollectionNotLoaded(100)
	})
	check := checks.acquire(100)
	defer check.release()
	replica, _, err := route.nextReplica(t.Context(), nil, nil, check.check)
	require.NoError(t, err)
	require.EqualValues(t, 7, replica.ReplicaID)
}

func TestReplicaLoadCheckSharesProbesAndReleasesEntries(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	checks := patchLoadCheck(t, func(context.Context, *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-resume
		return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success()}, nil
	})
	first := checks.acquire(100)
	second := checks.acquire(100)
	require.Same(t, first, second)
	results := make(chan error, 20)
	var started sync.WaitGroup
	started.Add(20)
	for range 20 {
		go func() { started.Done(); _, err := first.check(t.Context()); results <- err }()
	}
	started.Wait()
	<-entered
	close(resume)
	for range 20 {
		require.NoError(t, <-results)
	}
	require.EqualValues(t, 1, calls.Load())
	first.release()
	require.Len(t, checks.collections, 1)
	second.release()
	require.Empty(t, checks.collections)
}

func TestReplicaLoadCheckPreservesErrorsAndDoesNotCacheNotLoaded(t *testing.T) {
	for _, failure := range []error{merr.WrapErrCollectionNotLoaded(100), merr.WrapErrServiceNotReadyMsg("recovering"), context.Canceled} {
		t.Run(failure.Error(), func(t *testing.T) {
			calls := 0
			checks := patchLoadCheck(t, func(context.Context, *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
				calls++
				if calls == 1 {
					return nil, failure
				}
				return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success()}, nil
			})
			check := checks.acquire(100)
			defer check.release()
			_, err := check.check(t.Context())
			require.ErrorIs(t, err, failure)
			_, err = check.check(t.Context())
			require.NoError(t, err)
			require.Equal(t, 2, calls)
		})
	}
}

func TestReplicaLoadCheckAllQueryEntrypoints(t *testing.T) {
	r, _ := newTestReplicaRouter(t)
	checks := patchLoadCheck(t, func(_ context.Context, req *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
		require.EqualValues(t, 100, req.GetCollectionID())
		return nil, merr.WrapErrCollectionNotLoaded(100)
	})
	client := newShardViewQueryClient(1, nil, nil)
	client.replicas, client.loadChecks = r, checks
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := client.Search(ctx, &ShardSearchRequest{VChannel: "p_100v0", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
	require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
	_, err = client.Query(ctx, &ShardQueryRequest{VChannel: "p_100v0", Req: &internalpb.RetrieveRequest{CollectionID: 100}, Reducer: newLegacyQueryCollector()})
	require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
	_, err = client.statistics(ctx, "p_100v0", 100, &viewpb.StatisticsRequest{}, 0)
	require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
	require.Empty(t, checks.collections)
}

func TestReplicaLoadCheckCancellationDoesNotCancelOtherQueries(t *testing.T) {
	entered := make(chan struct{})
	var calls atomic.Int64
	checks := patchLoadCheck(t, func(ctx context.Context, _ *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success()}, nil
	})
	check := checks.acquire(100)
	defer check.release()
	ownerCtx, cancelOwner := context.WithCancel(t.Context())
	defer cancelOwner()
	ownerDone := make(chan error, 1)
	go func() { _, err := check.check(ownerCtx); ownerDone <- err }()
	<-entered
	// A waiter may time out without terminating the shared RPC or changing its result.
	waiterCtx, cancelWaiter := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancelWaiter()
	_, err := check.check(waiterCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.EqualValues(t, 1, calls.Load())
	otherDone := make(chan error, 1)
	go func() { _, err := check.check(t.Context()); otherDone <- err }()
	cancelOwner()
	require.ErrorIs(t, <-ownerDone, context.Canceled)
	require.NoError(t, <-otherDone, "a canceled RPC owner must not fail another live query")
	require.EqualValues(t, 2, calls.Load())
	_, err = check.check(ownerCtx)
	require.ErrorIs(t, err, context.Canceled, "cancellation must beat a cached successful check")
}

func TestReplicaLoadCheckMissingDependency(t *testing.T) {
	checks := newCollectionLoadChecks(nil)
	check := checks.acquire(100)
	defer check.release()
	_, err := check.check(t.Context())
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.NotErrorIs(t, err, merr.ErrCollectionNotLoaded)
}
