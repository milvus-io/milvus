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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
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

func TestReplicaLoadCheckWaitsBeyondOneSecond(t *testing.T) {
	route := &replicaRoute{changed: make(chan struct{}), snapshot: qviews.ReplicaSnapshot{Valid: true}}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	checks := patchLoadCheck(t, func(probeCtx context.Context, _ *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
		timer := time.NewTimer(1200 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-probeCtx.Done():
			return nil, status.FromContextError(probeCtx.Err()).Err()
		case <-timer.C:
		}
		publishTestRoute(route, qviews.ReplicaSnapshot{Valid: true, Replicas: []qviews.Replica{{ReplicaID: 7}}})
		return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success()}, nil
	})
	check := checks.acquire(100)
	defer check.release()
	replica, _, err := route.nextReplica(ctx, nil, nil, check.check)
	require.NoError(t, err, "a slow load check must use the query deadline")
	require.EqualValues(t, 7, replica.ReplicaID)
	require.NoError(t, ctx.Err())
}

func TestReplicaLoadCheckHonorsQueryDeadline(t *testing.T) {
	route := &replicaRoute{changed: make(chan struct{}), snapshot: qviews.ReplicaSnapshot{Valid: true}}
	checks := patchLoadCheck(t, func(ctx context.Context, _ *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	})
	check := checks.acquire(100)
	defer check.release()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, _, err := route.nextReplica(ctx, nil, nil, check.check)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, check.last, "a canceled probe must not cache a successful load check")
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
	for _, failure := range []error{merr.WrapErrCollectionNotLoaded(100), merr.WrapErrServiceNotReadyMsg("recovering"), context.Canceled, context.DeadlineExceeded, status.Error(codes.Canceled, "remote cancellation"), status.Error(codes.DeadlineExceeded, "remote timeout")} {
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

// loadCheckWaitContext signals when a query selects on an in-flight probe.
type loadCheckWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *loadCheckWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestReplicaLoadCheckCancellationDoesNotCancelOtherQueries(t *testing.T) {
	for name, rpcError := range map[string]func(context.Context) error{
		"context": func(ctx context.Context) error { return ctx.Err() },
		"grpc":    func(ctx context.Context) error { return status.FromContextError(ctx.Err()).Err() },
		"wrapped grpc": func(ctx context.Context) error {
			return merr.Wrap(status.FromContextError(ctx.Err()).Err(), "load check")
		},
	} {
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{})
			var calls atomic.Int64
			checks := patchLoadCheck(t, func(ctx context.Context, _ *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
				if calls.Add(1) == 1 {
					close(entered)
					<-ctx.Done()
					return nil, rpcError(ctx)
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
			waiting := make(chan struct{})
			otherCtx := &loadCheckWaitContext{Context: t.Context(), waiting: waiting}
			go func() { _, err := check.check(otherCtx); otherDone <- err }()
			<-waiting
			cancelOwner()
			require.ErrorIs(t, <-ownerDone, context.Canceled)
			require.NoError(t, <-otherDone, "a canceled RPC owner must not fail another live query")
			require.EqualValues(t, 2, calls.Load())
			_, err = check.check(ownerCtx)
			require.ErrorIs(t, err, context.Canceled, "cancellation must beat a cached successful check")
		})
	}
}

func TestReplicaLoadCheckMissingDependency(t *testing.T) {
	checks := newCollectionLoadChecks(nil)
	check := checks.acquire(100)
	defer check.release()
	_, err := check.check(t.Context())
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.NotErrorIs(t, err, merr.ErrCollectionNotLoaded)
}

func TestRetiredSourceReplansWholeQueryAtPublishedGeneration(t *testing.T) {
	sn := newLineageTestSN()
	workers := newLineageTestWorkers()
	targets := []*viewpb.DataViewOfShard{{Vchannel: lineageT1}, {Vchannel: lineageT2}}
	generation := uint64(1)
	checks := patchLoadCheck(t, func(context.Context, *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
		generation++
		info := &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), ServingShards: targets, ShardSetEpoch: 3}
		if generation == 2 {
			info.ShardSetEpoch = 2
			info.ServingShards = append([]*viewpb.DataViewOfShard{{Vchannel: lineageS, Lineage: &viewpb.QueryViewLineage{Vchannels: []string{lineageT1, lineageT2}, SwitchTimetick: lineageSwitch}}}, targets...)
		}
		return info, nil
	})
	client := newLegacyClient(ViewQueryClientConfig{LoadInfoClient: checks.client, MaxRetries: 2}, sn, workers, nil)
	for _, vc := range []string{lineageS, lineageT1, lineageT2} {
		snapshot := qviews.ReplicaSnapshot{Valid: true}
		if vc != lineageS {
			snapshot.Replicas = []qviews.Replica{{ReplicaID: 1}}
		}
		client.shardClient.replicas.channels[vc] = &replicaRoute{changed: make(chan struct{}), snapshot: snapshot}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 600*time.Millisecond)
	defer cancel()
	result, err := client.Query(ctx, &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 100}, BarrierMVCC: 150})
	require.NoError(t, err)
	require.Len(t, result.Plans, 2)
	for _, target := range []string{lineageT1, lineageT2} {
		plans := sn.planRequests(target)
		require.Len(t, plans, 2)
		require.EqualValues(t, 2, plans[0].GetShardSetEpoch())
		require.EqualValues(t, 3, plans[1].GetShardSetEpoch())
		require.EqualValues(t, 150, plans[1].GetBarrierMvcc())
	}
	require.Empty(t, sn.planRequests(lineageS))
}

func TestSharedLoadProbeChecksEachPlannedShard(t *testing.T) {
	calls := 0
	checks := patchLoadCheck(t, func(context.Context, *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
		calls++
		return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), ShardSetEpoch: 2, ServingShards: []*viewpb.DataViewOfShard{{Vchannel: lineageT1}}}, nil
	})
	check := checks.acquire(100)
	defer check.release()
	for _, test := range []struct {
		name    string
		epoch   uint64
		invalid bool
	}{{lineageT1, 2, false}, {lineageS, 2, true}, {lineageS, 1, true}, {lineageT2, 3, false}} {
		ctx := context.WithValue(t.Context(), servingShardContextKey{}, resolver.Shard{VChannel: test.name, ShardSetEpoch: test.epoch})
		_, err := check.check(ctx)
		if test.invalid {
			require.Error(t, err)
			require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		} else {
			require.NoError(t, err)
		}
	}
	require.Equal(t, 1, calls)
}
