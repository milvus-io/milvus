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

func TestLoadCheckAllQueryEntrypoints(t *testing.T) {
	plans := &fakeQueryPlanClient{}
	patch := mockey.Mock((*fakeQueryPlanClient).GetQueryPlan).To(func(_ *fakeQueryPlanClient, _ context.Context, shard qviews.ShardID, _ *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
		require.Equal(t, qviews.UnknownReplicaID, shard.ReplicaID)
		return nil, viewerror.NewViewNotFound("no serving view")
	}).Build()
	defer patch.UnPatch()
	checks := patchLoadCheck(t, func(_ context.Context, req *querypb.GetQueryViewLoadInfoRequest) (*querypb.GetQueryViewLoadInfoResponse, error) {
		require.EqualValues(t, 100, req.GetCollectionID())
		require.Zero(t, req.GetVersion())
		return nil, merr.WrapErrCollectionNotLoaded(100)
	})
	client := newShardViewQueryClient(1, plans, nil)
	defer client.Close()
	client.loadChecks = checks
	_, err := client.Search(t.Context(), &ShardSearchRequest{VChannel: "p_100v0", Req: &internalpb.SearchRequest{CollectionID: 100}, Reducer: newLegacySearchCollector()})
	require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
	_, err = client.Query(t.Context(), &ShardQueryRequest{VChannel: "p_100v0", Req: &internalpb.RetrieveRequest{CollectionID: 100}, Reducer: newLegacyQueryCollector()})
	require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
	_, err = client.statistics(t.Context(), "p_100v0", 100, &viewpb.StatisticsRequest{}, 0)
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
