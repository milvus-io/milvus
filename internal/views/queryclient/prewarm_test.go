package queryclient

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type prewarmRoutingFixture struct {
	loadUnavailable atomic.Bool
	QueryPlanClient
	ViewQueryServiceClient
	mu          sync.Mutex
	handlers    map[string]wal.ReplicaWatchHandler
	snapshot    qviews.ReplicaSnapshot
	plans       chan *viewpb.GetPrewarmPlanRequest
	workers     chan *qviews.PrewarmPlan
	loadVersion atomic.Uint64
	planError   error
	workerError error
	borrow      chan struct{}
}

type prewarmSubscription struct{ wal.ReplicaWatchSubscription }

func (prewarmSubscription) Close() error { return nil }

func (f *prewarmRoutingFixture) SubscribeReplicas(vc string, h wal.ReplicaWatchHandler) (wal.ReplicaWatchSubscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers[vc] = h
	h(f.snapshot)
	return prewarmSubscription{}, nil
}

func (f *prewarmRoutingFixture) publish(replicas ...qviews.Replica) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshot = qviews.ReplicaSnapshot{Valid: true, Replicas: replicas}
	for _, h := range f.handlers {
		h(f.snapshot)
	}
}

func (f *prewarmRoutingFixture) GetQueryViewLoadInfo(context.Context, *querypb.GetQueryViewLoadInfoRequest, ...grpc.CallOption) (*querypb.GetQueryViewLoadInfoResponse, error) {
	if f.loadUnavailable.Load() {
		return nil, merr.WrapErrServiceNotReadyMsg("injected metadata failure")
	}
	version := f.loadVersion.Load()
	if version == 0 {
		return nil, merr.WrapErrCollectionNotLoaded(100)
	}
	return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), Version: version, ReplicaIds: []int64{1, 2}}, nil
}

func (f *prewarmRoutingFixture) GetPrewarmPlan(_ context.Context, _ qviews.ShardID, req *viewpb.GetPrewarmPlanRequest) (*qviews.PrewarmPlan, error) {
	f.plans <- req
	if f.planError != nil {
		return nil, f.planError
	}
	return &qviews.PrewarmPlan{Plan: &viewpb.QueryPlan{ShardId: req.ShardId, Version: req.Version, WorkNodes: []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 11}}}}}}, nil
}

func (f *prewarmRoutingFixture) PrewarmOnView(ctx context.Context, _ qviews.WorkNode, plan *qviews.PrewarmPlan) error {
	f.workers <- plan
	if f.borrow != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.borrow:
		}
	}
	return f.workerError
}

func newPrewarmRoutingFixture(t *testing.T) (*prewarmRoutingFixture, Client) {
	t.Helper()
	f := &prewarmRoutingFixture{handlers: make(map[string]wal.ReplicaWatchHandler), plans: make(chan *viewpb.GetPrewarmPlanRequest, 16), workers: make(chan *qviews.PrewarmPlan, 16)}
	f.loadVersion.Store(7)
	c := NewLegacyViewQueryClient(ViewQueryClientConfig{LoadInfoClient: f}, f, f, &legacyResolver{vchannels: []string{"p_100v0", "p_100v1"}})
	t.Cleanup(c.Close)
	return f, c
}

func warmReplica(id, version int64) qviews.Replica {
	return qviews.Replica{ReplicaID: id, MaxUpViewVersion: qviews.QueryViewVersion{QueryVersion: version}}
}

func awaitPrewarm[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("prewarm did not progress")
		var zero T
		return zero
	}
}

func TestPrewarmUnsupportedClient(t *testing.T) {
	require.ErrorIs(t, Prewarm(t.Context(), struct{ Client }{}, 100), merr.ErrServiceUnimplemented)
}

func TestPrewarmAllReplicasIndependentlyAndOnlyOnce(t *testing.T) {
	f, c := newPrewarmRoutingFixture(t)
	f.borrow = make(chan struct{})
	f.publish(warmReplica(1, 10))
	done := make(chan error, 1)
	go func() { done <- Prewarm(t.Context(), c, 100) }()
	// Replica 1 must start while replica 2 is still cold.
	for range 2 {
		plan := awaitPrewarm(t, f.workers)
		require.EqualValues(t, 1, plan.Plan.ShardId.ReplicaId)
		require.EqualValues(t, 10, plan.Plan.Version.QueryVersion)
	}
	// Replacing the first View must neither cancel its borrowers nor enqueue a
	// second attempt. A new replica is outside this request's captured set.
	f.publish(warmReplica(1, 11), warmReplica(2, 20), warmReplica(3, 30))
	for range 2 {
		plan := awaitPrewarm(t, f.workers)
		require.EqualValues(t, 2, plan.Plan.ShardId.ReplicaId)
		require.EqualValues(t, 20, plan.Plan.Version.QueryVersion)
	}
	select {
	case err := <-done:
		t.Fatalf("View replacement canceled borrowers: %v", err)
	default:
	}
	close(f.borrow)
	require.NoError(t, awaitPrewarm(t, done))
	require.Len(t, f.plans, 4)
	require.Empty(t, f.workers)
	// A second explicit request is allowed to warm the newly advertised version.
	require.NoError(t, Prewarm(t.Context(), c, 100))
	require.Len(t, f.plans, 8)
}

func TestPrewarmFailuresAreNotRetried(t *testing.T) {
	for _, phase := range []string{"plan", "worker"} {
		t.Run(phase, func(t *testing.T) {
			f, c := newPrewarmRoutingFixture(t)
			injected := viewerror.NewViewNotFound("injected retired View")
			if phase == "plan" {
				f.planError = injected
			} else {
				f.workerError = injected
			}
			f.publish(warmReplica(1, 10), warmReplica(2, 20))
			require.Error(t, Prewarm(t.Context(), c, 100))
			require.Len(t, f.plans, 4, "each replica/shard is planned once even on retryable errors")
			if phase == "plan" {
				require.Empty(t, f.workers)
			} else {
				require.Len(t, f.workers, 4, "one failed worker does not cancel other replicas")
			}
		})
	}
}

func TestPrewarmReleaseReplacementAndShutdownCancelWaitsAndBorrowers(t *testing.T) {
	for _, event := range []string{"release", "reload", "close", "cancel", "metadata failure"} {
		t.Run(event, func(t *testing.T) {
			f, c := newPrewarmRoutingFixture(t)
			f.borrow = make(chan struct{})
			f.publish(warmReplica(1, 10)) // Replica 2 remains waiting for Up.
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- Prewarm(ctx, c, 100) }()
			awaitPrewarm(t, f.workers)
			awaitPrewarm(t, f.workers)
			switch event {
			case "release":
				f.loadVersion.Store(0)
			case "reload":
				f.loadVersion.Store(8)
			case "close":
				c.Close()
			case "cancel":
				cancel()
			case "metadata failure":
				f.loadUnavailable.Store(true)
			}
			require.Error(t, awaitPrewarm(t, done))
			require.Len(t, f.plans, 2)
		})
	}
}

func TestPrewarmReleaseBeforeAnyViewIsUp(t *testing.T) {
	f, c := newPrewarmRoutingFixture(t)
	done := make(chan error, 1)
	go func() { done <- Prewarm(t.Context(), c, 100) }()
	require.Eventually(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(f.handlers) == 2 }, time.Second, time.Millisecond)
	f.loadVersion.Store(0)
	require.Error(t, awaitPrewarm(t, done))
	require.Empty(t, f.plans)
	require.Empty(t, f.workers)
}

func TestPrewarmDeadlineDoesNotDispatch(t *testing.T) {
	f, c := newPrewarmRoutingFixture(t)
	f.publish(warmReplica(1, 10), warmReplica(2, 20))
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	require.Error(t, Prewarm(ctx, c, 100))
	require.Empty(t, f.plans)
	require.Empty(t, f.workers)
}
