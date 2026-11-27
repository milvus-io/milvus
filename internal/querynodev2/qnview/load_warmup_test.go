package qnview

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type warmupCall struct {
	ctx  context.Context
	read segments.ReadState
	done chan error
}

func (*resourceTestRead) Prewarm(context.Context, []int64) error { return nil }

func recordWarmups(t *testing.T) <-chan warmupCall {
	calls := make(chan warmupCall, 8)
	mockey.Mock((*resourceTestRead).Prewarm).To(func(read *resourceTestRead, ctx context.Context, fields []int64) error {
		require.Empty(t, fields)
		call := warmupCall{ctx: ctx, read: read, done: make(chan error, 1)}
		calls <- call
		select {
		case err := <-call.done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}).Build()
	return calls
}

func awaitWarmup(t *testing.T, calls <-chan warmupCall) warmupCall {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(5 * time.Second):
		t.Fatal("segment was not warmed")
		return warmupCall{}
	}
}

func expectNoWarmup(t *testing.T, calls <-chan warmupCall) {
	t.Helper()
	select {
	case <-calls:
		t.Fatal("unexpected warm")
	case <-time.After(100 * time.Millisecond):
	}
}

func startWarm(ctx context.Context, f *resourceTestFixture, req AcquireSegments) <-chan error {
	done := make(chan error, 1)
	go func() { done <- f.manager.Prewarm(ctx, req.Key, req.View) }()
	return done
}

func TestPrewarmBorrowsReadStateAcrossViewReplacement(t *testing.T) {
	mockey.PatchConvey("one request keeps its old read while new views remain cold", t, func() {
		f := setupResourceTest(t, nil)
		warms := recordWarmups(t)
		old := f.acquire(1, 1, 1)
		f.await(t, 1)
		expectNoWarmup(t, warms)
		done := startWarm(t.Context(), f, old)
		call := awaitWarmup(t, warms)
		f.manager.mu.Lock()
		read := f.manager.views[old.Key].bindings[1000].prepared.read
		f.manager.mu.Unlock()
		require.Same(t, read, call.read)

		// Queries remain usable while prewarm runs.
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), old.Key, old.View, 0)
		require.NoError(t, err)
		handles[0].Release()
		next := f.acquire(2, 1, 1)
		f.await(t, 1)
		dropped := f.beginDrop(old)
		require.NoError(t, call.ctx.Err(), "view retirement cannot cancel an acquired read")
		expectNoWarmup(t, warms)
		call.done <- nil
		require.NoError(t, <-done)
		<-dropped
		require.EqualValues(t, 1, f.loads.Load())

		// A later explicit request may warm the same physical segment again.
		again := startWarm(t.Context(), f, next)
		awaitWarmup(t, warms).done <- nil
		require.NoError(t, <-again)
		expectNoWarmup(t, warms)
		f.drop(t, next)
	})
}

func TestPrewarmCancellationPreservesSharingQueries(t *testing.T) {
	mockey.PatchConvey("cancel stops prewarm after the final query releases its shared cache loads", t, func() {
		f := setupResourceTest(t, nil)
		warms := recordWarmups(t)
		req := f.acquire(1, 1, 1)
		f.await(t, 1)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := startWarm(ctx, f, req)
		call := awaitWarmup(t, warms)
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), req.Key, req.View, 0)
		require.NoError(t, err)
		cancel()
		require.Never(t, func() bool { return call.ctx.Err() != nil }, 100*time.Millisecond, time.Millisecond)
		handles[0].Release()
		require.ErrorIs(t, <-done, context.Canceled)
		require.ErrorIs(t, call.ctx.Err(), context.Canceled)
		f.drop(t, req)
	})
}

func TestPrewarmFailureDoesNotRequeueOrFollowReplacement(t *testing.T) {
	mockey.PatchConvey("only an explicit request can retry a failed pass", t, func() {
		f := setupResourceTest(t, nil)
		warms := recordWarmups(t)
		req := f.acquire(1, 1, 1)
		f.await(t, 1)
		done := startWarm(t.Context(), f, req)
		err := errors.New("storage throttled")
		awaitWarmup(t, warms).done <- err
		require.ErrorIs(t, <-done, err)
		next := f.acquire(2, 1, 1)
		f.await(t, 1)
		f.drop(t, req)
		expectNoWarmup(t, warms)
		f.drop(t, next)
	})
}

func TestPrewarmHandlerCloseCancelsDuringMigration(t *testing.T) {
	mockey.PatchConvey("handler close cancels prewarm while the RPC and migration are still live", t, func() {
		f := setupResourceTest(t, nil)
		warms := recordWarmups(t)
		h := NewQNQueryViewHandler(f.manager)
		req := f.request(1, 1, 1)
		view := qviews.NewQueryViewAtQueryNode(req.Meta, req.View)
		reports := make(chan qviews.QueryViewState, 4)
		h.ApplyViews([]handler.ApplyView{{View: view, OnReport: func(v qviews.QueryViewAtWorkNode) {
			reports <- v.State()
		}}})
		select {
		case state := <-reports:
			require.Equal(t, qviews.QueryViewStateReady, state)
		case <-time.After(5 * time.Second):
			t.Fatal("view did not become ready")
		}

		warmReq := &viewpb.PrewarmOnViewRequest{ShardId: req.Key.ShardID.IntoProto(), Version: req.Key.QueryViewVersion.IntoProto()}
		rpcCtx, cancelRPC := context.WithCancel(t.Context())
		defer cancelRPC()
		warmed := make(chan error, 1)
		go func() { warmed <- h.Prewarm(rpcCtx, warmReq) }()
		call := awaitWarmup(t, warms)

		migrationCtx, cancelMigration := context.WithTimeout(t.Context(), 5*time.Second)
		closed := make(chan error, 1)
		go func() { closed <- h.Close(migrationCtx) }()
		defer func() {
			cancelMigration()
			cancelRPC()
			awaitShutdown(t, h.closed)
		}()

		select {
		case err := <-warmed:
			require.ErrorIs(t, err, context.Canceled)
		case <-migrationCtx.Done():
			t.Fatal("prewarm did not cancel during migration")
		}
		require.NoError(t, rpcCtx.Err())
		require.NoError(t, migrationCtx.Err())
		require.ErrorIs(t, call.ctx.Err(), context.Canceled)
		require.Equal(t, qviews.QueryViewStateReady, shutdownViewState(h, req.Key))
		require.ErrorIs(t, h.Prewarm(migrationCtx, warmReq), context.Canceled)
		require.Empty(t, warms)

		h.ApplyViews([]handler.ApplyView{{View: newDroppedQNView(1, 1)}})
		require.NoError(t, <-closed)
		require.EqualValues(t, 1, f.releases.Load())
	})
}

func TestPrewarmNodeCloseCancelsWithoutQueryConsumers(t *testing.T) {
	mockey.PatchConvey("node shutdown discards the request instead of persisting work", t, func() {
		f := setupResourceTest(t, nil)
		warms := recordWarmups(t)
		req := f.acquire(1, 1, 1)
		f.await(t, 1)
		done := startWarm(t.Context(), f, req)
		awaitWarmup(t, warms)
		closed := make(chan struct{})
		go func() { f.manager.Close(); close(closed) }()
		require.ErrorIs(t, <-done, context.Canceled)
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("node close blocked on prewarm")
		}
	})
}
