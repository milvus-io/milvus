//go:build test && dynamic

package snview

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type queryLeaseResult struct {
	lease *QueryViewLease
	err   error
}

// waitForRecoveryWaiter returns once a query has parked on a recovering shard
// of testVChannel: waiters take the shard's recovery notification under the
// same lock that inspects the state, so a registered channel means "waiting".
func waitForRecoveryWaiter(t *testing.T, h *SNQueryViewHandler) {
	t.Helper()
	require.Eventually(t, func() bool {
		h.mu.Lock()
		shards := make([]*snShardView, 0, len(h.shards))
		for id, shard := range h.shards {
			if id.VChannel == testVChannel {
				shards = append(shards, shard)
			}
		}
		h.mu.Unlock()
		for _, shard := range shards {
			shard.mu.Lock()
			waiting := shard.recoveryChanged != nil
			shard.mu.Unlock()
			if waiting {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond)
}

func startRecoveryQuery(ctx context.Context, h *SNQueryViewHandler, id qviews.ShardID, version *qviews.QueryViewVersion) <-chan queryLeaseResult {
	result := make(chan queryLeaseResult, 1)
	go func() {
		var lease *QueryViewLease
		var err error
		if version == nil {
			lease, err = h.AcquireLatestUpView(ctx, id)
		} else {
			lease, err = h.AcquireUpView(ctx, id, *version)
		}
		result <- queryLeaseResult{lease, err}
	}()
	return result
}

func receiveRecoveryQuery(t *testing.T, result <-chan queryLeaseResult) queryLeaseResult {
	t.Helper()
	select {
	case got := <-result:
		if got.lease != nil {
			t.Cleanup(got.lease.Release)
		}
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("query did not leave recovery wait")
		return queryLeaseResult{}
	}
}

func TestSNHandler_RecoveryWaitExits(t *testing.T) {
	for _, exact := range []bool{false, true} {
		mode := "latest"
		if exact {
			mode = "exact"
		}
		for _, event := range []string{"unrecoverable", "down", "dropped", "canceled", "deadline", "shutdown"} {
			t.Run(mode+"/"+event, func(t *testing.T) {
				view := newFullSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp, 101)
				mgr := newMockResourceManager()
				h := recoverSNQueryViewHandler(context.Background(), testPChannel, newMockCatalog(), mgr, []*viewpb.QueryViewOfShard{view.IntoProto()})
				recovery, ok := mgr.getAcquired(view.QueryViewKey())
				require.True(t, ok)
				timeout := 5 * time.Second
				if event == "deadline" {
					timeout = 100 * time.Millisecond
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				var version *qviews.QueryViewVersion
				if exact {
					v := view.Version()
					version = &v
				}
				result := startRecoveryQuery(ctx, h, view.ShardID(), version)
				waitForRecoveryWaiter(t, h)
				switch event {
				case "unrecoverable":
					recovery.OnUnrecoverable()
				case "down":
					h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDown)}})
				case "dropped":
					h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped)}})
				case "canceled":
					cancel()
				case "shutdown":
					h.StopQueryAcquisition()
					h.StopQueryAcquisition()
				}
				got := receiveRecoveryQuery(t, result)
				require.Nil(t, got.lease)
				switch event {
				case "canceled":
					require.ErrorIs(t, got.err, context.Canceled)
				case "deadline":
					require.ErrorIs(t, got.err, context.DeadlineExceeded)
				case "shutdown":
					require.True(t, viewerror.AsViewError(got.err).IsOnShutdown(), "%v", got.err)
				default:
					viewErr := viewerror.AsViewError(got.err)
					require.True(t, viewErr.IsRetryable(), "%v", got.err)
					if exact {
						require.True(t, viewErr.IsViewInvalidated())
					} else {
						require.True(t, viewErr.IsViewNotFound())
					}
					// A delayed completion cannot resurrect a failed or retired view.
					recovery.OnReady()
					lease, err := h.AcquireUpView(ctx, view.ShardID(), view.Version())
					require.Nil(t, lease)
					require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
				}
			})
		}
	}
}

func TestSNHandler_RecoveryWaitPinsRequestedVersion(t *testing.T) {
	old := newFullSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp, 101)
	newer := newFullSNViewWithState(2, viewpb.QueryViewState_QueryViewStateUp, 102)
	mgr := newMockResourceManager()
	h := recoverSNQueryViewHandler(context.Background(), testPChannel, newMockCatalog(), mgr, []*viewpb.QueryViewOfShard{old.IntoProto(), newer.IntoProto()})
	oldRecovery, _ := mgr.getAcquired(old.QueryViewKey())
	newRecovery, _ := mgr.getAcquired(newer.QueryViewKey())
	oldRecovery.OnReady()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Phase 1 must use an existing Up version, even if a newer one is recovering.
	lease, err := h.AcquireLatestUpView(ctx, old.ShardID())
	require.NoError(t, err)
	require.Equal(t, old.Version(), lease.Version)
	lease.Release()
	version := newer.Version()
	result := startRecoveryQuery(ctx, h, newer.ShardID(), &version)
	waitForRecoveryWaiter(t, h)
	newRecovery.OnUnrecoverable()
	got := receiveRecoveryQuery(t, result)
	require.Nil(t, got.lease)
	require.True(t, viewerror.AsViewError(got.err).IsViewInvalidated())
	// A missing version also fails immediately rather than using the old Up view.
	_, err = h.AcquireUpView(ctx, old.ShardID(), newPreparingSNView(3).Version())
	require.True(t, viewerror.AsViewError(err).IsViewNotFound())
}

func TestSNHandler_RecoveryWaitCancellationIsPerRequest(t *testing.T) {
	view := newFullSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp, 101)
	mgr := newMockResourceManager()
	h := recoverSNQueryViewHandler(context.Background(), testPChannel, newMockCatalog(), mgr, []*viewpb.QueryViewOfShard{view.IntoProto()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cancelCtx, cancelOne := context.WithCancel(ctx)
	defer cancelOne()
	first := startRecoveryQuery(cancelCtx, h, view.ShardID(), nil)
	var remaining []<-chan queryLeaseResult
	for range 8 {
		remaining = append(remaining, startRecoveryQuery(ctx, h, view.ShardID(), nil))
	}
	waitForRecoveryWaiter(t, h)
	cancelOne()
	require.ErrorIs(t, receiveRecoveryQuery(t, first).err, context.Canceled)
	// Canceling one request must not wake or fail the others.
	time.Sleep(50 * time.Millisecond)
	for _, result := range remaining {
		select {
		case got := <-result:
			t.Fatalf("query left recovery wait early: %v", got.err)
		default:
		}
	}
	recovery, _ := mgr.getAcquired(view.QueryViewKey())
	recovery.OnReady()
	for _, result := range remaining {
		require.NoError(t, receiveRecoveryQuery(t, result).err)
	}
}

func TestSNHandler_RecoveryWaitDoesNotMissStateChanges(t *testing.T) {
	mockey.PatchConvey("recovery finishes between scan and wait", t, func() {
		view := newFullSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp, 101)
		mgr := newMockResourceManager()
		h := recoverSNQueryViewHandler(context.Background(), testPChannel, newMockCatalog(), mgr, []*viewpb.QueryViewOfShard{view.IntoProto()})
		recovery, _ := mgr.getAcquired(view.QueryViewKey())
		var original func(*snShardView, context.Context, *int32) (*QueryViewLease, <-chan struct{}, error)
		attempts := 0
		mockey.Mock((*snShardView).tryAcquireUpViewForSchema).Origin(&original).To(func(s *snShardView, ctx context.Context, schema *int32) (*QueryViewLease, <-chan struct{}, error) {
			lease, changed, err := original(s, ctx, schema)
			attempts++
			if attempts == 1 {
				// Recovery completes after the scan released the shard lock but
				// before the caller waits on the returned notification.
				recovery.OnReady()
			}
			return lease, changed, err
		}).Build()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		lease, err := h.AcquireLatestUpView(ctx, view.ShardID())
		require.NoError(t, err)
		lease.Release()
	})
}

func TestSNHandler_QueryWaitsForRecovery(t *testing.T) {
	for _, mode := range []string{"latest", "unknown replica", "exact version"} {
		t.Run(mode, func(t *testing.T) {
			view := newFullSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp, 101, 102)
			mgr := newMockResourceManager()
			h := recoverSNQueryViewHandler(context.Background(), testPChannel, newMockCatalog(), mgr, []*viewpb.QueryViewOfShard{view.IntoProto()})
			recovery, ok := mgr.getAcquired(view.QueryViewKey())
			require.True(t, ok)
			reports := &reportCollector{}
			h.ApplyViews([]handler.ApplyView{{View: view, OnReport: reports.onReport}})
			// Coord still sees Up while the node is recovering locally.
			require.Equal(t, qviews.QueryViewStateUp, reports.last().State())

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan queryLeaseResult, 1)
			go func() {
				var lease *QueryViewLease
				var err error
				switch mode {
				case "latest":
					lease, err = h.AcquireLatestUpView(ctx, view.ShardID())
				case "unknown replica":
					lease, err = h.AcquireLatestUpView(ctx, qviews.ShardID{VChannel: testVChannel, ReplicaID: qviews.UnknownReplicaID})
				default:
					lease, err = h.AcquireUpView(ctx, view.ShardID(), view.Version())
				}
				result <- queryLeaseResult{lease, err}
			}()
			select {
			case early := <-result:
				if early.lease != nil {
					early.lease.Release()
				}
				t.Fatalf("query returned before recovery completed: %v", early.err)
			case <-time.After(50 * time.Millisecond):
			}

			recovery.OnReady()
			select {
			case got := <-result:
				require.NoError(t, got.err)
				defer got.lease.Release()
				require.Equal(t, view.Version(), got.lease.Version)
				require.True(t, proto.Equal(view.IntoProto(), got.lease.View))
			case <-ctx.Done():
				t.Fatal("query did not resume after recovery")
			}
		})
	}
}
