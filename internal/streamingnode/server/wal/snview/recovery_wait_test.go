package snview

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestQueryViewWaitForRecovery(t *testing.T) {
	for _, mode := range []string{"latest", "schema", "any-replica", "execution"} {
		for _, outcome := range []string{"up", "unrecoverable", "down", "dropped", "close", "cancel", "deadline"} {
			t.Run(mode+"/"+outcome, func(t *testing.T) {
				meta := buildHandlerTestMeta(1)
				meta.State = viewpb.QueryViewState_QueryViewStateUp
				meta.SchemaVersion = 7
				mgr := newMockResourceManager()
				h := recoverSNQueryViewHandler(t.Context(), testPChannel, newMockCatalog(), mgr, []*viewpb.QueryViewOfShard{{Meta: meta, StreamingNode: &viewpb.QueryViewOfStreamingNode{}}})
				id := qviews.NewShardIDFromQVMeta(meta)
				shard := h.shards[id]
				version := qviews.FromProtoQueryViewVersion(meta.Version)
				acquired, ok := mgr.getAcquired(qviews.QueryViewKey{ShardID: id, QueryViewVersion: version})
				require.True(t, ok)
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				if outcome == "deadline" {
					var cancelDeadline context.CancelFunc
					ctx, cancelDeadline = context.WithTimeout(ctx, 100*time.Millisecond)
					defer cancelDeadline()
				}
				done := make(chan error, 1)
				go func() {
					var lease *QueryViewLease
					var err error
					switch mode {
					case "latest":
						lease, err = h.AcquireLatestUpView(ctx, id)
					case "schema":
						lease, err = h.AcquireUpViewAtSchema(ctx, id, 7)
					case "any-replica":
						lease, err = h.AcquireUpViewAtSchema(ctx, qviews.ShardID{VChannel: id.VChannel}, 7)
					case "execution":
						lease, err = h.acquireUpView(ctx, id, version, false)
					}
					if lease != nil {
						lease.Release()
					}
					done <- err
				}()
				// Observe registration under the same lock as the predicate, without
				// relying on a sleep to guess whether the request has started waiting.
				require.Eventually(t, func() bool { shard.mu.Lock(); defer shard.mu.Unlock(); return shard.recoveryChanged != nil }, time.Second, time.Millisecond)
				select {
				case err := <-done:
					t.Fatalf("returned before recovery: %v", err)
				default:
				}
				switch outcome {
				case "up":
					acquired.OnReady()
				case "unrecoverable":
					acquired.OnUnrecoverable()
				case "down", "dropped":
					state := viewpb.QueryViewState_QueryViewStateDown
					if outcome == "dropped" {
						state = viewpb.QueryViewState_QueryViewStateDropped
					}
					h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, state)}})
				case "close":
					patch := mockey.Mock((*mockResourceManager).Release).To(func(_ *mockResourceManager, req ReleaseResource) { go req.OnDropped() }).Build()
					defer patch.UnPatch()
					h.CloseForHandoff()
				case "cancel":
					cancel()
				}
				select {
				case err := <-done:
					switch outcome {
					case "up":
						require.NoError(t, err)
					case "cancel":
						require.ErrorIs(t, err, context.Canceled)
					case "deadline":
						require.ErrorIs(t, err, context.DeadlineExceeded)
					default:
						require.True(t, viewerror.AsViewError(err).IsViewNotFound() || viewerror.AsViewError(err).IsViewInvalidated(), "%v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("recovery waiter did not wake")
				}
			})
		}
	}
}

func TestRecoverySelectionUsesAvailableView(t *testing.T) {
	mgr := newMockResourceManager()
	var views []*viewpb.QueryViewOfShard
	for _, replica := range []int64{testReplicaID, testReplicaID + 1} {
		for _, version := range []int64{1, 2} {
			meta := buildHandlerTestMeta(version)
			meta.State = viewpb.QueryViewState_QueryViewStateUp
			meta.ReplicaId = replica
			meta.SchemaVersion = 7
			views = append(views, &viewpb.QueryViewOfShard{Meta: meta, StreamingNode: &viewpb.QueryViewOfStreamingNode{}})
		}
	}
	h := recoverSNQueryViewHandler(t.Context(), testPChannel, newMockCatalog(), mgr, views)
	key := newPreparingSNView(1).QueryViewKey()
	acquired, ok := mgr.getAcquired(key)
	require.True(t, ok)
	acquired.OnReady()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for _, replica := range []int64{testReplicaID, qviews.UnknownReplicaID} {
		id := qviews.ShardID{ReplicaID: replica, VChannel: testVChannel}
		for i := 0; i < 32; i++ {
			lease, err := h.AcquireUpViewAtSchema(ctx, id, 7)
			require.NoError(t, err)
			require.Equal(t, key.QueryViewVersion, lease.Version)
			lease.Release()
		}
		_, err := h.AcquireUpViewAtSchema(ctx, id, 8)
		require.True(t, viewerror.AsViewError(err).IsViewNotFound(), "unrelated recovering schemas must not cause waiting")
	}
}

func TestRecoveryWaitWakesForAnyReplica(t *testing.T) {
	mgr := newMockResourceManager()
	var views []*viewpb.QueryViewOfShard
	for _, replica := range []int64{testReplicaID, testReplicaID + 1} {
		meta := buildHandlerTestMeta(1)
		meta.State = viewpb.QueryViewState_QueryViewStateUp
		meta.ReplicaId = replica
		views = append(views, &viewpb.QueryViewOfShard{Meta: meta, StreamingNode: &viewpb.QueryViewOfStreamingNode{}})
	}
	h := recoverSNQueryViewHandler(t.Context(), testPChannel, newMockCatalog(), mgr, views)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 8)
	for i := 0; i < cap(done); i++ {
		go func() {
			lease, err := h.AcquireLatestUpView(ctx, qviews.ShardID{VChannel: testVChannel})
			if lease != nil {
				lease.Release()
			}
			done <- err
		}()
	}
	require.Eventually(t, func() bool {
		for _, shard := range h.shards {
			shard.mu.Lock()
			waiting := shard.recoveryChanged != nil
			shard.mu.Unlock()
			if !waiting {
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond)
	key := newPreparingSNView(1).QueryViewKey()
	key.ShardID.ReplicaID++
	resource, ok := mgr.getAcquired(key)
	require.True(t, ok)
	resource.OnReady()
	for i := 0; i < cap(done); i++ {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("waiters blocked on the other recovering replica")
		}
	}
}
