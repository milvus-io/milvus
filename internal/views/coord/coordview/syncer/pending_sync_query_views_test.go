//go:build test && dynamic

package syncer

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestPending_UpsertAndDrainUnsent(t *testing.T) {
	p := newPendingSyncQueryViews()

	// Upsert 3 entries with different versions (different keys).
	for i := int64(1); i <= 3; i++ {
		sv := newTestSyncView(1, i, nil, nil)
		p.Upsert(sv)
	}

	protos := p.DrainUnsent()
	assert.Len(t, protos, 3)

	// Second drain returns nil.
	protos = p.DrainUnsent()
	assert.Nil(t, protos)
}

func TestPending_UpsertReplace(t *testing.T) {
	p := newPendingSyncQueryViews()

	var callCount atomic.Int32
	cb1 := func(qviews.QueryViewAtWorkNode) bool { callCount.Add(1); return true }
	cb2 := func(qviews.QueryViewAtWorkNode) bool { callCount.Add(10); return true }

	sv1 := newTestSyncView(1, 1, cb1, nil)
	sv2 := newTestSyncView(1, 1, cb2, nil) // same key as sv1

	p.Upsert(sv1)
	p.Upsert(sv2)

	// entries should have the latest callback (cb2).
	// unsent should have both protos (duplicate is expected).
	protos := p.DrainUnsent()
	assert.Len(t, protos, 2, "both upserts should produce unsent protos")

	// Only one entry should exist in entries map.
	collected := p.CollectProtos()
	assert.Len(t, collected, 1)

	// Trigger MatchResponse to verify cb2 is used, not cb1.
	p.MatchResponse(sv2.View.IntoProto())
	assert.Equal(t, int32(10), callCount.Load(), "should invoke the replacement callback")
}

func TestPending_MatchResponse_RemoveOnTrue(t *testing.T) {
	p := newPendingSyncQueryViews()
	sv := newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool { return true }, nil)
	p.Upsert(sv)

	p.MatchResponse(sv.View.IntoProto())

	// Entry should be removed.
	assert.Nil(t, p.CollectProtos())
}

func TestPending_MatchResponse_KeepOnFalse(t *testing.T) {
	p := newPendingSyncQueryViews()
	sv := newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool { return false }, nil)
	p.Upsert(sv)

	p.MatchResponse(sv.View.IntoProto())

	// Entry should be retained.
	assert.Len(t, p.CollectProtos(), 1)
}

func TestPending_MatchResponse_NoStaleDelete(t *testing.T) {
	// Scenario: Upsert(v1) → Upsert(v1') replaces entry → old MatchResponse returns true
	// The new entry must NOT be deleted because the callback belongs to the old entry.
	p := newPendingSyncQueryViews()

	var firstCalled atomic.Bool
	cb1 := func(qviews.QueryViewAtWorkNode) bool {
		firstCalled.Store(true)
		return true
	}
	cb2 := func(qviews.QueryViewAtWorkNode) bool { return false }

	sv1 := newTestSyncView(1, 1, cb1, nil)
	p.Upsert(sv1)

	// Replace with sv2 (same key, different callback).
	sv2 := newTestSyncView(1, 1, cb2, nil)
	p.Upsert(sv2)

	// MatchResponse uses the proto from sv1.
	// Since sv2 replaced sv1, the callback is now cb2 which returns false.
	p.MatchResponse(sv1.View.IntoProto())

	// Entry should still exist because cb2 returned false.
	assert.Len(t, p.CollectProtos(), 1)
	assert.False(t, firstCalled.Load(), "old callback should not be invoked")
}

func TestPending_MatchResponse_UnknownKey(t *testing.T) {
	p := newPendingSyncQueryViews()

	// MatchResponse for a non-existent key should be a no-op.
	sv := newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool {
		t.Fatal("should not be called")
		return true
	}, nil)
	p.MatchResponse(sv.View.IntoProto())
}

func TestPending_MatchResponse_CallbackCanEnqueueFollowUpSync(t *testing.T) {
	p := newPendingSyncQueryViews()

	original := newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool {
		p.Upsert(newTestSyncView(1, 2, nil, nil))
		return true
	}, nil)
	p.Upsert(original)

	done := make(chan struct{})
	go func() {
		p.MatchResponse(original.View.IntoProto())
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	protos := p.CollectProtos()
	require.Len(t, protos, 1)
	assert.Equal(t, newTestSyncView(1, 2, nil, nil).View.QueryViewKey(),
		qviews.NewQueryViewAtWorkNodeFromProto(protos[0]).QueryViewKey())
}

func TestPending_MatchResponse_CallbackCanReplacePendingEntry(t *testing.T) {
	p := newPendingSyncQueryViews()

	var replacementCalled atomic.Bool
	var replacement SyncView
	original := newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool {
		p.Upsert(replacement)
		return true
	}, nil)
	replacement = newTestSyncView(1, 1, func(qviews.QueryViewAtWorkNode) bool {
		replacementCalled.Store(true)
		return false
	}, nil)

	p.Upsert(original)

	done := make(chan struct{})
	go func() {
		p.MatchResponse(original.View.IntoProto())
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	assert.Len(t, p.CollectProtos(), 1, "callback replacement should remain pending")

	p.MatchResponse(replacement.View.IntoProto())
	assert.True(t, replacementCalled.Load(), "replacement callback should handle later responses")
	assert.Len(t, p.CollectProtos(), 1)
}

func TestPending_Drain(t *testing.T) {
	p := newPendingSyncQueryViews()

	var lostCount atomic.Int32
	onQueryNodeLost := func(qviews.QueryNode) { lostCount.Add(1) }

	for i := int64(1); i <= 3; i++ {
		sv := newTestSyncView(1, i, nil, onQueryNodeLost)
		p.Upsert(sv)
	}

	p.Drain(qviews.NewQueryNode(1))

	assert.Equal(t, int32(3), lostCount.Load())
	assert.Nil(t, p.CollectProtos(), "entries should be empty after drain")
	assert.Nil(t, p.DrainUnsent(), "unsent should be cleared after drain")
}

func TestPending_DrainSkipsStreamingNode(t *testing.T) {
	p := newPendingSyncQueryViews()

	var lostCalled atomic.Bool
	p.Upsert(SyncView{
		View:            newTestSNView(1),
		OnQueryNodeLost: func(qviews.QueryNode) { lostCalled.Store(true) },
	})

	p.Drain(qviews.NewStreamingNodeFromVChannel(testVChannel))

	assert.False(t, lostCalled.Load(), "StreamingNode drain should not invoke OnQueryNodeLost")
	assert.Nil(t, p.CollectProtos(), "entries should be empty after drain")
	assert.Nil(t, p.DrainUnsent(), "unsent should be cleared after drain")
}

func TestPending_CollectProtos(t *testing.T) {
	p := newPendingSyncQueryViews()

	for i := int64(1); i <= 3; i++ {
		sv := newTestSyncView(1, i, nil, nil)
		p.Upsert(sv)
	}

	protos := p.CollectProtos()
	require.Len(t, protos, 3)

	// CollectProtos should be idempotent.
	protos2 := p.CollectProtos()
	assert.Len(t, protos2, 3)
}

func TestPending_ReadySignal(t *testing.T) {
	p := newPendingSyncQueryViews()

	// Initially not ready.
	select {
	case <-p.Ready():
		t.Fatal("should not be ready initially")
	default:
	}

	// Upsert signals ready.
	sv := newTestSyncView(1, 1, nil, nil)
	p.Upsert(sv)

	assert.True(t, waitFor(p.Ready(), 100*testTimeUnit), "should be ready after upsert")

	// Multiple upserts before drain should coalesce into one signal.
	p.Upsert(newTestSyncView(1, 2, nil, nil))
	p.Upsert(newTestSyncView(1, 3, nil, nil))

	// At most one signal should be queued (channel capacity is 1).
	select {
	case <-p.Ready():
	default:
	}
	select {
	case <-p.Ready():
		t.Fatal("second signal should not exist — coalesced")
	default:
	}
}
