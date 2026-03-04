//go:build test && dynamic

package snview

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// buildPersistKey constructs a unique persistence key from view metadata.
// Used only by mockCatalog to simulate catalog behavior.
func buildPersistKey(meta *viewpb.QueryViewMeta) string {
	return fmt.Sprintf("%d/%s/%d/%d/%d",
		meta.ReplicaId,
		meta.Vchannel,
		meta.Version.DataVersion.StreamingVersion,
		meta.Version.DataVersion.CompactVersion,
		meta.Version.QueryVersion,
	)
}

// ---------------------------------------------------------------------------
// Mock catalog
// ---------------------------------------------------------------------------

type mockCatalog struct {
	mu    sync.Mutex
	saved map[string]*viewpb.QueryViewOfShard
}

func newMockCatalog() *mockCatalog {
	return &mockCatalog{
		saved: make(map[string]*viewpb.QueryViewOfShard),
	}
}

func (c *mockCatalog) SaveQueryView(view *viewpb.QueryViewOfShard) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := buildPersistKey(view.Meta)
	persistState := qviews.QueryViewState(view.Meta.State)
	switch persistState {
	case qviews.QueryViewStateUp:
		c.saved[key] = view
	default:
		delete(c.saved, key)
	}
	return nil
}

func (c *mockCatalog) savedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.saved)
}

// ---------------------------------------------------------------------------
// Mock ResourceManager
// ---------------------------------------------------------------------------

type mockResourceManager struct {
	mu              sync.Mutex
	acquired        map[qviews.QueryViewKey]AcquireResource
	recovered       map[qviews.QueryViewKey]RecoverResource
	released        []qviews.QueryViewKey
	releaseCallback map[qviews.QueryViewKey]func() // captured OnDropped callbacks
}

func newMockResourceManager() *mockResourceManager {
	return &mockResourceManager{
		acquired:        make(map[qviews.QueryViewKey]AcquireResource),
		recovered:       make(map[qviews.QueryViewKey]RecoverResource),
		releaseCallback: make(map[qviews.QueryViewKey]func()),
	}
}

func (m *mockResourceManager) Acquire(req AcquireResource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acquired[req.Key] = req
}

func (m *mockResourceManager) Recover(req RecoverResource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recovered[req.Key] = req
}

func (m *mockResourceManager) Release(req ReleaseResource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.released = append(m.released, req.Key)
	m.releaseCallback[req.Key] = req.OnDropped
}

func (m *mockResourceManager) invokeReleaseCallback(key qviews.QueryViewKey) {
	m.mu.Lock()
	cb := m.releaseCallback[key]
	delete(m.releaseCallback, key)
	m.mu.Unlock()
	if cb != nil {
		cb()
	}
}

func (m *mockResourceManager) getAcquired(key qviews.QueryViewKey) (AcquireResource, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	req, ok := m.acquired[key]
	return req, ok
}

func (m *mockResourceManager) getRecovered(key qviews.QueryViewKey) (RecoverResource, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	req, ok := m.recovered[key]
	return req, ok
}

func (m *mockResourceManager) acquiredCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.acquired)
}

func (m *mockResourceManager) recoveredCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.recovered)
}

func (m *mockResourceManager) releasedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.released)
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func buildHandlerTestMeta(version int64) *viewpb.QueryViewMeta {
	return &viewpb.QueryViewMeta{
		CollectionId: testCollectionID,
		ReplicaId:    testReplicaID,
		Vchannel:     testVChannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: version, CompactVersion: 1},
			QueryVersion: version,
		},
		State: viewpb.QueryViewState_QueryViewStatePreparing,
	}
}

func newPreparingSNView(version int64) qviews.QueryViewAtWorkNode {
	return qviews.NewQueryViewAtStreamingNode(buildHandlerTestMeta(version), &viewpb.QueryViewOfStreamingNode{})
}

func newSNViewWithState(version int64, state viewpb.QueryViewState) qviews.QueryViewAtWorkNode {
	meta := buildHandlerTestMeta(version)
	meta.State = state
	return qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
}

type reportCollector struct {
	mu      sync.Mutex
	reports []qviews.QueryViewAtWorkNode
}

func (c *reportCollector) onReport(report qviews.QueryViewAtWorkNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reports = append(c.reports, report)
}

func (c *reportCollector) get() []qviews.QueryViewAtWorkNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]qviews.QueryViewAtWorkNode{}, c.reports...)
}

func (c *reportCollector) last() qviews.QueryViewAtWorkNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reports) == 0 {
		return nil
	}
	return c.reports[len(c.reports)-1]
}

func (c *reportCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.reports)
}

// ---------------------------------------------------------------------------
// 1. ApplyViews — new Preparing view triggers Acquire
// ---------------------------------------------------------------------------

func TestSNHandler_ApplyViews_NewPreparing(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	// SN SM generates Preparing report on construction.
	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStatePreparing, rc.last().State())
	// No persistence for Preparing.
	assert.Equal(t, 0, cat.savedCount())
	// ResourceManager should have been called with Acquire.
	key := view.QueryViewKey()
	_, ok := mgr.getAcquired(key)
	require.True(t, ok)
}

func TestSNHandler_ApplyViews_UnknownViewReportsUnrecoverable(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	// Non-Preparing, non-Dropped state for unknown view → Unrecoverable.
	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateReady), OnReport: rc.onReport},
	})

	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, rc.last().State())
	assert.Equal(t, 0, mgr.acquiredCount())
}

func TestSNHandler_ApplyViews_DroppedOnUnknownViewReportsBack(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	// Coord pushes Dropped for a view SN doesn't know (e.g., after restart).
	// SN must report Dropped back so Coord can finish cleanup.
	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
	})

	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())
	assert.Equal(t, 0, mgr.acquiredCount())
}

// ---------------------------------------------------------------------------
// 2. ResourceManager callback → Ready
// ---------------------------------------------------------------------------

func TestSNHandler_ResourceManagerCallback_TransitionToReady(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	key := view.QueryViewKey()
	req, _ := mgr.getAcquired(key)

	// ResourceManager calls OnReady.
	req.OnReady()

	require.Equal(t, 2, rc.count()) // Preparing + Ready
	assert.Equal(t, qviews.QueryViewStateReady, rc.last().State())
	// No persistence for Ready.
	assert.Equal(t, 0, cat.savedCount())
}

// ---------------------------------------------------------------------------
// 3. Coord Up — Ready → Up (persists)
// ---------------------------------------------------------------------------

func TestSNHandler_CoordUp_PersistsRecoveryInfo(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	key := view.QueryViewKey()
	req, _ := mgr.getAcquired(key)
	req.OnReady()

	// Coord pushes Up.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport},
	})

	require.Equal(t, 3, rc.count()) // Preparing + Ready + Up
	assert.Equal(t, qviews.QueryViewStateUp, rc.last().State())
	assert.Equal(t, 1, cat.savedCount())
}

// ---------------------------------------------------------------------------
// 4. Coord Down — Up → Down (deletes recovery info)
// ---------------------------------------------------------------------------

func TestSNHandler_CoordDown_DeletesRecoveryInfo(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	key := view.QueryViewKey()
	req, _ := mgr.getAcquired(key)
	req.OnReady()

	// Coord Up.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport},
	})
	assert.Equal(t, 1, cat.savedCount())

	// Coord Down.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDown), OnReport: rc.onReport},
	})
	assert.Equal(t, qviews.QueryViewStateDown, rc.last().State())
	assert.Equal(t, 0, cat.savedCount()) // deleted
}

// ---------------------------------------------------------------------------
// 5. Coord Dropped — Dropping → Release → Dropped
// ---------------------------------------------------------------------------

func TestSNHandler_CoordDropped_DroppingThenDropped(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	key := view.QueryViewKey()
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})
	assert.Equal(t, 1, rc.count()) // Preparing

	// Push Dropped → SM enters Dropping, Release called.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
	})

	// No report yet — SM is in Dropping, waiting for Release callback.
	assert.Equal(t, 1, rc.count()) // still just Preparing
	assert.Equal(t, 1, mgr.releasedCount())

	// ResourceManager completes release → SM transitions Dropping → Dropped.
	mgr.invokeReleaseCallback(key)

	require.Equal(t, 2, rc.count())
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())

	// Entry removed: stale OnReady callback is a no-op.
	req, _ := mgr.getAcquired(key)
	req.OnReady()
	assert.Equal(t, 2, rc.count())
}

func TestSNHandler_CoordDropped_WhileDropping_Ignored(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	key := view.QueryViewKey()
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	// Push Dropped → SM enters Dropping.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
	})
	assert.Equal(t, 1, mgr.releasedCount())

	// Coord re-pushes Dropped while SM is Dropping → no additional Release.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
	})
	assert.Equal(t, 1, mgr.releasedCount()) // no double Release

	// Release callback completes → Dropped report.
	mgr.invokeReleaseCallback(key)
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())
}

// ---------------------------------------------------------------------------
// 6. ResourceManager callback → Unrecoverable
// ---------------------------------------------------------------------------

func TestSNHandler_ResourceManagerCallback_Unrecoverable(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	key := view.QueryViewKey()
	req, _ := mgr.getAcquired(key)
	req.OnUnrecoverable()

	assert.Equal(t, qviews.QueryViewStateUnrecoverable, rc.last().State())
	// No persist needed from Preparing → Unrecoverable.
	assert.Equal(t, 0, cat.savedCount())
}

func TestSNHandler_ResourceManagerCallback_UnrecoverableFromUpRecovering(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()

	// Simulate persisted Up view.
	meta := buildHandlerTestMeta(1)
	meta.State = viewpb.QueryViewState_QueryViewStateUp
	persistedView := &viewpb.QueryViewOfShard{
		Meta:          meta,
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}
	cat.SaveQueryView(persistedView)
	assert.Equal(t, 1, cat.savedCount())

	// Recover.
	h := RecoverSNQueryViewHandler(cat, mgr, []*viewpb.QueryViewOfShard{persistedView})

	// Set up report callback via ApplyViews re-push.
	rc := &reportCollector{}
	preparingView := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: preparingView, OnReport: rc.onReport},
	})
	// UpRecovering SM reports nothing for Preparing re-push (waits for WAL).

	// ResourceManager calls OnUnrecoverable via Recover callback.
	key := preparingView.QueryViewKey()
	recoverReq, ok := mgr.getRecovered(key)
	require.True(t, ok)
	recoverReq.OnUnrecoverable()

	// No report: UpRecovering→Unrecoverable is not reported to Coord.
	// The query path will detect the unavailable view.
	assert.Equal(t, 0, rc.count())
	// Recovery info retained — not deleted.
	assert.Equal(t, 1, cat.savedCount())
}

// ---------------------------------------------------------------------------
// 7. Recover — crash recovery
// ---------------------------------------------------------------------------

func TestSNHandler_Recover_CreatesUpRecoveringViews(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()

	meta := buildHandlerTestMeta(1)
	meta.State = viewpb.QueryViewState_QueryViewStateUp
	persistedView := &viewpb.QueryViewOfShard{
		Meta:          meta,
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}

	h := RecoverSNQueryViewHandler(cat, mgr, []*viewpb.QueryViewOfShard{persistedView})

	// ResourceManager should have Recover called.
	key := newPreparingSNView(1).QueryViewKey()
	recoverReq, ok := mgr.getRecovered(key)
	require.True(t, ok)

	// Register callback via ApplyViews (simulating Coord re-push).
	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newPreparingSNView(1), OnReport: rc.onReport},
	})

	// UpRecovering: Coord re-push Preparing → no report (SM suppresses).
	assert.Equal(t, 0, rc.count())

	// WAL catches up via ResourceManager callback.
	recoverReq.OnRecoveringDone()

	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateUp, rc.last().State())
	// Already persisted as Up — no new save (catalog save count unchanged).
}

// ---------------------------------------------------------------------------
// 8. Callback replacement on re-apply
// ---------------------------------------------------------------------------

func TestSNHandler_CallbackReplacement(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	rc1 := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc1.onReport},
	})
	assert.Equal(t, 1, rc1.count()) // Preparing

	// Make Ready via callback.
	key := view.QueryViewKey()
	req, _ := mgr.getAcquired(key)
	req.OnReady()
	assert.Equal(t, 2, rc1.count()) // Preparing + Ready

	// Re-apply with new callback.
	rc2 := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newPreparingSNView(1), OnReport: rc2.onReport},
	})

	// rc1 unchanged, rc2 gets re-report.
	assert.Equal(t, 2, rc1.count())
	require.Equal(t, 1, rc2.count())
	assert.Equal(t, qviews.QueryViewStateReady, rc2.last().State())
}

// ---------------------------------------------------------------------------
// 9. Full lifecycle
// ---------------------------------------------------------------------------

func TestSNHandler_FullLifecycle(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	key := view.QueryViewKey()

	// 1. Apply Preparing → report Preparing, Acquire called.
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})
	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStatePreparing, rc.last().State())

	// 2. ResourceManager OnReady → Ready.
	req, _ := mgr.getAcquired(key)
	req.OnReady()
	require.Equal(t, 2, rc.count())
	assert.Equal(t, qviews.QueryViewStateReady, rc.last().State())

	// 3. Coord Up → persist.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport},
	})
	require.Equal(t, 3, rc.count())
	assert.Equal(t, qviews.QueryViewStateUp, rc.last().State())
	assert.Equal(t, 1, cat.savedCount())

	// 4. Coord Down → delete persist.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDown), OnReport: rc.onReport},
	})
	require.Equal(t, 4, rc.count())
	assert.Equal(t, qviews.QueryViewStateDown, rc.last().State())
	assert.Equal(t, 0, cat.savedCount())

	// 5. Coord Dropped → Dropping, Release called.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
	})
	assert.Equal(t, 4, rc.count()) // no report yet (Dropping)
	assert.Equal(t, 1, mgr.releasedCount())

	// 6. Release callback → Dropped.
	mgr.invokeReleaseCallback(key)
	require.Equal(t, 5, rc.count())
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())

	// 7. Further callbacks are no-op.
	req.OnReady()
	assert.Equal(t, 5, rc.count())
}

// ---------------------------------------------------------------------------
// 10. Multiple versions in same shard
// ---------------------------------------------------------------------------

func TestSNHandler_MultipleVersions(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	rc1 := &reportCollector{}
	rc2 := &reportCollector{}
	view1 := newPreparingSNView(1)
	view2 := newPreparingSNView(2)

	h.ApplyViews([]handler.ApplyView{
		{View: view1, OnReport: rc1.onReport},
		{View: view2, OnReport: rc2.onReport},
	})

	// Both get Preparing report.
	assert.Equal(t, 1, rc1.count())
	assert.Equal(t, 1, rc2.count())

	// Only notify version 1 ready via callback.
	key1 := view1.QueryViewKey()
	req1, _ := mgr.getAcquired(key1)
	req1.OnReady()

	assert.Equal(t, 2, rc1.count())
	assert.Equal(t, qviews.QueryViewStateReady, rc1.last().State())
	assert.Equal(t, 1, rc2.count()) // version 2 unaffected
}

// ---------------------------------------------------------------------------
// 11. Concurrency safety
// ---------------------------------------------------------------------------

func TestSNHandler_ConcurrentApplyAndCallback(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	const numViews = 20
	var wg sync.WaitGroup
	var readyCount atomic.Int32

	for i := int64(1); i <= numViews; i++ {
		view := newPreparingSNView(i)
		h.ApplyViews([]handler.ApplyView{
			{View: view, OnReport: func(report qviews.QueryViewAtWorkNode) {
				if report.State() == qviews.QueryViewStateReady {
					readyCount.Add(1)
				}
			}},
		})
	}

	// Invoke all callbacks concurrently.
	for i := int64(1); i <= numViews; i++ {
		wg.Add(1)
		go func(version int64) {
			defer wg.Done()
			view := newPreparingSNView(version)
			key := view.QueryViewKey()
			req, ok := mgr.getAcquired(key)
			if ok {
				req.OnReady()
			}
		}(i)
	}

	wg.Wait()
	assert.Equal(t, int32(numViews), readyCount.Load())
}

// ---------------------------------------------------------------------------
// 13. Recover with callback via ApplyViews re-push, then Coord Down
// ---------------------------------------------------------------------------

func TestSNHandler_Recover_ThenCoordDown(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()

	meta := buildHandlerTestMeta(1)
	meta.State = viewpb.QueryViewState_QueryViewStateUp
	persistedView := &viewpb.QueryViewOfShard{
		Meta:          meta,
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}
	cat.SaveQueryView(persistedView)

	h := RecoverSNQueryViewHandler(cat, mgr, []*viewpb.QueryViewOfShard{persistedView})

	// Coord pushes Down to recovered view.
	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDown), OnReport: rc.onReport},
	})

	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateDown, rc.last().State())
	// Recovery info deleted.
	assert.Equal(t, 0, cat.savedCount())
}

// ---------------------------------------------------------------------------
// 14. Recover callback on unknown shard/version ignored
// ---------------------------------------------------------------------------

func TestSNHandler_RecoverCallback_AfterDropped_Ignored(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()

	// Recover a view, then drop it, then invoke the stale Recover callback.
	meta := buildHandlerTestMeta(1)
	meta.State = viewpb.QueryViewState_QueryViewStateUp
	persistedView := &viewpb.QueryViewOfShard{
		Meta:          meta,
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}

	h := RecoverSNQueryViewHandler(cat, mgr, []*viewpb.QueryViewOfShard{persistedView})

	key := newPreparingSNView(1).QueryViewKey()
	recoverReq, ok := mgr.getRecovered(key)
	require.True(t, ok)

	// Drop the view via Coord push.
	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
	})
	mgr.invokeReleaseCallback(key)
	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())

	// Stale Recover callback after entry removed — should be no-op, no panic.
	recoverReq.OnRecoveringDone()
	assert.Equal(t, 1, rc.count())
}

// ---------------------------------------------------------------------------
// 15. Dropped from Up — deletes recovery info via Dropping
// ---------------------------------------------------------------------------

func TestSNHandler_DroppedFromUp_DeletesRecoveryInfo(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	h := RecoverSNQueryViewHandler(cat, mgr, nil)

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	key := view.QueryViewKey()

	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})
	req, _ := mgr.getAcquired(key)
	req.OnReady()

	// Up.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport},
	})
	assert.Equal(t, 1, cat.savedCount())

	// Dropped from Up → Dropping (persist deleted immediately), Release called.
	h.ApplyViews([]handler.ApplyView{
		{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport},
	})
	assert.Equal(t, 0, cat.savedCount()) // recovery info deleted immediately
	assert.Equal(t, 1, mgr.releasedCount())

	// Release callback → Dropped.
	mgr.invokeReleaseCallback(key)
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())
}

// ---------------------------------------------------------------------------
// 16. Recover async callback flow
// ---------------------------------------------------------------------------

func TestSNHandler_Recover_AsyncCallbackFlow(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()

	meta := buildHandlerTestMeta(1)
	meta.State = viewpb.QueryViewState_QueryViewStateUp
	persistedView := &viewpb.QueryViewOfShard{
		Meta:          meta,
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}

	h := RecoverSNQueryViewHandler(cat, mgr, []*viewpb.QueryViewOfShard{persistedView})

	// Verify Recover was called (not Acquire).
	assert.Equal(t, 0, mgr.acquiredCount())
	assert.Equal(t, 1, mgr.recoveredCount())

	key := newPreparingSNView(1).QueryViewKey()
	recoverReq, ok := mgr.getRecovered(key)
	require.True(t, ok)
	assert.NotNil(t, recoverReq.OnRecoveringDone)
	assert.NotNil(t, recoverReq.OnUnrecoverable)

	// Register callback via ApplyViews.
	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newPreparingSNView(1), OnReport: rc.onReport},
	})
	assert.Equal(t, 0, rc.count()) // UpRecovering suppresses

	// WAL catch-up completes.
	recoverReq.OnRecoveringDone()

	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateUp, rc.last().State())
}
