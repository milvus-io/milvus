package coordview

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// ---------------------------------------------------------------------------
// Test mocks
// ---------------------------------------------------------------------------

// mockCatalog records all persist calls, tracking per-call batches.
type mockCatalog struct {
	mu        sync.Mutex
	saved     []*viewpb.QueryViewOfShard   // accumulated across all calls
	saveCalls [][]*viewpb.QueryViewOfShard // per-call batches
	listed    []*viewpb.QueryViewOfShard   // returned by ListQueryViews
}

func newMockCatalog() *mockCatalog {
	return &mockCatalog{}
}

func (c *mockCatalog) ListQueryViews(ctx context.Context) ([]*viewpb.QueryViewOfShard, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*viewpb.QueryViewOfShard, len(c.listed))
	for i, v := range c.listed {
		out[i] = proto.Clone(v).(*viewpb.QueryViewOfShard)
	}
	return out, nil
}

func (c *mockCatalog) SaveQueryViews(ctx context.Context, views []*viewpb.QueryViewOfShard) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	batch := make([]*viewpb.QueryViewOfShard, len(views))
	for i, v := range views {
		batch[i] = proto.Clone(v).(*viewpb.QueryViewOfShard)
	}
	c.saveCalls = append(c.saveCalls, batch)
	c.saved = append(c.saved, batch...)
	return nil
}

func (c *mockCatalog) savedStates() []viewpb.QueryViewState {
	c.mu.Lock()
	defer c.mu.Unlock()
	states := make([]viewpb.QueryViewState, len(c.saved))
	for i, v := range c.saved {
		states[i] = v.Meta.State
	}
	return states
}

func (c *mockCatalog) numSaveCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.saveCalls)
}

func (c *mockCatalog) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.saved = nil
	c.saveCalls = nil
}

// mockSyncer records all SyncViews calls and allows triggering callbacks.
type mockSyncer struct {
	mu          sync.Mutex
	syncedViews []syncer.SyncView  // accumulated across all calls
	syncCalls   []syncer.SyncGroup // per-call groups
}

func newMockSyncer() *mockSyncer {
	return &mockSyncer{}
}

func (s *mockSyncer) SyncViews(_ context.Context, group syncer.SyncGroup) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncCalls = append(s.syncCalls, group)
	for _, views := range group.ViewsByNode {
		s.syncedViews = append(s.syncedViews, views...)
	}
	return nil
}

func (s *mockSyncer) Close() error { return nil }

// findOnSyncResponse returns the latest OnSyncResponse callback for the given work node and version.
func (s *mockSyncer) findOnSyncResponse(node qviews.WorkNode, version qviews.QueryViewVersion) func(resp qviews.QueryViewAtWorkNode) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Search in reverse to find the latest.
	for i := len(s.syncedViews) - 1; i >= 0; i-- {
		sv := s.syncedViews[i]
		if sv.View.Version().EQ(version) && sv.View.WorkNode().Key() == node.Key() {
			return sv.OnSyncResponse
		}
	}
	return nil
}

// findOnQueryNodeLost returns the latest OnQueryNodeLost callback for the given node and version.
func (s *mockSyncer) findOnQueryNodeLost(node qviews.WorkNode, version qviews.QueryViewVersion) func(qviews.QueryNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.syncedViews) - 1; i >= 0; i-- {
		sv := s.syncedViews[i]
		if sv.View.Version().EQ(version) && sv.View.WorkNode().Key() == node.Key() {
			return sv.OnQueryNodeLost
		}
	}
	return nil
}

func (s *mockSyncer) syncViewCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.syncedViews)
}

func (s *mockSyncer) numSyncCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.syncCalls)
}

func (s *mockSyncer) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncedViews = nil
	s.syncCalls = nil
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

var (
	testShardID = qviews.ShardID{ReplicaID: testReplicaID, VChannel: testVChannel}
	testSN      = qviews.NewStreamingNodeFromVChannel(testVChannel)
	testQN1     = qviews.NewQueryNode(1)
)

func testVersion(sv, cv, qv int64) qviews.QueryViewVersion {
	return qviews.QueryViewVersion{
		DataVersion:  qviews.DataVersion{StreamingVersion: sv, CompactVersion: cv},
		QueryVersion: qv,
	}
}

// testBuilder creates a QueryViewAtCoordBuilder with the given DataVersion and numQN query nodes.
// QN IDs are 1..numQN, each assigned partition 10 with segment 1000+i.
func testBuilder(sv, cv int64, numQN int) *qviews.QueryViewAtCoordBuilder {
	dataView := &viewpb.DataViewOfCollection{
		CollectionId: testCollectionID,
		Shards: []*viewpb.DataViewOfShard{
			{Vchannel: testVChannel},
		},
		DataVersion: &viewpb.DataVersion{
			StreamingVersion: sv,
			CompactVersion:   cv,
		},
	}
	b := qviews.NewQueryViewAtCoordBuilder(testReplicaID, dataView, testVChannel)
	assignments := make(map[int64]map[int64][]int64, numQN)
	for i := 1; i <= numQN; i++ {
		assignments[int64(i)] = map[int64][]int64{
			10: {1000 + int64(i)},
		}
	}
	b.SetAssignments(assignments)
	return b
}

func buildTestViewWithVersion(numQN int, sv, cv, qv int64) *viewpb.QueryViewOfShard {
	view := buildTestView(numQN)
	view.Meta.Version = &viewpb.QueryViewVersion{
		DataVersion:  &viewpb.DataVersion{StreamingVersion: sv, CompactVersion: cv},
		QueryVersion: qv,
	}
	return view
}

func newTestManager(catalog *mockCatalog, s *mockSyncer, recovered ...*viewpb.QueryViewOfShard) *ShardViewManager {
	return NewShardViewManager(
		context.Background(),
		testShardID,
		catalog,
		s,
		recovered,
	)
}

// simulateNodeResponse simulates a node responding by finding and invoking the OnSyncResponse callback.
func simulateNodeResponse(t *testing.T, s *mockSyncer, node qviews.WorkNode, version qviews.QueryViewVersion, state qviews.QueryViewState, readySegs ...int64) bool {
	t.Helper()
	cb := s.findOnSyncResponse(node, version)
	require.NotNil(t, cb, "no OnSyncResponse found for node=%v version=%v", node, version)

	meta := &viewpb.QueryViewMeta{
		CollectionId: testCollectionID,
		ReplicaId:    testReplicaID,
		Vchannel:     testVChannel,
		Version:      version.IntoProto(),
		State:        viewpb.QueryViewState(state),
	}

	var resp qviews.QueryViewAtWorkNode
	switch n := node.(type) {
	case qviews.StreamingNode:
		resp = qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
	case qviews.QueryNode:
		resp = qviews.NewQueryViewAtQueryNode(meta, &viewpb.QueryViewOfQueryNode{
			NodeId: n.ID,
			Partitions: []*viewpb.QueryViewOfPartition{
				{PartitionId: 10, ReadySegmentIds: readySegs},
			},
		})
	}
	return cb(resp)
}

// ===========================================================================
// AddPreparing tests
// ===========================================================================

func TestAddPreparing_Success(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	b := testBuilder(1, 1, 2)
	err := mgr.AddPreparing(context.Background(), b)
	require.NoError(t, err)

	// Should persist Preparing (write-ahead) in one SaveQueryViews call.
	require.Equal(t, 1, catalog.numSaveCalls())
	require.Len(t, catalog.savedStates(), 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStatePreparing, catalog.savedStates()[0])

	// Should sync to SN + QN1 + QN2 = 3 SyncViews, in one SyncViews call.
	assert.Equal(t, 1, s.numSyncCalls())
	assert.Equal(t, 3, s.syncViewCount())

	// QueryVersion should be auto-assigned to 1.
	mgr.mu.Lock()
	require.Len(t, mgr.views, 1)
	ver := testVersion(1, 1, 1)
	sm, ok := mgr.views[ver]
	require.True(t, ok)
	assert.Equal(t, int64(1), sm.Version().QueryVersion)
	mgr.mu.Unlock()
}

func TestAddPreparing_SameDataVersionIncrementsQueryVersion(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	// Add first view with DV(1,1) → QV=1.
	b1 := testBuilder(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b1))
	ver1 := testVersion(1, 1, 1)

	// Drive v1 to Up so the Preparing slot is free.
	simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateUp)

	// Add second view with same DV(1,1) → QV should be 2.
	b2 := testBuilder(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b2))

	mgr.mu.Lock()
	require.Len(t, mgr.views, 2)
	// Find the Preparing view.
	for _, sm := range mgr.views {
		if sm.State() == qviews.QueryViewStatePreparing {
			assert.Equal(t, int64(2), sm.Version().QueryVersion)
		}
	}
	mgr.mu.Unlock()
}

func TestAddPreparing_PreemptsPreparing(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	// Add v1 Preparing.
	b1 := testBuilder(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b1))

	// Add v2 with higher DV → should preempt v1.
	catalog.reset()
	s.reset()
	b2 := testBuilder(2, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b2))

	// Should have persisted in ONE SaveQueryViews call (batch atomicity):
	// Unrecoverable(v1) + Preparing(v2).
	require.Equal(t, 1, catalog.numSaveCalls())
	states := catalog.savedStates()
	require.Len(t, states, 2)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateUnrecoverable, states[0]) // v1 preempted
	assert.Equal(t, viewpb.QueryViewState_QueryViewStatePreparing, states[1])     // v2 new

	// Should have synced in ONE SyncViews call.
	assert.Equal(t, 1, s.numSyncCalls())

	// v1 should be in Dropping, v2 should be in Preparing.
	mgr.mu.Lock()
	require.Len(t, mgr.views, 2)
	mgr.mu.Unlock()
}

func TestAddPreparing_PreemptsReady(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	// Add and drive v1 to Ready.
	b1 := testBuilder(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b1))
	ver1 := testVersion(1, 1, 1)
	simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateReady)

	mgr.mu.Lock()
	require.Equal(t, qviews.QueryViewStateReady, mgr.views[ver1].State())
	mgr.mu.Unlock()

	// Add v2 → should preempt Ready v1.
	catalog.reset()
	s.reset()
	b2 := testBuilder(2, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b2))

	// v1 should be in Dropping (Unrecoverable → Dropping), v2 in Preparing.
	mgr.mu.Lock()
	require.Len(t, mgr.views, 2)
	var states []qviews.QueryViewState
	for _, sm := range mgr.views {
		states = append(states, sm.State())
	}
	mgr.mu.Unlock()
	assert.Contains(t, states, qviews.QueryViewStateDropping)
	assert.Contains(t, states, qviews.QueryViewStatePreparing)
}

func TestAddPreparing_DataVersionRollbackRejected(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	// Add v1 with DV(2,1).
	b1 := testBuilder(2, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b1))

	// Try to add v2 with DV(1,1) → should fail with DataVersion rollback.
	b2 := testBuilder(1, 1, 1)
	err := mgr.AddPreparing(context.Background(), b2)
	assert.ErrorIs(t, err, errDataVersionRollback)
}

func TestAddPreparing_BatchPersistAtomicity(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	// Add v1 Preparing.
	b1 := testBuilder(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b1))

	// Preempt with v2: should result in exactly ONE SaveQueryViews call
	// containing both the preempted view's Unrecoverable and the new Preparing.
	catalog.reset()
	b2 := testBuilder(2, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b2))

	assert.Equal(t, 1, catalog.numSaveCalls(), "preemption+new should be a single SaveQueryViews call")
	assert.Len(t, catalog.savedStates(), 2)
}

// ===========================================================================
// Normal flow: AddPreparing → all Ready → Up → cascade Down old
// ===========================================================================

func TestNormalFlow_PrepareToUpCascade(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	// Add v1 and drive it to Up.
	b1 := testBuilder(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b1))
	ver1 := testVersion(1, 1, 1)

	// QN1 Ready.
	simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateReady)

	// SN Ready → Preparing→Ready, push Up to SN.
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateReady)

	// SN Up → Ready→Up, persist Up.
	catalog.reset()
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateUp)
	require.Len(t, catalog.savedStates(), 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateUp, catalog.savedStates()[0])

	// Now add v2. v1 is Up, v2 is Preparing.
	catalog.reset()
	s.reset()
	b2 := testBuilder(2, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b2))
	ver2 := testVersion(2, 1, 1)

	// Drive v2 to Up.
	simulateNodeResponse(t, s, testQN1, ver2, qviews.QueryViewStateReady)
	catalog.reset()
	simulateNodeResponse(t, s, testSN, ver2, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver2, qviews.QueryViewStateUp)

	// After v2 reaches Up, v1 should be cascaded to Down.
	// Persisted: v2 Up + v1 Down.
	states := catalog.savedStates()
	require.Len(t, states, 2)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateUp, states[0])   // v2 Up
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDown, states[1]) // v1 Down

	// SN should have received Down push for v1.
	cb := s.findOnSyncResponse(testSN, ver1)
	assert.NotNil(t, cb, "should have a Down sync for v1 to SN")
}

func TestSyncResponseCompletesCurrentNodeSync(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	b := testBuilder(1, 1, 1)
	ver1 := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b))

	// Preparing is synced to QN and SN. Each node completes that specific sync
	// when it reports Ready; the view itself is not removed yet.
	done := simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateReady)
	assert.True(t, done, "QN Ready should complete its Preparing sync")

	done = simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateReady)
	assert.True(t, done, "SN Ready should complete its Preparing sync")

	// Ready pushes Up only to SN; SN Up completes that Up sync.
	done = simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateUp)
	assert.True(t, done, "SN Up should complete its Up sync")

	// Release pushes Down only to SN; SN Down completes that Down sync and
	// moves the view to Dropping.
	require.NoError(t, mgr.RequestRelease(context.Background()))
	done = simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateDown)
	assert.True(t, done, "SN Down should complete its Down sync")

	// Dropping pushes Dropped to all nodes. Each node completes its own Dropped
	// sync independently; the final response also removes the view.
	done = simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateDropped)
	assert.True(t, done, "SN Dropped should complete its Dropped sync")
	done = simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateDropped)
	assert.True(t, done, "QN Dropped should complete its Dropped sync")
}

func TestStreamingNodeSyncHasNoQueryNodeLostCallback(t *testing.T) {
	s := newMockSyncer()
	mgr := newTestManager(newMockCatalog(), s)

	ver1 := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), testBuilder(1, 1, 1)))

	assert.Nil(t, s.findOnQueryNodeLost(testSN, ver1))
	assert.NotNil(t, s.findOnQueryNodeLost(testQN1, ver1))
}

// ===========================================================================
// Full lifecycle: Down → Dropping → Dropped
// ===========================================================================

func TestFullLifecycle_DownToDropped(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	// Add and drive v1 to Up.
	b1 := testBuilder(1, 1, 1)
	ver1 := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b1))
	simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateUp)

	// Add v2 and drive to Up → v1 cascades to Down.
	b2 := testBuilder(2, 1, 1)
	ver2 := testVersion(2, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b2))
	simulateNodeResponse(t, s, testQN1, ver2, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver2, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver2, qviews.QueryViewStateUp)
	// v1 is now Down.

	// SN confirms Down for v1 → v1 goes Dropping, pushes Dropped to all.
	catalog.reset()
	prevSyncCount := s.syncViewCount()
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateDown)

	// Should have pushed Dropped to SN + QN1 for v1.
	assert.Equal(t, prevSyncCount+2, s.syncViewCount()) // SN + QN1

	// All nodes confirm Dropped.
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateDropped)
	catalog.reset()
	done := simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateDropped)

	// v1 should be removed.
	assert.True(t, done, "callback should return true when view is Dropped")
	// Persist Dropped = delete from ETCD.
	require.Len(t, catalog.savedStates(), 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDropped, catalog.savedStates()[0])

	// Manager should only have v2 left.
	mgr.mu.Lock()
	assert.Len(t, mgr.views, 1)
	mgr.mu.Unlock()
}

// ===========================================================================
// Unrecoverable flow via OnQueryNodeLost
// ===========================================================================

func TestQueryNodeLost_TriggersUnrecoverable(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	b := testBuilder(1, 1, 1)
	ver1 := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b))

	// Simulate QN1 lost via OnQueryNodeLost.
	onQueryNodeLost := s.findOnQueryNodeLost(testQN1, ver1)
	require.NotNil(t, onQueryNodeLost)

	// Invoke OnQueryNodeLost — should inject synthetic Unrecoverable internally.
	catalog.reset()
	s.reset()
	onQueryNodeLost(testQN1)

	// Should persist Unrecoverable but NOT advance to Dropping yet.
	states := catalog.savedStates()
	require.Len(t, states, 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateUnrecoverable, states[0])

	// View stays Unrecoverable, no Dropped sync pushed.
	mgr.mu.Lock()
	require.Len(t, mgr.views, 1)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, mgr.views[ver1].State())
	mgr.mu.Unlock()
	assert.Equal(t, 0, s.syncViewCount())

	// Adding a replacement view advances the old one to Dropping atomically.
	catalog.reset()
	s.reset()
	b2 := testBuilder(2, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b2))
	ver2 := testVersion(2, 1, 1)

	// Single flush: Preparing(v2) persist + Dropped(v1) sync + Preparing(v2) sync.
	assert.Equal(t, 1, catalog.numSaveCalls())
	assert.Equal(t, 1, s.numSyncCalls())

	// Confirm all nodes Dropped for v1 → view removed.
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateDropped)
	done := simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateDropped)
	assert.True(t, done)

	// v2 still active.
	mgr.mu.Lock()
	require.Len(t, mgr.views, 1)
	assert.Equal(t, qviews.QueryViewStatePreparing, mgr.views[ver2].State())
	mgr.mu.Unlock()
}

func TestQueryNodeLost_DroppingCleanupCompletes(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	b := testBuilder(1, 1, 2)
	ver1 := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b))

	// Drive v1 to Up.
	simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateReady, 1001)
	simulateNodeResponse(t, s, qviews.NewQueryNode(2), ver1, qviews.QueryViewStateReady, 1002)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateUp)

	// Release v1 and move it into Dropping.
	require.NoError(t, mgr.RequestRelease(context.Background()))
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateDown)

	mgr.mu.Lock()
	require.Len(t, mgr.views, 1)
	assert.Equal(t, qviews.QueryViewStateDropping, mgr.views[ver1].State())
	mgr.mu.Unlock()

	// QN2 is removed while Dropped is pending. The manager should count it as
	// cleaned up and complete once SN and the remaining QN report Dropped.
	onQueryNodeLost := s.findOnQueryNodeLost(qviews.NewQueryNode(2), ver1)
	require.NotNil(t, onQueryNodeLost)
	onQueryNodeLost(qviews.NewQueryNode(2))

	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateDropped)
	catalog.reset()
	done := simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateDropped)

	assert.True(t, done, "callback should return true when view is Dropped")
	require.Len(t, catalog.savedStates(), 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDropped, catalog.savedStates()[0])

	mgr.mu.Lock()
	assert.Empty(t, mgr.views)
	mgr.mu.Unlock()
}

// ===========================================================================
// RequestRelease tests
// ===========================================================================

func TestRequestRelease_UpView(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	// Drive v1 to Up.
	b := testBuilder(1, 1, 1)
	ver1 := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b))
	simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateUp)

	// RequestRelease: Up view → Down.
	catalog.reset()
	require.NoError(t, mgr.RequestRelease(context.Background()))

	// Should persist Down.
	states := catalog.savedStates()
	require.Len(t, states, 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateDown, states[0])

	// Verify view is in Down state.
	mgr.mu.Lock()
	require.Len(t, mgr.views, 1)
	assert.Equal(t, qviews.QueryViewStateDown, mgr.views[ver1].State())
	mgr.mu.Unlock()
}

func TestRequestRelease_PreparingView(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	b := testBuilder(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b))

	// RequestRelease: Preparing view → Unrecoverable → Dropping.
	catalog.reset()
	s.reset()
	require.NoError(t, mgr.RequestRelease(context.Background()))

	// Should persist Unrecoverable.
	states := catalog.savedStates()
	require.Len(t, states, 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateUnrecoverable, states[0])

	// View should be in Dropping state (Unrecoverable→Dropping immediate).
	ver1 := testVersion(1, 1, 1)
	mgr.mu.Lock()
	require.Len(t, mgr.views, 1)
	assert.Equal(t, qviews.QueryViewStateDropping, mgr.views[ver1].State())
	mgr.mu.Unlock()

	// Should have pushed Dropped to all nodes.
	assert.True(t, s.syncViewCount() > 0)
}

func TestRequestRelease_MixedViews(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	// v1: drive to Up.
	b1 := testBuilder(1, 1, 1)
	ver1 := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b1))
	simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateReady)
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateUp)

	// v2: still Preparing.
	b2 := testBuilder(2, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b2))

	catalog.reset()
	require.NoError(t, mgr.RequestRelease(context.Background()))

	// v2 (Preparing→Unrecoverable) and v1 (Up→Down) persisted in a single batch.
	states := catalog.savedStates()
	require.Len(t, states, 2)
	assert.Contains(t, states, viewpb.QueryViewState_QueryViewStateDown)
	assert.Contains(t, states, viewpb.QueryViewState_QueryViewStateUnrecoverable)

	// Should be one SaveQueryViews call for batch atomicity.
	assert.Equal(t, 1, catalog.numSaveCalls())
}

// ===========================================================================
// Recovery tests
// ===========================================================================

func TestRecovery_PreparingView(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()

	// Recover a Preparing view.
	v1 := buildTestViewWithVersion(1, 1, 1, 1)
	v1.Meta.State = viewpb.QueryViewState_QueryViewStatePreparing
	mgr := newTestManager(catalog, s, v1)

	// Should re-push Preparing to all nodes.
	assert.Equal(t, 2, s.syncViewCount()) // SN + QN1

	// Manager has 1 view in Preparing.
	ver1 := testVersion(1, 1, 1)
	mgr.mu.Lock()
	require.Len(t, mgr.views, 1)
	assert.Equal(t, qviews.QueryViewStatePreparing, mgr.views[ver1].State())
	mgr.mu.Unlock()
}

func TestRecovery_UnrecoverableView(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()

	// Recover an Unrecoverable view.
	v1 := buildTestViewWithVersion(1, 1, 1, 1)
	v1.Meta.State = viewpb.QueryViewState_QueryViewStateUnrecoverable
	mgr := newTestManager(catalog, s, v1)

	// Stays Unrecoverable, waits for AddPreparing to advance to Dropping.
	ver1 := testVersion(1, 1, 1)
	mgr.mu.Lock()
	require.Len(t, mgr.views, 1)
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, mgr.views[ver1].State())
	mgr.mu.Unlock()

	// No sync pushed.
	assert.Equal(t, 0, s.syncViewCount())
}

func TestRecovery_FastForward_SNReportsUp(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()

	// Recover a Preparing view. SN already has Up from previous run.
	v1 := buildTestViewWithVersion(1, 1, 1, 1)
	v1.Meta.State = viewpb.QueryViewState_QueryViewStatePreparing
	mgr := newTestManager(catalog, s, v1)
	ver1 := testVersion(1, 1, 1)

	// QN1 Ready.
	simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateReady)

	// SN reports Up (fast-forward from recovery).
	catalog.reset()
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateUp)

	// Should fast-forward to Up.
	mgr.mu.Lock()
	require.Len(t, mgr.views, 1)
	assert.Equal(t, qviews.QueryViewStateUp, mgr.views[ver1].State())
	mgr.mu.Unlock()

	// Should persist Up.
	states := catalog.savedStates()
	require.Len(t, states, 1)
	assert.Equal(t, viewpb.QueryViewState_QueryViewStateUp, states[0])
}

// ===========================================================================
// Edge case: callback for removed view returns true
// ===========================================================================

func TestCallback_RemovedView_ReturnsTrue(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	b := testBuilder(1, 1, 1)
	ver1 := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b))

	// Grab a callback before the view is removed.
	cb := s.findOnSyncResponse(testSN, ver1)
	require.NotNil(t, cb)

	// Force release and drive to Dropped.
	require.NoError(t, mgr.RequestRelease(context.Background()))
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateDropped)
	simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateDropped)

	// View is removed. Old callback should return true.
	meta := &viewpb.QueryViewMeta{
		CollectionId: testCollectionID,
		ReplicaId:    testReplicaID,
		Vchannel:     testVChannel,
		Version:      ver1.IntoProto(),
		State:        viewpb.QueryViewState_QueryViewStateDropped,
	}
	resp := qviews.NewQueryViewAtStreamingNode(meta, &viewpb.QueryViewOfStreamingNode{})
	done := cb(resp)
	assert.True(t, done, "callback for removed view should return true")
}

// ===========================================================================
// Edge case: OnQueryNodeLost for already removed view is no-op
// ===========================================================================

func TestOnQueryNodeLost_RemovedView_NoOp(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	mgr := newTestManager(catalog, s)

	b := testBuilder(1, 1, 1)
	ver1 := testVersion(1, 1, 1)
	require.NoError(t, mgr.AddPreparing(context.Background(), b))

	// Grab OnQueryNodeLost before the view is removed.
	onQueryNodeLost := s.findOnQueryNodeLost(testQN1, ver1)
	require.NotNil(t, onQueryNodeLost)

	// Force release and drive to Dropped.
	require.NoError(t, mgr.RequestRelease(context.Background()))
	simulateNodeResponse(t, s, testSN, ver1, qviews.QueryViewStateDropped)
	simulateNodeResponse(t, s, testQN1, ver1, qviews.QueryViewStateDropped)

	mgr.mu.Lock()
	assert.Empty(t, mgr.views)
	mgr.mu.Unlock()

	// OnQueryNodeLost for removed view should be a no-op (no panic).
	onQueryNodeLost(testQN1)

	mgr.mu.Lock()
	assert.Empty(t, mgr.views)
	mgr.mu.Unlock()
}
