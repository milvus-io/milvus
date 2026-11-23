package coordview

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type capturedDirtyViewTaskScheduler struct {
	mu    sync.Mutex
	tasks []nodescheduler.Task
}

func (s *capturedDirtyViewTaskScheduler) Submit(task nodescheduler.Task, opts ...nodescheduler.Option) nodescheduler.TaskHandle {
	s.mu.Lock()
	s.tasks = append(s.tasks, task)
	s.mu.Unlock()
	return noopDirtyViewTaskHandle{}
}

func (s *capturedDirtyViewTaskScheduler) snapshot() []nodescheduler.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]nodescheduler.Task(nil), s.tasks...)
}

type noopDirtyViewTaskHandle struct{}

func (noopDirtyViewTaskHandle) Cancel()                    {}
func (noopDirtyViewTaskHandle) Wait(context.Context) error { return nil }

type capturedDirtyViewEventSubmitter struct {
	mu     sync.Mutex
	events []dirtyViewEvent
}

func (s *capturedDirtyViewEventSubmitter) Submit(event dirtyViewEvent) {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
}

func (s *capturedDirtyViewEventSubmitter) snapshot() []dirtyViewEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]dirtyViewEvent(nil), s.events...)
}

type lockCheckingDirtyViewEventSubmitter struct {
	manager  *ShardViewManager
	lockHeld bool
}

func (s *lockCheckingDirtyViewEventSubmitter) Submit(dirtyViewEvent) {
	if s.manager.mu.TryLock() {
		s.manager.mu.Unlock()
		return
	}
	s.lockHeld = true
}

type blockingFirstDirtyViewEventSubmitter struct {
	mu           sync.Mutex
	states       []viewpb.QueryViewState
	firstStarted chan struct{}
	releaseFirst chan struct{}
	submitted    chan struct{}
}

func (s *blockingFirstDirtyViewEventSubmitter) Submit(event dirtyViewEvent) {
	state := event.persists[0].GetMeta().GetState()
	if state == viewpb.QueryViewState_QueryViewStatePreparing {
		close(s.firstStarted)
		<-s.releaseFirst
	}
	s.mu.Lock()
	s.states = append(s.states, state)
	s.mu.Unlock()
	s.submitted <- struct{}{}
}

func (s *blockingFirstDirtyViewEventSubmitter) snapshot() []viewpb.QueryViewState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]viewpb.QueryViewState(nil), s.states...)
}

type concurrentSaveCatalog struct {
	queryview.QueryViewCatalog
	started chan struct{}
	release chan struct{}
}

type blockingFlushCatalog struct {
	*mockCatalog
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type blockingFlushSyncer struct {
	*mockSyncer
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type blockNextFlushCatalog struct {
	*mockCatalog

	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
}

func (c *blockingFlushCatalog) SaveQueryViews(ctx context.Context, views []*viewpb.QueryViewOfShard) error {
	c.once.Do(func() { close(c.started) })
	select {
	case <-c.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return c.mockCatalog.SaveQueryViews(ctx, views)
}

func (s *blockingFlushSyncer) SyncViews(ctx context.Context, group syncer.SyncGroup) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.mockSyncer.SyncViews(ctx, group)
}

func (c *blockNextFlushCatalog) blockNext() (<-chan struct{}, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.started = make(chan struct{})
	c.release = make(chan struct{})
	release := c.release
	return c.started, func() { close(release) }
}

func (c *blockNextFlushCatalog) SaveQueryViews(ctx context.Context, views []*viewpb.QueryViewOfShard) error {
	c.mu.Lock()
	started := c.started
	release := c.release
	c.started = nil
	c.release = nil
	c.mu.Unlock()

	if started != nil {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.mockCatalog.SaveQueryViews(ctx, views)
}

func newTestDirtyViewFlushScheduler(
	t *testing.T,
	catalog queryview.QueryViewCatalog,
	s syncer.ReliableSyncer,
	maxTxnOps int,
) *DirtyViewFlushScheduler {
	t.Helper()
	nodeScheduler := nodescheduler.New(1)
	t.Cleanup(nodeScheduler.Close)
	scheduler := newDirtyViewFlushScheduler(catalog, s, maxTxnOps, nodeScheduler)
	t.Cleanup(scheduler.Close)
	return scheduler
}

func (c *concurrentSaveCatalog) SaveQueryViews(ctx context.Context, _ []*viewpb.QueryViewOfShard) error {
	c.started <- struct{}{}
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func dirtyPersistEvent(shardID qviews.ShardID, queryVersion int64) dirtyViewEvent {
	view := buildTestViewWithVersion(1, 1, 1, queryVersion)
	view.Meta.ReplicaId = shardID.ReplicaID
	view.Meta.Vchannel = shardID.VChannel
	return dirtyViewEvent{
		shardID:  shardID,
		persists: []*viewpb.QueryViewOfShard{view},
	}
}

func testBuilderForShard(collectionID int64, shardID qviews.ShardID) *qviews.QueryViewAtCoordBuilder {
	dataView := &viewpb.DataViewOfCollection{
		CollectionId: collectionID,
		Shards: []*viewpb.DataViewOfShard{
			{Vchannel: shardID.VChannel},
		},
		DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
	}
	builder := qviews.NewQueryViewAtCoordBuilder(shardID.ReplicaID, dataView, shardID.VChannel)
	builder.SetAssignments(map[int64]map[int64][]int64{1: {10: {1001}}})
	return builder
}

func TestDirtyViewFlushSchedulerBeginCommitBatchesAcrossShards(t *testing.T) {
	catalog := newMockCatalog()
	s := newMockSyncer()
	nodeScheduler := &capturedDirtyViewTaskScheduler{}
	scheduler := newDirtyViewFlushScheduler(catalog, s, 128, nodeScheduler)
	t.Cleanup(scheduler.Close)

	batch := scheduler.Begin()
	scheduler.Submit(dirtyPersistEvent(qviews.ShardID{ReplicaID: 1, VChannel: "v1"}, 1))
	scheduler.Submit(dirtyPersistEvent(qviews.ShardID{ReplicaID: 1, VChannel: "v2"}, 1))
	assert.Empty(t, nodeScheduler.snapshot())

	batch.Commit()
	tasks := nodeScheduler.snapshot()
	require.Len(t, tasks, 1)
	require.NoError(t, tasks[0].Execute(context.Background()))
	assert.Equal(t, 1, catalog.numSaveCalls())
	assert.Len(t, catalog.saved, 2)
}

func TestDirtyViewFlushSchedulerNestedCommitDispatchesOnce(t *testing.T) {
	nodeScheduler := &capturedDirtyViewTaskScheduler{}
	scheduler := newDirtyViewFlushScheduler(newMockCatalog(), newMockSyncer(), 128, nodeScheduler)
	t.Cleanup(scheduler.Close)

	outer := scheduler.Begin()
	inner := scheduler.Begin()
	scheduler.Submit(dirtyPersistEvent(qviews.ShardID{ReplicaID: 1, VChannel: "v1"}, 1))
	inner.Commit()
	assert.Empty(t, nodeScheduler.snapshot())
	outer.Commit()
	assert.Len(t, nodeScheduler.snapshot(), 1)
	outer.Commit()
	assert.Len(t, nodeScheduler.snapshot(), 1)
}

func TestDirtyViewFlushSchedulerCommitSplitsLargeBatch(t *testing.T) {
	nodeScheduler := &capturedDirtyViewTaskScheduler{}
	scheduler := newDirtyViewFlushScheduler(newMockCatalog(), newMockSyncer(), 1, nodeScheduler)
	t.Cleanup(scheduler.Close)

	batch := scheduler.Begin()
	for i := int64(1); i <= 3; i++ {
		scheduler.Submit(dirtyPersistEvent(qviews.ShardID{ReplicaID: 1, VChannel: "v" + string(rune('0'+i))}, 1))
	}
	batch.Commit()
	assert.Len(t, nodeScheduler.snapshot(), 3)
}

func TestDirtyViewFlushSchedulerRunsDifferentShardsConcurrently(t *testing.T) {
	catalog := &concurrentSaveCatalog{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	nodeScheduler := nodescheduler.New(2)
	t.Cleanup(nodeScheduler.Close)
	scheduler := newDirtyViewFlushScheduler(catalog, newMockSyncer(), 1, nodeScheduler)
	t.Cleanup(scheduler.Close)

	scheduler.Submit(dirtyPersistEvent(qviews.ShardID{ReplicaID: 1, VChannel: "v1"}, 1))
	scheduler.Submit(dirtyPersistEvent(qviews.ShardID{ReplicaID: 1, VChannel: "v2"}, 1))

	for range 2 {
		select {
		case <-catalog.started:
		case <-time.After(5 * time.Second):
			t.Fatal("different shard batches did not execute concurrently")
		}
	}
	close(catalog.release)
	require.NoError(t, scheduler.Flush(context.Background()))
}

func TestDirtyViewFlushSchedulerSerializesSameShard(t *testing.T) {
	catalog := &concurrentSaveCatalog{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	nodeScheduler := nodescheduler.New(2)
	t.Cleanup(nodeScheduler.Close)
	scheduler := newDirtyViewFlushScheduler(catalog, newMockSyncer(), 1, nodeScheduler)
	t.Cleanup(scheduler.Close)
	shardID := qviews.ShardID{ReplicaID: 1, VChannel: "v1"}

	scheduler.Submit(dirtyPersistEvent(shardID, 1))
	select {
	case <-catalog.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first shard batch did not start")
	}

	scheduler.Submit(dirtyPersistEvent(shardID, 2))
	select {
	case <-catalog.started:
		t.Fatal("same shard executed concurrently")
	case <-time.After(100 * time.Millisecond):
	}

	close(catalog.release)
	select {
	case <-catalog.started:
	case <-time.After(5 * time.Second):
		t.Fatal("successor batch for the same shard did not start")
	}
	require.NoError(t, scheduler.Flush(context.Background()))
}

func TestDirtyViewFlushSchedulerPersistsBeforeSync(t *testing.T) {
	catalog := &blockingFlushCatalog{
		mockCatalog: newMockCatalog(),
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	s := newMockSyncer()
	nodeScheduler := nodescheduler.New(1)
	t.Cleanup(nodeScheduler.Close)
	scheduler := newDirtyViewFlushScheduler(catalog, s, 128, nodeScheduler)
	t.Cleanup(scheduler.Close)

	view := buildTestViewWithVersion(1, 1, 1, 1)
	event := dirtyViewEvent{
		shardID:  testShardID,
		persists: []*viewpb.QueryViewOfShard{view},
		syncs: []syncer.SyncView{{
			View: qviews.NewFullQueryViewAtStreamingNode(view.Meta, view.StreamingNode, view.QueryNode),
		}},
	}
	scheduler.Submit(event)

	select {
	case <-catalog.started:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog persist did not start")
	}
	assert.Zero(t, s.numSyncCalls())
	close(catalog.release)
	require.NoError(t, scheduler.Flush(context.Background()))
	assert.Equal(t, 1, s.numSyncCalls())
}

func TestDirtyViewFlushSchedulerRunsAfterPersistCallbacksAfterCatalogSave(t *testing.T) {
	catalog := &blockingFlushCatalog{
		mockCatalog: newMockCatalog(),
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	scheduler := newTestDirtyViewFlushScheduler(t, catalog, newMockSyncer(), 128)

	called := make(chan struct{})
	scheduler.Submit(dirtyViewEvent{
		shardID: testShardID,
		persists: []*viewpb.QueryViewOfShard{
			buildTestViewWithVersion(1, 1, 1, 1),
		},
		afterPersist: []func(){func() { close(called) }},
	})

	select {
	case <-catalog.started:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog persist did not start")
	}
	select {
	case <-called:
		t.Fatal("afterPersist callback ran before catalog save completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(catalog.release)
	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("afterPersist callback did not run after catalog save")
	}
	require.NoError(t, scheduler.Flush(context.Background()))
}

func TestShardViewManagerSubmitsOneShardScopedDirtyEvent(t *testing.T) {
	submitter := &capturedDirtyViewEventSubmitter{}
	manager := newShardViewManager(context.Background(), testShardID, submitter, nil, stubDataViewRefProvider{})

	require.NoError(t, manager.AddPreparing(context.Background(), testBuilder(1, 1, 1)))
	events := submitter.snapshot()
	require.Len(t, events, 1)
	assert.Equal(t, testShardID, events[0].shardID)
	assert.Len(t, events[0].persists, 1)
	assert.Len(t, events[0].syncs, 2)
}

func TestShardViewManagerSubmitsDirtyEventWhileHoldingManagerLock(t *testing.T) {
	submitter := &lockCheckingDirtyViewEventSubmitter{}
	manager := newShardViewManager(context.Background(), testShardID, submitter, nil, stubDataViewRefProvider{})
	submitter.manager = manager

	require.NoError(t, manager.AddPreparing(context.Background(), testBuilder(1, 1, 1)))
	assert.True(t, submitter.lockHeld)
}

func TestShardViewManagerPreservesTransitionOrderWhenSubmitOverlaps(t *testing.T) {
	submitter := &blockingFirstDirtyViewEventSubmitter{
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
		submitted:    make(chan struct{}, 2),
	}
	manager := newShardViewManager(context.Background(), testShardID, submitter, nil, stubDataViewRefProvider{})

	addDone := make(chan error, 1)
	go func() {
		addDone <- manager.AddPreparing(context.Background(), testBuilder(1, 1, 1))
	}()
	select {
	case <-submitter.firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first event submission did not start")
	}

	releaseDone := make(chan error, 1)
	go func() {
		releaseDone <- manager.RequestRelease(context.Background())
	}()
	select {
	case <-submitter.submitted:
		t.Fatal("a newer transition was submitted before the blocked older event")
	case <-time.After(100 * time.Millisecond):
	}

	close(submitter.releaseFirst)
	require.NoError(t, <-addDone)
	require.NoError(t, <-releaseDone)
	require.Len(t, submitter.submitted, 2)
	assert.Equal(t, []viewpb.QueryViewState{
		viewpb.QueryViewState_QueryViewStatePreparing,
		viewpb.QueryViewState_QueryViewStateUnrecoverable,
	}, submitter.snapshot())
}

func TestShardViewManagerDoesNotReuseQueryVersionBeforeDroppedPersist(t *testing.T) {
	catalog := &blockNextFlushCatalog{mockCatalog: newMockCatalog()}
	s := newMockSyncer()
	scheduler := newTestDirtyViewFlushScheduler(t, catalog, s, 128)
	manager := newShardViewManager(context.Background(), testShardID, scheduler, nil, stubDataViewRefProvider{})
	flush := func() { require.NoError(t, scheduler.Flush(context.Background())) }
	version1 := testVersion(1, 1, 1)

	require.NoError(t, manager.AddPreparing(context.Background(), testBuilder(1, 1, 1)))
	flush()

	// QN lost → v1 Unrecoverable, retained without a successor.
	onQueryNodeLost := s.findOnQueryNodeLost(testQN1, version1)
	require.NotNil(t, onQueryNodeLost)
	onQueryNodeLost(testQN1)
	flush()

	// Re-prepare the same DV while v1 (1,1,1) is still retained mid-teardown:
	// the manager must not reuse QueryVersion (1,1,1).
	require.NoError(t, manager.AddPreparing(context.Background(), testBuilder(1, 1, 1)))
	flush()
	manager.mu.Lock()
	_, oldViewRetained := manager.views[version1]
	_, newViewCreated := manager.views[testVersion(1, 1, 2)]
	manager.mu.Unlock()
	assert.True(t, oldViewRetained)
	assert.True(t, newViewCreated)

	// v1's Dropped persist is pending: it must stay retained until the persist
	// completes, and only then be removed.
	persistStarted, releasePersist := catalog.blockNext()
	simulateNodeResponse(t, s, testSN, version1, qviews.QueryViewStateDropped)
	flush()
	simulateNodeResponse(t, s, testQN1, version1, qviews.QueryViewStateDropped)
	select {
	case <-persistStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Dropped persist did not start")
	}
	manager.mu.Lock()
	_, oldViewRetained = manager.views[version1]
	manager.mu.Unlock()
	assert.True(t, oldViewRetained)

	releasePersist()
	flush()
	manager.mu.Lock()
	_, oldViewRetained = manager.views[version1]
	_, newViewCreated = manager.views[testVersion(1, 1, 2)]
	manager.mu.Unlock()
	assert.False(t, oldViewRetained)
	assert.True(t, newViewCreated)
}

// failingFlushCatalog returns a fixed error from SaveQueryViews, simulating a
// non-shutdown flush failure (e.g. a TiKV undetermined write).
type failingFlushCatalog struct {
	queryview.QueryViewCatalog
	err error
}

func (c *failingFlushCatalog) SaveQueryViews(context.Context, []*viewpb.QueryViewOfShard) error {
	return c.err
}

// ctxErrorFlushCatalog returns the context error from SaveQueryViews,
// simulating the flush observing the task's cancellation signal.
type ctxErrorFlushCatalog struct {
	queryview.QueryViewCatalog
}

func (c *ctxErrorFlushCatalog) SaveQueryViews(ctx context.Context, _ []*viewpb.QueryViewOfShard) error {
	return ctx.Err()
}

// failingFlushSyncer returns a fixed error from SyncViews.
type failingFlushSyncer struct {
	syncer.ReliableSyncer
	err error
}

func (s *failingFlushSyncer) SyncViews(context.Context, syncer.SyncGroup) error {
	return s.err
}

func TestDirtyViewFlushTaskPanicsOnUnrecoverableFlushError(t *testing.T) {
	catalog := &failingFlushCatalog{
		QueryViewCatalog: newMockCatalog(),
		err:              errors.New("simulated non-shutdown flush failure"),
	}
	nodeScheduler := &capturedDirtyViewTaskScheduler{}
	scheduler := newDirtyViewFlushScheduler(catalog, newMockSyncer(), 128, nodeScheduler)
	t.Cleanup(scheduler.Close)

	scheduler.Submit(dirtyPersistEvent(testShardID, 1))
	tasks := nodeScheduler.snapshot()
	require.Len(t, tasks, 1)
	task := tasks[0].(*dirtyViewFlushTask)

	defer func() {
		r := recover()
		require.NotNil(t, r, "Execute must panic on an unrecoverable flush error")
		msg, ok := r.(string)
		require.True(t, ok, "panic value must be a string, got %v", r)
		assert.Contains(t, msg, "unrecoverable flush failure")
		assert.Contains(t, msg, "simulated non-shutdown flush failure")
		assert.Contains(t, msg, testShardID.String())
	}()
	_ = task.Execute(context.Background())
	t.Fatal("Execute must panic on an unrecoverable flush error")
}

func TestDirtyViewFlushTaskDoesNotPanicOnCanceledContext(t *testing.T) {
	catalog := &ctxErrorFlushCatalog{QueryViewCatalog: newMockCatalog()}
	nodeScheduler := &capturedDirtyViewTaskScheduler{}
	scheduler := newDirtyViewFlushScheduler(catalog, newMockSyncer(), 128, nodeScheduler)
	t.Cleanup(scheduler.Close)

	scheduler.Submit(dirtyPersistEvent(testShardID, 1))
	tasks := nodeScheduler.snapshot()
	require.Len(t, tasks, 1)
	task := tasks[0].(*dirtyViewFlushTask)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := task.Execute(ctx)
	require.ErrorIs(t, err, context.Canceled, "Execute must return the cancellation error")
}

func TestDirtyViewFlushTaskPanicsOnSyncerClosedError(t *testing.T) {
	view := buildTestViewWithVersion(1, 1, 1, 1)
	event := dirtyViewEvent{
		shardID: testShardID,
		syncs: []syncer.SyncView{{
			View: qviews.NewFullQueryViewAtStreamingNode(view.Meta, view.StreamingNode, view.QueryNode),
		}},
	}
	s := &failingFlushSyncer{
		ReliableSyncer: newMockSyncer(),
		err:            syncer.ErrSyncerClosed,
	}
	nodeScheduler := &capturedDirtyViewTaskScheduler{}
	scheduler := newDirtyViewFlushScheduler(newMockCatalog(), s, 128, nodeScheduler)
	t.Cleanup(scheduler.Close)

	scheduler.Submit(event)
	tasks := nodeScheduler.snapshot()
	require.Len(t, tasks, 1)
	task := tasks[0].(*dirtyViewFlushTask)

	defer func() {
		r := recover()
		require.NotNil(t, r, "Execute must panic on ErrSyncerClosed: it is not a cancellation signal")
		msg, ok := r.(string)
		require.True(t, ok, "panic value must be a string, got %v", r)
		assert.Contains(t, msg, "unrecoverable flush failure")
		assert.Contains(t, msg, syncer.ErrSyncerClosed.Error())
	}()
	_ = task.Execute(context.Background())
	t.Fatal("Execute must panic on ErrSyncerClosed")
}

func TestDirtyViewFlushSchedulerChunksOversizedSingleShardAtFlush(t *testing.T) {
	catalog := newMockCatalog()
	scheduler := newTestDirtyViewFlushScheduler(t, catalog, newMockSyncer(), 2)
	t.Cleanup(scheduler.Close)

	// A single shard accumulates more persists than maxTxnOps while earlier
	// flushes are in flight. Claim's budget only caps multi-shard batches, so
	// this event is claimed whole and chunked at flush time into transactions
	// of at most maxTxnOps, in version-ascending order.
	views := []*viewpb.QueryViewOfShard{
		buildTestViewWithVersion(1, 1, 1, 1),
		buildTestViewWithVersion(1, 1, 1, 2),
		buildTestViewWithVersion(1, 1, 1, 3),
		buildTestViewWithVersion(1, 1, 1, 4),
		buildTestViewWithVersion(1, 1, 1, 5),
	}
	scheduler.Submit(dirtyViewEvent{shardID: testShardID, persists: views})

	require.NoError(t, scheduler.Flush(context.Background()))

	catalog.mu.Lock()
	batches := catalog.saveCalls
	catalog.mu.Unlock()
	require.Len(t, batches, 3) // 2 + 2 + 1
	total := 0
	for i, b := range batches {
		require.LessOrEqual(t, len(b), 2, "batch %d exceeds maxTxnOps", i)
		total += len(b)
		versions := make([]int64, 0, len(b))
		for _, v := range b {
			versions = append(versions, v.GetMeta().GetVersion().GetQueryVersion())
		}
		require.True(t, sort.SliceIsSorted(versions, func(a, b int) bool { return versions[a] < versions[b] }),
			"batch %d is not version-ascending: %v", i, versions)
	}
	require.Equal(t, len(views), total)
}

func TestPersistViewsSortsVersionAscendingBeforeChunk(t *testing.T) {
	catalog := newMockCatalog()
	scheduler := newTestDirtyViewFlushScheduler(t, catalog, newMockSyncer(), 2)

	// Deliberately shuffled: the chunked persist must be deterministic and
	// version-ascending so any committed prefix is recovery-safe.
	views := []*viewpb.QueryViewOfShard{
		buildTestViewWithVersion(1, 1, 1, 5),
		buildTestViewWithVersion(1, 1, 1, 2),
		buildTestViewWithVersion(1, 1, 1, 4),
		buildTestViewWithVersion(1, 1, 1, 1),
		buildTestViewWithVersion(1, 1, 1, 3),
	}
	require.NoError(t, scheduler.persistViews(context.Background(), views))

	catalog.mu.Lock()
	batches := catalog.saveCalls
	catalog.mu.Unlock()
	require.Len(t, batches, 3)
	expected := [][]int64{{1, 2}, {3, 4}, {5}}
	for i, b := range batches {
		versions := make([]int64, 0, len(b))
		for _, v := range b {
			versions = append(versions, v.GetMeta().GetVersion().GetQueryVersion())
		}
		assert.Equal(t, expected[i], versions, "batch %d versions", i)
	}
}

func scoredFlushEvent(replicaID, version int64, priority int32, state qviews.QueryViewState) dirtyViewEvent {
	id := qviews.ShardID{ReplicaID: replicaID, VChannel: testVChannel}
	event := dirtyPersistEvent(id, version)
	meta := event.persists[0].Meta
	meta.State = viewpb.QueryViewState(state)
	if priority != 0 {
		meta.Control = &viewpb.QueryViewControl{PreparationPriority: priority}
	}
	return event
}

func TestFlushClaimsLatePreparingAheadOfRecoveryBacklog(t *testing.T) {
	const backlog = 10000
	catalog := newMockCatalog()
	workers := &capturedDirtyViewTaskScheduler{}
	s := newDirtyViewFlushScheduler(catalog, newMockSyncer(), 8, workers)
	t.Cleanup(s.Close)
	for id := int64(1); id <= backlog; id++ {
		s.Submit(scoredFlushEvent(id, 1, -3, qviews.QueryViewStatePreparing))
	}
	firstTask := workers.snapshot()[0]
	cold := qviews.ShardID{ReplicaID: backlog + 1, VChannel: testVChannel}
	manager := newShardViewManager(t.Context(), cold, s, nil, stubDataViewRefProvider{})
	require.NoError(t, manager.AddPreparing(t.Context(), testBuilderForShard(1, cold).SetPreparationPriority(12)))
	require.NoError(t, firstTask.Execute(t.Context()))
	require.Len(t, catalog.saved, 8)
	for _, view := range catalog.saved {
		if view.Meta.ReplicaId == cold.ReplicaID {
			require.EqualValues(t, 12, view.Meta.GetControl().GetPreparationPriority())
			return
		}
	}
	t.Fatal("late Preparing missing from the first persisted batch")
}

func TestFlushReordersMergedEffectsAndPreservesAge(t *testing.T) {
	s := newDirtyViewFlushScheduler(newMockCatalog(), newMockSyncer(), 1, &capturedDirtyViewTaskScheduler{})
	t.Cleanup(s.Close)
	submit := func(id, version int64, priority int32, state qviews.QueryViewState) {
		s.Submit(scoredFlushEvent(id, version, priority, state))
	}
	prepare, drop := qviews.QueryViewStatePreparing, qviews.QueryViewStateDropped
	batch := s.Begin()
	submit(1, 1, 12, prepare)
	submit(2, 1, -7, prepare)
	submit(3, 1, 0, prepare)
	submit(4, 1, 0, prepare)
	submit(5, 1, -5, prepare)
	submit(6, 1, 15, drop)
	batch.Commit()
	// Replacing A's high-scoring view removes its boost, even with an older
	// per-node sync pending. B's new view raises its score to C and D's tier.
	old := scoredFlushEvent(1, 1, 12, prepare).persists[0]
	s.Submit(dirtyViewEvent{shardID: qviews.NewShardIDFromQVMeta(old.Meta), syncs: []syncer.SyncView{
		{View: qviews.NewQueryViewAtQueryNode(old.Meta, old.QueryNode[0])},
	}})
	submit(1, 1, 12, drop)
	submit(1, 2, -3, prepare)
	submit(2, 2, 0, qviews.QueryViewStateUp)
	for _, id := range []int64{2, 3, 4, 6, 1, 5} {
		claimed := s.claim()
		require.Len(t, claimed, 1)
		require.Contains(t, claimed, qviews.ShardID{ReplicaID: id, VChannel: testVChannel})
	}
}

func TestFlushSyncOnlyPriorityAndDeadline(t *testing.T) {
	s := newDirtyViewFlushScheduler(newMockCatalog(), newMockSyncer(), 1, &capturedDirtyViewTaskScheduler{})
	t.Cleanup(s.Close)
	push := func(id int64, score int32, state qviews.QueryViewState, deadline time.Time) qviews.ShardID {
		event := scoredFlushEvent(id, 1, score, state)
		view := event.persists[0]
		event.persists = nil
		event.syncs = []syncer.SyncView{{View: qviews.NewQueryViewAtStreamingNode(view.Meta, view.StreamingNode)}}
		event.notBefore = deadline
		s.Submit(event)
		return event.shardID
	}
	ordinary := scoredFlushEvent(4, 1, 2, qviews.QueryViewStatePreparing)
	s.Submit(ordinary)
	ready := push(1, 8, qviews.QueryViewStateUp, time.Time{})
	high := push(2, 12, qviews.QueryViewStatePreparing, time.Time{})
	push(2, 12, qviews.QueryViewStatePreparing, time.Now().Add(time.Hour))
	require.Contains(t, s.claim(), ready)
	require.Contains(t, s.claim(), ordinary.shardID)
	require.Empty(t, s.claim())
	push(2, 12, qviews.QueryViewStateDown, time.Time{})
	low := push(3, -2, qviews.QueryViewStatePreparing, time.Time{})
	require.Contains(t, s.claim(), high)
	require.Contains(t, s.claim(), low)
}

func TestFlushFairnessAcrossBatchBoundaries(t *testing.T) {
	s := newDirtyViewFlushScheduler(newMockCatalog(), newMockSyncer(), 1, &capturedDirtyViewTaskScheduler{})
	t.Cleanup(s.Close)
	cleanup := scoredFlushEvent(1, 1, 20, qviews.QueryViewStateDropped)
	recovery := scoredFlushEvent(2, 1, -10, qviews.QueryViewStatePreparing)
	s.Submit(cleanup)
	s.Submit(recovery)
	for i := 1; i <= 2*(flushPriorityBurst+1); i++ {
		// Fresh positive-score work exists at every claim, including fairness turns.
		s.Submit(scoredFlushEvent(int64(2+i), 1, 12, qviews.QueryViewStatePreparing))
		claimed := s.claim()
		require.Len(t, claimed, 1)
		switch i {
		case flushPriorityBurst + 1:
			require.Contains(t, claimed, cleanup.shardID)
		case 2 * (flushPriorityBurst + 1):
			require.Contains(t, claimed, recovery.shardID)
		default:
			require.NotContains(t, claimed, cleanup.shardID)
			require.NotContains(t, claimed, recovery.shardID)
		}
	}
}

func TestFlushLeavesNonFittingHeadForNextBatch(t *testing.T) {
	s := newDirtyViewFlushScheduler(newMockCatalog(), newMockSyncer(), 2, &capturedDirtyViewTaskScheduler{})
	t.Cleanup(s.Close)
	a := scoredFlushEvent(1, 1, 12, qviews.QueryViewStatePreparing)
	b := scoredFlushEvent(2, 1, 8, qviews.QueryViewStatePreparing)
	c := scoredFlushEvent(3, 1, 1, qviews.QueryViewStatePreparing)
	for _, event := range []dirtyViewEvent{a, b, scoredFlushEvent(2, 2, 8, qviews.QueryViewStatePreparing), c} {
		s.Submit(event)
	}
	for _, event := range []dirtyViewEvent{a, b, c} {
		batch := s.claim()
		require.Len(t, batch, 1)
		require.Contains(t, batch, event.shardID)
	}
}
