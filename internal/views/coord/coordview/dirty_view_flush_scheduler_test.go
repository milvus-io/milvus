package coordview

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	qvobserve "github.com/milvus-io/milvus/internal/views/qviews/observe"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type capturedDirtyViewTaskScheduler struct {
	mu    sync.Mutex
	tasks []nodescheduler.Task
}

func (s *capturedDirtyViewTaskScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
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

type recordedQueryViewEvents struct {
	mu     sync.Mutex
	events []qvobserve.Event
}

func (r *recordedQueryViewEvents) Observe(_ context.Context, event qvobserve.Event) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *recordedQueryViewEvents) ioEvents(shardID qviews.ShardID) (int, []qvobserve.CoordSyncViewAcceptedEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	persisted := 0
	accepted := make([]qvobserve.CoordSyncViewAcceptedEvent, 0)
	for _, event := range r.events {
		switch event := event.(type) {
		case qvobserve.CoordPersistViewEvent:
			if event.View.ShardID == shardID {
				persisted++
			}
		case qvobserve.CoordSyncViewAcceptedEvent:
			if event.View.ShardID == shardID {
				accepted = append(accepted, event)
			}
		}
	}
	return persisted, accepted
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

func TestDirtyViewFlushSchedulerObservesCompletedIO(t *testing.T) {
	catalog := &blockingFlushCatalog{
		mockCatalog: newMockCatalog(),
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	s := &blockingFlushSyncer{
		mockSyncer: newMockSyncer(),
		started:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	scheduler := newTestDirtyViewFlushScheduler(t, catalog, s, 128)
	recorder := &recordedQueryViewEvents{}
	qvobserve.Register(recorder)

	shardID := qviews.ShardID{ReplicaID: 10001, VChannel: "io-event-boundary"}
	event := dirtyPersistEvent(shardID, 1)
	view := event.persists[0]
	event.syncs = []syncer.SyncView{
		{View: qviews.NewFullQueryViewAtStreamingNode(view.Meta, view.StreamingNode, view.QueryNode)},
		{View: qviews.NewQueryViewAtQueryNode(view.Meta, view.QueryNode[0])},
	}
	scheduler.Submit(event)

	select {
	case <-catalog.started:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog persist did not start")
	}
	persisted, accepted := recorder.ioEvents(shardID)
	assert.Zero(t, persisted)
	assert.Empty(t, accepted)

	close(catalog.release)
	select {
	case <-s.started:
	case <-time.After(5 * time.Second):
		t.Fatal("sync enqueue did not start")
	}
	persisted, accepted = recorder.ioEvents(shardID)
	assert.Equal(t, 1, persisted)
	assert.Empty(t, accepted)

	close(s.release)
	require.NoError(t, scheduler.Flush(context.Background()))
	persisted, accepted = recorder.ioEvents(shardID)
	assert.Equal(t, 1, persisted)
	require.Len(t, accepted, 2)
	assert.Equal(t, 1, s.numSyncCalls())
	nodes := map[qviews.WorkNodeKey]struct{}{}
	for _, event := range accepted {
		nodes[event.Node.Key()] = struct{}{}
	}
	assert.Contains(t, nodes, event.syncs[0].View.WorkNode().Key())
	assert.Contains(t, nodes, event.syncs[1].View.WorkNode().Key())
}

func TestShardViewManagerSubmitsOneShardScopedDirtyEvent(t *testing.T) {
	submitter := &capturedDirtyViewEventSubmitter{}
	manager := newShardViewManager(context.Background(), testShardID, submitter, nil)

	require.NoError(t, manager.AddPreparing(context.Background(), testBuilder(1, 1, 1)))
	events := submitter.snapshot()
	require.Len(t, events, 1)
	assert.Equal(t, testShardID, events[0].shardID)
	assert.Len(t, events[0].persists, 1)
	assert.Len(t, events[0].syncs, 2)
}
