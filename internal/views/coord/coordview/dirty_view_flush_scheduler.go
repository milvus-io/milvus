package coordview

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	qvobserve "github.com/milvus-io/milvus/internal/views/qviews/observe"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// dirtyViewEvent is one immutable shard-scoped set of external effects emitted
// by ShardViewManager after an in-memory state transition.
type dirtyViewEvent struct {
	shardID      qviews.ShardID
	persists     []*viewpb.QueryViewOfShard
	syncs        []syncer.SyncView
	afterPersist []func()
}

type dirtyViewEventSubmitter interface {
	// Submit must only enqueue the event and must not synchronously execute its
	// external effects or callbacks.
	Submit(dirtyViewEvent)
}

func (e dirtyViewEvent) empty() bool {
	return len(e.persists) == 0 && len(e.syncs) == 0 && len(e.afterPersist) == 0
}

type pendingDirtyViewEvent struct {
	persists     map[qviews.QueryViewKey]*viewpb.QueryViewOfShard
	syncs        map[dirtyViewSyncKey]syncer.SyncView
	afterPersist []func()
}

type dirtyViewSyncKey struct {
	view qviews.QueryViewKey
	node qviews.WorkNodeKey
}

func newPendingDirtyViewEvent() *pendingDirtyViewEvent {
	return &pendingDirtyViewEvent{
		persists: make(map[qviews.QueryViewKey]*viewpb.QueryViewOfShard),
		syncs:    make(map[dirtyViewSyncKey]syncer.SyncView),
	}
}

func (e *pendingDirtyViewEvent) merge(event dirtyViewEvent) {
	for _, view := range event.persists {
		e.persists[queryViewKeyFromProto(view)] = view
	}
	for _, view := range event.syncs {
		key := dirtyViewSyncKey{
			view: view.View.QueryViewKey(),
			node: view.View.WorkNode().Key(),
		}
		e.syncs[key] = view
	}
	e.afterPersist = append(e.afterPersist, event.afterPersist...)
}

func (e *pendingDirtyViewEvent) operationCount() int {
	return max(1, len(e.persists))
}

func queryViewKeyFromProto(view *viewpb.QueryViewOfShard) qviews.QueryViewKey {
	meta := view.GetMeta()
	return qviews.QueryViewKey{
		ShardID:          qviews.NewShardIDFromQVMeta(meta),
		QueryViewVersion: qviews.FromProtoQueryViewVersion(meta.GetVersion()),
	}
}

// DirtyViewFlushScheduler batches shard-scoped QueryView effects and submits
// disjoint batches to the node-level NodeScheduler. Effects within one ShardID
// lane are serialized; different lanes may execute concurrently.
type DirtyViewFlushScheduler struct {
	catalog       queryview.QueryViewCatalog
	syncer        syncer.ReliableSyncer
	maxTxnOps     int
	nodeScheduler nodescheduler.Scheduler

	mu          sync.Mutex
	pending     map[qviews.ShardID]*pendingDirtyViewEvent
	ready       map[qviews.ShardID]struct{}
	readyOps    int
	inflight    map[qviews.ShardID]struct{}
	held        map[qviews.ShardID]struct{}
	batchDepth  int
	queuedTasks int
	tasks       map[*dirtyViewFlushTask]nodescheduler.TaskHandle
	closed      bool
	notify      chan struct{}
}

type dirtyViewBatchCommit struct {
	scheduler *DirtyViewFlushScheduler
	once      sync.Once
}

type DirtyViewBatch interface {
	Commit()
}

func (c *dirtyViewBatchCommit) Commit() {
	if c == nil || c.scheduler == nil {
		return
	}
	c.once.Do(c.scheduler.commit)
}

type dirtyViewFlushTask struct {
	scheduler *DirtyViewFlushScheduler
}

func newDirtyViewFlushScheduler(
	catalog queryview.QueryViewCatalog,
	s syncer.ReliableSyncer,
	maxTxnOps int,
	nodeScheduler nodescheduler.Scheduler,
) *DirtyViewFlushScheduler {
	if maxTxnOps <= 0 {
		maxTxnOps = 1
	}
	return &DirtyViewFlushScheduler{
		catalog:       catalog,
		syncer:        s,
		maxTxnOps:     maxTxnOps,
		nodeScheduler: nodeScheduler,
		pending:       make(map[qviews.ShardID]*pendingDirtyViewEvent),
		ready:         make(map[qviews.ShardID]struct{}),
		inflight:      make(map[qviews.ShardID]struct{}),
		held:          make(map[qviews.ShardID]struct{}),
		tasks:         make(map[*dirtyViewFlushTask]nodescheduler.TaskHandle),
		notify:        make(chan struct{}, 1),
	}
}

// Begin opens an explicit batching window. Existing tasks continue running;
// events submitted while any window is open are held until the outermost
// Commit.
func (s *DirtyViewFlushScheduler) Begin() DirtyViewBatch {
	commit := &dirtyViewBatchCommit{scheduler: s}
	if s == nil {
		return commit
	}
	s.mu.Lock()
	if !s.closed {
		s.batchDepth++
	}
	s.mu.Unlock()
	return commit
}

func (s *DirtyViewFlushScheduler) commit() {
	s.mu.Lock()
	if s.batchDepth > 0 {
		s.batchDepth--
		if s.batchDepth == 0 {
			held := s.held
			s.held = make(map[qviews.ShardID]struct{})
			for shardID := range held {
				s.markReadyLocked(shardID)
			}
			s.scheduleReadyTasksLocked()
		}
	}
	s.signalLocked()
	s.mu.Unlock()
}

// Submit merges one shard-scoped event and opportunistically schedules enough
// batch tasks to process all currently ready lanes.
func (s *DirtyViewFlushScheduler) Submit(event dirtyViewEvent) {
	if s == nil || event.empty() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	pending := s.pending[event.shardID]
	if pending == nil {
		pending = newPendingDirtyViewEvent()
		s.pending[event.shardID] = pending
	}
	_, wasReady := s.ready[event.shardID]
	if wasReady {
		s.readyOps -= pending.operationCount()
	}
	pending.merge(event)
	if wasReady {
		s.readyOps += pending.operationCount()
	}
	if s.batchDepth > 0 {
		s.removeReadyLocked(event.shardID)
		s.held[event.shardID] = struct{}{}
	} else {
		s.markReadyLocked(event.shardID)
		s.scheduleReadyTasksLocked()
	}
	s.signalLocked()
}

func (s *DirtyViewFlushScheduler) scheduleReadyTasksLocked() {
	if s.closed || s.batchDepth > 0 {
		return
	}
	if len(s.ready) == 0 {
		return
	}
	desiredTasks := min(len(s.ready), (s.readyOps+s.maxTxnOps-1)/s.maxTxnOps)
	for s.queuedTasks < desiredTasks {
		task := &dirtyViewFlushTask{scheduler: s}
		s.queuedTasks++
		handle := s.nodeScheduler.Submit(task)
		s.tasks[task] = handle
	}
}

func (s *DirtyViewFlushScheduler) claim() map[qviews.ShardID]*pendingDirtyViewEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queuedTasks > 0 {
		s.queuedTasks--
	}
	claimed := make(map[qviews.ShardID]*pendingDirtyViewEvent)
	if s.closed {
		return claimed
	}
	usedOps := 0
	for shardID := range s.ready {
		event := s.pending[shardID]
		ops := event.operationCount()
		if len(claimed) > 0 && usedOps+ops > s.maxTxnOps {
			continue
		}
		claimed[shardID] = event
		s.removeReadyLocked(shardID)
		delete(s.pending, shardID)
		s.inflight[shardID] = struct{}{}
		usedOps += ops
		if usedOps >= s.maxTxnOps {
			break
		}
	}
	s.scheduleReadyTasksLocked()
	s.signalLocked()
	return claimed
}

func (s *DirtyViewFlushScheduler) complete(
	task *dirtyViewFlushTask,
	claimed map[qviews.ShardID]*pendingDirtyViewEvent,
	err error,
) {
	s.mu.Lock()
	delete(s.tasks, task)
	for shardID := range claimed {
		delete(s.inflight, shardID)
		if _, held := s.held[shardID]; !held {
			s.markReadyLocked(shardID)
		}
	}
	// A failed batch must not reschedule more work: on shutdown the scheduler is
	// closing anyway, and on a real failure Execute panics (unrecoverable path)
	// right after this returns, so there is nothing left to schedule.
	if err == nil {
		s.scheduleReadyTasksLocked()
	}
	s.signalLocked()
	s.mu.Unlock()
}

// Execute flushes one claimed batch and enforces the scheduler's failure
// contract.
//
// A flush failure is deliberately unrecoverable: the batch's external effects
// (persist, syncs and afterPersist callbacks) have been destructively consumed,
// and the in-memory shard state may have advanced past the catalog. Replaying
// the batch is unsafe (afterPersist callbacks are not idempotent, and an
// undetermined write may or may not have committed), while silently dropping it
// would leave the coordinator's views diverged from the catalog with no signal
// to the operator. The only safe recovery is a coordinator restart, which
// rebuilds every shard from the catalog via RecoverShardViewRegistry — so any
// non-shutdown failure panics to fail fast and loudly instead of continuing
// with a poisoned scheduler.
//
// Shutdown is the single exception: a flush task may only stop gracefully
// through its own context being canceled (the coordinator is stopping and the
// error is expected). Everything else — an invariant violation, a failure of a
// conditional write, or ErrSyncerClosed (which means the syncer closed while a
// flush task was still executing, i.e. a shutdown-ordering violation) — panics.
// Undetermined write results from the catalog's predicate-free Set persist are
// retried inside ReliableWriteMetaKv and do not escape here.
func (t *dirtyViewFlushTask) Execute(ctx context.Context) error {
	claimed := t.scheduler.claim()
	err := t.scheduler.flushBatch(ctx, claimed)
	t.scheduler.complete(t, claimed, err)
	if err != nil && !isShutdownError(ctx, err) {
		panic(fmt.Sprintf(
			"dirty view flush scheduler: unrecoverable flush failure (shards %s): %v",
			claimedShardList(claimed), err,
		))
	}
	return err
}

// isShutdownError reports whether err is the cancellation signal of the task
// context, the only expected failure while the coordinator is stopping. Any
// other error is an unrecoverable flush failure.
func isShutdownError(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// claimedShardList returns the sorted shard IDs of a claimed batch for error
// reporting.
func claimedShardList(claimed map[qviews.ShardID]*pendingDirtyViewEvent) []string {
	ids := make([]string, 0, len(claimed))
	for shardID := range claimed {
		ids = append(ids, shardID.String())
	}
	sort.Strings(ids)
	return ids
}

func (s *DirtyViewFlushScheduler) flushBatch(
	ctx context.Context,
	batch map[qviews.ShardID]*pendingDirtyViewEvent,
) error {
	if len(batch) == 0 {
		return nil
	}
	persists := make([]*viewpb.QueryViewOfShard, 0)
	viewsByNode := make(map[qviews.WorkNodeKey][]syncer.SyncView)
	afterPersist := make([]func(), 0)
	for _, event := range batch {
		for _, view := range event.persists {
			persists = append(persists, view)
		}
		for _, view := range event.syncs {
			nodeKey := view.View.WorkNode().Key()
			viewsByNode[nodeKey] = append(viewsByNode[nodeKey], view)
		}
		afterPersist = append(afterPersist, event.afterPersist...)
	}
	if len(persists) > 0 {
		if err := s.persistViews(ctx, persists); err != nil {
			return err
		}
		for _, view := range persists {
			qvobserve.Observe(ctx, qvobserve.CoordPersistViewEvent{
				View:  queryViewKeyFromProto(view),
				State: qviews.QueryViewState(view.GetMeta().GetState()),
			})
		}
	}
	for _, callback := range afterPersist {
		callback()
	}
	if len(viewsByNode) > 0 {
		if err := s.syncer.SyncViews(ctx, syncer.SyncGroup{ViewsByNode: viewsByNode}); err != nil {
			return err
		}
	}
	for _, views := range viewsByNode {
		for _, view := range views {
			qvobserve.Observe(ctx, qvobserve.CoordSyncViewAcceptedEvent{
				View:  view.View.QueryViewKey(),
				Node:  view.View.WorkNode(),
				State: view.View.State(),
			})
		}
	}
	return nil
}

// persistViews writes all views of one flush batch to the catalog in one or
// more transactions of at most maxTxnOps.
//
// A batch is capped at maxTxnOps by claim()'s budget as long as it spans more
// than one shard, so this only ever chunks when the batch is a single shard's
// event that grew past the limit while earlier flushes were in flight (the
// first shard is not subject to the claim budget). In that case the persist
// list is sorted version-ascending within each shard and split into
// transactions of at most maxTxnOps.
//
// The ascending order is what keeps a partially committed prefix recoverable:
// a prefix that committed leaves "older view state changed, newer view state
// absent" on the catalog, so recovery can never observe two active views for
// one shard (a regression and a promotion both durable). A flush failure is
// unrecoverable by design (see dirtyViewFlushTask.Execute), so the partial
// prefix is repaired by the coordinator restart that follows.
func (s *DirtyViewFlushScheduler) persistViews(ctx context.Context, persists []*viewpb.QueryViewOfShard) error {
	if len(persists) <= s.maxTxnOps {
		return s.catalog.SaveQueryViews(ctx, persists)
	}
	// The persist list is a local slice collected by flushBatch and consumed
	// only here, so it is sorted in place.
	sort.Slice(persists, func(i, j int) bool {
		ki, kj := queryViewKeyFromProto(persists[i]), queryViewKeyFromProto(persists[j])
		if ki.ShardID != kj.ShardID {
			return ki.ShardID.String() < kj.ShardID.String()
		}
		return kj.QueryViewVersion.GT(ki.QueryViewVersion) // ascending
	})
	for start := 0; start < len(persists); start += s.maxTxnOps {
		end := min(start+s.maxTxnOps, len(persists))
		if err := s.catalog.SaveQueryViews(ctx, persists[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// Flush waits until all committed events have been processed. Callers must not
// leave an explicit Begin window open while waiting.
func (s *DirtyViewFlushScheduler) Flush(ctx context.Context) error {
	if s == nil {
		return nil
	}
	for {
		s.mu.Lock()
		idle := len(s.pending) == 0 && len(s.inflight) == 0 && len(s.tasks) == 0 && s.queuedTasks == 0
		notify := s.notify
		s.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-notify:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *DirtyViewFlushScheduler) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	clear(s.pending)
	clear(s.ready)
	s.readyOps = 0
	clear(s.held)
	handles := make([]nodescheduler.TaskHandle, 0, len(s.tasks))
	for _, handle := range s.tasks {
		handles = append(handles, handle)
	}
	s.signalLocked()
	s.mu.Unlock()

	for _, handle := range handles {
		handle.Cancel()
	}
	for _, handle := range handles {
		_ = handle.Wait(context.Background())
	}
}

func (s *DirtyViewFlushScheduler) signalLocked() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *DirtyViewFlushScheduler) markReadyLocked(shardID qviews.ShardID) {
	if _, ok := s.ready[shardID]; ok {
		return
	}
	if _, ok := s.inflight[shardID]; ok {
		return
	}
	if _, ok := s.held[shardID]; ok {
		return
	}
	event := s.pending[shardID]
	if event == nil {
		return
	}
	s.ready[shardID] = struct{}{}
	s.readyOps += event.operationCount()
}

func (s *DirtyViewFlushScheduler) removeReadyLocked(shardID qviews.ShardID) {
	if _, ok := s.ready[shardID]; !ok {
		return
	}
	delete(s.ready, shardID)
	if event := s.pending[shardID]; event != nil {
		s.readyOps -= event.operationCount()
	}
}

var _ nodescheduler.Task = (*dirtyViewFlushTask)(nil)
