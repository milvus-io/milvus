package broadcaster

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RecoverBroadcaster recovers the broadcaster from the recovery info.
func RecoverBroadcaster(ctx context.Context) (Broadcaster, error) {
	tasks, err := resource.Resource().StreamingCatalog().ListBroadcastTask(ctx)
	if err != nil {
		return nil, err
	}
	return newBroadcastTaskManager(tasks), nil
}

// newBroadcastTaskManager creates a new broadcast task manager with recovery info.
// return the manager, the pending broadcast tasks and the pending ack callback tasks.
func newBroadcastTaskManager(protos []*streamingpb.BroadcastTask) *broadcastTaskManager {
	logger := resource.Resource().Logger().With(mlog.FieldComponent("broadcaster"))
	metrics := newBroadcasterMetrics()
	rkLocker := newResourceKeyLocker()
	ackScheduler := newAckCallbackScheduler(logger)

	recoveryTasks := make([]*broadcastTask, 0, len(protos))
	for _, proto := range protos {
		t := newBroadcastTaskFromProto(proto, metrics, ackScheduler)
		t.SetLogger(logger)
		recoveryTasks = append(recoveryTasks, t)
	}
	tasks := make(map[uint64]*broadcastTask, len(recoveryTasks))
	pendingTasks := make([]*pendingBroadcastTask, 0, len(recoveryTasks))
	pendingAckCallbackTasks := make([]*broadcastTask, 0, len(recoveryTasks))
	tombstoneIDs := make([]uint64, 0, len(recoveryTasks))
	idxOfKeys := newIdempotencyIndex()
	for _, task := range recoveryTasks {
		switch task.task.State {
		case streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_WAIT_ACK:
			guards, err := rkLocker.FastLock(task.Header().ResourceKeys.Collect()...)
			if err != nil {
				panic(err)
			}
			task.WithResourceKeyLockGuards(guards)

			if newPending := newPendingBroadcastTask(task); newPending != nil {
				// if there's some pending messages that is not appended, it should be continued to be appended.
				pendingTasks = append(pendingTasks, newPending)
			} else {
				// if there's no pending messages, it should be added to the pending ack callback tasks
				// to call the ack callback function.
				pendingAckCallbackTasks = append(pendingAckCallbackTasks, task)
			}
		case streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED:
			// The task is recovered from the remote cluster, so it doesn't hold the resource lock.
			// but the task execution order should be protected by the order of broadcastID (by ackCallbackScheduler)
			if task.isControlChannelAcked() || isAllDone(task.task) {
				pendingAckCallbackTasks = append(pendingAckCallbackTasks, task)
			}
		case streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE:
			tombstoneIDs = append(tombstoneIDs, task.Header().BroadcastID)
		}
		tasks[task.Header().BroadcastID] = task
		// Rebuild the idempotency index across EVERY state, tombstones included:
		// a tombstoned task is exactly what a late retry must still hit.
		idxOfKeys.Add(task.IdempotencyScope(), task.Header().BroadcastID)
	}

	m := &broadcastTaskManager{
		lifetime:           typeutil.NewLifetime(),
		mu:                 &sync.Mutex{},
		tasks:              tasks,
		idempotencyIndex:   idxOfKeys,
		resourceKeyLocker:  rkLocker,
		metrics:            metrics,
		broadcastScheduler: newBroadcasterScheduler(pendingTasks, logger),
		ackScheduler:       ackScheduler,
	}

	// Set the broadcast task manager reference for accessing incomplete tasks.
	ackScheduler.bm = m

	// add the pending ack callback tasks into the ack scheduler.
	ackScheduler.Initialize(pendingAckCallbackTasks, tombstoneIDs, m)
	m.SetLogger(logger)
	return m
}

// broadcastTaskManager is the manager of the broadcast task.
type broadcastTaskManager struct {
	mlog.Binder

	lifetime           *typeutil.Lifetime
	mu                 *sync.Mutex
	tasks              map[uint64]*broadcastTask // map the broadcastID to the broadcastTaskState
	idempotencyIndex   *idempotencyIndex         // map the idempotency key to the broadcastID that owns it
	resourceKeyLocker  *resourceKeyLocker
	metrics            *broadcasterMetrics
	broadcastScheduler *broadcasterScheduler // the scheduler of the broadcast task
	ackScheduler       *ackCallbackScheduler // the scheduler of the ack task
}

// WithResourceKeys acquires the resource keys for the broadcast task.
func (bm *broadcastTaskManager) WithResourceKeys(ctx context.Context, resourceKeys ...message.ResourceKey) (BroadcastAPI, error) {
	startLockInstant := time.Now()
	resourceKeys = bm.appendSharedClusterRK(resourceKeys...)
	guards := bm.resourceKeyLocker.Lock(resourceKeys...)

	id, err := resource.Resource().IDAllocator().Allocate(ctx)
	if err != nil {
		guards.Unlock()
		return nil, merr.Wrapf(err, "allocate new id failed")
	}

	if err := bm.checkClusterRole(ctx); err != nil {
		// unlock the guards if the cluster role is not primary.
		guards.Unlock()
		return nil, err
	}
	bm.metrics.ObserveAcquireLockDuration(startLockInstant, guards.ResourceKeys())

	return &broadcasterWithRK{
		broadcaster: bm,
		broadcastID: id,
		guards:      guards,
	}, nil
}

// WithSecondaryClusterResourceKey acquires an exclusive cluster-level resource key
// and verifies the cluster is secondary. Returns error if the cluster is primary.
// This is used for force promote operations that should only be executed on secondary clusters.
func (bm *broadcastTaskManager) WithSecondaryClusterResourceKey(ctx context.Context) (BroadcastAPI, error) {
	id, err := resource.Resource().IDAllocator().Allocate(ctx)
	if err != nil {
		return nil, merr.Wrapf(err, "allocate new id failed")
	}

	startLockInstant := time.Now()
	// Acquire an exclusive cluster resource key to block all other broadcasts
	resourceKeys := []message.ResourceKey{message.NewExclusiveClusterResourceKey()}
	guards := bm.resourceKeyLocker.Lock(resourceKeys...)

	// Check if the cluster is secondary
	if err := bm.checkClusterRoleSecondary(ctx); err != nil {
		// unlock the guards if the cluster role is not secondary.
		guards.Unlock()
		return nil, err
	}
	bm.metrics.ObserveAcquireLockDuration(startLockInstant, guards.ResourceKeys())

	return &broadcasterWithRK{
		broadcaster: bm,
		broadcastID: id,
		guards:      guards,
	}, nil
}

// checkClusterRole checks if the cluster status is primary, otherwise return error.
func (bm *broadcastTaskManager) checkClusterRole(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Check if the cluster status is primary, otherwise return error.
	b, err := balance.GetWithContext(ctx)
	if err != nil {
		return err
	}
	if b.ReplicateRole() != replicateutil.RolePrimary {
		// a non-primary cluster cannot do any broadcast operation.
		return ErrNotPrimary
	}
	return nil
}

// checkClusterRoleSecondary checks if the cluster status is secondary, otherwise return error.
// This is used for force promote operations that should only be executed on secondary clusters.
func (bm *broadcastTaskManager) checkClusterRoleSecondary(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Check if the cluster status is secondary, otherwise return error.
	b, err := balance.GetWithContext(ctx)
	if err != nil {
		return err
	}
	if b.ReplicateRole() != replicateutil.RoleSecondary {
		// Force promote can only be performed on a secondary cluster.
		return ErrNotSecondary
	}
	return nil
}

// appendSharedClusterRK appends the shared cluster resource key to the resource keys.
// shared cluster resource key is required for all broadcast messages.
func (bm *broadcastTaskManager) appendSharedClusterRK(resourceKeys ...message.ResourceKey) []message.ResourceKey {
	for _, rk := range resourceKeys {
		if rk.Domain == messagespb.ResourceDomain_ResourceDomainCluster {
			return resourceKeys
		}
	}
	return append(resourceKeys, message.NewSharedClusterResourceKey())
}

// broadcast broadcasts the message to all vchannels, unless its idempotency scope
// is already owned by an earlier broadcast.
//
// dup is non-nil exactly when nothing was registered and nothing will reach the
// WAL; the caller still holds its lock guards in that case.
//
// guardsConsumed says the caller no longer holds its lock guards, either because
// the registered task took them over or because this function released them. It is
// reported separately from err because the two are independent: once the task is
// registered it keeps broadcasting in the background and releases the guards from
// its ack callback, even if the wait below fails, so the caller must stop releasing
// them itself. Deriving that from the error at the caller would put the answer at
// every error site rather than at the one place that knows.
//
// The two ways to consume them are reported as one fact on purpose: what the caller
// has to decide is only whether to keep releasing them, and answering that with
// "the task owns them" alone would leave the shutdown path relying on Unlock being
// harmless to call twice. It is, on one goroutine -- lockGuards.Unlock empties its
// own slice -- but that is not a property to build ownership on, since nothing
// synchronizes it against the ack callback's release on another goroutine.
func (bm *broadcastTaskManager) broadcast(
	ctx context.Context,
	msg message.BroadcastMutableMessage,
	broadcastID uint64,
	guards *lockGuards,
) (result *types.BroadcastAppendResult, dup *broadcastTask, guardsConsumed bool, err error) {
	if !bm.lifetime.Add(typeutil.LifetimeStateWorking) {
		// Released here rather than left to the caller, and reported as consumed: the
		// caller's deferred Close() must not reach these guards again.
		guards.Unlock()
		return nil, nil, true, errors.Mark(status.NewOnShutdownError("broadcaster is closing"), ErrBroadcastTaskNotCreated)
	}
	defer bm.lifetime.Done()

	// Validation is now done before calling broadcast (in DataCoord for import operations)
	// CheckCallback mechanism has been removed as part of the import refactoring

	dup, task := bm.getOrAddBroadcastTask(ctx, msg, broadcastID, guards)
	if dup != nil {
		return nil, dup, false, nil
	}
	pendingTask := newPendingBroadcastTask(task)

	// Add it into broadcast scheduler to broadcast the message into all vchannels.
	result, err = bm.broadcastScheduler.AddTask(ctx, pendingTask)
	return result, nil, true, err
}

// LegacyAck is the legacy ack function for the broadcast task.
// It will not be used after upgrading to 2.6.1, only used for compatibility.
func (bm *broadcastTaskManager) LegacyAck(ctx context.Context, broadcastID uint64, vchannel string) error {
	task, ok := bm.getBroadcastTaskByID(broadcastID)
	if !ok {
		bm.Logger().Warn(ctx,
			"broadcast task not found, it may already acked, ignore the request", mlog.Uint64("broadcastID", broadcastID), mlog.String("vchannel", vchannel))
		return nil
	}
	msg := task.GetImmutableMessageFromVChannel(vchannel)
	if msg == nil {
		task.Logger().Warn(ctx, "vchannel is already acked, ignore the ack request", mlog.String("vchannel", vchannel))
		return nil
	}
	return bm.Ack(ctx, msg)
}

// Ack acknowledges the message at the specified vchannel.
func (bm *broadcastTaskManager) Ack(ctx context.Context, msg message.ImmutableMessage) error {
	if !bm.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("broadcaster is closing")
	}
	defer bm.lifetime.Done()

	t, ok := bm.getOrCreateBroadcastTask(msg)
	if !ok {
		bm.Logger().Debug(ctx,
			"task is tombstone, ignored the ack request",
			mlog.Uint64("broadcastID", msg.BroadcastHeader().BroadcastID),
			mlog.String("vchannel", msg.VChannel()))
		return nil
	}
	return t.Ack(ctx, msg)
}

// DropTombstone drops the tombstone task from the manager.
func (bm *broadcastTaskManager) DropTombstone(ctx context.Context, broadcastID uint64) error {
	if !bm.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("broadcaster is closing")
	}
	defer bm.lifetime.Done()

	t, ok := bm.getBroadcastTaskByID(broadcastID)
	if !ok {
		bm.Logger().Debug(ctx, "task is not found, ignored the drop tombstone request", mlog.Uint64("broadcastID", broadcastID))
		return nil
	}
	if err := t.DropTombstone(ctx); err != nil {
		return err
	}
	bm.removeBroadcastTask(broadcastID)
	return nil
}

// Close closes the broadcast task manager.
func (bm *broadcastTaskManager) Close() {
	bm.lifetime.SetState(typeutil.LifetimeStateStopped)
	bm.lifetime.Wait()

	bm.broadcastScheduler.Close()
	bm.ackScheduler.Close()
}

// getOrAddBroadcastTask resolves the idempotency scope and registers the task in
// ONE critical section.
//
// The lookup and the insert used to be two critical sections on bm.mu, and only
// the caller's resource lock kept two same-key requests out of the gap between
// them. That works only when the lock is exclusive on the very object the scope
// names. It is not: import scopes by collection ID (so a retry still dedups after
// a rename) but locks by collection name, and RenameCollection takes DB-level keys
// only. A rename between name resolution and lock acquisition therefore leaves two
// same-key requests holding non-conflicting collection-name locks -- both miss,
// both register, both reach the WAL, and idempotencyIndex.Add silently keeps the
// first. Deciding under bm.mu retires that caller obligation instead of restating
// it in a comment.
//
// Returns the owning task on a hit, having registered nothing; the caller keeps
// its lock guards. Returns the newly registered task on a miss, which now owns
// the guards.
func (bm *broadcastTaskManager) getOrAddBroadcastTask(
	ctx context.Context,
	msg message.BroadcastMutableMessage,
	broadcastID uint64,
	guards *lockGuards,
) (dup *broadcastTask, created *broadcastTask) {
	// The scope comes from the message alone, so the hit path and the miss path
	// cannot drift apart. It touches no shared state, so it is derived outside the
	// lock.
	scope := idempotencyScopeOfMessage(msg)

	bm.mu.Lock()
	defer bm.mu.Unlock()

	if ownerID, ok := bm.idempotencyIndex.Get(scope); ok {
		if t, ok := bm.tasks[ownerID]; ok {
			return t, nil
		}
		// tasks and idempotencyIndex are written together under this lock and
		// removeBroadcastTask drops the index entry first, so a scope whose owner
		// has no task is unreachable. Say so rather than trusting the entry, and
		// fall through to registration.
		bm.Logger().Warn(ctx, "idempotency index entry has no task, treating it as a miss",
			mlog.Uint64("ownerBroadcastID", ownerID))
	}
	// Constructed only once the broadcast is certain to be registered: construction
	// counts the task into the PENDING gauge, and the hit path above transitions no
	// state, so an earlier construction would leak that count for the lifetime of the
	// process. It builds no shared state, but it is cheap enough to hold bm.mu across
	// at DDL frequency.
	newIncomingTask := newBroadcastTaskFromBroadcastMessage(msg, bm.metrics, bm.ackScheduler)
	newIncomingTask.SetLogger(bm.Logger())
	newIncomingTask.WithResourceKeyLockGuards(guards)
	bm.tasks[broadcastID] = newIncomingTask
	bm.idempotencyIndex.Add(scope, broadcastID)
	return nil, newIncomingTask
}

// getOrCreateBroadcastTask returns the task by the broadcastID
// return false if the task is tombstone.
// if the task is not found, it will create a new task.
func (bm *broadcastTaskManager) getOrCreateBroadcastTask(msg message.ImmutableMessage) (*broadcastTask, bool) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	bh := msg.BroadcastHeader()
	t, ok := bm.tasks[msg.BroadcastHeader().BroadcastID]
	if ok {
		return t, t.State() != streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
	}
	if msg.ReplicateHeader() == nil {
		bm.Logger().Warn(context.TODO(), "try to recover task from the wal from non-replicate message, ignore it")
		return nil, false
	}

	newBroadcastTask := newBroadcastTaskFromImmutableMessage(msg, bm.metrics, bm.ackScheduler)
	newBroadcastTask.SetLogger(bm.Logger())
	bm.tasks[bh.BroadcastID] = newBroadcastTask
	bm.idempotencyIndex.Add(newBroadcastTask.IdempotencyScope(), bh.BroadcastID)
	return newBroadcastTask, true
}

// getBroadcastTaskByID return the task by the broadcastID.
func (bm *broadcastTaskManager) getBroadcastTaskByID(broadcastID uint64) (*broadcastTask, bool) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	t, ok := bm.tasks[broadcastID]
	return t, ok
}

// removeBroadcastTask removes the broadcast task by the broadcastID.
func (bm *broadcastTaskManager) removeBroadcastTask(broadcastID uint64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if t, ok := bm.tasks[broadcastID]; ok {
		bm.idempotencyIndex.Remove(t.IdempotencyScope(), broadcastID)
	}
	delete(bm.tasks, broadcastID)
}

// getIncompleteBroadcastTasks returns all incomplete broadcast tasks that have pending messages.
// Tasks in PENDING or REPLICATED state with pending messages are considered incomplete.
func (bm *broadcastTaskManager) getIncompleteBroadcastTasks() []*broadcastTask {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	var result []*broadcastTask
	for _, task := range bm.tasks {
		state := task.State()
		if state != streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING &&
			state != streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED {
			continue
		}
		msgs := task.PendingBroadcastMessages()
		if len(msgs) == 0 {
			continue
		}
		result = append(result, task)
	}
	return result
}

// GetPendingSchemaFileResources returns collection ID -> file resource IDs
// for all non-tombstone schema broadcast tasks. Used during recovery to rebuild
// file resource refCnt for resources referenced by pending schema changes.
func (bm *broadcastTaskManager) GetPendingSchemaFileResources() map[int64][]int64 {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	result := make(map[int64][]int64)
	for _, task := range bm.tasks {
		if task.State() == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE {
			continue
		}
		switch task.msg.MessageTypeWithVersion() {
		case message.MessageTypeCreateCollectionV1:
			createMsg, err := message.AsMutableCreateCollectionMessageV1(task.msg)
			if err != nil {
				continue
			}
			body := createMsg.MustBody()
			ids := body.CollectionSchema.GetFileResourceIds()
			appendPendingFileResourceIDs(result, createMsg.Header().CollectionId, ids)
		case message.MessageTypeAlterCollectionV2:
			alterMsg, err := message.AsMutableAlterCollectionMessageV2(task.msg)
			if err != nil {
				continue
			}
			schema := alterMsg.MustBody().GetUpdates().GetSchema()
			ids := schema.GetFileResourceIds()
			appendPendingFileResourceIDs(result, alterMsg.Header().CollectionId, ids)
		default:
			continue
		}
	}
	return result
}

func appendPendingFileResourceIDs(result map[int64][]int64, collectionID int64, ids []int64) {
	if len(ids) == 0 {
		return
	}
	seen := make(map[int64]struct{}, len(result[collectionID])+len(ids))
	for _, id := range result[collectionID] {
		seen[id] = struct{}{}
	}
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		result[collectionID] = append(result[collectionID], id)
		seen[id] = struct{}{}
	}
}
