package broadcaster

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	logger := resource.Resource().Logger().With(log.FieldComponent("broadcaster"))
	metrics := newBroadcasterMetrics()
	rkLocker := newResourceKeyLocker(metrics)
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
	}

	m := &broadcastTaskManager{
		lifetime:           typeutil.NewLifetime(),
		mu:                 &sync.Mutex{},
		tasks:              tasks,
		resourceKeyLocker:  rkLocker,
		metrics:            metrics,
		broadcastScheduler: newBroadcasterScheduler(pendingTasks, logger),
		ackScheduler:       ackScheduler,
	}

	// add the pending ack callback tasks into the ack scheduler.
	ackScheduler.Initialize(pendingAckCallbackTasks, tombstoneIDs, m)
	m.SetLogger(logger)
	return m
}

// broadcastTaskManager is the manager of the broadcast task.
type broadcastTaskManager struct {
	log.Binder

	lifetime           *typeutil.Lifetime
	mu                 *sync.Mutex
	tasks              map[uint64]*broadcastTask // map the broadcastID to the broadcastTaskState
	resourceKeyLocker  *resourceKeyLocker
	metrics            *broadcasterMetrics
	broadcastScheduler *broadcasterScheduler // the scheduler of the broadcast task
	ackScheduler       *ackCallbackScheduler // the scheduler of the ack task
}

// WithResourceKeys acquires the resource keys for the broadcast task.
func (bm *broadcastTaskManager) WithResourceKeys(ctx context.Context, resourceKeys ...message.ResourceKey) (BroadcastAPI, error) {
	id, err := resource.Resource().IDAllocator().Allocate(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "allocate new id failed")
	}

	startLockInstant := time.Now()
	resourceKeys = bm.appendSharedClusterRK(resourceKeys...)
	guards := bm.resourceKeyLocker.Lock(resourceKeys...)

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

// broadcast broadcasts the message to all vchannels.
// it will block until the message is broadcasted to all vchannels
func (bm *broadcastTaskManager) broadcast(ctx context.Context, msg message.BroadcastMutableMessage, broadcastID uint64, guards *lockGuards) (*types.BroadcastAppendResult, error) {
	if !bm.lifetime.Add(typeutil.LifetimeStateWorking) {
		guards.Unlock()
		return nil, status.NewOnShutdownError("broadcaster is closing")
	}
	defer bm.lifetime.Done()

	// check if the message is valid to be broadcasted.
	// TODO: the message check callback should not be an component of broadcaster,
	// it should be removed after the import operation refactory.
	if err := registry.CallMessageCheckCallback(ctx, msg); err != nil {
		guards.Unlock()
		return nil, err
	}

	task := bm.addBroadcastTask(msg, broadcastID, guards)
	pendingTask := newPendingBroadcastTask(task)

	// Add it into broadcast scheduler to broadcast the message into all vchannels.
	return bm.broadcastScheduler.AddTask(ctx, pendingTask)
}

// LegacyAck is the legacy ack function for the broadcast task.
// It will not be used after upgrading to 2.6.1, only used for compatibility.
func (bm *broadcastTaskManager) LegacyAck(ctx context.Context, broadcastID uint64, vchannel string) error {
	task, ok := bm.getBroadcastTaskByID(broadcastID)
	if !ok {
		bm.Logger().Warn("broadcast task not found, it may already acked, ignore the request", zap.Uint64("broadcastID", broadcastID), zap.String("vchannel", vchannel))
		return nil
	}
	msg := task.GetImmutableMessageFromVChannel(vchannel)
	if msg == nil {
		task.Logger().Warn("vchannel is already acked, ignore the ack request", zap.String("vchannel", vchannel))
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
		bm.Logger().Debug(
			"task is tombstone, ignored the ack request",
			zap.Uint64("broadcastID", msg.BroadcastHeader().BroadcastID),
			zap.String("vchannel", msg.VChannel()))
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
		bm.Logger().Debug("task is not found, ignored the drop tombstone request", zap.Uint64("broadcastID", broadcastID))
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

// addBroadcastTask adds the broadcast task into the manager.
func (bm *broadcastTaskManager) addBroadcastTask(msg message.BroadcastMutableMessage, broadcastID uint64, guards *lockGuards) *broadcastTask {
	msg = msg.OverwriteBroadcastHeader(broadcastID, guards.ResourceKeys()...)
	newIncomingTask := newBroadcastTaskFromBroadcastMessage(msg, bm.metrics, bm.ackScheduler)
	newIncomingTask.SetLogger(bm.Logger())
	newIncomingTask.WithResourceKeyLockGuards(guards)

	bm.mu.Lock()
	bm.tasks[broadcastID] = newIncomingTask
	bm.mu.Unlock()
	return newIncomingTask
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
		bm.Logger().Warn("try to recover task from the wal from non-replicate message, ignore it")
		return nil, false
	}

	newBroadcastTask := newBroadcastTaskFromImmutableMessage(msg, bm.metrics, bm.ackScheduler)
	newBroadcastTask.SetLogger(bm.Logger())
	bm.tasks[bh.BroadcastID] = newBroadcastTask
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

	delete(bm.tasks, broadcastID)
}
