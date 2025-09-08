package broadcaster

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
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

	recoveryTasks := make([]*broadcastTask, 0, len(protos))
	for _, proto := range protos {
		t := newBroadcastTaskFromProto(proto, metrics)
		t.SetLogger(logger)
		recoveryTasks = append(recoveryTasks, t)
	}
	tasks := make(map[uint64]*broadcastTask, len(recoveryTasks))
	pendingTasks := make([]*pendingBroadcastTask, 0, len(recoveryTasks))
	pendingAckCallbackTasks := make([]*broadcastTask, 0, len(recoveryTasks))
	for _, task := range recoveryTasks {
		switch task.task.State {
		case streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_WAIT_ACK:
			guards, err := rkLocker.FastLock(task.Header().BroadcastID, task.Header().ResourceKeys.Collect()...)
			if err != nil {
				panic(err)
			}
			task.WithResourceKeyLockGuards(guards)

			if newPending := newPendingBroadcastTask(task); newPending != nil {
				// if there's some pending messages that is not appended, it should be continued to be appended.
				pendingTasks = append(pendingTasks, newPending)
			} else {
				// if there's no pending messages, it should be added to the pending ack callback tasks.
				pendingAckCallbackTasks = append(pendingAckCallbackTasks, task)
			}
		case streamingpb.BroadcastTaskState_BORADCAST_TASK_STATE_REPLICATED:
			// The task is recovered from the remote cluster, so it doesn't hold the resource lock.
			// but the task execution order should be protected by the order of broadcastID (by ackCallbackScheduler)
			pendingAckCallbackTasks = append(pendingAckCallbackTasks, task)
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
		ackScheduler:       newAckCallbackScheduler(pendingAckCallbackTasks, logger),
	}
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
	guards, err := bm.resourceKeyLocker.Lock(ctx, id, resourceKeys...)
	if err != nil {
		return nil, err
	}
	return &broadcasterWithRK{
		broadcaster: bm,
		guards:      guards,
	}, nil
}

// broadcast broadcasts the message to all vchannels.
// it will block until the message is broadcasted to all vchannels
func (bm *broadcastTaskManager) broadcast(ctx context.Context, msg message.BroadcastMutableMessage, guards *lockGuards) (*types.BroadcastAppendResult, error) {
	if !bm.lifetime.Add(typeutil.LifetimeStateWorking) {
		guards.Unlock()
		return nil, status.NewOnShutdownError("broadcaster is closing")
	}
	defer bm.lifetime.Done()

	task := bm.addBroadcastTask(msg, guards)
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
	firstAllDone, err := t.Ack(ctx, msg)
	if err != nil {
		return err
	}
	if firstAllDone {
		// add it into ack scheduler to ack the message if all the vchannels are acked at first time.
		bm.ackScheduler.AddTask(t)
	}
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
func (bm *broadcastTaskManager) addBroadcastTask(msg message.BroadcastMutableMessage, guards *lockGuards) *broadcastTask {
	msg = msg.OverwriteBroadcastHeader(guards.BroadcastID(), guards.ResourceKeys()...)
	newIncomingTask := newBroadcastTaskFromBroadcastMessage(msg, bm.metrics)
	newIncomingTask.SetLogger(bm.Logger())
	newIncomingTask.WithResourceKeyLockGuards(guards)

	bm.mu.Lock()
	bm.tasks[guards.BroadcastID()] = newIncomingTask
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
	if msg.ReplicateHeader() != nil {
		bm.Logger().Warn("try to recover task from the wal from non-replicate message, ignore it")
		return nil, false
	}

	newBroadcastTask := newBroadcastTaskFromImmutableMessage(msg, bm.metrics)
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
