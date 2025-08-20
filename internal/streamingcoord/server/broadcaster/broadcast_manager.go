package broadcaster

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// newBroadcastTaskManager creates a new broadcast task manager with recovery info.
func newBroadcastTaskManager(protos []*streamingpb.BroadcastTask) (*broadcastTaskManager, []*pendingBroadcastTask) {
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
	for _, task := range recoveryTasks {
		guards, err := rkLocker.FastLock(task.Header().BroadcastID, task.Header().ResourceKeys.Collect()...)
		if err != nil {
			panic(err)
		}
		task.WithResourceKeyLockGuards(guards)
		tasks[task.Header().BroadcastID] = task
		if task.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING {
			// only the task is pending need to be reexecuted.
			pendingTasks = append(pendingTasks, newPendingBroadcastTask(task))
		}
	}
	m := &broadcastTaskManager{
		Binder:            log.Binder{},
		mu:                &sync.Mutex{},
		tasks:             tasks,
		resourceKeyLocker: rkLocker,
		metrics:           metrics,
	}
	m.SetLogger(logger)
	return m, pendingTasks
}

// broadcastTaskManager is the manager of the broadcast task.
type broadcastTaskManager struct {
	log.Binder
	mu                *sync.Mutex
	tasks             map[uint64]*broadcastTask // map the broadcastID to the broadcastTaskState
	resourceKeyLocker *resourceKeyLocker
	metrics           *broadcasterMetrics
}

// AcquireResourceKeys acquires the resource keys for the broadcast task.
func (bm *broadcastTaskManager) AcquireResourceKeys(ctx context.Context, resourceKeys ...message.ResourceKey) (*lockGuards, error) {
	id, err := resource.Resource().IDAllocator().Allocate(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "allocate new id failed")
	}
	guards, err := bm.resourceKeyLocker.Lock(ctx, id, resourceKeys...)
	if err != nil {
		return nil, err
	}
	return guards, nil
}

// AddTask adds a new broadcast task into the manager.
func (bm *broadcastTaskManager) AddTask(ctx context.Context, msg message.BroadcastMutableMessage, guards *lockGuards) *pendingBroadcastTask {
	task := bm.addBroadcastTask(ctx, msg, guards)
	return newPendingBroadcastTask(task)
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
	if err := registry.CallMessageAckCallback(ctx, msg); err != nil {
		bm.Logger().Warn("message ack callback failed", log.FieldMessage(msg), zap.Error(err))
		return err
	}
	bm.Logger().Warn("message ack callback success", log.FieldMessage(msg))

	broadcastID := msg.BroadcastHeader().BroadcastID
	vchannel := msg.VChannel()
	task, ok := bm.getBroadcastTaskByID(broadcastID)
	if !ok {
		bm.Logger().Info("broadcast task not found, it may already acked or not replicate from the different milvus cluster", zap.Uint64("broadcastID", broadcastID), zap.String("vchannel", vchannel))
		return nil
	}
	if err := task.Ack(ctx, msg); err != nil {
		return err
	}
	if task.State() == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
		bm.removeBroadcastTask(broadcastID)
	}
	return nil
}

// addBroadcastTask adds the broadcast task into the manager.
func (bm *broadcastTaskManager) addBroadcastTask(ctx context.Context, msg message.BroadcastMutableMessage, guards *lockGuards) *broadcastTask {
	msg = msg.OverwriteBroadcastHeader(guards.BroadcastID(), guards.ResourceKeys()...)
	newIncomingTask := newBroadcastTaskFromBroadcastMessage(msg, bm.metrics)
	newIncomingTask.SetLogger(bm.Logger())
	newIncomingTask.WithResourceKeyLockGuards(guards)

	bm.mu.Lock()
	bm.tasks[guards.BroadcastID()] = newIncomingTask
	bm.mu.Unlock()
	return newIncomingTask
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
