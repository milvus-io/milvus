package broadcaster

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// newBroadcastTaskManager creates a new broadcast task manager with recovery info.
func newBroadcastTaskManager(protos []*streamingpb.BroadcastTask) (*broadcastTaskManager, []*pendingBroadcastTask) {
	logger := resource.Resource().Logger().With(log.FieldComponent("broadcaster"))
	metrics := newBroadcasterMetrics()

	recoveryTasks := make([]*broadcastTask, 0, len(protos))
	for _, proto := range protos {
		t := newBroadcastTaskFromProto(proto, metrics)
		t.SetLogger(logger.With(zap.Uint64("broadcastID", t.header.BroadcastID)))
		recoveryTasks = append(recoveryTasks, t)
	}
	rks := make(map[message.ResourceKey]uint64, len(recoveryTasks))
	tasks := make(map[uint64]*broadcastTask, len(recoveryTasks))
	pendingTasks := make([]*pendingBroadcastTask, 0, len(recoveryTasks))
	for _, task := range recoveryTasks {
		for rk := range task.header.ResourceKeys {
			if oldTaskID, ok := rks[rk]; ok {
				panic(fmt.Sprintf("unreachable: dirty recovery info in metastore, broadcast ids: [%d, %d]", oldTaskID, task.header.BroadcastID))
			}
			rks[rk] = task.header.BroadcastID
			metrics.IncomingResourceKey(rk.Domain)
		}
		tasks[task.header.BroadcastID] = task
		if task.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING {
			// only the task is pending need to be reexecuted.
			pendingTasks = append(pendingTasks, newPendingBroadcastTask(task))
		}
	}
	m := &broadcastTaskManager{
		Binder:       log.Binder{},
		cond:         syncutil.NewContextCond(&sync.Mutex{}),
		tasks:        tasks,
		resourceKeys: rks,
		version:      1,
		metrics:      metrics,
	}
	m.SetLogger(logger)
	return m, pendingTasks
}

// broadcastTaskManager is the manager of the broadcast task.
type broadcastTaskManager struct {
	log.Binder
	cond         *syncutil.ContextCond
	tasks        map[uint64]*broadcastTask      // map the broadcastID to the broadcastTaskState
	resourceKeys map[message.ResourceKey]uint64 // map the resource key to the broadcastID
	version      int                            // version is used to make sure that there's no update lost for watcher.
	metrics      *broadcasterMetrics
}

// AddTask adds a new broadcast task into the manager.
func (bm *broadcastTaskManager) AddTask(ctx context.Context, msg message.BroadcastMutableMessage) (*pendingBroadcastTask, error) {
	var err error
	if msg, err = bm.assignID(ctx, msg); err != nil {
		return nil, err
	}
	task, err := bm.addBroadcastTask(msg)
	if err != nil {
		return nil, err
	}
	return newPendingBroadcastTask(task), nil
}

func (bm *broadcastTaskManager) assignID(ctx context.Context, msg message.BroadcastMutableMessage) (message.BroadcastMutableMessage, error) {
	// TODO: current implementation the header cannot be seen at flusher itself.
	// only import message use it, so temporarily set the broadcast id here.
	// need to refactor the message to make the broadcast header visible to flusher.
	if msg.MessageType() == message.MessageTypeImport {
		importMsg, err := message.AsMutableImportMessageV1(msg)
		if err != nil {
			return nil, err
		}
		body, err := importMsg.Body()
		if err != nil {
			return nil, err
		}
		return msg.WithBroadcastID(uint64(body.JobID)), nil
	}

	id, err := resource.Resource().IDAllocator().Allocate(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "allocate new id failed")
	}
	msg = msg.WithBroadcastID(id)
	return msg, nil
}

// Ack acknowledges the message at the specified vchannel.
func (bm *broadcastTaskManager) Ack(ctx context.Context, broadcastID uint64, vchannel string) error {
	task, ok := bm.getBroadcastTaskByID(broadcastID)
	if !ok {
		bm.Logger().Warn("broadcast task not found, it may already acked, ignore the request", zap.Uint64("broadcastID", broadcastID), zap.String("vchannel", vchannel))
		return nil
	}
	if err := task.Ack(ctx, vchannel); err != nil {
		return err
	}

	if task.State() == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
		bm.removeBroadcastTask(broadcastID)
	} else {
		bm.increaseVersion()
	}
	return nil
}

// WatchAtVersion watches the version of the broadcast task manager.
// When the version is greater than the input version, the watcher will be notified.
func (bm *broadcastTaskManager) WatchAtVersion(version int) <-chan struct{} {
	bm.cond.L.Lock()
	if bm.version > version {
		bm.cond.L.Unlock()
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return bm.cond.WaitChan()
}

// CurrentVersion returns the current version of the broadcast task manager.
func (bm *broadcastTaskManager) CurrentVersion() int {
	bm.cond.L.Lock()
	defer bm.cond.L.Unlock()
	return bm.version
}

// GetBroadcastTaskByResourceKey returns the broadcast task by the resource key.
func (bm *broadcastTaskManager) GetBroadcastTaskByResourceKey(resourceKey message.ResourceKey) (*broadcastTask, bool) {
	bm.cond.L.Lock()
	defer bm.cond.L.Unlock()

	broadcastID, ok := bm.resourceKeys[resourceKey]
	if !ok {
		return nil, false
	}
	task, ok := bm.tasks[broadcastID]
	return task, ok
}

// addBroadcastTask adds the broadcast task into the manager.
func (bm *broadcastTaskManager) addBroadcastTask(msg message.BroadcastMutableMessage) (*broadcastTask, error) {
	newIncomingTask := newBroadcastTaskFromBroadcastMessage(msg, bm.metrics)
	header := newIncomingTask.Header()
	newIncomingTask.SetLogger(bm.Logger().With(zap.Uint64("broadcastID", header.BroadcastID)))

	bm.cond.L.Lock()
	defer bm.cond.L.Unlock()
	// Check if the resource key is held by other task.
	for key := range header.ResourceKeys {
		if _, ok := bm.resourceKeys[key]; ok {
			return nil, status.NewResourceAcquired(fmt.Sprintf("domain: %s, key: %s", key.Domain.String(), key.Key))
		}
	}
	// setup the resource keys to make resource exclusive held.
	for key := range header.ResourceKeys {
		bm.resourceKeys[key] = header.BroadcastID
		bm.metrics.IncomingResourceKey(key.Domain)
	}
	bm.tasks[header.BroadcastID] = newIncomingTask
	return newIncomingTask, nil
}

// getBroadcastTaskByID return the task by the broadcastID.
func (bm *broadcastTaskManager) getBroadcastTaskByID(broadcastID uint64) (*broadcastTask, bool) {
	bm.cond.L.Lock()
	defer bm.cond.L.Unlock()
	t, ok := bm.tasks[broadcastID]
	return t, ok
}

// removeBroadcastTask removes the broadcast task by the broadcastID.
func (bm *broadcastTaskManager) removeBroadcastTask(broadcastID uint64) {
	bm.cond.LockAndBroadcast()
	defer bm.cond.L.Unlock()

	task, ok := bm.tasks[broadcastID]
	if !ok {
		return
	}
	bm.version++
	// remove the related resource keys
	for key := range task.header.ResourceKeys {
		delete(bm.resourceKeys, key)
		bm.metrics.GoneResourceKey(key.Domain)
	}
	delete(bm.tasks, broadcastID)
}

func (bm *broadcastTaskManager) increaseVersion() {
	bm.cond.LockAndBroadcast()
	bm.version++
	bm.cond.L.Unlock()
}
