package broadcaster

import (
	"sort"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"go.uber.org/zap"
)

// newAckCallbackScheduler creates a new ack callback scheduler.
func newAckCallbackScheduler(pendingAckedTasks []*broadcastTask, logger *log.MLogger) *ackCallbackScheduler {
	sortByBroadcastID(pendingAckedTasks)
	s := &ackCallbackScheduler{
		notifier:          syncutil.NewAsyncTaskNotifier[struct{}](),
		pending:           make(chan *broadcastTask, 1024),
		triggerChan:       make(chan struct{}, 1),
		pendingAckedTasks: pendingAckedTasks,
		rkLocker:          newResourceKeyLocker(newBroadcasterMetrics()),
	}
	s.SetLogger(logger)
	go s.background()
	return s
}

type ackCallbackScheduler struct {
	log.Binder

	notifier          *syncutil.AsyncTaskNotifier[struct{}]
	pending           chan *broadcastTask
	triggerChan       chan struct{}
	pendingAckedTasks []*broadcastTask // sorted by the broadcastID
	// For the task that hold the conflicted resource-key (which is protected by the resource-key lock),
	// broadcastID is always increasing,
	// the task which broadcastID is smaller happens before the task which broadcastID is larger.
	// Meanwhile the timetick order of any vchannel of those two tasks are same with the order of broadcastID,
	// so the smaller broadcastID task is always acked before the larger broadcastID task.
	// so we can exeucte the tasks by the order of the broadcastID to promise the ack order is same with wal order.
	rkLocker *resourceKeyLocker // it is used to lock the resource-key of ack operation.
	// it is not same instance with the resourceKeyLocker in the broadcastTaskManager.
	// because it is just used to check if the resource-key is locked when acked.
	// For primary milvus cluster, it makes no sense, because the execution order is already protected by the broadcastTaskManager.
	// But for secondary milvus cluster, it is necessary to use this rkLocker to protect the resource-key when acked to avoid the execution order broken.
}

// AddTask adds a new broadcast task into the ack scheduler.
func (s *ackCallbackScheduler) AddTask(task *broadcastTask) {
	select {
	case <-s.notifier.Context().Done():
		panic("unreachable: ack scheduler is closing when adding new task")
	case s.pending <- task:
	}
}

// Close closes the ack scheduler.
func (s *ackCallbackScheduler) Close() {
	s.notifier.Cancel()
	s.notifier.BlockUntilFinish()
}

// background is the background task of the ack scheduler.
func (s *ackCallbackScheduler) background() {
	for {
		s.triggerAckCallback()
		select {
		case <-s.notifier.Context().Done():
			return
		case task := <-s.pending:
			s.addBroadcastTask(task)
		case <-s.triggerChan:
		}
	}
}

// addBroadcastTask adds a broadcast task into the pending acked tasks.
func (s *ackCallbackScheduler) addBroadcastTask(task *broadcastTask) error {
	s.pendingAckedTasks = append(s.pendingAckedTasks, task)
	sortByBroadcastID(s.pendingAckedTasks)
	return nil
}

// triggerAckCallback triggers the ack callback.
func (s *ackCallbackScheduler) triggerAckCallback() {
	pendingTasks := make([]*broadcastTask, 0, len(s.pendingAckedTasks))
	for _, task := range s.pendingAckedTasks {
		if task.State() != streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING &&
			task.State() != streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_WAIT_ACK {
			continue
		}
		g, err := s.rkLocker.FastLock(task.Header().BroadcastID, task.Header().ResourceKeys.Collect()...)
		if err != nil {
			s.Logger().Warn("lock is occupied, delay the ack callback", zap.Uint64("broadcastID", task.Header().BroadcastID), zap.Error(err))
			pendingTasks = append(pendingTasks, task)
			continue
		}
		// Execute the ack callback in background.
		go s.doAckCallback(task, g)
	}
	s.pendingAckedTasks = pendingTasks
}

// doAckCallback executes the ack callback.
func (s *ackCallbackScheduler) doAckCallback(bt *broadcastTask, g *lockGuards) error {
	defer func() {
		g.Unlock()
		s.triggerChan <- struct{}{}
	}()

	msg, result := bt.BroadcastResult()
	// TODO: retry until success.
	if err := registry.CallMessageAckCallback(s.notifier.Context(), msg, result); err != nil {
		return err
	}
	return bt.MarkAckCallbackDone(s.notifier.Context())
}

func sortByBroadcastID(tasks []*broadcastTask) {
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Header().BroadcastID < tasks[j].Header().BroadcastID
	})
}
