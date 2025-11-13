package broadcaster

import (
	"context"
	"sort"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// newAckCallbackScheduler creates a new ack callback scheduler.
func newAckCallbackScheduler(logger *log.MLogger) *ackCallbackScheduler {
	s := &ackCallbackScheduler{
		notifier:           syncutil.NewAsyncTaskNotifier[struct{}](),
		pending:            make(chan *broadcastTask, 16),
		triggerChan:        make(chan struct{}, 1),
		rkLocker:           newResourceKeyLocker(newBroadcasterMetrics()),
		tombstoneScheduler: newTombstoneScheduler(logger),
	}
	s.SetLogger(logger)
	return s
}

type ackCallbackScheduler struct {
	log.Binder

	notifier           *syncutil.AsyncTaskNotifier[struct{}]
	pending            chan *broadcastTask
	triggerChan        chan struct{}
	tombstoneScheduler *tombstoneScheduler
	pendingAckedTasks  []*broadcastTask // should already sorted by the broadcastID
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

// Initialize initializes the ack scheduler with a list of broadcast tasks.
func (s *ackCallbackScheduler) Initialize(tasks []*broadcastTask, tombstoneIDs []uint64, bm *broadcastTaskManager) {
	// when initializing, the tasks in recovery info may be out of order, so we need to sort them by the broadcastID.
	sortByControlChannelTimeTick(tasks)
	s.tombstoneScheduler.Initialize(bm, tombstoneIDs)
	s.pendingAckedTasks = tasks
	go s.background()
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

	// close the tombstone scheduler after the ack scheduler is closed.
	s.tombstoneScheduler.Close()
}

// background is the background task of the ack scheduler.
func (s *ackCallbackScheduler) background() {
	defer func() {
		s.notifier.Finish(struct{}{})
		s.Logger().Info("ack scheduler background exit")
	}()
	s.Logger().Info("ack scheduler background start")

	// it's weired to find that FastLock may be failure even if there's no resource-key locked,
	// also see: #45285
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var triggerTicker <-chan time.Time
	for {
		s.triggerAckCallback()
		if len(s.pendingAckedTasks) > 0 {
			// if there's pending tasks, trigger the ack callback after a delay.
			triggerTicker = ticker.C
		} else {
			triggerTicker = nil
		}
		select {
		case <-s.notifier.Context().Done():
			return
		case task := <-s.pending:
			s.addBroadcastTask(task)
		case <-s.triggerChan:
		case <-triggerTicker:
		}
	}
}

// addBroadcastTask adds a broadcast task into the pending acked tasks.
func (s *ackCallbackScheduler) addBroadcastTask(task *broadcastTask) error {
	s.pendingAckedTasks = append(s.pendingAckedTasks, task)
	return nil
}

// triggerAckCallback triggers the ack callback.
func (s *ackCallbackScheduler) triggerAckCallback() {
	pendingTasks := make([]*broadcastTask, 0, len(s.pendingAckedTasks))
	for _, task := range s.pendingAckedTasks {
		g, err := s.rkLocker.FastLock(task.Header().ResourceKeys.Collect()...)
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
func (s *ackCallbackScheduler) doAckCallback(bt *broadcastTask, g *lockGuards) (err error) {
	logger := s.Logger().With(zap.Uint64("broadcastID", bt.Header().BroadcastID))
	defer func() {
		g.Unlock()
		s.triggerChan <- struct{}{}
		if err == nil {
			logger.Info("execute ack callback done")
		} else {
			logger.Warn("execute ack callback failed", zap.Error(err))
		}
	}()
	logger.Info("start to execute ack callback")
	if err := bt.BlockUntilAllAck(s.notifier.Context()); err != nil {
		return err
	}
	logger.Debug("all vchannels are acked")

	msg, result := bt.BroadcastResult()
	makeMap := make(map[string]*message.AppendResult, len(result))
	for vchannel, result := range result {
		makeMap[vchannel] = &message.AppendResult{
			MessageID:              result.MessageID,
			LastConfirmedMessageID: result.LastConfirmedMessageID,
			TimeTick:               result.TimeTick,
		}
	}
	// call the ack callback until done.
	bt.ObserveAckCallbackBegin()
	if err := s.callMessageAckCallbackUntilDone(s.notifier.Context(), msg, makeMap); err != nil {
		return err
	}
	bt.ObserveAckCallbackDone()

	logger.Debug("ack callback done")
	if err := bt.MarkAckCallbackDone(s.notifier.Context()); err != nil {
		// The catalog is reliable to write, so we can mark the ack callback done without retrying.
		return err
	}
	s.tombstoneScheduler.AddPending(bt.Header().BroadcastID)
	return nil
}

// callMessageAckCallbackUntilDone calls the message ack callback until done.
func (s *ackCallbackScheduler) callMessageAckCallbackUntilDone(ctx context.Context, msg message.BroadcastMutableMessage, result map[string]*message.AppendResult) error {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 10 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()

	for {
		err := registry.CallMessageAckCallback(ctx, msg, result)
		if err == nil {
			return nil
		}
		nextInterval := backoff.NextBackOff()
		s.Logger().Warn("failed to call message ack callback, wait for retry...",
			log.FieldMessage(msg),
			zap.Duration("nextInterval", nextInterval),
			zap.Error(err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nextInterval):
		}
	}
}

// sortByControlChannelTimeTick sorts the tasks by the time tick of the control channel.
func sortByControlChannelTimeTick(tasks []*broadcastTask) {
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ControlChannelTimeTick() < tasks[j].ControlChannelTimeTick()
	})
}
