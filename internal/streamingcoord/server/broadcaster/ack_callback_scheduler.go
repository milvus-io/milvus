package broadcaster

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
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
		rkLockerMu:         sync.Mutex{},
		rkLocker:           newResourceKeyLocker(),
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
	rkLockerMu sync.Mutex // because batch lock operation will be executed on rkLocker,
	// so we may encounter following cases:
	// 1. task A, B, C are competing with rkLocker, and we want the operation is executed in order of A -> B -> C.
	// 2. A is on running, and B, C are waiting for the lock.
	// 3. When triggerAckCallback, B is failed to acquire the lock, C is pending to call FastLock.
	// 4. Then A is done, the lock is released, C acquires the lock and executes the ack callback, the order is broken as A -> C -> B.
	// To avoid the order broken, we need to use a mutex to protect the batch lock operation.
	rkLocker *resourceKeyLocker // it is used to lock the resource-key of ack operation.
	// it is not same instance with the resourceKeyLocker in the broadcastTaskManager.
	// because it is just used to check if the resource-key is locked when acked.
	// For primary milvus cluster, it makes no sense, because the execution order is already protected by the broadcastTaskManager.
	// But for secondary milvus cluster, it is necessary to use this rkLocker to protect the resource-key when acked to avoid the execution order broken.

	bm *broadcastTaskManager // reference to the broadcast task manager for accessing incomplete tasks
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
	s.rkLockerMu.Lock()
	defer s.rkLockerMu.Unlock()

	pendingTasks := make([]*broadcastTask, 0, len(s.pendingAckedTasks))
	for _, task := range s.pendingAckedTasks {
		if task.IsForcePromoteMessage() {
			// Force promote: fix incomplete broadcasts in background (BlockUntilAllAck → fix).
			// The task still goes through normal FastLock → doAckCallback below.
			go s.doForcePromoteFixIncompleteBroadcasts(task)
		}

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

// doForcePromoteFixIncompleteBroadcasts waits for all acks, then fixes incomplete broadcasts.
// The actual ack callback is handled by the normal doAckCallback path.
func (s *ackCallbackScheduler) doForcePromoteFixIncompleteBroadcasts(bt *broadcastTask) {
	logger := s.Logger().With(zap.Uint64("broadcastID", bt.Header().BroadcastID))

	if err := bt.BlockUntilAllAck(s.notifier.Context()); err != nil {
		logger.Warn("force promote BlockUntilAllAck failed", zap.Error(err))
		return
	}

	if err := s.fixIncompleteBroadcastsForForcePromote(s.notifier.Context()); err != nil {
		logger.Warn("failed to fix incomplete broadcasts for force promote", zap.Error(err))
		return
	}
	logger.Info("completed fixing incomplete broadcasts for force promote")
}

// fixIncompleteBroadcastsForForcePromote fixes incomplete broadcasts for force promote.
// It marks incomplete AlterReplicateConfig messages with ignore=true before supplementing
// all incomplete messages to remaining vchannels.
func (s *ackCallbackScheduler) fixIncompleteBroadcastsForForcePromote(ctx context.Context) error {
	incompleteTasks := s.bm.getIncompleteBroadcastTasks()

	// Sort by broadcastID to preserve the original order of DDL messages.
	sort.Slice(incompleteTasks, func(i, j int) bool {
		return incompleteTasks[i].Header().BroadcastID < incompleteTasks[j].Header().BroadcastID
	})

	// Separate AlterReplicateConfig from other broadcast messages
	var alterReplicateConfigTasks []*broadcastTask
	var otherBroadcastTasks []*broadcastTask
	for _, task := range incompleteTasks {
		if task.IsAlterReplicateConfigMessage() {
			alterReplicateConfigTasks = append(alterReplicateConfigTasks, task)
		} else {
			otherBroadcastTasks = append(otherBroadcastTasks, task)
		}
	}

	totalTasks := len(alterReplicateConfigTasks) + len(otherBroadcastTasks)
	if totalTasks == 0 {
		s.Logger().Info("No incomplete broadcasts to fix for force promote")
		return nil
	}

	s.Logger().Info("Fixing incomplete broadcasts for force promote",
		zap.Int("alterReplicateConfigTasks", len(alterReplicateConfigTasks)),
		zap.Int("otherBroadcastTasks", len(otherBroadcastTasks)))

	// Mark AlterReplicateConfig tasks with ignore=true (to prevent old config overwriting force promote config)
	for _, task := range alterReplicateConfigTasks {
		s.Logger().Info("Marking AlterReplicateConfig task with ignore=true",
			zap.Uint64("broadcastID", task.Header().BroadcastID))

		if err := task.MarkIgnoreAndSave(ctx); err != nil {
			s.Logger().Error("Failed to mark task with ignore",
				zap.Uint64("broadcastID", task.Header().BroadcastID),
				zap.Error(err))
			return errors.Wrapf(err, "failed to mark task %d with ignore", task.Header().BroadcastID)
		}
	}

	// Collect pending messages from ALL incomplete tasks for supplementation
	var pendingMessages []message.MutableMessage
	for _, task := range alterReplicateConfigTasks {
		msgs := task.PendingBroadcastMessages()
		if len(msgs) > 0 {
			s.Logger().Info("Supplementing AlterReplicateConfig messages to remaining vchannels",
				zap.Uint64("broadcastID", task.Header().BroadcastID),
				zap.Int("pendingVChannels", len(msgs)))
			pendingMessages = append(pendingMessages, msgs...)
		}
	}
	for _, task := range otherBroadcastTasks {
		msgs := task.PendingBroadcastMessages()
		if len(msgs) > 0 {
			s.Logger().Info("Supplementing broadcast messages to remaining vchannels",
				zap.Uint64("broadcastID", task.Header().BroadcastID),
				zap.String("messageType", task.msg.MessageType().String()),
				zap.Int("pendingVChannels", len(msgs)))
			pendingMessages = append(pendingMessages, msgs...)
		}
	}

	if len(pendingMessages) == 0 {
		s.Logger().Info("No pending messages to supplement")
		return nil
	}

	// Append pending messages to their respective vchannels
	appendResults := streaming.WAL().AppendMessages(ctx, pendingMessages...)

	var lastResultErr error
	supplementCount := 0
	failureCount := 0
	for i, result := range appendResults.Responses {
		if result.Error != nil {
			s.Logger().Warn("Failed to supplement message",
				zap.String("vchannel", pendingMessages[i].VChannel()),
				zap.Error(result.Error))
			failureCount++
			lastResultErr = result.Error
			continue
		}
		supplementCount++
	}

	s.Logger().Info("Completed fixing incomplete broadcasts for force promote",
		zap.Int("supplementCount", supplementCount),
		zap.Int("failureCount", failureCount),
		zap.Int("totalPending", len(pendingMessages)))

	return lastResultErr
}

// doAckCallback executes the ack callback.
func (s *ackCallbackScheduler) doAckCallback(bt *broadcastTask, g *lockGuards) (err error) {
	logger := s.Logger().With(zap.Uint64("broadcastID", bt.Header().BroadcastID))
	defer func() {
		s.rkLockerMu.Lock()
		g.Unlock()
		s.rkLockerMu.Unlock()

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
