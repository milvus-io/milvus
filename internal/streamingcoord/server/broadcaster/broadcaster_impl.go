package broadcaster

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/contextutil"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func RecoverBroadcaster(
	ctx context.Context,
	appendOperator AppendOperator,
) (Broadcaster, error) {
	logger := resource.Resource().Logger().With(log.FieldComponent("broadcaster"))
	tasks, err := resource.Resource().StreamingCatalog().ListBroadcastTask(ctx)
	if err != nil {
		return nil, err
	}
	pendings := make([]*broadcastTask, 0, len(tasks))
	for _, task := range tasks {
		if task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING {
			// recover pending task
			t := newTask(task, logger)
			pendings = append(pendings, t)
		}
	}
	b := &broadcasterImpl{
		logger:                 logger,
		lifetime:               typeutil.NewLifetime(),
		backgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		pendings:               pendings,
		backoffs:               typeutil.NewHeap[*broadcastTask](&broadcastTaskArray{}),
		backoffChan:            make(chan *broadcastTask),
		pendingChan:            make(chan *broadcastTask),
		workerChan:             make(chan *broadcastTask),
		appendOperator:         appendOperator,
	}
	go b.execute()
	return b, nil
}

// broadcasterImpl is the implementation of Broadcaster
type broadcasterImpl struct {
	logger                 *log.MLogger
	lifetime               *typeutil.Lifetime
	backgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	pendings               []*broadcastTask
	backoffs               typeutil.Heap[*broadcastTask]
	pendingChan            chan *broadcastTask
	backoffChan            chan *broadcastTask
	workerChan             chan *broadcastTask
	appendOperator         AppendOperator
}

// Broadcast broadcasts the message to all channels.
func (b *broadcasterImpl) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (result *types.BroadcastAppendResult, err error) {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("broadcaster is closing")
	}
	defer func() {
		if err != nil {
			b.logger.Warn("broadcast message failed", zap.Error(err))
			return
		}
	}()

	// Once the task is persisted, it must be successful.
	task, err := b.persistBroadcastTask(ctx, msg)
	if err != nil {
		return nil, err
	}
	t := newTask(task, b.logger)
	select {
	case <-b.backgroundTaskNotifier.Context().Done():
		// We can only check the background context but not the request context here.
		// Because we want the new incoming task must be delivered to the background task queue
		// otherwise the broadcaster is closing
		return nil, status.NewOnShutdownError("broadcaster is closing")
	case b.pendingChan <- t:
	}

	// Wait both request context and the background task context.
	ctx, _ = contextutil.MergeContext(ctx, b.backgroundTaskNotifier.Context())
	return t.BlockUntilTaskDone(ctx)
}

// persistBroadcastTask persists the broadcast task into catalog.
func (b *broadcasterImpl) persistBroadcastTask(ctx context.Context, msg message.BroadcastMutableMessage) (*streamingpb.BroadcastTask, error) {
	defer b.lifetime.Done()

	id, err := resource.Resource().IDAllocator().Allocate(ctx)
	if err != nil {
		return nil, status.NewInner("allocate new id failed, %s", err.Error())
	}
	task := &streamingpb.BroadcastTask{
		TaskId:  int64(id),
		Message: &messagespb.Message{Payload: msg.Payload(), Properties: msg.Properties().ToRawMap()},
		State:   streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
	}
	// Save the task into catalog to help recovery.
	if err := resource.Resource().StreamingCatalog().SaveBroadcastTask(ctx, task); err != nil {
		return nil, status.NewInner("save broadcast task failed, %s", err.Error())
	}
	return task, nil
}

func (b *broadcasterImpl) Close() {
	b.lifetime.SetState(typeutil.LifetimeStateStopped)
	b.lifetime.Wait()

	b.backgroundTaskNotifier.Cancel()
	b.backgroundTaskNotifier.BlockUntilFinish()
}

// execute the broadcaster
func (b *broadcasterImpl) execute() {
	b.logger.Info("broadcaster start to execute")
	defer func() {
		b.backgroundTaskNotifier.Finish(struct{}{})
		b.logger.Info("broadcaster execute exit")
	}()

	// Start n workers to handle the broadcast task.
	wg := sync.WaitGroup{}
	for i := 0; i < 4; i++ {
		i := i
		// Start n workers to handle the broadcast task.
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.worker(i)
		}()
	}
	defer wg.Wait()

	b.dispatch()
}

func (b *broadcasterImpl) dispatch() {
	for {
		var workerChan chan *broadcastTask
		var nextTask *broadcastTask
		var nextBackOff <-chan time.Time
		// Wait for new task.
		if len(b.pendings) > 0 {
			workerChan = b.workerChan
			nextTask = b.pendings[0]
		}
		if b.backoffs.Len() > 0 {
			var nextInterval time.Duration
			nextBackOff, nextInterval = b.backoffs.Peek().NextTimer()
			b.logger.Info("backoff task", zap.Duration("nextInterval", nextInterval))
		}

		select {
		case <-b.backgroundTaskNotifier.Context().Done():
			return
		case task := <-b.pendingChan:
			b.pendings = append(b.pendings, task)
		case task := <-b.backoffChan:
			// task is backoff, push it into backoff queue to make a delay retry.
			b.backoffs.Push(task)
		case <-nextBackOff:
			// backoff is done, move all the backoff done task into pending to retry.
			for b.backoffs.Len() > 0 && b.backoffs.Peek().NextInterval() < time.Millisecond {
				b.pendings = append(b.pendings, b.backoffs.Pop())
			}
		case workerChan <- nextTask:
			// The task is sent to worker, remove it from pending list.
			b.pendings = b.pendings[1:]
		}
	}
}

func (b *broadcasterImpl) worker(no int) {
	defer func() {
		b.logger.Info("broadcaster worker exit", zap.Int("no", no))
	}()

	for {
		select {
		case <-b.backgroundTaskNotifier.Context().Done():
			return
		case task := <-b.workerChan:
			if err := task.Poll(b.backgroundTaskNotifier.Context(), b.appendOperator); err != nil {
				// If the task is not done, repush it into pendings and retry infinitely.
				select {
				case <-b.backgroundTaskNotifier.Context().Done():
					return
				case b.backoffChan <- task:
				}
			}
		}
	}
}
