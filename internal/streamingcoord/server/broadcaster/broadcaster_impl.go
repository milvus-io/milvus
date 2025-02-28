package broadcaster

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func RecoverBroadcaster(
	ctx context.Context,
	appendOperator *syncutil.Future[AppendOperator],
) (Broadcaster, error) {
	tasks, err := resource.Resource().StreamingCatalog().ListBroadcastTask(ctx)
	if err != nil {
		return nil, err
	}
	manager, pendings := newBroadcastTaskManager(tasks)
	b := &broadcasterImpl{
		manager:                manager,
		lifetime:               typeutil.NewLifetime(),
		backgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		pendings:               pendings,
		backoffs:               typeutil.NewHeap[*pendingBroadcastTask](&pendingBroadcastTaskArray{}),
		backoffChan:            make(chan *pendingBroadcastTask),
		pendingChan:            make(chan *pendingBroadcastTask),
		workerChan:             make(chan *pendingBroadcastTask),
		appendOperator:         appendOperator,
	}
	go b.execute()
	return b, nil
}

// broadcasterImpl is the implementation of Broadcaster
type broadcasterImpl struct {
	manager                *broadcastTaskManager
	lifetime               *typeutil.Lifetime
	backgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	pendings               []*pendingBroadcastTask
	backoffs               typeutil.Heap[*pendingBroadcastTask]
	pendingChan            chan *pendingBroadcastTask
	backoffChan            chan *pendingBroadcastTask
	workerChan             chan *pendingBroadcastTask
	appendOperator         *syncutil.Future[AppendOperator] // TODO: we can remove those lazy future in 2.6.0, by remove the msgstream broadcaster.
}

// Broadcast broadcasts the message to all channels.
func (b *broadcasterImpl) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage) (result *types.BroadcastAppendResult, err error) {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("broadcaster is closing")
	}
	defer func() {
		b.lifetime.Done()
		if err != nil {
			b.Logger().Warn("broadcast message failed", zap.Error(err))
			return
		}
	}()

	t, err := b.manager.AddTask(ctx, msg)
	if err != nil {
		return nil, err
	}
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

// Ack acknowledges the message at the specified vchannel.
func (b *broadcasterImpl) Ack(ctx context.Context, req types.BroadcastAckRequest) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("broadcaster is closing")
	}
	defer b.lifetime.Done()

	return b.manager.Ack(ctx, req.BroadcastID, req.VChannel)
}

func (b *broadcasterImpl) NewWatcher() (Watcher, error) {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("broadcaster is closing")
	}
	defer b.lifetime.Done()

	return newWatcher(b), nil
}

func (b *broadcasterImpl) Close() {
	b.lifetime.SetState(typeutil.LifetimeStateStopped)
	b.lifetime.Wait()

	b.backgroundTaskNotifier.Cancel()
	b.backgroundTaskNotifier.BlockUntilFinish()
}

func (b *broadcasterImpl) Logger() *log.MLogger {
	return b.manager.Logger()
}

// execute the broadcaster
func (b *broadcasterImpl) execute() {
	workers := int(float64(hardware.GetCPUNum()) * paramtable.Get().StreamingCfg.WALBroadcasterConcurrencyRatio.GetAsFloat())
	if workers < 1 {
		workers = 1
	}
	b.Logger().Info("broadcaster start to execute", zap.Int("workerNum", workers))

	defer func() {
		b.backgroundTaskNotifier.Finish(struct{}{})
		b.Logger().Info("broadcaster execute exit")
	}()

	// Wait for appendOperator ready
	appendOperator, err := b.appendOperator.GetWithContext(b.backgroundTaskNotifier.Context())
	if err != nil {
		b.Logger().Info("broadcaster is closed before appendOperator ready")
		return
	}
	b.Logger().Info("broadcaster appendOperator ready, begin to start workers and dispatch")

	// Start n workers to handle the broadcast task.
	wg := sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		i := i
		// Start n workers to handle the broadcast task.
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.worker(i, appendOperator)
		}()
	}
	defer wg.Wait()

	b.dispatch()
}

func (b *broadcasterImpl) dispatch() {
	for {
		var workerChan chan *pendingBroadcastTask
		var nextTask *pendingBroadcastTask
		var nextBackOff <-chan time.Time
		// Wait for new task.
		if len(b.pendings) > 0 {
			workerChan = b.workerChan
			nextTask = b.pendings[0]
		}
		if b.backoffs.Len() > 0 {
			var nextInterval time.Duration
			nextBackOff, nextInterval = b.backoffs.Peek().NextTimer()
			b.Logger().Info("backoff task", zap.Duration("nextInterval", nextInterval))
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
			newPops := make([]*pendingBroadcastTask, 0)
			for b.backoffs.Len() > 0 && b.backoffs.Peek().NextInterval() < time.Millisecond {
				newPops = append(newPops, b.backoffs.Pop())
			}
			if len(newPops) > 0 {
				// Push the backoff task into pendings front.
				b.pendings = append(newPops, b.pendings...)
			}
		case workerChan <- nextTask:
			// The task is sent to worker, remove it from pending list.
			b.pendings = b.pendings[1:]
		}
	}
}

func (b *broadcasterImpl) worker(no int, appendOperator AppendOperator) {
	logger := b.Logger().With(zap.Int("workerNo", no))
	defer func() {
		logger.Info("broadcaster worker exit")
	}()

	for {
		select {
		case <-b.backgroundTaskNotifier.Context().Done():
			return
		case task := <-b.workerChan:
			if err := task.Execute(b.backgroundTaskNotifier.Context(), appendOperator); err != nil {
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
