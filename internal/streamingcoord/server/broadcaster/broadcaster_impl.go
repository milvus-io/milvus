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
}

func (b *broadcasterImpl) WithResourceKeys(ctx context.Context, resourceKeys ...message.ResourceKey) (BroadcastAPI, error) {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("broadcaster is closing")
	}
	defer b.lifetime.Done()

	guards, err := b.manager.AcquireResourceKeys(ctx, resourceKeys...)
	if err != nil {
		return nil, err
	}
	return &broadcasterWithRK{
		broadcaster: b,
		guards:      guards,
	}, nil
}

// Broadcast broadcasts the message to all channels.
func (b *broadcasterImpl) Broadcast(ctx context.Context, msg message.BroadcastMutableMessage, guards *lockGuards) (result *types.BroadcastAppendResult, err error) {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		guards.Unlock()
		return nil, status.NewOnShutdownError("broadcaster is closing")
	}
	defer func() {
		b.lifetime.Done()
		if err != nil {
			b.Logger().Warn("broadcast message failed", zap.Error(err))
			return
		}
	}()

	t := b.manager.AddTask(ctx, msg, guards)
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
	r, err := t.BlockUntilTaskDone(ctx)
	if err != nil {
		return nil, err
	}

	// Fast ack all the vchannels after the message is already write into wal.
	// The operation order of broadcast message is determined by the wal,
	// the order of message in wal is protected by the broadcast resource key lock.
	// So when we do a broadcast operation done, the order of message in wal is determined,
	// we can fast ack all the vchannels after the message is already write into wal
	// without waiting for the message to be acked by the streamingnode.
	// These optimization can reduce the latency of the broadcast message ack that is suffering from timetick commit at streamingnode.
	if err := t.FastAckAll(ctx); err != nil {
		t.Logger().Warn("fast ack all failed, fallback to the normal ack from streamingnode", zap.Error(err))
	} else {
		t.Logger().Info("fast ack all success")
	}

	// wait for all the vchannels acked.
	if err := t.BlockUntilAllAck(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (b *broadcasterImpl) LegacyAck(ctx context.Context, broadcastID uint64, vchannel string) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("broadcaster is closing")
	}
	defer b.lifetime.Done()

	return b.manager.LegacyAck(ctx, broadcastID, vchannel)
}

// Ack acknowledges the message at the specified vchannel.
func (b *broadcasterImpl) Ack(ctx context.Context, msg message.ImmutableMessage) error {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return status.NewOnShutdownError("broadcaster is closing")
	}
	defer b.lifetime.Done()

	return b.manager.Ack(ctx, msg)
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

	// Start n workers to handle the broadcast task.
	wg := sync.WaitGroup{}
	for i := 0; i < workers; i++ {
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

func (b *broadcasterImpl) worker(no int) {
	logger := b.Logger().With(zap.Int("workerNo", no))
	defer func() {
		logger.Info("broadcaster worker exit")
	}()

	for {
		select {
		case <-b.backgroundTaskNotifier.Context().Done():
			return
		case task := <-b.workerChan:
			if err := task.Execute(b.backgroundTaskNotifier.Context()); err != nil {
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
