package broadcaster

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// newBroadcasterScheduler creates a new broadcaster scheduler.
func newBroadcasterScheduler(pendings []*pendingBroadcastTask, logger *log.MLogger) *broadcasterScheduler {
	b := &broadcasterScheduler{
		backgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		pendings:               pendings,
		backoffs:               typeutil.NewHeap[*pendingBroadcastTask](&pendingBroadcastTaskArray{}),
		pendingChan:            make(chan *pendingBroadcastTask),
		backoffChan:            make(chan *pendingBroadcastTask),
		workerChan:             make(chan *pendingBroadcastTask),
	}
	b.SetLogger(logger)
	go b.execute()
	return b
}

// broadcasterScheduler is the implementation of Broadcaster
type broadcasterScheduler struct {
	log.Binder

	backgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	pendings               []*pendingBroadcastTask
	backoffs               typeutil.Heap[*pendingBroadcastTask]
	pendingChan            chan *pendingBroadcastTask
	backoffChan            chan *pendingBroadcastTask
	workerChan             chan *pendingBroadcastTask
}

func (b *broadcasterScheduler) AddTask(ctx context.Context, task *pendingBroadcastTask) (*types.BroadcastAppendResult, error) {
	select {
	case <-b.backgroundTaskNotifier.Context().Done():
		// We can only check the background context but not the request context here.
		// Because we want the new incoming task must be delivered to the background task queue
		// otherwise the broadcaster is closing
		panic("unreachable: broadcaster is closing when adding new task")
	case b.pendingChan <- task:
	}

	// Wait both request context and the background task context.
	ctx, _ = contextutil.MergeContext(ctx, b.backgroundTaskNotifier.Context())
	// wait for all the vchannels acked.
	result, err := task.BlockUntilAllAck(ctx)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (b *broadcasterScheduler) Close() {
	b.backgroundTaskNotifier.Cancel()
	b.backgroundTaskNotifier.BlockUntilFinish()
}

// execute the broadcaster
func (b *broadcasterScheduler) execute() {
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

func (b *broadcasterScheduler) dispatch() {
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

func (b *broadcasterScheduler) worker(no int) {
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
