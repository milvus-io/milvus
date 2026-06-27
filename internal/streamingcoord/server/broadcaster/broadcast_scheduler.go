package broadcaster

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// newBroadcasterScheduler creates a new broadcaster scheduler.
func newBroadcasterScheduler(pendings []*pendingBroadcastTask, logger *mlog.Logger) *broadcasterScheduler {
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
	mlog.Binder

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
		// The broadcaster is closing while a task is still being submitted. This is
		// reachable under concurrent shutdown: broadcastTaskManager.Close cancels the
		// broadcaster before the ack scheduler, so an in-flight
		// doForcePromoteFixIncompleteBroadcasts goroutine can still deliver a supplement
		// task here after the background queue is gone. Returning an error instead of
		// panicking lets the caller abort gracefully; the task stays incomplete and is
		// re-driven on the next startup. See #50550 for the sibling fix in
		// tombstoneScheduler.AddPending.
		return nil, status.NewOnShutdownError("broadcaster is closing, cannot add new task")
	case b.pendingChan <- task:
	}

	// Wait both request context and the background task context.
	ctx, cancel := contextutil.MergeContext(ctx, b.backgroundTaskNotifier.Context())
	defer cancel()
	// wait for all the vchannels acked.
	result, err := task.BlockUntilDone(ctx)
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
	b.Logger().Info(context.TODO(),

		"broadcaster start to execute", mlog.Int("workerNum", workers))

	defer func() {
		b.backgroundTaskNotifier.Finish(struct{}{})
		b.Logger().Info(context.TODO(),

			"broadcaster execute exit")
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
			b.Logger().Info(context.TODO(),

				"backoff task", mlog.Duration("nextInterval", nextInterval))
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
	logger := b.Logger().With(mlog.Int("workerNo", no))
	defer func() {
		logger.Info(context.TODO(), "broadcaster worker exit")
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
