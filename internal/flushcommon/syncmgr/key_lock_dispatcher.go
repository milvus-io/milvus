package syncmgr

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type Task interface {
	SegmentID() int64
	Checkpoint() *msgpb.MsgPosition
	StartPosition() *msgpb.MsgPosition
	ChannelName() string
	Run(context.Context) error
	HandleError(error)
	IsFlush() bool
	IsDrop() bool
}

// pendingTask wraps a task queued for execution.
type pendingTask struct {
	ctx       context.Context
	task      Task
	callbacks []func(error) error
	resultCh  chan error // buffered(1); result sent then channel closed on completion
	enqueueAt time.Time  // for queue duration metric
}

// keyLockDispatcher provides per-key serial execution with cross-key concurrency.
//
// For each key, tasks are queued in FIFO order and executed one at a time.
// Different keys execute concurrently up to the worker pool capacity.
// A semaphore limits total pending (queued + in-flight) tasks to provide backpressure.
type keyLockDispatcher[K comparable] struct {
	mu          sync.Mutex
	queues      map[K]*list.List // per-key FIFO queue of *pendingTask
	inFlight    map[K]bool       // true if a task for this key is currently running
	closed      bool
	closeCtx    context.Context
	closeCancel context.CancelFunc
	workerPool  *conc.Pool[struct{}]
	semaphore   *syncutil.Semaphore
	accepted    int
	completed   *syncutil.ContextCond
}

func newKeyLockDispatcher[K comparable](maxParallel int) *keyLockDispatcher[K] {
	// closeCancel is retained by the dispatcher and invoked exactly once by
	// beginClose; it cannot be deferred in this constructor.
	//nolint:gosec
	closeCtx, closeFn := context.WithCancel(context.Background())
	semCap := maxParallel * 2
	if semCap < 4 {
		semCap = 4
	}
	dispatcher := &keyLockDispatcher[K]{
		queues:      make(map[K]*list.List),
		inFlight:    make(map[K]bool),
		closeCtx:    closeCtx,
		closeCancel: closeFn,
		workerPool:  conc.NewPool[struct{}](maxParallel, conc.WithPreAlloc(false)),
		semaphore:   syncutil.NewSemaphore(semCap),
	}
	dispatcher.completed = syncutil.NewContextCond(&dispatcher.mu)
	return dispatcher
}

// Submit enqueues a task for the given key and returns a Future.
//
// If no task for this key is currently in-flight, the task is dispatched to the
// worker pool immediately. Otherwise it is queued and will be dispatched when
// the current in-flight task for this key completes.
//
// Backpressure: blocks the caller when total pending tasks reach the semaphore
// capacity. This is the mechanism that slows down the pipeline goroutine when
// sync throughput cannot keep up with the write rate. The caller can cancel via
// ctx to unblock during shutdown.
func (d *keyLockDispatcher[K]) Submit(ctx context.Context, key K, t Task, callbacks ...func(error) error) *conc.Future[struct{}] {
	nodeID := paramtable.GetStringNodeID()

	// The close fence and completion accounting share d.mu. Every Submit that
	// crosses this gate before beginClose is included in waitClosed, including
	// calls still blocked on semaphore admission or asynchronous pool handoff.
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		pt, future := newPendingTaskFuture(ctx, t, callbacks)
		panicValue := d.finishDetached(pt, context.Canceled, false, false)
		repanicAsync(panicValue)
		return future
	}
	d.accepted++
	d.mu.Unlock()

	// Backpressure: acquire a semaphore slot. Blocks if all slots are taken.
	// Returns early if ctx is canceled (e.g. during shutdown).
	acquireCtx, cancelAcquire := context.WithCancel(ctx)
	stopCloseCancel := context.AfterFunc(d.closeCtx, cancelAcquire)
	err := d.semaphore.Acquire(acquireCtx)
	stopCloseCancel()
	cancelAcquire()
	if err != nil {
		pt, future := newPendingTaskFuture(ctx, t, callbacks)
		panicValue := d.finishDetached(pt, err, false, true)
		repanicAsync(panicValue)
		return future
	}

	metrics.WALFlusherSyncDispatcherTaskTotal.WithLabelValues(nodeID).Inc()
	metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(nodeID).Inc()

	pt, future := newPendingTaskFuture(ctx, t, callbacks)

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		panicValue := d.finishDetached(pt, context.Canceled, true, true)
		repanicAsync(panicValue)
		return future
	}
	q, ok := d.queues[key]
	if !ok {
		q = list.New()
		d.queues[key] = q
	}
	q.PushBack(pt)
	d.tryDrainLocked(key)
	d.mu.Unlock()

	return future
}

func newPendingTaskFuture(ctx context.Context, task Task, callbacks []func(error) error) (*pendingTask, *conc.Future[struct{}]) {
	pt := &pendingTask{
		ctx:       ctx,
		task:      task,
		callbacks: callbacks,
		resultCh:  make(chan error, 1),
		enqueueAt: time.Now(),
	}
	future := conc.Go(func() (struct{}, error) {
		err := <-pt.resultCh
		return struct{}{}, err
	})
	return pt, future
}

// runCallbacks is deliberately panic-tolerant per callback. HandleError is the
// first callback installed by SyncManager and may be fatal; write-buffer
// lifecycle callbacks after it still have to release segment state and payload
// ownership before that panic is propagated.
func runCallbacks(initialErr error, callbacks []func(error) error) (any, error) {
	finalErr := initialErr
	var firstPanic any
	var panicResultErr error
	for _, callback := range callbacks {
		if firstPanic != nil {
			// Once a callback panics, every remaining lifecycle callback must see
			// the preserved failure even if an earlier cleanup callback returned nil.
			finalErr = panicResultErr
		}
		func() {
			defer func() {
				if panicValue := recover(); panicValue != nil {
					if firstPanic == nil {
						firstPanic = panicValue
						// Preserve the error being handled when the first panic happened
						// so downstream lifecycle callbacks and the Future retain its
						// errors.Is/merr identity. A panic on an otherwise successful
						// attempt becomes a System error instead of reported success.
						panicResultErr = finalErr
						if panicResultErr == nil {
							panicResultErr = merr.WrapErrServiceInternalMsg("sync task callback panicked: %v", panicValue)
						}
					}
					finalErr = panicResultErr
				}
			}()
			finalErr = callback(finalErr)
		}()
	}
	if firstPanic != nil {
		finalErr = panicResultErr
	}
	return firstPanic, finalErr
}

func publishPendingTaskResult(pt *pendingTask, err error) {
	pt.resultCh <- err
	close(pt.resultCh)
}

func repanicAsync(panicValue any) {
	if panicValue == nil {
		return
	}
	go func() {
		panic(panicValue)
	}()
}

func (d *keyLockDispatcher[K]) finishAccepted() {
	d.completed.LockAndBroadcast()
	d.accepted--
	if d.accepted < 0 {
		d.mu.Unlock()
		panic("sync dispatcher completed more tasks than it accepted")
	}
	d.mu.Unlock()
}

// finishDetached completes a task that will never enter the worker pool. It
// releases any admission slot before callbacks (so callbacks may re-enter),
// runs every callback, and publishes the Future result last. The caller decides
// when to re-panic so Close can drain all detached tasks first.
func (d *keyLockDispatcher[K]) finishDetached(pt *pendingTask, err error, releaseSlot, accepted bool) any {
	if releaseSlot {
		d.semaphore.Release()
		metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(paramtable.GetStringNodeID()).Dec()
	}
	panicValue, finalErr := runCallbacks(err, pt.callbacks)
	publishPendingTaskResult(pt, finalErr)
	if accepted {
		d.finishAccepted()
	}
	return panicValue
}

// tryDrainLocked dispatches the next queued task for key if no task is in-flight.
// Must be called with d.mu held.
func (d *keyLockDispatcher[K]) tryDrainLocked(key K) {
	if d.closed {
		delete(d.queues, key)
		delete(d.inFlight, key)
		return
	}
	if d.inFlight[key] {
		return
	}
	q, ok := d.queues[key]
	if !ok || q.Len() == 0 {
		delete(d.queues, key)
		delete(d.inFlight, key)
		return
	}

	elem := q.Front()
	q.Remove(elem)
	if q.Len() == 0 {
		delete(d.queues, key)
	}

	pt := elem.Value.(*pendingTask)
	d.inFlight[key] = true

	d.dispatchLocked(key, pt)
}

// dispatchLocked submits a task to the worker pool.
// Must be called with d.mu held. Uses a goroutine to avoid deadlock when called
// from within a worker's completion path (the current worker hasn't returned to
// the pool yet, so a direct workerPool.Submit would block waiting for a free slot).
//
// The cleanup logic (notify resultCh, release semaphore, reset inFlight, drain queue)
// is guarded by sync.Once to handle the race between normal task completion and pool
// rejection (e.g., during shutdown). Both paths call onComplete; only the first wins.
func (d *keyLockDispatcher[K]) dispatchLocked(key K, pt *pendingTask) {
	var once sync.Once
	complete := func(err error) {
		once.Do(func() {
			d.semaphore.Release()
			metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(paramtable.GetStringNodeID()).Dec()
			panicValue, finalErr := runCallbacks(err, pt.callbacks)

			d.mu.Lock()
			d.inFlight[key] = false
			d.tryDrainLocked(key)
			d.mu.Unlock()

			// Future completion is the public completion fence: once Await returns,
			// callbacks, semaphore accounting, and per-key ownership are all clean.
			publishPendingTaskResult(pt, finalErr)
			d.finishAccepted()
			if panicValue != nil {
				panic(panicValue)
			}
		})
	}

	// Must use a goroutine for workerPool.Submit to avoid deadlock.
	// tryDrainLocked → dispatchLocked is called from within a pool worker's
	// onComplete callback, so the current worker has not yet returned its slot.
	// A direct workerPool.Submit here would block waiting for a free slot,
	// but that slot cannot be freed until this function returns — deadlock.
	// By spawning a goroutine, the current worker function can return and
	// release its slot, allowing the goroutine's Submit to proceed.
	go func() {
		f := d.workerPool.Submit(func() (struct{}, error) {
			nodeID := paramtable.GetStringNodeID()
			metrics.WALFlusherSyncDispatcherQueueDuration.WithLabelValues(nodeID).Observe(time.Since(pt.enqueueAt).Seconds())

			startTime := time.Now()
			err := pt.task.Run(pt.ctx)
			metrics.WALFlusherSyncDispatcherExecuteDuration.WithLabelValues(nodeID).Observe(time.Since(startTime).Seconds())
			complete(err)
			return struct{}{}, err
		})

		// Watch every pool future. Normal execution calls complete before the
		// future closes; pool rejection or a panic skips that call, and the watcher
		// drives the same exact-once callback/cleanup funnel instead.
		<-f.Inner()
		complete(f.Err())
	}()
}

// beginClose fences new submissions, cancels semaphore waiters, and completes
// every task that is still queued in the dispatcher. Tasks already handed to,
// or asynchronously being handed to, the worker pool remain accepted and are
// completed by their normal/rejection path.
//
// Worker-pool shutdown must happen after beginClose. Once ReleaseTimeout
// succeeds, waitClosed makes the asynchronous Submit handoff part of the Close
// completion fence without waiting forever for a worker after pool timeout.
func (d *keyLockDispatcher[K]) beginClose() any {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.closeCancel()
	pending := make([]*pendingTask, 0)
	for key, q := range d.queues {
		for q.Len() > 0 {
			elem := q.Front()
			q.Remove(elem)
			pending = append(pending, elem.Value.(*pendingTask))
		}
		delete(d.queues, key)
	}
	d.mu.Unlock()

	// Callbacks are external code and may re-enter the dispatcher or the write
	// buffer. Complete detached tasks only after releasing d.mu. Drain every
	// task even if one callback panics, then preserve the first panic after all
	// futures and semaphore slots have been cleaned up.
	var firstPanic any
	for _, pt := range pending {
		if panicValue := d.finishDetached(pt, context.Canceled, true, true); panicValue != nil && firstPanic == nil {
			firstPanic = panicValue
		}
	}
	return firstPanic
}

// waitClosed waits until every task accepted before beginClose has completed
// its callbacks, Future publication, and semaphore accounting. The context is
// the shutdown bound; callers must not wait here after worker-pool timeout.
func (d *keyLockDispatcher[K]) waitClosed(ctx context.Context) error {
	d.mu.Lock()
	for d.accepted > 0 {
		if err := d.completed.Wait(ctx); err != nil {
			return err
		}
	}
	d.mu.Unlock()
	return nil
}

// Close is the dispatcher-only close fence used by tests and callback
// re-entry. SyncManager performs the complete bounded shutdown sequence with
// beginClose, workerPool.ReleaseTimeout, and waitClosed.
func (d *keyLockDispatcher[K]) Close() {
	if panicValue := d.beginClose(); panicValue != nil {
		panic(panicValue)
	}
}

// Pending returns the total number of pending tasks (queued + in-flight).
func (d *keyLockDispatcher[K]) Pending() int {
	return d.semaphore.Current()
}

// SetSemaphoreCapacity dynamically adjusts the semaphore capacity that controls
// the maximum number of pending (queued + in-flight) tasks.
func (d *keyLockDispatcher[K]) SetSemaphoreCapacity(capacity int) {
	d.semaphore.SetCapacity(capacity)
}
