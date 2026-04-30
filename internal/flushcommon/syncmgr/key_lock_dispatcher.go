package syncmgr

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
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
	mu         sync.Mutex
	queues     map[K]*list.List // per-key FIFO queue of *pendingTask
	inFlight   map[K]bool       // true if a task for this key is currently running
	workerPool *conc.Pool[struct{}]
	semaphore  *syncutil.Semaphore
}

func newKeyLockDispatcher[K comparable](maxParallel int) *keyLockDispatcher[K] {
	semCap := maxParallel * 2
	if semCap < 4 {
		semCap = 4
	}
	return &keyLockDispatcher[K]{
		queues:     make(map[K]*list.List),
		inFlight:   make(map[K]bool),
		workerPool: conc.NewPool[struct{}](maxParallel, conc.WithPreAlloc(false)),
		semaphore:  syncutil.NewSemaphore(semCap),
	}
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

	// Backpressure: acquire a semaphore slot. Blocks if all slots are taken.
	// Returns early if ctx is canceled (e.g. during shutdown).
	if err := d.semaphore.Acquire(ctx); err != nil {
		return conc.Go(func() (struct{}, error) {
			return struct{}{}, err
		})
	}

	metrics.WALFlusherSyncDispatcherTaskTotal.WithLabelValues(nodeID).Inc()
	metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(nodeID).Inc()

	pt := &pendingTask{
		ctx:       ctx,
		task:      t,
		callbacks: callbacks,
		resultCh:  make(chan error, 1),
		enqueueAt: time.Now(),
	}

	// Create a Future that resolves when the task completes.
	// The goroutine spawned by conc.Go is parked on resultCh until the task
	// finishes. The number of such goroutines is bounded by the semaphore capacity.
	future := conc.Go(func() (struct{}, error) {
		err := <-pt.resultCh
		return struct{}{}, err
	})

	d.mu.Lock()
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

// tryDrainLocked dispatches the next queued task for key if no task is in-flight.
// Must be called with d.mu held.
func (d *keyLockDispatcher[K]) tryDrainLocked(key K) {
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
	onComplete := func(err error) {
		once.Do(func() {
			pt.resultCh <- err
			close(pt.resultCh)

			d.semaphore.Release()
			metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(paramtable.GetStringNodeID()).Dec()

			d.mu.Lock()
			d.inFlight[key] = false
			d.tryDrainLocked(key)
			d.mu.Unlock()
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
			for _, cb := range pt.callbacks {
				err = cb(err)
			}
			metrics.WALFlusherSyncDispatcherExecuteDuration.WithLabelValues(nodeID).Observe(time.Since(startTime).Seconds())
			onComplete(err)
			return struct{}{}, err
		})

		// Detect pool rejection (e.g., pool closed during shutdown).
		// When the pool rejects a submission, it closes f's channel synchronously
		// before Submit returns, so a non-blocking receive succeeds immediately.
		// When the pool accepts, f's channel is still open (closed only after the
		// task function completes), so the default branch is taken and this
		// goroutine exits without blocking.
		select {
		case <-f.Inner():
			onComplete(f.Err())
		default:
		}
	}()
}

// Close drains all remaining queued tasks across all keys, notifying each
// pending Future with context.Canceled. Should be called after the worker pool
// has been released to clean up tasks that were never dispatched.
func (d *keyLockDispatcher[K]) Close() {
	nodeID := paramtable.GetStringNodeID()
	err := context.Canceled
	d.mu.Lock()
	defer d.mu.Unlock()
	for key, q := range d.queues {
		for q.Len() > 0 {
			elem := q.Front()
			q.Remove(elem)
			pt := elem.Value.(*pendingTask)
			pt.resultCh <- err
			close(pt.resultCh)
			d.semaphore.Release()
			metrics.WALFlusherSyncDispatcherPendingTasks.WithLabelValues(nodeID).Dec()
		}
		delete(d.queues, key)
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
