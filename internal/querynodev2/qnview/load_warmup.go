package qnview

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
)

// Every LoadCollection warmup hint gets a newer epoch, which the query views
// built for it carry. Warming never gates readiness: a physical base serves its
// bindings as soon as they are prepared, and is warmed in the background for
// the newest epoch of those bindings, once per epoch, without a reload. Warms
// run on their own workers, so they never hold up reconciliation on the node
// scheduler. A running warm is an operation of its resource, and it is canceled
// only when its base retires or its resource closes, after the last query using
// the base has released it, so the cancel never fails a query that shares one
// of its cache loads. A resource starts closing while queries may still hold its
// bases, so a warm runs on a context the resource's cancel does not reach.

// warmupRetryDelay is the jittered backoff before retrying the warm of a base
// that failed the given number of times in a row.
func warmupRetryDelay(failures int) time.Duration {
	backoff := min(time.Second<<min(failures-1, 6), time.Minute)
	return backoff/2 + rand.N(backoff/2)
}

// segmentWarmTask warms a physical base for a warmup epoch. It is queued,
// running, or waiting to retry after a failure.
type segmentWarmTask struct {
	resource *segmentResource
	base     *segmentBase
	epoch    int64
	// running is set while a worker runs the warm as an operation of the
	// resource; cancel stops it.
	running bool
	ctx     context.Context
	cancel  context.CancelFunc
}

// warmupEpochLocked returns the newest warmup epoch of the bindings prepared on
// the base, or the epoch it is warmed for when that is newer.
//
// Must be called under manager.mu.
func (r *segmentResource) warmupEpochLocked(base *segmentBase) int64 {
	epoch := base.warmedEpoch
	for _, b := range r.bindings {
		if b.prepared != nil && b.prepared.base == base {
			epoch = max(epoch, b.view.req.Meta.GetWarmupEpoch())
		}
	}
	return epoch
}

// startWarmsLocked returns new tasks warming the bases whose bindings need a
// newer warmup epoch than they are warmed for. A warm that has not started
// adopts the newer epoch; a running one started before the newer request, so
// its completion wakes the resource to warm again.
//
// Must be called under manager.mu.
func (r *segmentResource) startWarmsLocked() []*segmentWarmTask {
	var tasks []*segmentWarmTask
	for _, base := range r.bases {
		epoch := r.warmupEpochLocked(base)
		switch {
		case epoch == base.warmedEpoch:
		case base.warm == nil:
			base.warm = &segmentWarmTask{resource: r, base: base, epoch: epoch}
			tasks = append(tasks, base.warm)
		case !base.warm.running:
			base.warm.epoch = epoch
		}
	}
	return tasks
}

// submitWarms queues warm tasks for at most one worker per CPU, the size of
// the load executor that runs Prewarm: a warm pins its segment while it waits
// for that executor, so more workers would only hold pins that delay releases.
func (m *SegmentResourceManager) submitWarms(tasks []*segmentWarmTask) {
	if len(tasks) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.warmTasks = append(m.warmTasks, tasks...)
	for m.warmWorkers < min(len(m.warmTasks), max(hardware.GetCPUNum(), 1)) {
		m.warmWorkers++
		go m.warmWorker()
	}
}

func (m *SegmentResourceManager) warmWorker() {
	for {
		m.mu.Lock()
		if len(m.warmTasks) == 0 {
			m.warmTasks = nil
			m.warmWorkers--
			m.mu.Unlock()
			return
		}
		task := m.warmTasks[0]
		m.warmTasks[0] = nil
		m.warmTasks = m.warmTasks[1:]
		r, base := task.resource, task.base
		// Base retirement drops a warm that has not started, and a closing
		// resource takes no new operations.
		if base.warm != task || r.closing {
			m.mu.Unlock()
			continue
		}
		task.ctx, task.cancel = context.WithCancel(context.WithoutCancel(base.ctx)) //nolint:gosec // canceled by completeWarm
		task.running = true
		r.operations.Add(1)
		m.mu.Unlock()
		m.completeWarm(task, m.loader.Prewarm(task.ctx, base.segment))
		r.operations.Done()
	}
}

// cancelIdleWarmsLocked stops the running warms of a closing resource on the bases
// no query uses any more; the release of a base's last operation wakes the
// resource to come back for the others.
//
// Must be called under manager.mu.
func (r *segmentResource) cancelIdleWarmsLocked() {
	if !r.closing {
		return
	}
	for _, base := range r.bases {
		if base.operationRefs == 0 && base.warm != nil && base.warm.running {
			base.warm.cancel()
		}
	}
}

// completeWarm records a finished warm, or retries a failed one with backoff.
// Each retry warms for the newest epoch the base's bindings still need.
func (m *SegmentResourceManager) completeWarm(task *segmentWarmTask, err error) {
	r, base := task.resource, task.base
	m.mu.Lock()
	task.running = false
	if err == nil {
		base.warmedEpoch, base.warmFailures = task.epoch, 0
	}
	if err == nil || task.ctx.Err() != nil {
		base.warm = nil
		// Reconciliation retires a base whose warm was canceled, and warms one
		// whose bindings got a newer epoch while this warm ran.
		if task.ctx.Err() != nil || r.warmupEpochLocked(base) > base.warmedEpoch {
			r.wakeLocked()
		}
		m.mu.Unlock()
		task.cancel()
		return
	}
	base.warmFailures++
	// Reconciliation may adopt a newer epoch into this task until the retry
	// replaces it, so read the epoch under the lock.
	epoch, failures := task.epoch, base.warmFailures
	m.mu.Unlock()
	delay := warmupRetryDelay(failures)
	mlog.RatedWarn(task.ctx, 1, "warmup of segment failed, retry later",
		mlog.FieldSegmentID(base.segment.ID()),
		mlog.Int64("warmupEpoch", epoch),
		mlog.Int("failures", failures),
		mlog.Duration("retryAfter", delay),
		mlog.Err(err))
	task.cancel()
	time.AfterFunc(delay, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if base.warm == task {
			base.warm = nil
			r.wakeLocked()
		}
	})
}
