package idf

import (
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
)

// The background warms of oracles that serve QueryViews with a LoadCollection
// warmup hint share one node-wide queue, drained in arrival order by at most a
// CPU count of workers started on demand, as on QueryNode. Every hinted view
// requests a warm when a StreamingNode recovers its views, and a warm waiting
// for a worker or a retry holds no goroutine.
var warms warmQueue

type warmQueue struct {
	mu      sync.Mutex
	pending []*oracleRuntime
	workers int
}

func (q *warmQueue) submit(r *oracleRuntime) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, r)
	if q.workers < hardware.GetCPUNum() {
		q.workers++
		go q.work()
	}
}

func (q *warmQueue) work() {
	for {
		q.mu.Lock()
		if len(q.pending) == 0 {
			q.pending = nil
			q.workers--
			q.mu.Unlock()
			return
		}
		r := q.pending[0]
		q.pending[0] = nil
		q.pending = q.pending[1:]
		q.mu.Unlock()
		r.warmStep()
	}
}

// warm queues a background materialization of a lazy aggregate, as the first
// BM25 query would. At most one warm per oracle is queued, running or waiting
// to retry.
func (r *oracleRuntime) warm() {
	r.mu.RLock()
	needed := !r.closed && r.currentStats == nil
	r.mu.RUnlock()
	if needed && r.warming.CompareAndSwap(false, true) {
		warms.submit(r)
	}
}

// warmStep makes one materialization attempt. A failure, or a newer target
// that canceled the attempt, queues the next attempt after a jittered backoff
// from 1 second up to 1 minute. The warm ends once the aggregate is
// materialized or the oracle closes.
func (r *oracleRuntime) warmStep() {
	retry, err := r.awaitMaterialization(r.ctx)
	if err == nil || r.ctx.Err() != nil {
		r.warming.Store(false)
		return
	}
	r.mu.Lock()
	if r.warmRetry == nil {
		r.warmRetry = backoff.NewExponentialBackOff()
		r.warmRetry.InitialInterval = time.Second
		r.warmRetry.MaxInterval = time.Minute
		r.warmRetry.MaxElapsedTime = 0
		r.warmRetry.Reset()
	}
	delay := r.warmRetry.NextBackOff()
	r.mu.Unlock()
	if !retry {
		mlog.RatedWarn(r.ctx, 1, "BM25 statistics warmup failed, retry later",
			mlog.FieldCollectionID(r.collectionID),
			mlog.FieldVChannel(r.vchannel),
			mlog.Duration("retryAfter", delay),
			mlog.Err(err))
	}
	time.AfterFunc(delay, func() { warms.submit(r) })
}
