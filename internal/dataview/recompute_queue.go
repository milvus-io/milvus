// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataview

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// errRecomputeQueueFull is returned by Enqueue when the bounded queue is at
// capacity and the request is dropped; the snapshot converges via the next
// trigger or the recovery rebuild.
var errRecomputeQueueFull = errors.New("DataView recompute queue full")

// dataViewRecomputeQueue asynchronously reconciles DataView snapshots with
// SegmentMeta after Flushed->Flushed mutations (compaction, import, copy,
// refresh, truncate, partition drop). It lives inside the Manager: Recompute
// is a non-blocking request and the queue owns the deduplication and the
// worker, so all DataView reconciliation logic stays in this package. It is an
// in-memory, per-Collection deduplicated queue: a single worker drains
// collectionIDs and runs the reconciliation against the injected projection,
// so multiple pending mutations of one Collection collapse into a single
// snapshot write ("only the last view is updated"). The queue is best-effort -
// a lost entry (crash, failed recompute, dropped enqueue) is converged by the
// recovery rebuild.
type dataViewRecomputeQueue struct {
	manager *dataViewManager

	mu      sync.Mutex
	ch      chan int64 // bounded; over-capacity enqueues are dropped
	pending map[int64]struct{}
}

func newDataViewRecomputeQueue(manager *dataViewManager) *dataViewRecomputeQueue {
	return &dataViewRecomputeQueue{
		manager: manager,
		ch:      make(chan int64, 1024),
		pending: make(map[int64]struct{}),
	}
}

// Enqueue requests a reconciliation for collectionID. It is non-blocking and
// deduplicated: a Collection already queued is not queued again; the next
// drain reads the latest SegmentMeta anyway, so one entry per burst suffices.
// When the queue is full the enqueue is dropped - errRecomputeQueueFull is
// returned so callers and monitoring can observe the gap - rather than
// blocking a coordinator RPC; the recovery rebuild converges the snapshot.
func (q *dataViewRecomputeQueue) Enqueue(collectionID int64) error {
	q.mu.Lock()
	if _, ok := q.pending[collectionID]; ok {
		q.mu.Unlock()
		return nil
	}
	q.pending[collectionID] = struct{}{}
	q.mu.Unlock()

	select {
	case q.ch <- collectionID:
		return nil
	default:
		// Queue full: unmark and drop; the recovery rebuild converges.
		q.mu.Lock()
		delete(q.pending, collectionID)
		q.mu.Unlock()
		mlog.Warn(q.manager.workerCtx, "DataView recompute queue full, dropping enqueue",
			mlog.Int64("collectionID", collectionID))
		return errRecomputeQueueFull
	}
}

// run drains the queue until ctx is cancelled. A failed reconciliation is
// retried by re-enqueueing after a short pause; a persistently failing
// projection is ultimately converged by the recovery rebuild.
func (q *dataViewRecomputeQueue) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case collectionID := <-q.ch:
			q.mu.Lock()
			delete(q.pending, collectionID)
			q.mu.Unlock()
			projector := q.manager.getProjector()
			if _, err := q.manager.recomputeNow(ctx, collectionID, projector); err != nil {
				mlog.Warn(ctx, "DataView Recompute failed, re-enqueue",
					mlog.Int64("collectionID", collectionID),
					mlog.Err(err))
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
					return
				}
				_ = q.Enqueue(collectionID)
			}
		}
	}
}
