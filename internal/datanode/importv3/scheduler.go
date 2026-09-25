// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

// The import V3 node slot scheduler. It is pure admission policy: the task
// queue and every task's state live in the TaskManager, and the scheduler only
// decides which queued task starts next and asks the manager to start it.
//
// Its view of the node is deliberately narrow: only import V3 tasks flow
// through it, and only their own slots count against the node's total slot
// capacity. Index-build and compaction usage is not consulted here; DataCoord's
// global water-filling already accounts for every executor through QuerySlot.
//
// Backpressure works in both directions:
//   - admission: a task starts only when capacity - usage covers its slot;
//   - reporting: queued slots count as used in QuerySlot, so DataCoord stops
//     water-filling new tasks onto this node instead of over-assigning.
// The queue is unbounded by design: DataCoord's own slot accounting bounds how
// many tasks ever reach this node, so the queue cannot grow without limit.
// A queued task answers Pending to Query (see TaskManager.Query), never "not
// found", so DataCoord waits for its slot instead of retrying the task
// elsewhere.
//
// The admission loop is edge-triggered with a periodic resync, the same shape
// as a controller: a Submit, a Drop or a task completion wakes the loop
// (Notify), and decisions are always computed from the store's current state.
// The one-second ticker is a safety net for a missed wakeup, not the primary
// trigger.

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Candidate is one queued import V3 worker task as seen by the scheduler: the
// identity, slot cost and enqueue time it needs to decide admission. The
// scheduler keeps no task state of its own.
type Candidate struct {
	TaskID   int64
	RunID    int64
	Kind     string
	Slot     int64
	Enqueued time.Time
}

// TaskStore is the task-state surface the scheduler reads and acts on. The
// TaskManager implements it; the queue lives there, so the scheduler never owns
// task state.
type TaskStore interface {
	// Slots reports the slots that started (running or awaiting execution)
	// tasks hold. Queued tasks are reported through Queued instead.
	Slots() int64
	// PendingTasks returns the queued tasks in FIFO admission order.
	PendingTasks() []Candidate
	// Queued reports the number and slot cost of tasks still waiting to start.
	Queued() (tasks int, slots int64)
	// Stats returns the per-kind state distribution of started tasks.
	Stats() map[string]map[datapb.ImportTaskStateV2]int
	// Start admits one queued task into execution. It is idempotent for a run
	// that already started.
	Start(taskID, runID int64) error
}

// Scheduler admits import V3 worker tasks into the node's slot budget. It owns
// no queue and no task state: starting a task hands it to the task manager.
type Scheduler struct {
	// capacity reports the node's total slot budget. Reading it through a
	// function keeps the scheduler decoupled from the slot policy.
	capacity func() int64
	store    TaskStore
	metrics  *Metrics

	wakeChan  chan struct{}
	closeChan chan struct{}
	closeOnce sync.Once
}

func NewScheduler(capacity func() int64, store TaskStore, metrics *Metrics) *Scheduler {
	return &Scheduler{
		capacity:  capacity,
		store:     store,
		metrics:   metrics,
		wakeChan:  make(chan struct{}, 1),
		closeChan: make(chan struct{}),
	}
}

// Notify wakes the admission loop. It never blocks: a pending wakeup already
// covers the new one, so concurrent Submits, Drops and completions coalesce
// into a single admission pass.
func (s *Scheduler) Notify() {
	if s == nil {
		return
	}
	select {
	case s.wakeChan <- struct{}{}:
	default:
	}
}

// admit starts every queued task that fits the node's free slots, in queue
// order. A task that does not fit keeps its place and is retried on the next
// cycle; smaller tasks behind it may still start (no head-of-line blocking).
// A task whose Start fails is dropped: its executor-side retry resubmits.
//
// A task whose own slot exceeds the node's total capacity can never satisfy
// usage+slot <= capacity while anything else runs, so once the node has no
// import V3 task running it is admitted on its own instead of being queued
// forever: DataCoord assigned it here, so the node owes it a run.
//
// The pass is level-triggered: it reads the current used slots and the current
// queue and recomputes every decision, so a missed or duplicated wakeup cannot
// corrupt the outcome.
func (s *Scheduler) admit() {
	if s == nil || s.store == nil {
		return
	}
	used, capacity := s.store.Slots(), s.capacity()
	for _, c := range s.store.PendingTasks() {
		if used > 0 && used+c.Slot > capacity {
			continue
		}
		// Start runs with no scheduler lock held: it may block on the
		// executor's worker pool, and that backpressure must never stall the
		// scheduler or the RPC callers that woke it.
		if err := s.store.Start(c.TaskID, c.RunID); err != nil {
			mlog.Warn(context.TODO(), "slot scheduler failed to start task, dropping it",
				mlog.FieldTaskID(c.TaskID), mlog.Int64("runID", c.RunID),
				mlog.String("kind", c.Kind), mlog.Err(err))
			continue
		}
		used += c.Slot
		s.metrics.ObserveQueueLatency(c.Kind, time.Since(c.Enqueued))
		mlog.Info(context.TODO(), "slot scheduler started task",
			mlog.FieldTaskID(c.TaskID), mlog.Int64("runID", c.RunID),
			mlog.String("kind", c.Kind), mlog.Int64("slot", c.Slot),
			mlog.Duration("queueWait", time.Since(c.Enqueued)))
	}
	queuedTasks, queuedSlots := s.store.Queued()
	s.metrics.SetQueue(queuedTasks, queuedSlots)
	s.metrics.SetTaskStates(s.store.Stats())
}

// Start runs the admission loop until Close. It is a singleton loop like the
// other datanode schedulers, so a plain ticker is used instead of a pool.
func (s *Scheduler) Start() {
	mlog.Info(context.TODO(), "start slot scheduler")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.closeChan:
			mlog.Info(context.TODO(), "slot scheduler exited")
			return
		case <-s.wakeChan:
			s.admit()
		case <-ticker.C:
			s.admit()
		}
	}
}

func (s *Scheduler) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		close(s.closeChan)
	})
}
