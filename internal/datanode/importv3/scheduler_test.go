// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// fakeStore is a scripted TaskStore: a mutable used-slot counter, a FIFO
// candidate queue and a start log. Start removes the candidate and records its
// id; store.used is left untouched so the scheduler's own reservation is what
// bounds a pass.
type fakeStore struct {
	mu       sync.Mutex
	used     int64
	queue    []Candidate
	started  []int64
	startErr map[int64]error
	stats    map[string]map[datapb.ImportTaskStateV2]int
}

func (f *fakeStore) Slots() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.used
}

func (f *fakeStore) PendingTasks() []Candidate {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Candidate, len(f.queue))
	copy(out, f.queue)
	return out
}

func (f *fakeStore) Queued() (int, int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var slots int64
	for _, c := range f.queue {
		slots += c.Slot
	}
	return len(f.queue), slots
}

func (f *fakeStore) Stats() map[string]map[datapb.ImportTaskStateV2]int { return f.stats }

func (f *fakeStore) Start(taskID, runID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, c := range f.queue {
		if c.TaskID == taskID && c.RunID == runID {
			f.queue = append(f.queue[:i], f.queue[i+1:]...)
			break
		}
	}
	if err := f.startErr[taskID]; err != nil {
		return err
	}
	f.started = append(f.started, taskID)
	return nil
}

func (f *fakeStore) queued() int {
	n, _ := f.Queued()
	return n
}

func candidateAt(taskID, runID, slot int64) Candidate {
	return Candidate{TaskID: taskID, RunID: runID, Kind: "reshard", Slot: slot, Enqueued: time.Now()}
}

func newTestScheduler(capacity int64, store *fakeStore) *Scheduler {
	return NewScheduler(func() int64 { return capacity }, store, NewMetrics(1))
}

func TestSlotSchedulerStartsWhenFree(t *testing.T) {
	store := &fakeStore{queue: []Candidate{candidateAt(10, 1, 2)}}
	s := newTestScheduler(4, store)
	s.admit()
	require.Equal(t, []int64{10}, store.started)
	require.Zero(t, store.queued())
}

func TestSlotSchedulerQueuesAndStartsAfterFreed(t *testing.T) {
	store := &fakeStore{used: 3, queue: []Candidate{candidateAt(11, 1, 2)}}
	s := newTestScheduler(4, store)
	s.admit()
	require.Empty(t, store.started, "an over-slot task must stay queued")
	require.Equal(t, 1, store.queued())
	_, queuedSlots := store.Queued()
	require.Equal(t, int64(2), queuedSlots)

	store.used = 1
	s.admit()
	require.Equal(t, []int64{11}, store.started)
	require.Zero(t, store.queued())
}

// A task that does not fit keeps its place; a smaller task behind it may still
// start (no head-of-line blocking), and the skipped task runs once slots free.
func TestSlotSchedulerSkipsTaskThatDoesNotFit(t *testing.T) {
	store := &fakeStore{used: 3, queue: []Candidate{candidateAt(12, 1, 2), candidateAt(13, 1, 1)}}
	s := newTestScheduler(4, store)
	s.admit()
	require.Equal(t, []int64{13}, store.started)
	require.Equal(t, 1, store.queued(), "the skipped task keeps its place")

	store.used = 1
	s.admit()
	require.Equal(t, []int64{13, 12}, store.started)
}

// A task whose slot exceeds the node's total capacity can never fit alongside
// anything, so it must still run alone once the node is idle instead of being
// queued forever.
func TestSlotSchedulerRunsOverCapacityTaskAlone(t *testing.T) {
	store := &fakeStore{queue: []Candidate{candidateAt(20, 1, 8)}}
	s := newTestScheduler(4, store)
	s.admit()
	require.Equal(t, []int64{20}, store.started, "an over-capacity task must run, not queue forever")

	store.used = 8
	store.queue = append(store.queue, candidateAt(21, 1, 1))
	s.admit()
	require.Equal(t, []int64{20}, store.started, "a task must not overlap the over-capacity run")
	require.Equal(t, 1, store.queued())

	store.used = 0
	s.admit()
	require.Equal(t, []int64{20, 21}, store.started)
}

func TestSlotSchedulerDropsFailedStart(t *testing.T) {
	store := &fakeStore{
		queue:    []Candidate{candidateAt(16, 1, 1)},
		startErr: map[int64]error{16: errors.New("boom")},
	}
	s := newTestScheduler(4, store)
	s.admit()
	require.Empty(t, store.started, "a task whose Start fails is dropped, not stuck")
	require.Zero(t, store.queued())
}

func TestSlotSchedulerNilStoreAndStatsDoNotPanic(t *testing.T) {
	require.NotPanics(t, func() { NewScheduler(func() int64 { return 4 }, nil, NewMetrics(1)).admit() })
	store := &fakeStore{stats: map[string]map[datapb.ImportTaskStateV2]int{"reshard": {datapb.ImportTaskStateV2_InProgress: 1}}}
	s := newTestScheduler(4, store)
	require.NotPanics(t, func() { s.admit() })
}

func TestSlotSchedulerNotifyCoalescesAndCloseStops(t *testing.T) {
	store := &fakeStore{}
	s := newTestScheduler(4, store)
	// Repeated wakeups must not block even with no admission loop running.
	require.NotPanics(t, func() { s.Notify(); s.Notify(); s.Notify() })

	done := make(chan struct{})
	go func() {
		s.Start()
		close(done)
	}()
	s.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop after Close")
	}
}
