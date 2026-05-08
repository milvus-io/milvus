// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package syncutil

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

// fakeTask is a hand-rolled StagedTask used to drive the StagedScheduler through
// specific code paths (ErrContinue, retryable error, terminal error, success).
type fakeTask struct {
	cpuBound bool
	// pollResults is consumed one per call. Empty means "return nil".
	pollResults []error
	pollCount   atomic.Int32
	// if true, flip cpuBound after every poll so the next stage runs on the other pool.
	flipOnPoll bool

	doneCh   chan error
	doneOnce atomic.Bool
}

func newFakeTask(cpuBound bool, pollResults ...error) *fakeTask {
	return &fakeTask{
		cpuBound:    cpuBound,
		pollResults: pollResults,
		doneCh:      make(chan error, 1),
	}
}

func (f *fakeTask) CPUBound() bool { return f.cpuBound }

func (f *fakeTask) Poll(ctx context.Context) error {
	if ctx.Err() != nil {
		f.complete(ErrStagedSchedulerClosed)
		return nil
	}
	i := int(f.pollCount.Add(1)) - 1
	if i >= len(f.pollResults) {
		f.complete(nil)
		return nil
	}
	if f.flipOnPoll {
		f.cpuBound = !f.cpuBound
	}
	err := f.pollResults[i]
	if err == nil || (!errors.Is(err, ErrContinue) && !IsRetryable(err)) {
		f.complete(err)
	}
	return err
}

func (f *fakeTask) complete(err error) {
	if f.doneOnce.Swap(true) {
		panic("task completed more than once")
	}
	f.doneCh <- err
}

func (f *fakeTask) Key() string { return "fake" }

// waitDone blocks up to d for the task to complete. Returns the completion error.
func (f *fakeTask) waitDone(t *testing.T, d time.Duration) error {
	t.Helper()
	select {
	case err := <-f.doneCh:
		return err
	case <-time.After(d):
		t.Fatalf("task did not complete within %s (polls=%d)", d, f.pollCount.Load())
		return nil
	}
}

func TestStagedScheduler_Success(t *testing.T) {
	s := NewStagedScheduler(1, 1)
	defer s.Close()

	task := newFakeTask(true /* cpu */)
	s.AddTask(task)
	assert.NoError(t, task.waitDone(t, time.Second))
	assert.Equal(t, int32(1), task.pollCount.Load())
}

func TestStagedScheduler_ErrContinueRoundtrips(t *testing.T) {
	s := NewStagedScheduler(1, 1)
	defer s.Close()

	// Stage 1: CPU work, yields (sets task to IO for the next poll).
	// Stage 2: IO work, yields (sets task to CPU for the next poll).
	// Stage 3: terminal success.
	task := newFakeTask(true, ErrContinue, ErrContinue)
	task.flipOnPoll = true
	s.AddTask(task)

	assert.NoError(t, task.waitDone(t, time.Second))
	assert.Equal(t, int32(3), task.pollCount.Load())
}

func TestStagedScheduler_RetryableErrorEventuallySucceeds(t *testing.T) {
	s := NewStagedScheduler(1, 1)
	defer s.Close()

	// First poll fails retryably; second poll succeeds.
	task := newFakeTask(false, NewRetryableError(errors.New("remote store offline")))
	start := time.Now()
	s.AddTask(task)

	err := task.waitDone(t, 3*time.Second)
	assert.NoError(t, err)
	// Retry backoff floor is 100ms, so the second poll must arrive at least that late.
	assert.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)
	assert.Equal(t, int32(2), task.pollCount.Load())
}

func TestStagedScheduler_RetryBackoffIncreasesAcrossAttempts(t *testing.T) {
	s := NewStagedScheduler(1, 1)
	defer s.Close()

	task := newFakeTask(
		false,
		NewRetryableError(errors.New("remote store offline")),
		NewRetryableError(errors.New("remote store still offline")),
	)
	start := time.Now()
	s.AddTask(task)

	assert.NoError(t, task.waitDone(t, 3*time.Second))
	assert.GreaterOrEqual(t, time.Since(start), 250*time.Millisecond)
	assert.Equal(t, int32(3), task.pollCount.Load())
}

func TestStagedScheduler_UnretryableErrorFailsOnce(t *testing.T) {
	s := NewStagedScheduler(1, 1)
	defer s.Close()

	fatal := errors.New("bad schema")
	task := newFakeTask(true, fatal)
	s.AddTask(task)

	err := task.waitDone(t, time.Second)
	assert.ErrorIs(t, err, fatal)
	// Only one Poll — the error is terminal.
	assert.Equal(t, int32(1), task.pollCount.Load())
}

// blockingTask's Poll blocks until ctx is canceled, then completes itself with
// ErrStagedSchedulerClosed.
type blockingTask struct {
	cpuBound atomic.Bool
	doneCh   chan error
	doneOnce atomic.Bool
	polled   chan struct{} // closed on first Poll entry
	once     atomic.Bool
}

func (b *blockingTask) CPUBound() bool { return b.cpuBound.Load() }
func (b *blockingTask) Key() string    { return "block" }
func (b *blockingTask) Poll(ctx context.Context) error {
	if !b.once.Swap(true) {
		close(b.polled)
	}
	<-ctx.Done()
	if b.doneOnce.Swap(true) {
		panic("task completed more than once")
	}
	b.doneCh <- ErrStagedSchedulerClosed
	return nil
}

func TestStagedScheduler_CloseCancelsInFlight(t *testing.T) {
	s := NewStagedScheduler(1, 1)

	b := &blockingTask{
		doneCh: make(chan error, 1),
		polled: make(chan struct{}),
	}
	b.cpuBound.Store(true)
	s.AddTask(b)

	// Wait for the task to be picked up and begin polling.
	select {
	case <-b.polled:
	case <-time.After(time.Second):
		t.Fatal("task was never dispatched")
	}
	s.Close()

	select {
	case err := <-b.doneCh:
		assert.ErrorIs(t, err, ErrStagedSchedulerClosed)
	case <-time.After(time.Second):
		t.Fatal("task was not completed after Close")
	}
}

func TestStagedScheduler_CloseDrainsQueued(t *testing.T) {
	s := NewStagedScheduler(0 /* cpu */, 0 /* io */)

	// Zero pool size means the task can never actually run; Close must drain it.
	task := newFakeTask(true, ErrContinue)
	s.AddTask(task)
	s.Close()

	err := task.waitDone(t, time.Second)
	assert.ErrorIs(t, err, ErrStagedSchedulerClosed)
}

func TestStagedScheduler_AddTaskAfterClose(t *testing.T) {
	s := NewStagedScheduler(1, 1)
	s.Close()

	task := newFakeTask(true)
	s.AddTask(task)

	err := task.waitDone(t, time.Second)
	assert.ErrorIs(t, err, ErrStagedSchedulerClosed)
}

func TestStagedScheduler_DrainAfterClose(t *testing.T) {
	s := NewStagedScheduler(1, 1)
	s.Close()

	assert.ErrorIs(t, s.Drain(context.Background()), ErrStagedSchedulerClosed)
}

func TestStagedScheduler_DrainWaitsForTaskCompletion(t *testing.T) {
	s := NewStagedScheduler(1, 1)
	defer s.Close()

	task := newFakeTask(true, ErrContinue, ErrContinue)
	task.flipOnPoll = true
	s.AddTask(task)

	assert.NoError(t, s.Drain(context.Background()))
	assert.Equal(t, int32(3), task.pollCount.Load())
	assert.True(t, task.doneOnce.Load())
}

type chainedTask struct {
	scheduler *StagedScheduler
	next      StagedTask
	doneOnce  atomic.Bool
}

func (c *chainedTask) CPUBound() bool { return true }
func (c *chainedTask) Poll(context.Context) error {
	if c.doneOnce.Swap(true) {
		panic("task completed more than once")
	}
	c.scheduler.AddTask(c.next)
	return nil
}
func (c *chainedTask) Key() string { return "chained" }

func TestStagedScheduler_DrainWaitsForCompletionScheduledTask(t *testing.T) {
	s := NewStagedScheduler(1, 1)
	defer s.Close()

	next := newFakeTask(true)
	first := &chainedTask{
		scheduler: s,
		next:      next,
	}
	s.AddTask(first)

	assert.NoError(t, s.Drain(context.Background()))
	assert.True(t, first.doneOnce.Load())
	assert.True(t, next.doneOnce.Load())
}

func TestComputeBackoff_ExpGrowthCappedAt60s(t *testing.T) {
	assert.Equal(t, retryInitialInterval, computeBackoff(1))
	// Attempt N should be retryInitialInterval * multiplier^(N-1), capped at retryMaxInterval.
	assert.Greater(t, computeBackoff(2), computeBackoff(1))
	assert.Equal(t, retryMaxInterval, computeBackoff(100))
}
