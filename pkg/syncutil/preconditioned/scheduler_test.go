package preconditioned

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchedulerRunsReadyTasksInParallelWithoutImplicitKeySerialization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := New(ctx, WithRetryInterval(time.Millisecond))
	defer s.Close()

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	secondStarted := make(chan struct{})

	s.Submit(newTestTask("first", AlwaysReady{}, func(ctx context.Context) error {
		close(firstStarted)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-firstRelease:
			return nil
		}
	}))
	s.Submit(newTestTask("second", AlwaysReady{}, func(context.Context) error {
		close(secondStarted)
		return nil
	}))

	require.Eventually(t, closed(firstStarted), time.Second, time.Millisecond)
	require.Eventually(t, closed(secondStarted), time.Second, time.Millisecond)

	close(firstRelease)
	require.NoError(t, s.WaitIdle(context.Background()))
}

func TestSchedulerAfterPreconditionSerializesExplicitDependency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := New(ctx, WithRetryInterval(time.Millisecond))
	defer s.Close()

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	secondStarted := make(chan struct{})

	firstHandle := s.Submit(newTestTask("first", AlwaysReady{}, func(ctx context.Context) error {
		close(firstStarted)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-firstRelease:
			return nil
		}
	}))
	secondHandle := s.Submit(newTestTask("second", After(firstHandle), func(context.Context) error {
		close(secondStarted)
		return nil
	}))

	require.Eventually(t, closed(firstStarted), time.Second, time.Millisecond)
	assert.False(t, firstHandle.Done())
	assert.False(t, secondHandle.Done())
	assert.Never(t, closed(secondStarted), 20*time.Millisecond, time.Millisecond)

	close(firstRelease)
	require.Eventually(t, closed(secondStarted), time.Second, time.Millisecond)
	require.Eventually(t, secondHandle.Done, time.Second, time.Millisecond)
}

func TestPreconditionFunc(t *testing.T) {
	ready := atomic.Bool{}
	precondition := PreconditionFunc(func() bool {
		return ready.Load()
	})

	assert.False(t, precondition.Ready())
	ready.Store(true)
	assert.True(t, precondition.Ready())
}

func TestSchedulerRunsReadyTasksWhenAnotherTaskIsBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := New(ctx, WithRetryInterval(time.Millisecond))
	defer s.Close()

	blocked := &testPrecondition{}
	readyTaskRan := make(chan struct{})

	s.Submit(newTestTask("blocked", blocked, func(context.Context) error {
		t.Fatal("blocked task should not run before its precondition is ready")
		return nil
	}))
	s.Submit(newTestTask("ready", AlwaysReady{}, func(context.Context) error {
		close(readyTaskRan)
		return nil
	}))

	require.Eventually(t, closed(readyTaskRan), time.Second, time.Millisecond)
	assert.False(t, blocked.ready.Load())
}

func TestSchedulerRechecksPendingTaskOnNotify(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := New(ctx, WithRetryInterval(time.Millisecond))
	defer s.Close()

	precondition := &testPrecondition{}
	ran := make(chan struct{})

	s.Submit(newTestTask("blocked", precondition, func(context.Context) error {
		close(ran)
		return nil
	}))

	assert.Never(t, closed(ran), 20*time.Millisecond, time.Millisecond)
	precondition.ready.Store(true)
	s.Notify()

	require.Eventually(t, closed(ran), time.Second, time.Millisecond)
}

func TestSchedulerRetriesRunnableTaskUntilSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := New(ctx, WithRetryInterval(time.Millisecond))
	defer s.Close()

	var attempts atomic.Int32
	done := make(chan struct{})
	handle := s.Submit(newTestTask("retry", AlwaysReady{}, func(context.Context) error {
		if attempts.Add(1) < 3 {
			return errors.New("not yet")
		}
		close(done)
		return nil
	}))

	require.Eventually(t, closed(done), time.Second, time.Millisecond)
	require.Eventually(t, handle.Done, time.Second, time.Millisecond)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestSchedulerWaitIdleWaitsForPendingAndRunningTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := New(ctx, WithRetryInterval(time.Millisecond))
	defer s.Close()

	release := make(chan struct{})
	done := make(chan struct{})
	s.Submit(newTestTask("running", AlwaysReady{}, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			close(done)
			return nil
		}
	}))

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer waitCancel()
	assert.Error(t, s.WaitIdle(waitCtx))

	close(release)
	require.Eventually(t, closed(done), time.Second, time.Millisecond)
	require.NoError(t, s.WaitIdle(context.Background()))
}

type testTask struct {
	name         string
	precondition Precondition
	run          func(context.Context) error
}

func newTestTask(name string, precondition Precondition, run func(context.Context) error) Task {
	return &testTask{name: name, precondition: precondition, run: run}
}

func (t *testTask) Name() string {
	return t.name
}

func (t *testTask) Precondition() Precondition {
	return t.precondition
}

func (t *testTask) Run(ctx context.Context) error {
	return t.run(ctx)
}

type testPrecondition struct {
	ready atomic.Bool
}

func (p *testPrecondition) Ready() bool {
	return p.ready.Load()
}

func closed(ch <-chan struct{}) func() bool {
	return func() bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
}
