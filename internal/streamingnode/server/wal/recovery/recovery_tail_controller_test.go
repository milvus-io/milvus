package recovery

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/ratelimit"
)

type recordingRecoveryTailRateLimiter struct {
	mu      sync.Mutex
	actions []recoveryTailAction
	checker ratelimit.SlowdownChecker
}

func (l *recordingRecoveryTailRateLimiter) EnterSlowdownMode(checker ratelimit.SlowdownChecker) {
	l.mu.Lock()
	l.actions = append(l.actions, recoveryTailActionSlowdown)
	l.checker = checker
	l.mu.Unlock()
}

func (l *recordingRecoveryTailRateLimiter) EnterRejectMode() {
	l.mu.Lock()
	l.actions = append(l.actions, recoveryTailActionReject)
	l.mu.Unlock()
}

func (l *recordingRecoveryTailRateLimiter) EnterRecoveryMode() {
	l.mu.Lock()
	l.actions = append(l.actions, recoveryTailActionRecover)
	l.mu.Unlock()
}

func (l *recordingRecoveryTailRateLimiter) snapshot() ([]recoveryTailAction, ratelimit.SlowdownChecker) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]recoveryTailAction(nil), l.actions...), l.checker
}

func TestRecoveryTailControllerFrontiersAndPressure(t *testing.T) {
	limiter := &recordingRecoveryTailRateLimiter{}
	controller := newRecoveryTailController(&config{
		tailLowWatermarkBytes:  10,
		tailSoftWatermarkBytes: 20,
		tailHighWatermarkBytes: 30,
	}, limiter, nil)

	controller.UpdateTrackerFrontiers(25, 15, 0, time.Time{})
	assert.True(t, controller.UnderSoftPressure())
	assert.Equal(t, recoveryTailSnapshot{
		ObservedOffset:  25,
		CompletedOffset: 15,
		RecoveryTail:    25,
		Blocking:        10,
		PublishLag:      15,
	}, controller.Snapshot())
	actions, checker := limiter.snapshot()
	assert.Equal(t, []recoveryTailAction{recoveryTailActionSlowdown}, actions)
	require.NotNil(t, checker)
	assert.True(t, checker.Check())

	controller.Publish(15, 0)
	assert.False(t, controller.UnderSoftPressure())
	assert.Equal(t, recoveryTailSnapshot{
		ObservedOffset:  25,
		CompletedOffset: 15,
		PublishedOffset: 15,
		RecoveryTail:    10,
		Blocking:        10,
	}, controller.Snapshot())
	actions, checker = limiter.snapshot()
	assert.Equal(t, []recoveryTailAction{
		recoveryTailActionSlowdown,
		recoveryTailActionRecover,
	}, actions)
	assert.False(t, checker.Check())

	controller.UpdateTrackerFrontiers(45, 25, 0, time.Time{})
	assert.True(t, controller.UnderSoftPressure())
	controller.UpdateTrackerFrontiers(46, 25, 0, time.Time{})
	controller.UpdateTrackerFrontiers(50, 25, 0, time.Time{})
	actions, _ = limiter.snapshot()
	assert.Equal(t, []recoveryTailAction{
		recoveryTailActionSlowdown,
		recoveryTailActionRecover,
		recoveryTailActionReject,
	}, actions)
}

func TestRecoveryTailControllerDoesNotRegressPublishedOffset(t *testing.T) {
	controller := newRecoveryTailController(&config{
		tailLowWatermarkBytes:  10,
		tailSoftWatermarkBytes: 20,
		tailHighWatermarkBytes: 30,
	}, nil, nil)
	controller.UpdateTrackerFrontiers(40, 40, 0, time.Time{})
	controller.Publish(30, 0)
	controller.Publish(20, 0)

	assert.Equal(t, uint64(30), controller.Snapshot().PublishedOffset)
}

func TestRecoveryTailCountStartsSameSoftPressureBelowByteLowWatermark(t *testing.T) {
	limiter := &recordingRecoveryTailRateLimiter{}
	c := newRecoveryTailController(&config{
		tailLowWatermarkBytes: 4 << 30, tailSoftWatermarkBytes: 8 << 30, tailHighWatermarkBytes: 16 << 30,
		tailSoftWatermarkMessages: 6144,
	}, limiter, nil)
	c.UpdateTrackerFrontiers(1<<20, 1<<20, 6143, time.Now())
	require.False(t, c.UnderSoftPressure())
	c.UpdateTrackerFrontiers(1<<20, 1<<20, 6144, time.Now())
	require.True(t, c.UnderSoftPressure())
	require.Zero(t, c.Snapshot().Blocking, "all ACKs can complete while the tail is still unpublished")
	actions, checker := limiter.snapshot()
	require.Equal(t, []recoveryTailAction{recoveryTailActionSlowdown}, actions)
	require.True(t, checker.Check(), "new triggers must hold slowdown even below 4GB")
	c.Publish(172, 1)
	require.False(t, c.UnderSoftPressure())
	require.False(t, checker.Check())
	require.Equal(t, uint64(6143), c.Snapshot().RecoveryTailMessages)
	actions, _ = limiter.snapshot()
	require.Equal(t, []recoveryTailAction{recoveryTailActionSlowdown, recoveryTailActionRecover}, actions)
}

func TestRecoveryTailIdleBoundariesAndClockOnlyTrigger(t *testing.T) {
	for _, test := range []struct {
		name    string
		bytes   uint64
		idle    time.Duration
		minSize uint64
		timeout time.Duration
		want    bool
	}{
		{"exact-size", 512 << 20, 10 * time.Minute, 512 << 20, 10 * time.Minute, false},
		{"before-timeout", 512<<20 + 1, 10*time.Minute - time.Nanosecond, 512 << 20, 10 * time.Minute, false},
		{"exact-timeout", 512<<20 + 1, 10 * time.Minute, 512 << 20, 10 * time.Minute, true},
		{"after-timeout", 512<<20 + 1, 11 * time.Minute, 512 << 20, 10 * time.Minute, true},
		{"disabled-size", 512<<20 + 1, 11 * time.Minute, 0, 10 * time.Minute, false},
		{"disabled-timeout", 512<<20 + 1, 11 * time.Minute, 512 << 20, 0, false},
		{"negative-timeout", 512<<20 + 1, 11 * time.Minute, 512 << 20, -time.Second, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now()
			limiter := &recordingRecoveryTailRateLimiter{}
			c := newRecoveryTailController(&config{
				tailLowWatermarkBytes: 4 << 30, tailSoftWatermarkBytes: 8 << 30, tailHighWatermarkBytes: 16 << 30,
				tailIdleMinBytes: test.minSize, tailIdleTimeout: test.timeout,
			}, limiter, nil)
			c.UpdateTrackerFrontiers(test.bytes, test.bytes, 1, now)
			require.False(t, c.UnderSoftPressure())
			now = now.Add(test.idle)
			c.mu.Lock()
			action := c.refreshLocked(now)
			pressured := c.softPressureLocked(now)
			c.mu.Unlock()
			c.applyAction(action)
			require.Equal(t, test.want, pressured)
			actions, checker := limiter.snapshot()
			if !test.want {
				require.Empty(t, actions)
				return
			}
			require.Equal(t, []recoveryTailAction{recoveryTailActionSlowdown}, actions)
			require.NotNil(t, checker)
			c.UpdateTrackerFrontiers(test.bytes+1, test.bytes+1, 2, now)
			require.False(t, c.UnderSoftPressure(), "a new non-TimeTick message resets idleness")
			require.False(t, checker.Check())
		})
	}
}

func TestRecoveryTailNewPressurePreservesHardRejectionAndReset(t *testing.T) {
	limiter := &recordingRecoveryTailRateLimiter{}
	c := newRecoveryTailController(&config{
		tailLowWatermarkBytes: 4 << 30, tailSoftWatermarkBytes: 8 << 30, tailHighWatermarkBytes: 16 << 30,
		tailSoftWatermarkMessages: 6144,
	}, limiter, nil)
	now := time.Now()
	c.UpdateTrackerFrontiers(16<<30, 16<<30, 7000, now)
	c.UpdateTrackerFrontiers(1, 1, 1, now.Add(-time.Hour))
	require.Equal(t, uint64(7000), c.Snapshot().ObservedMessages, "out-of-order completion callbacks cannot regress counters")
	require.Equal(t, now, c.lastMessageAt)
	c.Publish(12<<30, 1)
	actions, checker := limiter.snapshot()
	require.Equal(t, []recoveryTailAction{recoveryTailActionReject, recoveryTailActionSlowdown}, actions)
	require.True(t, checker.Check(), "at the byte low watermark, count pressure downgrades rejection to slowdown")
	c.Reset()
	require.Equal(t, recoveryTailSnapshot{}, c.Snapshot())
	require.True(t, c.lastMessageAt.IsZero())
	require.False(t, checker.Check())
	c.Reset()
}

func TestRecoveryTailIdlePollingUpdatesLimiterWithoutNewMessages(t *testing.T) {
	limiter := &recordingRecoveryTailRateLimiter{}
	c := newRecoveryTailController(&config{
		tailLowWatermarkBytes: 4 << 30, tailSoftWatermarkBytes: 8 << 30, tailHighWatermarkBytes: 16 << 30,
		tailIdleMinBytes: 512 << 20, tailIdleTimeout: 10 * time.Minute,
	}, limiter, nil)
	c.UpdateTrackerFrontiers(600<<20, 600<<20, 1, time.Now())
	require.False(t, c.UnderSoftPressure())
	// Backdate only this instance's observation to simulate elapsed idle time;
	// do not patch the process-wide clock while background workers are running.
	last := time.Now().Add(-11 * time.Minute)
	c.mu.Lock()
	c.lastMessageAt = last
	c.mu.Unlock()
	require.True(t, c.UnderSoftPressure())
	actions, checker := limiter.snapshot()
	require.Equal(t, []recoveryTailAction{recoveryTailActionSlowdown}, actions)
	require.True(t, checker.Check())
	c.Publish(300<<20, 1)
	require.False(t, c.UnderSoftPressure())
	require.False(t, checker.Check())
	require.Equal(t, last, c.lastMessageAt, "publishing a checkpoint is not new WAL activity")
}
