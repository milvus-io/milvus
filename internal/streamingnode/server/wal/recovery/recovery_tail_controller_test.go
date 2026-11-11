package recovery

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/ratelimit"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
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
		flushSizeBytes:         20,
	}, limiter, nil)

	controller.UpdateTrackerFrontiers(25, 15, 0, time.Time{})
	assert.True(t, controller.ShouldFlush())
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
	assert.False(t, controller.ShouldFlush())
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
	assert.True(t, controller.ShouldFlush())
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
		flushSizeBytes:         20,
	}, nil, nil)
	controller.UpdateTrackerFrontiers(40, 40, 0, time.Time{})
	controller.Publish(30, 0)
	controller.Publish(20, 0)

	assert.Equal(t, uint64(30), controller.Snapshot().PublishedOffset)
}

func TestRecoveryTailCountFlushDoesNotLimitAppends(t *testing.T) {
	limiter := &recordingRecoveryTailRateLimiter{}
	c := newRecoveryTailController(&config{
		tailLowWatermarkBytes: 4 << 30, tailSoftWatermarkBytes: 8 << 30, tailHighWatermarkBytes: 16 << 30,
		flushMessageCount: 6144,
	}, limiter, nil)
	c.UpdateTrackerFrontiers(1<<20, 1<<20, 6143, time.Now())
	require.False(t, c.ShouldFlush())
	c.UpdateTrackerFrontiers(1<<20, 1<<20, 6144, time.Now())
	require.True(t, c.ShouldFlush())
	require.Zero(t, c.Snapshot().Blocking, "all ACKs can complete while the tail is still unpublished")
	actions, _ := limiter.snapshot()
	require.Empty(t, actions, "the count trigger must not change append admission")
	require.False(t, (recoveryTailSlowdownChecker{controller: c}).Check())
	c.Publish(172, 1)
	require.False(t, c.ShouldFlush())
	require.Equal(t, uint64(6143), c.Snapshot().RecoveryTailMessages)
	actions, _ = limiter.snapshot()
	require.Empty(t, actions)
}

func TestRecoveryTailFlushSizeIndependentOfRateLimit(t *testing.T) {
	for _, test := range []struct {
		name      string
		flushSize uint64
		bytes     uint64
		flush     bool
		slowdown  bool
	}{
		{"below-flush", 6 << 30, 6<<30 - 1, false, false},
		{"exact-flush", 6 << 30, 6 << 30, true, false},
		{"both", 6 << 30, 8 << 30, true, true},
		{"rate-limit-only", 10 << 30, 8 << 30, false, true},
		{"flush-disabled", 0, 8 << 30, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			limiter := &recordingRecoveryTailRateLimiter{}
			c := newRecoveryTailController(&config{
				tailLowWatermarkBytes: 4 << 30, tailSoftWatermarkBytes: 8 << 30, tailHighWatermarkBytes: 16 << 30,
				flushSizeBytes: test.flushSize,
			}, limiter, nil)
			c.UpdateTrackerFrontiers(test.bytes, test.bytes, 0, time.Time{})
			require.Equal(t, test.flush, c.ShouldFlush())
			actions, _ := limiter.snapshot()
			if test.slowdown {
				require.Equal(t, []recoveryTailAction{recoveryTailActionSlowdown}, actions)
			} else {
				require.Empty(t, actions)
			}
		})
	}
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
				flushIdleMinBytes: test.minSize, flushIdleTimeout: test.timeout,
			}, limiter, nil)
			c.UpdateTrackerFrontiers(test.bytes, test.bytes, 1, now)
			require.False(t, c.ShouldFlush())
			now = now.Add(test.idle)
			c.mu.Lock()
			flush := c.shouldFlushLocked(now)
			c.mu.Unlock()
			require.Equal(t, test.want, flush)
			actions, _ := limiter.snapshot()
			require.Empty(t, actions)
			c.UpdateTrackerFrontiers(test.bytes+1, test.bytes+1, 2, now)
			require.False(t, c.ShouldFlush(), "a new non-TimeTick message resets idleness")
		})
	}
}

func TestRecoveryTailFlushPreservesByteRejectionAndReset(t *testing.T) {
	limiter := &recordingRecoveryTailRateLimiter{}
	c := newRecoveryTailController(&config{
		tailLowWatermarkBytes: 4 << 30, tailSoftWatermarkBytes: 8 << 30, tailHighWatermarkBytes: 16 << 30,
		flushMessageCount: 6144,
	}, limiter, nil)
	now := time.Now()
	c.UpdateTrackerFrontiers(16<<30, 16<<30, 7000, now)
	c.UpdateTrackerFrontiers(1, 1, 1, now.Add(-time.Hour))
	require.Equal(t, uint64(7000), c.Snapshot().ObservedMessages)
	require.Equal(t, now, c.lastMessageAt)
	c.Publish(12<<30, 1)
	require.True(t, c.ShouldFlush(), "flushing continues while the limiter recovers")
	actions, _ := limiter.snapshot()
	require.Equal(t, []recoveryTailAction{recoveryTailActionReject, recoveryTailActionRecover}, actions)
	c.Reset()
	require.Equal(t, recoveryTailSnapshot{}, c.Snapshot())
	require.True(t, c.lastMessageAt.IsZero())
	require.False(t, c.ShouldFlush())
	c.Reset()
	c.UpdateTrackerFrontiers(8<<30, 0, 1, now)
	c.Reset()
	actions, _ = limiter.snapshot()
	require.Equal(t, []recoveryTailAction{recoveryTailActionReject, recoveryTailActionRecover, recoveryTailActionSlowdown, recoveryTailActionRecover}, actions)
}

func TestRecoveryTailIdlePollingOnlyTriggersFlush(t *testing.T) {
	limiter := &recordingRecoveryTailRateLimiter{}
	c := newRecoveryTailController(&config{
		tailLowWatermarkBytes: 4 << 30, tailSoftWatermarkBytes: 8 << 30, tailHighWatermarkBytes: 16 << 30,
		flushIdleMinBytes: 512 << 20, flushIdleTimeout: 10 * time.Minute,
	}, limiter, nil)
	c.UpdateTrackerFrontiers(600<<20, 600<<20, 1, time.Now())
	require.False(t, c.ShouldFlush())
	last := time.Now().Add(-11 * time.Minute)
	c.mu.Lock()
	c.lastMessageAt = last
	c.mu.Unlock()
	require.True(t, c.ShouldFlush())
	actions, _ := limiter.snapshot()
	require.Empty(t, actions)
	c.Publish(300<<20, 1)
	require.False(t, c.ShouldFlush())
	require.Equal(t, last, c.lastMessageAt, "publishing a checkpoint is not new WAL activity")
}

func TestRecoveryTailRealLimiterRecoversWhileFlushStillRequired(t *testing.T) {
	mux := ratelimit.NewMuxRateLimitObserverRegistry()
	limiter := ratelimit.NewAdaptiveRateLimitController(types.PChannelInfo{Name: t.Name()}, "recoveryStorage", mux, ratelimit.DefaultAdaptiveRateLimitControllerConfigFetcher{})
	t.Cleanup(limiter.Close)
	c := newRecoveryTailController(&config{
		tailLowWatermarkBytes: 4 << 30, tailSoftWatermarkBytes: 8 << 30, tailHighWatermarkBytes: 16 << 30,
		flushMessageCount: 6144, flushIdleMinBytes: 512 << 20, flushIdleTimeout: time.Minute,
	}, limiter, nil)
	c.UpdateTrackerFrontiers(16<<30, 16<<30, 7000, time.Now().Add(-time.Hour))
	require.Eventually(t, mux.IsRejected, time.Second, time.Millisecond)
	c.Publish(12<<30, 0)
	require.True(t, c.ShouldFlush())
	require.Eventually(t, func() bool { return !mux.IsRejected() }, time.Second, time.Millisecond,
		"the real limiter must leave Reject at 4GB even while count and idle still require persistence")
}
