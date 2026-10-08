package recovery

import (
	"sync"
	"testing"

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

	controller.UpdateTrackerFrontiers(25, 15)
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

	controller.Publish(15)
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

	controller.UpdateTrackerFrontiers(45, 25)
	assert.True(t, controller.UnderSoftPressure())
	controller.UpdateTrackerFrontiers(46, 25)
	controller.UpdateTrackerFrontiers(50, 25)
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
	controller.UpdateTrackerFrontiers(40, 40)
	controller.Publish(30)
	controller.Publish(20)

	assert.Equal(t, uint64(30), controller.Snapshot().PublishedOffset)
}
