package recovery

import (
	"sync"
	"time"
)

type recoveryTailPressureState int

const (
	recoveryTailPressureNormal recoveryTailPressureState = iota
	recoveryTailPressureSlowdown
	recoveryTailPressureReject
)

type recoveryTailAction int

const (
	recoveryTailActionNone recoveryTailAction = iota
	recoveryTailActionSlowdown
	recoveryTailActionReject
	recoveryTailActionRecover
)

type recoveryTailSnapshot struct {
	ObservedOffset       uint64
	CompletedOffset      uint64
	PublishedOffset      uint64
	RecoveryTail         uint64
	Blocking             uint64
	PublishLag           uint64
	ObservedMessages     uint64
	PublishedMessages    uint64
	RecoveryTailMessages uint64
}

// recoveryTailController maintains relative byte frontiers for one RecoveryStorage.
// Offsets start at zero at the catalog-published startup checkpoint and are not
// persisted. The controller reuses the WAL's recovery-storage rate limiter for
// append pressure, while AckTracker owns VChannel persistence targeting.
type recoveryTailController struct {
	transitionMu sync.Mutex
	mu           sync.Mutex

	lowWatermark          uint64
	softWatermark         uint64
	highWatermark         uint64
	rateLimiter           RecoveryTailRateLimiter
	metrics               *recoveryMetrics
	softWatermarkMessages uint64
	idleMinBytes          uint64
	idleTimeout           time.Duration
	lastMessageAt         time.Time

	state     recoveryTailPressureState
	frontiers recoveryTailSnapshot
}

func newRecoveryTailController(
	cfg *config,
	rateLimiter RecoveryTailRateLimiter,
	metrics *recoveryMetrics,
) *recoveryTailController {
	return &recoveryTailController{
		lowWatermark:          cfg.tailLowWatermarkBytes,
		softWatermark:         cfg.tailSoftWatermarkBytes,
		highWatermark:         cfg.tailHighWatermarkBytes,
		rateLimiter:           rateLimiter,
		metrics:               metrics,
		softWatermarkMessages: cfg.tailSoftWatermarkMessages,
		idleMinBytes:          cfg.tailIdleMinBytes,
		idleTimeout:           cfg.tailIdleTimeout,
	}
}

func (c *recoveryTailController) UpdateTrackerFrontiers(observed, completed, messages uint64, lastMessageAt time.Time) {
	c.transitionMu.Lock()
	defer c.transitionMu.Unlock()
	c.mu.Lock()
	c.frontiers.ObservedOffset = max(c.frontiers.ObservedOffset, observed)
	c.frontiers.CompletedOffset = max(c.frontiers.CompletedOffset, completed)
	c.frontiers.ObservedMessages = max(c.frontiers.ObservedMessages, messages)
	if lastMessageAt.After(c.lastMessageAt) {
		c.lastMessageAt = lastMessageAt
	}
	action := c.refreshLocked(time.Now())
	c.mu.Unlock()
	c.applyAction(action)
}

func (c *recoveryTailController) Reset() {
	c.transitionMu.Lock()
	defer c.transitionMu.Unlock()
	c.mu.Lock()
	wasPressured := c.state != recoveryTailPressureNormal
	c.state = recoveryTailPressureNormal
	c.frontiers = recoveryTailSnapshot{}
	c.lastMessageAt = time.Time{}
	if c.metrics != nil {
		c.metrics.ObserveTailBytes(0, 0, 0)
	}
	c.mu.Unlock()
	if wasPressured {
		c.applyAction(recoveryTailActionRecover)
	}
}

func (c *recoveryTailController) Publish(offset, messages uint64) {
	c.transitionMu.Lock()
	defer c.transitionMu.Unlock()
	c.mu.Lock()
	if offset > c.frontiers.PublishedOffset {
		c.frontiers.PublishedOffset = offset
	}
	c.frontiers.PublishedMessages = max(c.frontiers.PublishedMessages, messages)
	action := c.refreshLocked(time.Now())
	c.mu.Unlock()
	c.applyAction(action)
}

func (c *recoveryTailController) UnderSoftPressure() bool {
	// Maintenance workers call this even without new WAL traffic, so an idle
	// tail must also enter the shared append limiter here.
	c.transitionMu.Lock()
	defer c.transitionMu.Unlock()
	c.mu.Lock()
	now := time.Now()
	action := c.refreshLocked(now)
	pressured := c.softPressureLocked(now)
	c.mu.Unlock()
	c.applyAction(action)
	return pressured
}

func (c *recoveryTailController) Snapshot() recoveryTailSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.frontiers
}

func (c *recoveryTailController) refreshLocked(now time.Time) recoveryTailAction {
	c.frontiers.RecoveryTail = saturatingSub(
		c.frontiers.ObservedOffset,
		c.frontiers.PublishedOffset,
	)
	c.frontiers.Blocking = saturatingSub(
		c.frontiers.ObservedOffset,
		c.frontiers.CompletedOffset,
	)
	c.frontiers.PublishLag = saturatingSub(
		c.frontiers.CompletedOffset,
		c.frontiers.PublishedOffset,
	)
	c.frontiers.RecoveryTailMessages = saturatingSub(c.frontiers.ObservedMessages, c.frontiers.PublishedMessages)
	if c.metrics != nil {
		c.metrics.ObserveTailBytes(
			c.frontiers.RecoveryTail,
			c.frontiers.Blocking,
			c.frontiers.PublishLag,
		)
	}

	switch {
	case c.frontiers.RecoveryTail >= c.highWatermark:
		if c.state != recoveryTailPressureReject {
			c.state = recoveryTailPressureReject
			return recoveryTailActionReject
		}
	case c.softPressureLocked(now):
		if c.state == recoveryTailPressureNormal ||
			(c.state == recoveryTailPressureReject && c.frontiers.RecoveryTail <= c.lowWatermark) {
			c.state = recoveryTailPressureSlowdown
			return recoveryTailActionSlowdown
		}
	case c.frontiers.RecoveryTail <= c.lowWatermark:
		if c.state != recoveryTailPressureNormal {
			c.state = recoveryTailPressureNormal
			return recoveryTailActionRecover
		}
	}
	return recoveryTailActionNone
}

func (c *recoveryTailController) softPressureLocked(now time.Time) bool {
	return c.frontiers.RecoveryTail >= c.softWatermark ||
		(c.softWatermarkMessages > 0 && c.frontiers.RecoveryTailMessages >= c.softWatermarkMessages) ||
		(c.idleMinBytes > 0 && c.idleTimeout > 0 && c.frontiers.RecoveryTail > c.idleMinBytes &&
			!c.lastMessageAt.IsZero() && !now.Before(c.lastMessageAt.Add(c.idleTimeout)))
}

func (c *recoveryTailController) applyAction(action recoveryTailAction) {
	if c.rateLimiter == nil {
		return
	}
	switch action {
	case recoveryTailActionSlowdown:
		c.rateLimiter.EnterSlowdownMode(recoveryTailSlowdownChecker{controller: c})
	case recoveryTailActionReject:
		c.rateLimiter.EnterRejectMode()
	case recoveryTailActionRecover:
		c.rateLimiter.EnterRecoveryMode()
	}
}

type recoveryTailSlowdownChecker struct {
	controller *recoveryTailController
}

func (c recoveryTailSlowdownChecker) Check() bool {
	c.controller.mu.Lock()
	defer c.controller.mu.Unlock()
	return c.controller.frontiers.RecoveryTail > c.controller.lowWatermark || c.controller.softPressureLocked(time.Now())
}

func (recoveryTailSlowdownChecker) SlowdownStartupHWM() int64 {
	return 0
}

func saturatingSub(left, right uint64) uint64 {
	if right >= left {
		return 0
	}
	return left - right
}
