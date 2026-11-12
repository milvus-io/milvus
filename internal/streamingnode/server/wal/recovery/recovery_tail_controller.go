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

	lowWatermark      uint64
	softWatermark     uint64
	highWatermark     uint64
	rateLimiter       RecoveryTailRateLimiter
	metrics           *recoveryMetrics
	flushSizeBytes    uint64
	flushMessageCount uint64
	idleMinBytes      uint64
	idleTimeout       time.Duration
	lastMessageAt     time.Time

	state     recoveryTailPressureState
	frontiers recoveryTailSnapshot
}

func newRecoveryTailController(
	cfg *config,
	rateLimiter RecoveryTailRateLimiter,
	metrics *recoveryMetrics,
) *recoveryTailController {
	return &recoveryTailController{
		lowWatermark:      cfg.tailLowWatermarkBytes,
		softWatermark:     cfg.tailSoftWatermarkBytes,
		highWatermark:     cfg.tailHighWatermarkBytes,
		rateLimiter:       rateLimiter,
		metrics:           metrics,
		flushSizeBytes:    cfg.flushSizeBytes,
		flushMessageCount: cfg.flushMessageCount,
		idleMinBytes:      cfg.flushIdleMinBytes,
		idleTimeout:       cfg.flushIdleTimeout,
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
	action := c.refreshLocked()
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
	action := c.refreshLocked()
	c.mu.Unlock()
	c.applyAction(action)
}

// ShouldFlush is polled by the persistence workers, including during idle
// traffic. Once append pressure starts, keep persisting until the byte tail
// reaches the recovery watermark, even after the independent flush triggers
// clear. This predicate never changes append admission or limiter state.
func (c *recoveryTailController) ShouldFlush() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state != recoveryTailPressureNormal || c.shouldFlushLocked(time.Now())
}

func (c *recoveryTailController) Snapshot() recoveryTailSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.frontiers
}

func (c *recoveryTailController) refreshLocked() recoveryTailAction {
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
	case c.frontiers.RecoveryTail >= c.softWatermark:
		if c.state == recoveryTailPressureNormal {
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

func (c *recoveryTailController) shouldFlushLocked(now time.Time) bool {
	return (c.flushSizeBytes > 0 && c.frontiers.RecoveryTail >= c.flushSizeBytes) ||
		(c.flushMessageCount > 0 && c.frontiers.RecoveryTailMessages >= c.flushMessageCount) ||
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
	return c.controller.frontiers.RecoveryTail > c.controller.lowWatermark
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
