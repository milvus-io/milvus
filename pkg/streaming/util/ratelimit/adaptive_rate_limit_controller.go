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

package ratelimit

import (
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	adaptiveRateLimitModeNormal adaptiveRateLimitMode = iota
	adaptiveRateLimitModeSlowdown
	adaptiveRateLimitModeReject
	adaptiveRateLimitModeRecovery
)

type adaptiveRateLimitMode int

func (m adaptiveRateLimitMode) String() string {
	switch m {
	case adaptiveRateLimitModeNormal:
		return "normal"
	case adaptiveRateLimitModeSlowdown:
		return "slowdown"
	case adaptiveRateLimitModeReject:
		return "reject"
	case adaptiveRateLimitModeRecovery:
		return "recovery"
	default:
		return ""
	}
}

// SlowdownChecker is an interface that checks if the slowdown should continue
// and provides the startup HWM for slowdown mode.
// It's called during each tick of slowdown mode.
type SlowdownChecker interface {
	// Check returns true if slowdown should continue, false if it should exit to recovery.
	Check() bool
	// SlowdownStartupHWM returns the high watermark to start slowdown from.
	// The actual starting rate will be min(currentRate, SlowdownStartupHWM(), cfg.HWM).
	// Return 0 to use the default behavior (min(currentRate, cfg.HWM)).
	SlowdownStartupHWM() int64
}

// modeTransitionRequest represents a request to transition to a new mode.
type modeTransitionRequest struct {
	targetMode      adaptiveRateLimitMode
	slowdownChecker SlowdownChecker // Only used for slowdown mode
}

// AdaptiveRateLimitController manages rate limiting state transitions for the scanner.
// It observes the scanner mode and adjusts rate limits accordingly:
// - When entering slowdown mode: starts at high watermark rate
// - In slowdown mode: decreases rate by ratio periodically until low watermark
// - When exiting slowdown (entering recovery): starts recovery, increasing rate incrementally periodically until high watermark.
// - Recovery completes: when rate reaches high watermark and stays for one more interval
//
// All public methods (EnterSlowdownMode, EnterRejectMode, EnterRecoveryMode) are thread-safe.
type AdaptiveRateLimitController struct {
	sourceName        string
	wg                sync.WaitGroup
	rateLimitRegistry *MuxRateLimitObserverRegistryImpl

	// modeTransitionCh is used to send mode transition requests to the background goroutine.
	modeTransitionCh chan modeTransitionRequest
	stopCh           chan struct{}
	configFetcher    AdaptiveRateLimitControllerConfigFetcher
	channel          types.PChannelInfo

	// Observable state (updated by background goroutine, read by external callers).
	mode        *atomic.Int32
	currentRate *atomic.Int64
}

// NewAdaptiveRateLimitController creates a new rate limit controller.
// The controller starts a background goroutine to handle ticker logic.
// All public methods are thread-safe.
func NewAdaptiveRateLimitController(
	channel types.PChannelInfo,
	sourceName string,
	rateLimitRegistry *MuxRateLimitObserverRegistryImpl,
	configFetcher AdaptiveRateLimitControllerConfigFetcher,
) *AdaptiveRateLimitController {
	c := &AdaptiveRateLimitController{
		sourceName:        sourceName,
		rateLimitRegistry: rateLimitRegistry,
		modeTransitionCh:  make(chan modeTransitionRequest, 1),
		stopCh:            make(chan struct{}),
		configFetcher:     configFetcher,
		channel:           channel,
		mode:              atomic.NewInt32(int32(adaptiveRateLimitModeNormal)),
		currentRate:       atomic.NewInt64(0),
	}
	c.wg.Add(1)
	go c.backgroundLoop()
	return c
}

// getMode returns the current mode (thread-safe).
func (c *AdaptiveRateLimitController) getMode() adaptiveRateLimitMode {
	return adaptiveRateLimitMode(c.mode.Load())
}

// getCurrentRate returns the current rate (thread-safe).
func (c *AdaptiveRateLimitController) getCurrentRate() int64 {
	return c.currentRate.Load()
}

// EnterSlowdownMode is called when the scanner enters slowdown mode.
// Sets rate to min(currentRate, HWM) and starts the decreasing timer.
// The checker function is called during each tick to determine if slowdown should continue.
// If checker returns false, the controller will exit slowdown and enter recovery mode.
// This method is thread-safe.
func (c *AdaptiveRateLimitController) EnterSlowdownMode(checker SlowdownChecker) {
	select {
	case c.modeTransitionCh <- modeTransitionRequest{targetMode: adaptiveRateLimitModeSlowdown, slowdownChecker: checker}:
	case <-c.stopCh:
	}
}

// EnterRejectMode is called when the scanner enters reject mode.
// This method is thread-safe.
func (c *AdaptiveRateLimitController) EnterRejectMode() {
	select {
	case c.modeTransitionCh <- modeTransitionRequest{targetMode: adaptiveRateLimitModeReject}:
	case <-c.stopCh:
	}
}

// EnterRecoveryMode is called when the scanner exits slowdown mode and enters recovery mode.
// Starts the recovery process to gradually increase rate.
// This method is thread-safe.
func (c *AdaptiveRateLimitController) EnterRecoveryMode() {
	select {
	case c.modeTransitionCh <- modeTransitionRequest{targetMode: adaptiveRateLimitModeRecovery}:
	case <-c.stopCh:
	}
}

// Close stops the background goroutine and cleans up resources.
func (c *AdaptiveRateLimitController) Close() {
	close(c.stopCh)
	c.wg.Wait()
	c.configFetcher.Close()
}

// backgroundLoop runs in a goroutine and handles all mode transitions and ticker logic.
func (c *AdaptiveRateLimitController) backgroundLoop() {
	defer c.wg.Done()

	state := &controllerState{
		mode:        adaptiveRateLimitModeNormal,
		currentRate: 0,
	}
	c.notify(state)

	var ticker *time.Ticker
	var tickerCh <-chan time.Time
	var delayTimer *time.Timer
	var delayTimerCh <-chan time.Time

	// slowdownCfg and recoveryCfg are cached configs for the current mode.
	var slowdownCfg SlowdownConfig
	var recoveryCfg RecoveryConfig

	// slowdownChecker is the checker function for the current slowdown mode.
	var slowdownChecker SlowdownChecker

	// firstSlowdownDelayExecuted tracks if the first slowdown delay has been executed.
	// The delay should only be applied the first time entering slowdown mode.
	firstSlowdownDelayExecuted := false

	// rateBeforeSlowdown saves the rate before entering slowdown mode.
	// Used to start slowdown from min(rateBeforeSlowdown, HWM).
	var rateBeforeSlowdown int64

	stopTicker := func() {
		if ticker != nil {
			ticker.Stop()
			ticker = nil
			tickerCh = nil
		}
	}
	stopDelayTimer := func() {
		if delayTimer != nil {
			delayTimer.Stop()
			delayTimer = nil
			delayTimerCh = nil
		}
	}

	defer func() {
		stopTicker()
		stopDelayTimer()
	}()

	for {
		select {
		case req := <-c.modeTransitionCh:
			// Handle mode transition request.
			switch req.targetMode {
			case adaptiveRateLimitModeSlowdown:
				if state.mode == adaptiveRateLimitModeSlowdown || state.mode == adaptiveRateLimitModeReject {
					// Already in slowdown or reject mode, ignore.
					continue
				}
				// Valid transition, stop existing timers.
				stopTicker()
				stopDelayTimer()

				// Save rate before entering slowdown.
				rateBeforeSlowdown = state.currentRate

				state.mode = adaptiveRateLimitModeSlowdown
				slowdownCfg = c.configFetcher.FetchSlowdownConfig()
				slowdownChecker = req.slowdownChecker
				state.currentRate = math.MaxInt64
				// Update atomic mode immediately so external callers can see the mode change.
				c.mode.Store(int32(state.mode))

				// Only apply first slowdown delay the first time entering slowdown.
				if !firstSlowdownDelayExecuted && slowdownCfg.FirstSlowdownDelay > 0 {
					// Start delay timer before applying slowdown.
					delayTimer = time.NewTimer(slowdownCfg.FirstSlowdownDelay)
					delayTimerCh = delayTimer.C
				} else {
					// Apply slowdown immediately.
					c.applySlowdownStart(state, slowdownCfg, rateBeforeSlowdown, slowdownChecker)
					ticker = time.NewTicker(slowdownCfg.DecreaseInterval)
					tickerCh = ticker.C
				}
				firstSlowdownDelayExecuted = true

			case adaptiveRateLimitModeReject:
				if state.mode == adaptiveRateLimitModeReject {
					// Already in reject mode, ignore.
					continue
				}
				// Valid transition, stop existing timers.
				stopTicker()
				stopDelayTimer()

				c.enterRejectMode(state)

			case adaptiveRateLimitModeRecovery:
				if state.mode == adaptiveRateLimitModeRecovery || state.mode == adaptiveRateLimitModeNormal {
					// Already in recovery or normal mode, ignore.
					continue
				}
				// Valid transition, stop existing timers.
				stopTicker()
				stopDelayTimer()

				if state.currentRate == math.MaxInt64 {
					// Slowdown was not started, enter normal mode directly.
					c.enterNormalMode(state)
					continue
				}
				state.mode = adaptiveRateLimitModeRecovery
				recoveryCfg = c.configFetcher.FetchRecoveryConfig()
				if state.currentRate < recoveryCfg.LWM {
					state.currentRate = recoveryCfg.LWM
				}
				c.notify(state)
				ticker = time.NewTicker(recoveryCfg.IncreaseInterval)
				tickerCh = ticker.C
			}

		case <-tickerCh:
			// Handle ticker event based on current mode.
			switch state.mode {
			case adaptiveRateLimitModeSlowdown:
				if c.tickSlowdown(state, slowdownCfg, slowdownChecker) {
					// Reached LWM or checker returned false, stop ticker and optionally start reject delay.
					stopTicker()
					if slowdownCfg.RejectDelayInterval > 0 {
						delayTimer = time.NewTimer(slowdownCfg.RejectDelayInterval)
						delayTimerCh = delayTimer.C
					}
				}
			case adaptiveRateLimitModeRecovery:
				if c.tickRecovery(state, recoveryCfg) {
					// Reached HWM, stop ticker and start normal delay.
					stopTicker()
					delayTimer = time.NewTimer(recoveryCfg.NormalDelayInterval)
					delayTimerCh = delayTimer.C
				}
			}

		case <-delayTimerCh:
			// Handle delay timer event.
			stopDelayTimer()
			switch state.mode {
			case adaptiveRateLimitModeSlowdown:
				if state.currentRate == math.MaxInt64 {
					// First slowdown delay completed, apply slowdown start.
					c.applySlowdownStart(state, slowdownCfg, rateBeforeSlowdown, slowdownChecker)
					ticker = time.NewTicker(slowdownCfg.DecreaseInterval)
					tickerCh = ticker.C
				} else {
					// Reject delay completed, enter reject mode.
					c.enterRejectMode(state)
				}
			case adaptiveRateLimitModeRecovery:
				// Normal delay completed, enter normal mode.
				c.enterNormalMode(state)
			}

		case <-c.stopCh:
			return
		}
	}
}

// controllerState holds the mutable state managed by the background goroutine.
type controllerState struct {
	mode        adaptiveRateLimitMode
	currentRate int64
}

// applySlowdownStart applies the initial slowdown rate.
// The starting rate is min(rateBeforeSlowdown, checkerHWM, cfg.HWM), or cfg.HWM if rateBeforeSlowdown is 0.
func (c *AdaptiveRateLimitController) applySlowdownStart(state *controllerState, cfg SlowdownConfig, rateBeforeSlowdown int64, checker SlowdownChecker) {
	hwm := cfg.HWM

	// If checker provides a custom HWM, use the minimum of checker's HWM and config HWM.
	if checker != nil {
		checkerHWM := checker.SlowdownStartupHWM()
		if checkerHWM > 0 && checkerHWM < hwm {
			hwm = checkerHWM
		}
	}

	if rateBeforeSlowdown == 0 || rateBeforeSlowdown > hwm {
		state.currentRate = hwm
	} else {
		state.currentRate = rateBeforeSlowdown
	}
	c.notify(state)
}

// tickSlowdown should be called periodically while in slowdown mode.
// Decreases rate by ratio until low watermark.
// If checker is provided and Check() returns false, the slowdown should stop.
// Returns true if reached LWM or checker returned false.
func (c *AdaptiveRateLimitController) tickSlowdown(state *controllerState, cfg SlowdownConfig, checker SlowdownChecker) (shouldStop bool) {
	// Check if slowdown should continue.
	if checker != nil && !checker.Check() {
		// Checker returned false, slowdown should stop.
		return true
	}

	newRate := int64(float64(state.currentRate) * cfg.DecreaseRatio)
	if newRate < cfg.LWM {
		newRate = cfg.LWM
	}
	state.currentRate = newRate
	c.notify(state)
	return state.currentRate == cfg.LWM
}

// tickRecovery should be called periodically while in recovery mode.
// Increases rate by incremental until reaching high watermark.
// Returns true if reached HWM.
func (c *AdaptiveRateLimitController) tickRecovery(state *controllerState, cfg RecoveryConfig) (reachedHWM bool) {
	newRate := state.currentRate + cfg.Incremental
	if newRate > cfg.HWM {
		newRate = cfg.HWM
	}
	state.currentRate = newRate
	c.notify(state)
	return state.currentRate == cfg.HWM
}

// enterNormalMode transitions to normal mode.
func (c *AdaptiveRateLimitController) enterNormalMode(state *controllerState) {
	state.mode = adaptiveRateLimitModeNormal
	state.currentRate = 0
	c.notify(state)
}

// enterRejectMode transitions to reject mode.
func (c *AdaptiveRateLimitController) enterRejectMode(state *controllerState) {
	state.mode = adaptiveRateLimitModeReject
	state.currentRate = 0
	c.notify(state)
}

// notify notifies the observer with the current state and updates observable fields.
func (c *AdaptiveRateLimitController) notify(state *controllerState) {
	// Update observable atomic fields for external callers.
	c.mode.Store(int32(state.mode))
	c.currentRate.Store(state.currentRate)

	switch state.mode {
	case adaptiveRateLimitModeSlowdown, adaptiveRateLimitModeRecovery:
		c.rateLimitRegistry.NotifySourceRateLimitState(c.sourceName, RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
			Rate:  state.currentRate,
		})
	case adaptiveRateLimitModeReject:
		c.rateLimitRegistry.NotifySourceRateLimitState(c.sourceName, RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT,
			Rate:  0,
		})
	case adaptiveRateLimitModeNormal:
		c.rateLimitRegistry.NotifySourceRateLimitState(c.sourceName, RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL,
			Rate:  0,
		})
	}
	c.clearMetrics()
	metrics.WALRateLimitControllerState.WithLabelValues(
		paramtable.GetStringNodeID(),
		c.channel.Name,
		c.sourceName,
		state.mode.String(),
	).Set(float64(state.currentRate))
}

func (c *AdaptiveRateLimitController) clearMetrics() {
	metrics.WALRateLimitControllerState.DeletePartialMatch(prometheus.Labels{
		metrics.WALRateLimitControllerSourceLabelName: c.sourceName,
		metrics.WALChannelLabelName:                   c.channel.Name,
	})
}
