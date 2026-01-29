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

package rate

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/ratelimit"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	SourceRecoveryStorage     = "recoveryStorage"
	SourceNodeMemory          = "memory"
	SourceFlusherRecovering   = "flusherRecovering"
	SourceAppendRate          = "appendRate"
	appendRateCheckerInterval = 2 * time.Second
)

// RateProvider is an interface that provides the current rate.
type RateProvider interface {
	Rate() float64
}

type WALRateLimitComponent struct {
	*ratelimit.MuxRateLimitObserverRegistryImpl
	channel           types.PChannelInfo
	RecoveryStorage   *ratelimit.AdaptiveRateLimitController
	FlusherRecovering *ratelimit.AdaptiveRateLimitController
	NodeMemory        *ratelimit.AdaptiveRateLimitController
	AppendRate        *ratelimit.AdaptiveRateLimitController
	handler           *hardware.SystemMetricsListener

	appendRateStopCh chan struct{}
	appendRateWg     sync.WaitGroup
}

// NewWALRateLimitComponent creates a new WAL rate limit component.
func NewWALRateLimitComponent(
	channel types.PChannelInfo,
) *WALRateLimitComponent {
	rateLimitRegistry := ratelimit.NewMuxRateLimitObserverRegistry()
	return &WALRateLimitComponent{
		MuxRateLimitObserverRegistryImpl: rateLimitRegistry,
		channel:                          channel,
		RecoveryStorage: ratelimit.NewAdaptiveRateLimitController(channel, SourceRecoveryStorage, rateLimitRegistry,
			newAdaptiveRateLimitControllerConfigFetcher(channel, SourceRecoveryStorage)),
		FlusherRecovering: ratelimit.NewAdaptiveRateLimitController(channel, SourceFlusherRecovering, rateLimitRegistry,
			newAdaptiveRateLimitControllerConfigFetcher(channel, SourceFlusherRecovering)),
		NodeMemory: ratelimit.NewAdaptiveRateLimitController(channel, SourceNodeMemory, rateLimitRegistry,
			newAdaptiveRateLimitControllerConfigFetcher(channel, SourceNodeMemory)),
		AppendRate: ratelimit.NewAdaptiveRateLimitController(channel, SourceAppendRate, rateLimitRegistry,
			newAdaptiveRateLimitControllerConfigFetcher(channel, SourceAppendRate)),
	}
}

// RegisterMemoryObserver registers the memory observer.
func (c *WALRateLimitComponent) RegisterMemoryObserver() {
	l := &hardware.SystemMetricsListener{
		Cooldown: 0 * time.Second,
		Condition: func(sm hardware.SystemMetrics, _ *hardware.SystemMetricsListener) bool {
			return true
		},
		Callback: c.hardwardCallback,
	}
	hardware.RegisterSystemMetricsListener(l)
	c.handler = l
}

// hardwardCallback is the callback function for the hardware metrics listener.
func (c *WALRateLimitComponent) hardwardCallback(sm hardware.SystemMetrics, _ *hardware.SystemMetricsListener) {
	slowdownThreshold := paramtable.Get().StreamingCfg.WALRateLimitNodeMemorySlowdownThreshold.GetAsFloat()
	recoverThreshold := paramtable.Get().StreamingCfg.WALRateLimitNodeMemoryRecoverThreshold.GetAsFloat()
	rejectThreshold := paramtable.Get().StreamingCfg.WALRateLimitNodeMemoryRejectThreshold.GetAsFloat()

	// Report threshold config metrics
	c.reportNodeMemoryThresholdMetrics(slowdownThreshold, rejectThreshold, recoverThreshold)

	usedRatio := sm.UsedRatio()
	if usedRatio > slowdownThreshold {
		// Create checker that stops slowdown when memory usage decreases.
		checker := newMemorySlowdownChecker(usedRatio)
		c.NodeMemory.EnterSlowdownMode(checker)
	}
	if usedRatio < recoverThreshold {
		c.NodeMemory.EnterRecoveryMode()
	}
	if usedRatio > rejectThreshold {
		c.NodeMemory.EnterRejectMode()
	}
}

// reportNodeMemoryThresholdMetrics reports the node memory threshold config metrics.
func (c *WALRateLimitComponent) reportNodeMemoryThresholdMetrics(slowdown, reject, recover float64) {
	metrics.WALRateLimitNodeMemorySlowdownThreshold.WithLabelValues(
		paramtable.GetStringNodeID(), c.channel.Name,
	).Set(slowdown)
	metrics.WALRateLimitNodeMemoryRejectThreshold.WithLabelValues(
		paramtable.GetStringNodeID(), c.channel.Name,
	).Set(reject)
	metrics.WALRateLimitNodeMemoryRecoverThreshold.WithLabelValues(
		paramtable.GetStringNodeID(), c.channel.Name,
	).Set(recover)
}

// memorySlowdownChecker implements ratelimit.SlowdownChecker for memory-based slowdown.
// It returns false when current memory usage is lower than the previous usage,
// indicating that memory pressure is reducing and slowdown should stop.
type memorySlowdownChecker struct {
	mu                sync.Mutex
	previousUsedRatio float64
}

// newMemorySlowdownChecker creates a new memory slowdown checker with the initial used ratio.
func newMemorySlowdownChecker(initialUsedRatio float64) *memorySlowdownChecker {
	return &memorySlowdownChecker{
		previousUsedRatio: initialUsedRatio,
	}
}

// Check returns true if slowdown should continue, false if it should stop.
// Returns false when current memory usage is lower than previous (memory pressure reducing).
func (c *memorySlowdownChecker) Check() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	totalMemory := hardware.GetMemoryCount()
	if totalMemory == 0 {
		return true // Continue slowdown if we can't get memory info
	}
	currentUsedRatio := float64(hardware.GetUsedMemoryCount()) / float64(totalMemory)

	// If current memory usage is lower than previous, stop slowdown
	if currentUsedRatio < c.previousUsedRatio {
		return false
	}
	// Update previous usage for next check
	c.previousUsedRatio = currentUsedRatio
	return true
}

// SlowdownStartupHWM returns 0 to use the default HWM from config.
func (c *memorySlowdownChecker) SlowdownStartupHWM() int64 {
	return 0
}

// RegisterAppendRateObserver registers the append rate observer.
// It starts a background goroutine to monitor the append rate and trigger rate limiting.
func (c *WALRateLimitComponent) RegisterAppendRateObserver(rateProvider RateProvider) {
	c.appendRateStopCh = make(chan struct{})
	c.appendRateWg.Add(1)
	go c.appendRateObserverLoop(rateProvider)
}

// appendRateObserverLoop is the background loop that monitors the append rate.
func (c *WALRateLimitComponent) appendRateObserverLoop(rateProvider RateProvider) {
	defer c.appendRateWg.Done()

	ticker := time.NewTicker(appendRateCheckerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.appendRateStopCh:
			return
		case <-ticker.C:
			c.checkAppendRate(rateProvider)
		}
	}
}

// checkAppendRate checks the current append rate and triggers rate limiting if needed.
func (c *WALRateLimitComponent) checkAppendRate(rateProvider RateProvider) {
	// Skip if append rate limiting is not enabled.
	if !paramtable.Get().StreamingCfg.WALRateLimitAppendRateEnabled.GetAsBool() {
		return
	}

	currentRate := rateProvider.Rate()

	slowdownThreshold := paramtable.Get().StreamingCfg.WALRateLimitAppendRateSlowdownThreshold.GetAsSize()
	recoverThreshold := paramtable.Get().StreamingCfg.WALRateLimitAppendRateRecoverThreshold.GetAsSize()

	// Report threshold config metrics
	c.reportAppendRateThresholdMetrics(slowdownThreshold, recoverThreshold)

	if currentRate > float64(slowdownThreshold) {
		// Enter slowdown mode when append rate exceeds the high watermark.
		checker := newAppendRateSlowdownChecker(rateProvider, currentRate)
		c.AppendRate.EnterSlowdownMode(checker)
	}
	if currentRate < float64(recoverThreshold) {
		// Enter recovery mode when append rate drops below the low watermark.
		c.AppendRate.EnterRecoveryMode()
	}
}

// reportAppendRateThresholdMetrics reports the append rate threshold config metrics.
func (c *WALRateLimitComponent) reportAppendRateThresholdMetrics(slowdown, recover int64) {
	metrics.WALRateLimitAppendRateSlowdownThreshold.WithLabelValues(
		paramtable.GetStringNodeID(), c.channel.Name,
	).Set(float64(slowdown))
	metrics.WALRateLimitAppendRateRecoverThreshold.WithLabelValues(
		paramtable.GetStringNodeID(), c.channel.Name,
	).Set(float64(recover))
}

// appendRateSlowdownChecker implements ratelimit.SlowdownChecker for append rate-based slowdown.
// It returns false when current append rate is lower than the previous rate,
// indicating that write pressure is reducing and slowdown should stop.
type appendRateSlowdownChecker struct {
	mu           sync.Mutex
	rateProvider RateProvider
	previousRate float64
}

// newAppendRateSlowdownChecker creates a new append rate slowdown checker.
func newAppendRateSlowdownChecker(rateProvider RateProvider, initialRate float64) *appendRateSlowdownChecker {
	return &appendRateSlowdownChecker{
		rateProvider: rateProvider,
		previousRate: initialRate,
	}
}

// Check returns true if slowdown should continue, false if it should stop.
// Returns false when current append rate is lower than previous (write pressure reducing).
func (c *appendRateSlowdownChecker) Check() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentRate := c.rateProvider.Rate()

	// If current append rate is lower than previous, stop slowdown
	if currentRate < c.previousRate {
		return false
	}
	// Update previous rate for next check
	c.previousRate = currentRate
	return true
}

// SlowdownStartupHWM returns the current rate as the startup HWM.
func (c *appendRateSlowdownChecker) SlowdownStartupHWM() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int64(c.previousRate)
}

// Close closes the WAL rate limit component.
// The API of WAL rate limit component is not concurrent-safe.
func (c *WALRateLimitComponent) Close() {
	if c.handler != nil {
		hardware.UnregisterSystemMetricsListener(c.handler)
	}
	if c.appendRateStopCh != nil {
		close(c.appendRateStopCh)
		c.appendRateWg.Wait()
	}
	c.RecoveryStorage.Close()
	c.FlusherRecovering.Close()
	c.NodeMemory.Close()
	c.AppendRate.Close()
	c.clearMetrics()
}

func (c *WALRateLimitComponent) clearMetrics() {
	metrics.WALRateLimitNodeMemorySlowdownThreshold.DeleteLabelValues(paramtable.GetStringNodeID(), c.channel.Name)
	metrics.WALRateLimitNodeMemoryRejectThreshold.DeleteLabelValues(paramtable.GetStringNodeID(), c.channel.Name)
	metrics.WALRateLimitNodeMemoryRecoverThreshold.DeleteLabelValues(paramtable.GetStringNodeID(), c.channel.Name)
	metrics.WALRateLimitAppendRateSlowdownThreshold.DeleteLabelValues(paramtable.GetStringNodeID(), c.channel.Name)
	metrics.WALRateLimitAppendRateRecoverThreshold.DeleteLabelValues(paramtable.GetStringNodeID(), c.channel.Name)
}
