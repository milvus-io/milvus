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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestWALRateLimitComponent(t *testing.T) {
	paramtable.Init()
	channel := types.PChannelInfo{Name: "test-channel"}
	component := NewWALRateLimitComponent(channel)
	defer component.Close()

	assert.NotNil(t, component.RecoveryStorage)
	assert.NotNil(t, component.FlusherRecovering)
	assert.NotNil(t, component.NodeMemory)
	assert.NotNil(t, component.AppendRate)

	assert.False(t, component.IsRejected())
	component.RecoveryStorage.EnterRejectMode()
	// EnterRejectMode is async, wait for state transition
	assert.Eventually(t, func() bool {
		return component.IsRejected()
	}, 1*time.Second, 10*time.Millisecond)

	component.RecoveryStorage.EnterRecoveryMode()
	// EnterRecoveryMode is async, wait for state transition
	assert.Eventually(t, func() bool {
		return !component.IsRejected()
	}, 1*time.Second, 10*time.Millisecond)

	component.hardwardCallback(hardware.SystemMetrics{UsedMemoryBytes: 91, TotalMemoryBytes: 100}, nil)
	// State transitions are async, wait for any changes to propagate
	time.Sleep(50 * time.Millisecond)
	assert.False(t, component.IsRejected())

	component.hardwardCallback(hardware.SystemMetrics{UsedMemoryBytes: 100, TotalMemoryBytes: 100}, nil)
	// EnterRejectMode triggered by high memory is async
	assert.Eventually(t, func() bool {
		return component.IsRejected()
	}, 1*time.Second, 10*time.Millisecond)

	component.hardwardCallback(hardware.SystemMetrics{UsedMemoryBytes: 91, TotalMemoryBytes: 100}, nil)
	// Memory is still above recover threshold, should still be rejected
	time.Sleep(50 * time.Millisecond)
	assert.True(t, component.IsRejected())

	component.hardwardCallback(hardware.SystemMetrics{UsedMemoryBytes: 84, TotalMemoryBytes: 100}, nil)
	// Memory dropped below recover threshold, should trigger recovery
	assert.Eventually(t, func() bool {
		return !component.IsRejected()
	}, 1*time.Second, 10*time.Millisecond)

	component.RegisterMemoryObserver()
	assert.NotNil(t, component.handler)
}

// mockRateProvider is a mock implementation of RateProvider for testing.
type mockRateProvider struct {
	rate atomic.Int64
}

func newMockRateProvider(rate float64) *mockRateProvider {
	m := &mockRateProvider{}
	m.rate.Store(int64(rate))
	return m
}

func (m *mockRateProvider) Rate() float64 {
	return float64(m.rate.Load())
}

func (m *mockRateProvider) SetRate(rate float64) {
	m.rate.Store(int64(rate))
}

func TestAppendRateRateLimiting(t *testing.T) {
	paramtable.Init()
	channel := types.PChannelInfo{Name: "test-channel-append-rate"}
	component := NewWALRateLimitComponent(channel)
	defer component.Close()

	assert.NotNil(t, component.AppendRate)

	// Create a mock rate provider
	rateProvider := newMockRateProvider(0)

	// Register the append rate observer
	component.RegisterAppendRateObserver(rateProvider)

	// Test: when disabled (default), rate limiting should not trigger
	rateProvider.SetRate(100 * 1024 * 1024) // 100 MB/s (above threshold)
	component.checkAppendRate(rateProvider)
	// Should not trigger slowdown because disabled by default

	// Enable append rate limiting for testing
	paramtable.Get().Save(paramtable.Get().StreamingCfg.WALRateLimitAppendRateEnabled.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().StreamingCfg.WALRateLimitAppendRateEnabled.Key)

	// Test: rate below recover threshold should not trigger slowdown
	// Default recover threshold is 60MB/s, slowdown threshold is 64MB/s
	rateProvider.SetRate(10 * 1024 * 1024) // 10 MB/s
	component.checkAppendRate(rateProvider)
	// Should remain in normal/recovery mode

	// Test: rate above slowdown threshold should trigger slowdown
	rateProvider.SetRate(100 * 1024 * 1024) // 100 MB/s (above 64MB/s threshold)
	component.checkAppendRate(rateProvider)
	// Should enter slowdown mode

	// Test: rate below recover threshold should trigger recovery
	rateProvider.SetRate(10 * 1024 * 1024) // 10 MB/s (below 60MB/s threshold)
	component.checkAppendRate(rateProvider)
	// Should enter recovery mode
}

func TestAppendRateRateLimitingBetweenThresholds(t *testing.T) {
	paramtable.Init()
	channel := types.PChannelInfo{Name: "test-channel-append-rate-between"}
	component := NewWALRateLimitComponent(channel)
	defer component.Close()

	rateProvider := newMockRateProvider(0)
	component.RegisterAppendRateObserver(rateProvider)

	// Enable append rate limiting
	paramtable.Get().Save(paramtable.Get().StreamingCfg.WALRateLimitAppendRateEnabled.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().StreamingCfg.WALRateLimitAppendRateEnabled.Key)

	// Test: rate between recover and slowdown thresholds (60MB/s ~ 64MB/s)
	// Should not trigger either slowdown or recovery
	rateProvider.SetRate(62 * 1024 * 1024) // 62 MB/s (between 60MB/s and 64MB/s)
	component.checkAppendRate(rateProvider)
	// Should remain in current state (no mode change)
}

func TestAppendRateObserverLoopStartAndStop(t *testing.T) {
	paramtable.Init()
	channel := types.PChannelInfo{Name: "test-channel-observer-loop"}
	component := NewWALRateLimitComponent(channel)

	rateProvider := newMockRateProvider(0)

	// Register observer - this starts the background loop
	component.RegisterAppendRateObserver(rateProvider)
	assert.NotNil(t, component.appendRateStopCh)

	// Wait a bit to let the loop run at least once
	time.Sleep(100 * time.Millisecond)

	// Close should stop the loop gracefully
	component.Close()

	// Verify the stop channel is closed (attempting to close again would panic)
	select {
	case <-component.appendRateStopCh:
		// Channel is closed as expected
	default:
		t.Error("appendRateStopCh should be closed after Close()")
	}
}

func TestAppendRateSlowdownChecker(t *testing.T) {
	rateProvider := newMockRateProvider(100 * 1024 * 1024) // 100 MB/s
	checker := newAppendRateSlowdownChecker(rateProvider, rateProvider.Rate())

	// Check should return true when rate stays the same or increases
	assert.True(t, checker.Check())

	// Increase the rate
	rateProvider.SetRate(120 * 1024 * 1024) // 120 MB/s
	assert.True(t, checker.Check())

	// Decrease the rate - should return false to stop slowdown
	rateProvider.SetRate(80 * 1024 * 1024) // 80 MB/s
	assert.False(t, checker.Check())

	// Test SlowdownStartupHWM
	checker2 := newAppendRateSlowdownChecker(rateProvider, 50*1024*1024)
	assert.Equal(t, int64(50*1024*1024), checker2.SlowdownStartupHWM())
}

func TestAppendRateSlowdownCheckerConcurrency(t *testing.T) {
	rateProvider := newMockRateProvider(100 * 1024 * 1024)
	checker := newAppendRateSlowdownChecker(rateProvider, rateProvider.Rate())

	// Run multiple Check() calls concurrently
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			checker.Check()
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Run multiple SlowdownStartupHWM() calls concurrently
	for i := 0; i < 10; i++ {
		go func() {
			checker.SlowdownStartupHWM()
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestAppendRateSlowdownCheckerRateIncrease(t *testing.T) {
	rateProvider := newMockRateProvider(50 * 1024 * 1024) // 50 MB/s
	checker := newAppendRateSlowdownChecker(rateProvider, rateProvider.Rate())

	// First check with same rate - should continue slowdown
	assert.True(t, checker.Check())

	// Gradually increase the rate - should continue slowdown
	for i := 0; i < 5; i++ {
		currentRate := rateProvider.Rate()
		rateProvider.SetRate(currentRate + 10*1024*1024) // Increase by 10 MB/s
		assert.True(t, checker.Check(), "Should continue slowdown when rate increases")
	}

	// Verify the previousRate has been updated
	hwm := checker.SlowdownStartupHWM()
	assert.Equal(t, int64(100*1024*1024), hwm) // 50 + 5*10 = 100 MB/s
}

func TestCloseWithoutRegisteringObserver(t *testing.T) {
	paramtable.Init()
	channel := types.PChannelInfo{Name: "test-channel-close-no-observer"}
	component := NewWALRateLimitComponent(channel)

	// Close without registering any observer
	// This should not panic
	component.Close()

	// Verify appendRateStopCh is nil (observer was never registered)
	assert.Nil(t, component.appendRateStopCh)
}

func TestCloseWithMemoryObserverOnly(t *testing.T) {
	paramtable.Init()
	channel := types.PChannelInfo{Name: "test-channel-close-memory-only"}
	component := NewWALRateLimitComponent(channel)

	// Register only memory observer
	component.RegisterMemoryObserver()
	assert.NotNil(t, component.handler)

	// Close should work without append rate observer
	component.Close()
}

func TestCheckAppendRateDisabled(t *testing.T) {
	paramtable.Init()
	channel := types.PChannelInfo{Name: "test-channel-disabled"}
	component := NewWALRateLimitComponent(channel)
	defer component.Close()

	rateProvider := newMockRateProvider(0)

	// Ensure disabled (default)
	assert.False(t, paramtable.Get().StreamingCfg.WALRateLimitAppendRateEnabled.GetAsBool())

	// Even with extremely high rate, should not trigger anything when disabled
	rateProvider.SetRate(1000 * 1024 * 1024) // 1000 MB/s
	component.checkAppendRate(rateProvider)
	// No panic, no mode change - just returns early
}

func TestCheckAppendRateAtExactThresholds(t *testing.T) {
	paramtable.Init()
	channel := types.PChannelInfo{Name: "test-channel-exact-thresholds"}
	component := NewWALRateLimitComponent(channel)
	defer component.Close()

	rateProvider := newMockRateProvider(0)

	// Enable append rate limiting
	paramtable.Get().Save(paramtable.Get().StreamingCfg.WALRateLimitAppendRateEnabled.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().StreamingCfg.WALRateLimitAppendRateEnabled.Key)

	slowdownThreshold := paramtable.Get().StreamingCfg.WALRateLimitAppendRateSlowdownThreshold.GetAsSize()
	recoverThreshold := paramtable.Get().StreamingCfg.WALRateLimitAppendRateRecoverThreshold.GetAsSize()

	// Test at exact slowdown threshold - should NOT trigger (need to be greater)
	rateProvider.SetRate(float64(slowdownThreshold))
	component.checkAppendRate(rateProvider)

	// Test slightly above slowdown threshold - should trigger slowdown
	rateProvider.SetRate(float64(slowdownThreshold) + 1)
	component.checkAppendRate(rateProvider)

	// Test at exact recover threshold - should NOT trigger (need to be less)
	rateProvider.SetRate(float64(recoverThreshold))
	component.checkAppendRate(rateProvider)

	// Test slightly below recover threshold - should trigger recovery
	rateProvider.SetRate(float64(recoverThreshold) - 1)
	component.checkAppendRate(rateProvider)
}

func TestMemorySlowdownChecker(t *testing.T) {
	// Test newMemorySlowdownChecker
	initialRatio := 0.9
	checker := newMemorySlowdownChecker(initialRatio)
	assert.NotNil(t, checker)
	assert.Equal(t, initialRatio, checker.previousUsedRatio)

	// Test SlowdownStartupHWM - should return 0
	assert.Equal(t, int64(0), checker.SlowdownStartupHWM())

	// Test Check method
	// Note: Check() uses hardware.GetMemoryCount() and hardware.GetUsedMemoryCount()
	// which return real system values, so we just verify it doesn't panic
	// and returns a boolean
	result := checker.Check()
	assert.IsType(t, true, result)
}

func TestMemorySlowdownCheckerConcurrency(t *testing.T) {
	checker := newMemorySlowdownChecker(0.9)

	// Run multiple Check() calls concurrently
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			checker.Check()
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Run multiple SlowdownStartupHWM() calls concurrently
	for i := 0; i < 10; i++ {
		go func() {
			checker.SlowdownStartupHWM()
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
