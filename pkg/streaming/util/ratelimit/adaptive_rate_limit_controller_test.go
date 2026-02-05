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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

type MockConfigFetcher struct {
	mock.Mock
}

func (m *MockConfigFetcher) FetchRecoveryConfig() RecoveryConfig {
	args := m.Called()
	return args.Get(0).(RecoveryConfig)
}

func (m *MockConfigFetcher) FetchSlowdownConfig() SlowdownConfig {
	args := m.Called()
	return args.Get(0).(SlowdownConfig)
}

func (m *MockConfigFetcher) Close() {}

func setupTest(_ *testing.T) (types.PChannelInfo, string, *MuxRateLimitObserverRegistryImpl, *MockRateLimitObserver, *MockConfigFetcher) {
	channel := types.PChannelInfo{Name: "test-channel"}
	sourceName := "test-source"
	mux := NewMuxRateLimitObserverRegistry()
	observer := new(MockRateLimitObserver)
	observer.On("UpdateRateLimitState", NewNormalRateLimitState()).Once()
	mux.Register(observer)
	fetcher := new(MockConfigFetcher)
	return channel, sourceName, mux, observer, fetcher
}

// mockSlowdownChecker implements SlowdownChecker interface for testing.
type mockSlowdownChecker struct {
	checkFunc func() bool
	hwm       int64
}

func (m *mockSlowdownChecker) Check() bool {
	if m.checkFunc != nil {
		return m.checkFunc()
	}
	return true // Always continue slowdown by default
}

func (m *mockSlowdownChecker) SlowdownStartupHWM() int64 {
	return m.hwm
}

// newAlwaysSlowdownChecker returns a checker that always continues slowdown.
func newAlwaysSlowdownChecker() SlowdownChecker {
	return &mockSlowdownChecker{checkFunc: func() bool { return true }, hwm: 0}
}

func TestAdaptiveRateLimitController_ModeString(t *testing.T) {
	assert.Equal(t, "normal", adaptiveRateLimitModeNormal.String())
	assert.Equal(t, "slowdown", adaptiveRateLimitModeSlowdown.String())
	assert.Equal(t, "reject", adaptiveRateLimitModeReject.String())
	assert.Equal(t, "recovery", adaptiveRateLimitModeRecovery.String())
	assert.Equal(t, "", adaptiveRateLimitMode(99).String())
}

func TestAdaptiveRateLimitController_ModeTransition(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	observer.On("UpdateRateLimitState", mock.Anything).Maybe()
	slowdownCfg := SlowdownConfig{
		HWM:                 100,
		LWM:                 50,
		DecreaseInterval:    10 * time.Millisecond,
		DecreaseRatio:       0.8,
		RejectDelayInterval: 0, // No reject delay so recovery can proceed
	}
	fetcher.On("FetchSlowdownConfig").Return(slowdownCfg)
	recoveryCfg := RecoveryConfig{
		HWM:                 100,
		LWM:                 60,
		IncreaseInterval:    10 * time.Millisecond,
		Incremental:         20,
		NormalDelayInterval: 10 * time.Millisecond,
	}
	fetcher.On("FetchRecoveryConfig").Return(recoveryCfg)

	alwaysSlowdown := newAlwaysSlowdownChecker()
	for i := 0; i < 50; i++ {
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		switch rand.Intn(3) {
		case 0:
			controller.EnterRejectMode()
		case 1:
			controller.EnterSlowdownMode(alwaysSlowdown)
		case 2:
			controller.EnterRecoveryMode()
		}
	}
	// Force enter reject mode then recovery to ensure a clean transition path
	controller.EnterRejectMode()
	time.Sleep(50 * time.Millisecond)
	controller.EnterRecoveryMode()
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeNormal
	}, 5*time.Second, 10*time.Millisecond)
	observer.AssertExpectations(t)
}

func TestAdaptiveRateLimitController_EnterRejectMode(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	rejectState := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT, Rate: 0}
	observer.On("UpdateRateLimitState", rejectState).Once()

	controller.EnterRejectMode()
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeReject
	}, 1*time.Second, 10*time.Millisecond)
	observer.AssertExpectations(t)

	// Enter again should do nothing
	controller.EnterRejectMode()
}

func TestAdaptiveRateLimitController_EnterSlowdownMode(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	slowdownCfg := SlowdownConfig{
		HWM:                 100,
		LWM:                 50,
		DecreaseInterval:    10 * time.Millisecond,
		DecreaseRatio:       0.8,
		RejectDelayInterval: 50 * time.Millisecond,
	}
	fetcher.On("FetchSlowdownConfig").Return(slowdownCfg)

	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 100}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 80}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 64}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 51}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 50}).Once()
	observer.On("UpdateRateLimitState", RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT, Rate: 0}).Once()

	// Use nil checker (always continue slowdown)
	controller.EnterSlowdownMode(nil)
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeReject
	}, 2*time.Second, 10*time.Millisecond)

	observer.AssertExpectations(t)
}

func TestAdaptiveRateLimitController_EnterRecoveryMode(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	slowdownCfg := SlowdownConfig{
		HWM:                 100,
		LWM:                 50,
		DecreaseInterval:    10 * time.Millisecond,
		DecreaseRatio:       0.5,
		RejectDelayInterval: 0, // No reject delay, will stop at LWM
	}
	fetcher.On("FetchSlowdownConfig").Return(slowdownCfg)

	recoveryCfg := RecoveryConfig{
		HWM:                 100,
		LWM:                 60,
		IncreaseInterval:    10 * time.Millisecond,
		Incremental:         15,
		NormalDelayInterval: 10 * time.Millisecond,
	}
	fetcher.On("FetchRecoveryConfig").Return(recoveryCfg)

	observer.On("UpdateRateLimitState", mock.Anything).Maybe()

	// First enter slowdown mode
	controller.EnterSlowdownMode(nil)
	// Wait for slowdown mode to be entered first
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeSlowdown
	}, 2*time.Second, 10*time.Millisecond)
	// Then wait for rate to reach LWM
	assert.Eventually(t, func() bool {
		return controller.getCurrentRate() == 50
	}, 2*time.Second, 10*time.Millisecond)

	// Now enter recovery mode
	controller.EnterRecoveryMode()
	controller.EnterRecoveryMode() // Second call should be ignored

	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeNormal
	}, 2*time.Second, 10*time.Millisecond)

	observer.AssertExpectations(t)
}

func TestAdaptiveRateLimitController_EnterRecoveryFromMaxInt64(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	// Configure slowdown with a first delay to allow entering recovery before slowdown actually starts
	slowdownCfg := SlowdownConfig{
		HWM:                 100,
		LWM:                 50,
		DecreaseInterval:    100 * time.Millisecond,
		DecreaseRatio:       0.8,
		RejectDelayInterval: 0,
		FirstSlowdownDelay:  500 * time.Millisecond, // Long delay so we can interrupt with recovery
	}
	fetcher.On("FetchSlowdownConfig").Return(slowdownCfg)

	// Also need to set up recovery config since it might be fetched
	recoveryCfg := RecoveryConfig{
		HWM:                 100,
		LWM:                 60,
		IncreaseInterval:    10 * time.Millisecond,
		Incremental:         15,
		NormalDelayInterval: 10 * time.Millisecond,
	}
	fetcher.On("FetchRecoveryConfig").Return(recoveryCfg).Maybe()

	observer.On("UpdateRateLimitState", mock.Anything).Maybe()

	// Enter slowdown mode (currentRate will be MaxInt64 until FirstSlowdownDelay passes)
	controller.EnterSlowdownMode(nil)
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeSlowdown
	}, 1*time.Second, 10*time.Millisecond)

	// Enter recovery before slowdown actually starts applying rates
	// Since currentRate is still MaxInt64, it should go directly to normal mode
	controller.EnterRecoveryMode()
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeNormal
	}, 1*time.Second, 10*time.Millisecond)

	observer.AssertExpectations(t)
}

func TestAdaptiveRateLimitController_SlowdownWithStartupDelay(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	slowdownCfg := SlowdownConfig{
		HWM:                 200,
		LWM:                 100,
		DecreaseInterval:    50 * time.Millisecond,
		DecreaseRatio:       0.5,
		RejectDelayInterval: 0,
		FirstSlowdownDelay:  50 * time.Millisecond,
	}
	fetcher.On("FetchSlowdownConfig").Return(slowdownCfg)

	observer.On("UpdateRateLimitState", mock.Anything).Maybe()

	start := time.Now()
	controller.EnterSlowdownMode(nil)
	controller.EnterSlowdownMode(nil) // Second call should be ignored

	// Wait for rate to reach LWM (100)
	// Flow: delay (50ms) -> HWM (200) -> tick (50ms) -> LWM (100)
	assert.Eventually(t, func() bool {
		return controller.getCurrentRate() == 100
	}, 2*time.Second, 10*time.Millisecond)

	assert.True(t, time.Since(start) >= slowdownCfg.FirstSlowdownDelay)
}

func TestAdaptiveRateLimitController_SlowdownStartsFromCurrentRate(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	slowdownCfg := SlowdownConfig{
		HWM:                 200,
		LWM:                 50,
		DecreaseInterval:    10 * time.Millisecond,
		DecreaseRatio:       0.5,
		RejectDelayInterval: 0,
	}
	fetcher.On("FetchSlowdownConfig").Return(slowdownCfg)

	recoveryCfg := RecoveryConfig{
		HWM:                 200,
		LWM:                 60,
		IncreaseInterval:    10 * time.Millisecond,
		Incremental:         20,
		NormalDelayInterval: 10 * time.Millisecond,
	}
	fetcher.On("FetchRecoveryConfig").Return(recoveryCfg)

	observer.On("UpdateRateLimitState", mock.Anything).Maybe()

	// First enter slowdown mode from normal (currentRate = 0, should start from HWM = 200)
	controller.EnterSlowdownMode(nil)
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeSlowdown
	}, 1*time.Second, 10*time.Millisecond)

	// Wait for rate to decrease a bit (200 -> 100 -> 50)
	assert.Eventually(t, func() bool {
		return controller.getCurrentRate() == 50
	}, 2*time.Second, 10*time.Millisecond)

	// Enter recovery mode
	controller.EnterRecoveryMode()
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeRecovery
	}, 1*time.Second, 10*time.Millisecond)

	// Wait for rate to increase to some value (e.g., 100)
	assert.Eventually(t, func() bool {
		return controller.getCurrentRate() >= 100
	}, 2*time.Second, 10*time.Millisecond)

	// Get current rate before re-entering slowdown
	rateBeforeSlowdown := controller.getCurrentRate()

	// Re-enter slowdown mode - should start from min(currentRate, HWM)
	// Since currentRate < HWM, it should start from currentRate
	controller.EnterSlowdownMode(nil)
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeSlowdown
	}, 1*time.Second, 10*time.Millisecond)

	// The initial rate should be rateBeforeSlowdown (not HWM)
	assert.Eventually(t, func() bool {
		rate := controller.getCurrentRate()
		// Rate should be <= rateBeforeSlowdown (started from there, then decreased)
		return rate > 0 && rate <= rateBeforeSlowdown
	}, 1*time.Second, 10*time.Millisecond)
}

func TestAdaptiveRateLimitController_FirstSlowdownDelayOnlyOnce(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	slowdownCfg := SlowdownConfig{
		HWM:                 100,
		LWM:                 50,
		DecreaseInterval:    10 * time.Millisecond,
		DecreaseRatio:       0.5,
		RejectDelayInterval: 0,
		FirstSlowdownDelay:  100 * time.Millisecond, // 100ms delay
	}
	fetcher.On("FetchSlowdownConfig").Return(slowdownCfg)

	recoveryCfg := RecoveryConfig{
		HWM:                 100,
		LWM:                 60,
		IncreaseInterval:    10 * time.Millisecond,
		Incremental:         20,
		NormalDelayInterval: 10 * time.Millisecond,
	}
	fetcher.On("FetchRecoveryConfig").Return(recoveryCfg)

	observer.On("UpdateRateLimitState", mock.Anything).Maybe()

	// First slowdown - should have delay
	start := time.Now()
	controller.EnterSlowdownMode(nil)
	assert.Eventually(t, func() bool {
		return controller.getCurrentRate() == 50
	}, 2*time.Second, 10*time.Millisecond)
	firstSlowdownDuration := time.Since(start)
	assert.True(t, firstSlowdownDuration >= slowdownCfg.FirstSlowdownDelay, "First slowdown should have delay")

	// Enter recovery
	controller.EnterRecoveryMode()
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeNormal
	}, 2*time.Second, 10*time.Millisecond)

	// Second slowdown - should NOT have delay (firstSlowdownDelayExecuted = true)
	start = time.Now()
	controller.EnterSlowdownMode(nil)
	assert.Eventually(t, func() bool {
		return controller.getCurrentRate() == 50
	}, 2*time.Second, 10*time.Millisecond)
	secondSlowdownDuration := time.Since(start)

	// Second slowdown should be faster (no delay)
	assert.True(t, secondSlowdownDuration < firstSlowdownDuration, "Second slowdown should not have delay")
}

func TestAdaptiveRateLimitController_SlowdownChecker(t *testing.T) {
	channel, sourceName, mux, observer, fetcher := setupTest(t)
	controller := NewAdaptiveRateLimitController(channel, sourceName, mux, fetcher)
	defer controller.Close()

	slowdownCfg := SlowdownConfig{
		HWM:                 100,
		LWM:                 10,
		DecreaseInterval:    10 * time.Millisecond,
		DecreaseRatio:       0.5,
		RejectDelayInterval: 0,
	}
	fetcher.On("FetchSlowdownConfig").Return(slowdownCfg)

	recoveryCfg := RecoveryConfig{
		HWM:                 100,
		LWM:                 60,
		IncreaseInterval:    10 * time.Millisecond,
		Incremental:         20,
		NormalDelayInterval: 10 * time.Millisecond,
	}
	fetcher.On("FetchRecoveryConfig").Return(recoveryCfg)

	observer.On("UpdateRateLimitState", mock.Anything).Maybe()

	// Create a checker that returns false after rate drops below 50
	checker := &mockSlowdownChecker{
		checkFunc: func() bool {
			rate := controller.getCurrentRate()
			return rate >= 50 // Continue slowdown while rate >= 50
		},
		hwm: 0, // Use default HWM from config
	}

	controller.EnterSlowdownMode(checker)
	assert.Eventually(t, func() bool {
		return controller.getMode() == adaptiveRateLimitModeSlowdown
	}, 1*time.Second, 10*time.Millisecond)

	// Wait for slowdown to stop (checker returns false when rate < 50)
	// The rate should stop around 50 (or slightly below due to one more tick)
	assert.Eventually(t, func() bool {
		rate := controller.getCurrentRate()
		// Rate should be around 50 or just below (last tick before checker returned false)
		return rate <= 50 && rate >= 25 // 100 -> 50 -> 25 (checker fails at 25)
	}, 2*time.Second, 10*time.Millisecond)

	// Mode should still be slowdown (stopped at LWM or checker)
	assert.Equal(t, adaptiveRateLimitModeSlowdown, controller.getMode())

	// Verify rate didn't go all the way down to LWM (10)
	assert.True(t, controller.getCurrentRate() > slowdownCfg.LWM, "Rate should not reach LWM when checker stops slowdown")
}
