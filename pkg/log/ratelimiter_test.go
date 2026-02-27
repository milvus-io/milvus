//go:build test

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

package log

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateLimiterInitialState(t *testing.T) {
	rl := NewRateLimiter(1.0, 10.0)

	// Initial balance should be maxBalance, so we can consume up to 10 credits.
	assert.True(t, rl.CheckCredit(10.0), "should have maxBalance credits available initially")
}

func TestRateLimiterExhaustsCredits(t *testing.T) {
	rl := NewRateLimiter(1.0, 5.0)

	// Consume all 5 credits.
	assert.True(t, rl.CheckCredit(5.0), "should succeed consuming all credits")
	// Now balance is 0 (plus tiny elapsed time replenishment); a cost of 1 should fail.
	assert.False(t, rl.CheckCredit(1.0), "should fail after credits exhausted")
}

func TestRateLimiterRecovery(t *testing.T) {
	// 100 credits/sec, max 10 credits. Start full, drain, then wait for recovery.
	rl := NewRateLimiter(100.0, 10.0)

	// Drain all credits.
	assert.True(t, rl.CheckCredit(10.0))
	assert.False(t, rl.CheckCredit(1.0))

	// Wait 100ms -> should recover ~10 credits (100 credits/sec * 0.1s = 10).
	time.Sleep(110 * time.Millisecond)

	assert.True(t, rl.CheckCredit(5.0), "should have recovered credits after sleeping")
}

func TestRateLimiterUpdateProRatesBalance(t *testing.T) {
	rl := NewRateLimiter(1.0, 10.0)

	// Consume 5, leaving balance ~5 out of 10 max.
	rl.CheckCredit(5.0)

	// Update maxBalance from 10 -> 20. Balance should be pro-rated: 5 * 20/10 = 10.
	rl.Update(1.0, 20.0)

	// The pro-rated balance should be ~10. Consuming 9 should succeed.
	assert.True(t, rl.CheckCredit(9.0), "pro-rated balance should allow consuming 9 credits")
}

func TestRateLimiterUpdateZeroMaxBalance(t *testing.T) {
	rl := NewRateLimiter(1.0, 10.0)

	// Update with maxBalance=0 should not panic (divide by zero guard).
	assert.NotPanics(t, func() {
		rl.Update(1.0, 0.0)
	})

	// After setting maxBalance to 0, the balance is 0 (pro-rated: old_balance * 0 / old_max = 0).
	// But since old maxBalance was 10 (>0), the pro-rate runs: balance = balance * 0 / 10 = 0.
	// Actually, after Update sets maxBalance to 0, the guard `if r.maxBalance > 0` now
	// refers to the OLD maxBalance (10), so the pro-rate runs: balance * 0/10 = 0.
	assert.False(t, rl.CheckCredit(1.0), "balance should be zero after updating maxBalance to 0")
}

func TestRateLimiterCheckCreditZeroCost(t *testing.T) {
	rl := NewRateLimiter(1.0, 1.0)

	// Drain all credits.
	rl.CheckCredit(1.0)

	// Zero-cost check should always succeed since balance >= 0 >= 0.
	assert.True(t, rl.CheckCredit(0.0), "zero cost should always succeed")
}

func TestRateLimiterBalanceCappedAtMax(t *testing.T) {
	// Even with high credits/sec, balance should never exceed maxBalance.
	rl := NewRateLimiter(1000.0, 5.0)

	time.Sleep(50 * time.Millisecond)

	// After sleeping, accrued credits would be ~50 but capped at maxBalance=5.
	assert.True(t, rl.CheckCredit(5.0), "should be able to consume up to maxBalance")
	assert.False(t, rl.CheckCredit(1.0), "should not have more than maxBalance credits")
}
