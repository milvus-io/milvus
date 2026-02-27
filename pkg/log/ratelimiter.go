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
	"sync"
	"time"
)

// RateLimiter is a simple token bucket rate limiter that replaces
// the deprecated jaeger-client-go utils.ReconfigurableRateLimiter.
type RateLimiter struct {
	sync.Mutex
	creditsPerSecond float64
	maxBalance       float64
	balance          float64
	lastTime         time.Time
}

// NewRateLimiter creates a rate limiter with the given rate and max balance.
func NewRateLimiter(creditsPerSecond, maxBalance float64) *RateLimiter {
	return &RateLimiter{
		creditsPerSecond: creditsPerSecond,
		maxBalance:       maxBalance,
		balance:          maxBalance,
		lastTime:         time.Now(),
	}
}

// CheckCredit checks if the given amount of credits is available.
func (r *RateLimiter) CheckCredit(itemCost float64) bool {
	r.Lock()
	defer r.Unlock()

	now := time.Now()
	elapsed := now.Sub(r.lastTime).Seconds()
	r.lastTime = now
	r.balance += elapsed * r.creditsPerSecond
	if r.balance > r.maxBalance {
		r.balance = r.maxBalance
	}
	if r.balance >= itemCost {
		r.balance -= itemCost
		return true
	}
	return false
}

// Update reconfigures the rate limiter.
func (r *RateLimiter) Update(creditsPerSecond, maxBalance float64) {
	r.Lock()
	defer r.Unlock()
	r.creditsPerSecond = creditsPerSecond
	r.maxBalance = maxBalance
}
