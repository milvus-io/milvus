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

package ratelimitutil

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// Partial implementations refer to https://github.com/golang/time/blob/master/rate/rate.go

// Limit defines the maximum frequency of some events.
// Limit is represented as number of events per second.
// A zero Limit allows no events.
type Limit float64

// Inf is the infinite rate limit; it allows all events.
const Inf = Limit(math.MaxFloat64)

// A Limiter controls how frequently events are allowed to happen.
// It implements a "token bucket" of size b, initially full and refilled
// at rate r tokens per second.
// See https://en.wikipedia.org/wiki/Token_bucket for more about token buckets.
//
// However, Limiter is a little different from token-bucket. It is based on a
// special punishment mechanism, a very large number of events are allowed
// as long as the number of tokens in bucket is greater or equal to 0.
// After these large number of events toke tokens from bucket, the number of tokens
// in bucket may be negative, and the latter events would be "punished",
// any event should wait for the tokens to be filled to greater or equal to 0.
type Limiter struct {
	mu     sync.Mutex
	limit  Limit
	burst  float64
	tokens float64
	// last is the last time the limiter's tokens field was updated
	last time.Time
}

// NewLimiter returns a new Limiter that allows events up to rate r.
func NewLimiter(r Limit, b float64) *Limiter {
	return &Limiter{
		limit: r,
		burst: b,
	}
}

// Limit returns the maximum overall event rate.
func (lim *Limiter) Limit() Limit {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.limit
}

// AllowN reports whether n events may happen at time now.
func (lim *Limiter) AllowN(now time.Time, n int) bool {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	if lim.limit == Inf {
		return true
	} else if lim.limit == 0 {
		var ok bool
		if lim.burst >= float64(n) {
			ok = true
			lim.burst -= float64(n)
		}
		return ok
	}

	now, last, tokens := lim.advance(now)

	ok := tokens >= 0

	// Calculate the remaining number of tokens resulting from the request.
	tokens -= float64(n)

	// Update state
	if ok {
		lim.last = now
		// tokens may be negative
		lim.tokens = tokens
	} else {
		lim.last = last
	}

	return ok
}

// SetLimit sets a new Limit for the limiter.
func (lim *Limiter) SetLimit(newLimit Limit) {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	now, _, tokens := lim.advance(time.Now())

	lim.last = now
	lim.tokens = tokens
	lim.limit = newLimit
	if newLimit >= math.MaxFloat64 {
		lim.burst = math.MaxInt
	} else {
		// use rate as burst, because Limiter is with punishment mechanism, burst is insignificant.
		lim.burst = float64(newLimit)
	}
}

// Cancel the AllowN operation and refund the tokens that have already been deducted by the limiter.
func (lim *Limiter) Cancel(n int) {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	lim.tokens += float64(n)
}

// advance calculates and returns an updated state for lim resulting from the passage of time.
// lim is not changed. advance requires that lim.mu is held.
func (lim *Limiter) advance(now time.Time) (newNow time.Time, newLast time.Time, newTokens float64) {
	last := lim.last
	if now.Before(last) {
		last = now
	}

	// Calculate the new number of tokens, due to time that passed.
	elapsed := now.Sub(last)
	delta := lim.limit.tokensFromDuration(elapsed)
	tokens := lim.tokens + delta
	if burst := lim.burst; tokens > burst {
		tokens = burst
	}
	return now, last, tokens
}

// String returns string of Limit.
func (limit Limit) String() string {
	if limit == Inf {
		return "+inf"
	}
	return fmt.Sprintf("%v", float64(limit))
}

// tokensFromDuration is a unit conversion function from a time duration to the number of tokens
// which could be accumulated during that duration at a rate of limit tokens per second.
func (limit Limit) tokensFromDuration(d time.Duration) float64 {
	if limit <= 0 {
		return 0
	}
	return d.Seconds() * float64(limit)
}
