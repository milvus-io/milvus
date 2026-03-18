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

package utility

import (
	"sync"
	"time"
)

const numBuckets = 20 // max error ~5% (1/numBuckets)

// AverageRateCounter tracks the average rate of bytes over a sliding time window.
// Rate() returns total / window (bytes per second over the last window duration).
// It maintains a running total for O(1) Rate() queries.
// It is safe for concurrent use.
type AverageRateCounter struct {
	mu       sync.Mutex
	window   time.Duration
	buckets  []int64
	total    int64 // running sum of all bucket bytes
	current  int
	lastTick time.Time
}

// NewAverageRateCounter creates a new rate counter with the given window duration.
// The window determines how far back in time to look when calculating the rate.
func NewAverageRateCounter(window time.Duration) *AverageRateCounter {
	return &AverageRateCounter{
		window:   window,
		buckets:  make([]int64, numBuckets),
		current:  0,
		lastTick: time.Now(),
	}
}

// Add adds the given number of bytes to the counter.
func (r *AverageRateCounter) Add(bytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	r.advanceTime(now)
	r.buckets[r.current] += bytes
	r.total += bytes
}

// Rate returns the average rate in bytes per second over the last window duration.
func (r *AverageRateCounter) Rate() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	r.advanceTime(now)

	return float64(r.total) / r.window.Seconds()
}

// advanceTime advances the current bucket if enough time has passed.
// When a bucket is evicted, its bytes are subtracted from the running total.
// Must be called with the lock held.
func (r *AverageRateCounter) advanceTime(now time.Time) {
	bucketDuration := r.window / time.Duration(len(r.buckets))

	for now.Sub(r.lastTick) >= bucketDuration {
		r.current = (r.current + 1) % len(r.buckets)
		r.total -= r.buckets[r.current]
		r.buckets[r.current] = 0
		r.lastTick = r.lastTick.Add(bucketDuration)
	}
}
