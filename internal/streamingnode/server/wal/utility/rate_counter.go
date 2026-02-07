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

// RateCounter tracks the rate of bytes over a sliding time window.
// It is safe for concurrent use.
type RateCounter struct {
	mu       sync.Mutex
	window   time.Duration
	buckets  []bucket
	current  int
	lastTick time.Time
}

type bucket struct {
	bytes int64
	time  time.Time
}

// NewRateCounter creates a new rate counter with the given window duration.
// The window determines how far back in time to look when calculating the rate.
func NewRateCounter(window time.Duration) *RateCounter {
	numBuckets := 10 // Use 10 buckets for the sliding window
	buckets := make([]bucket, numBuckets)
	now := time.Now()
	for i := range buckets {
		buckets[i].time = now
	}
	return &RateCounter{
		window:   window,
		buckets:  buckets,
		current:  0,
		lastTick: now,
	}
}

// Add adds the given number of bytes to the counter.
func (r *RateCounter) Add(bytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.advanceTime()
	r.buckets[r.current].bytes += bytes
}

// Rate returns the current rate in bytes per second.
func (r *RateCounter) Rate() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.advanceTime()

	now := time.Now()
	cutoff := now.Add(-r.window)

	var totalBytes int64
	var oldest time.Time = now
	var newest time.Time

	for _, b := range r.buckets {
		if b.time.After(cutoff) {
			totalBytes += b.bytes
			if b.time.Before(oldest) {
				oldest = b.time
			}
			if b.time.After(newest) {
				newest = b.time
			}
		}
	}

	// Calculate the actual time span
	duration := newest.Sub(oldest)
	if duration <= 0 {
		duration = r.window
	}

	if duration.Seconds() == 0 {
		return 0
	}
	return float64(totalBytes) / duration.Seconds()
}

// advanceTime advances the current bucket if enough time has passed.
// Must be called with the lock held.
func (r *RateCounter) advanceTime() {
	now := time.Now()
	bucketDuration := r.window / time.Duration(len(r.buckets))

	for now.Sub(r.lastTick) >= bucketDuration {
		r.current = (r.current + 1) % len(r.buckets)
		r.buckets[r.current].bytes = 0
		r.buckets[r.current].time = now
		r.lastTick = r.lastTick.Add(bucketDuration)
	}
}
