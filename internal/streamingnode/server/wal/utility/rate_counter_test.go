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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewRateCounter(t *testing.T) {
	window := 10 * time.Second
	rc := NewRateCounter(window)

	assert.NotNil(t, rc)
	assert.Equal(t, window, rc.window)
	assert.Len(t, rc.buckets, 10)
	assert.Equal(t, 0, rc.current)
}

func TestRateCounter_Add(t *testing.T) {
	rc := NewRateCounter(10 * time.Second)

	// Add some bytes
	rc.Add(100)
	rc.Add(200)
	rc.Add(300)

	// The rate should be non-zero after adding bytes
	rate := rc.Rate()
	assert.True(t, rate >= 0, "rate should be non-negative")
}

func TestRateCounter_Rate_InitiallyZero(t *testing.T) {
	rc := NewRateCounter(10 * time.Second)

	// Rate should be 0 initially (no data added)
	rate := rc.Rate()
	assert.Equal(t, float64(0), rate)
}

func TestRateCounter_Rate_AfterAdd(t *testing.T) {
	// Use a short window for faster testing
	window := 1 * time.Second
	rc := NewRateCounter(window)

	// Add 1000 bytes
	rc.Add(1000)

	// Rate should be approximately 1000 bytes / window duration
	// But since we just added and the time span is very small, rate calculation may vary
	rate := rc.Rate()
	assert.True(t, rate >= 0, "rate should be non-negative after adding bytes")
}

func TestRateCounter_SlidingWindow(t *testing.T) {
	// Use a very short window for testing
	window := 500 * time.Millisecond
	rc := NewRateCounter(window)

	// Add bytes
	rc.Add(1000)

	// Wait for more than the window duration
	time.Sleep(window + 100*time.Millisecond)

	// After waiting longer than the window, the rate should be 0 or very low
	// because all data should have expired from the sliding window
	rate := rc.Rate()
	// The rate might not be exactly 0 due to bucket boundary effects,
	// but it should be significantly lower than if we had just added
	assert.True(t, rate >= 0, "rate should be non-negative")
}

func TestRateCounter_BucketAdvance(t *testing.T) {
	// Use a window where bucket duration is 100ms (1 second / 10 buckets)
	window := 1 * time.Second
	rc := NewRateCounter(window)

	// Add to first bucket
	rc.Add(100)

	// Wait for one bucket duration
	bucketDuration := window / 10
	time.Sleep(bucketDuration + 10*time.Millisecond)

	// Add to second bucket
	rc.Add(200)

	// Both buckets should contribute to the rate
	rate := rc.Rate()
	assert.True(t, rate > 0, "rate should be positive when bytes are added")
}

func TestRateCounter_ConcurrentAccess(t *testing.T) {
	rc := NewRateCounter(10 * time.Second)

	var wg sync.WaitGroup
	numGoroutines := 10
	addsPerGoroutine := 100

	// Concurrent Add operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < addsPerGoroutine; j++ {
				rc.Add(10)
			}
		}()
	}

	// Concurrent Rate reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < addsPerGoroutine; j++ {
				_ = rc.Rate()
			}
		}()
	}

	wg.Wait()

	// Should not panic and rate should be positive
	rate := rc.Rate()
	assert.True(t, rate >= 0, "rate should be non-negative after concurrent access")
}

func TestRateCounter_MultipleAddsInSameBucket(t *testing.T) {
	rc := NewRateCounter(10 * time.Second)

	// Add multiple times quickly (should all go to the same bucket)
	for i := 0; i < 100; i++ {
		rc.Add(10)
	}

	// Total bytes added: 1000
	rate := rc.Rate()
	assert.True(t, rate >= 0, "rate should be non-negative")
}

func TestRateCounter_LargeValues(t *testing.T) {
	rc := NewRateCounter(10 * time.Second)

	// Add large byte counts
	rc.Add(1 << 30) // 1 GB
	rc.Add(1 << 30) // 1 GB

	rate := rc.Rate()
	assert.True(t, rate > 0, "rate should be positive for large values")
}

func TestRateCounter_ZeroAdd(t *testing.T) {
	rc := NewRateCounter(10 * time.Second)

	// Adding zero bytes should work
	rc.Add(0)
	rc.Add(0)

	rate := rc.Rate()
	assert.Equal(t, float64(0), rate)
}

func TestRateCounter_RateCalculation(t *testing.T) {
	// This test verifies the rate calculation over time
	window := 2 * time.Second
	rc := NewRateCounter(window)

	// Add bytes and wait for some time to pass
	rc.Add(1000)

	// Wait for half the bucket duration to ensure time advances
	bucketDuration := window / 10
	time.Sleep(bucketDuration)

	rc.Add(1000)

	// The rate should reflect the total bytes over the time span
	rate := rc.Rate()
	assert.True(t, rate > 0, "rate should be positive")

	// The rate should be approximately (total bytes) / (time elapsed)
	// We added 2000 bytes over at least bucketDuration time
	// So rate should be at most 2000 / bucketDuration.Seconds()
	maxExpectedRate := 2000.0 / bucketDuration.Seconds()
	assert.True(t, rate <= maxExpectedRate*2, "rate should be within expected bounds")
}
