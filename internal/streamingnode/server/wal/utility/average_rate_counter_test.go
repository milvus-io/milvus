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

func TestNewAverageRateCounter(t *testing.T) {
	window := 10 * time.Second
	rc := NewAverageRateCounter(window)

	assert.NotNil(t, rc)
	assert.Equal(t, window, rc.window)
	assert.Len(t, rc.buckets, numBuckets)
	assert.Equal(t, 0, rc.current)
	assert.Equal(t, int64(0), rc.total)
}

func TestAverageRateCounter_InitiallyZero(t *testing.T) {
	rc := NewAverageRateCounter(200 * time.Millisecond)
	assert.Equal(t, float64(0), rc.Rate())
}

func TestAverageRateCounter_AddAndRate(t *testing.T) {
	rc := NewAverageRateCounter(200 * time.Millisecond)

	rc.Add(1000)
	rate := rc.Rate()
	// Rate = 1000 / elapsed. elapsed is tiny, so rate should be very high.
	assert.True(t, rate > 0)
}

func TestAverageRateCounter_ZeroAdd(t *testing.T) {
	rc := NewAverageRateCounter(200 * time.Millisecond)

	rc.Add(0)
	rc.Add(0)
	assert.Equal(t, float64(0), rc.Rate())
}

func TestAverageRateCounter_ExpiresAfterWindow(t *testing.T) {
	window := 200 * time.Millisecond
	rc := NewAverageRateCounter(window)

	rc.Add(5000)
	rate := rc.Rate()
	assert.True(t, rate > 0, "rate should be positive right after add")

	// Wait for the entire window to pass, all data should expire.
	time.Sleep(window + 50*time.Millisecond)

	rate = rc.Rate()
	assert.Equal(t, float64(0), rate, "rate should be 0 after data expires")
	assert.Equal(t, int64(0), rc.total, "total should be 0 after all buckets expire")
}

func TestAverageRateCounter_TotalConsistency(t *testing.T) {
	window := 200 * time.Millisecond
	bucketDuration := window / numBuckets // 10ms per bucket
	rc := NewAverageRateCounter(window)

	// Add to first bucket.
	rc.Add(100)
	assert.Equal(t, int64(100), rc.total)

	// Advance to next bucket and add.
	time.Sleep(bucketDuration + 2*time.Millisecond)
	rc.Add(200)
	assert.Equal(t, int64(300), rc.total)

	// Wait for the window to expire all data.
	time.Sleep(window + 50*time.Millisecond)
	rc.Rate() // trigger advanceTime
	assert.Equal(t, int64(0), rc.total, "total should be 0 after full window elapses")
}

func TestAverageRateCounter_BucketAdvance(t *testing.T) {
	window := 200 * time.Millisecond
	bucketDuration := window / numBuckets // 10ms per bucket
	rc := NewAverageRateCounter(window)

	// Add to first bucket.
	rc.Add(100)

	// Wait for one bucket duration.
	time.Sleep(bucketDuration + 2*time.Millisecond)

	// Add to second bucket.
	rc.Add(200)

	// Both buckets should contribute. total = 300.
	rate := rc.Rate()
	assert.True(t, rate > 0)
	assert.Equal(t, int64(300), rc.total)
}

func TestAverageRateCounter_RateStableWithinWindow(t *testing.T) {
	window := 200 * time.Millisecond
	rc := NewAverageRateCounter(window)

	rc.Add(1000)

	// Rate = total / window = 1000 / 0.2 = 5000, stable within window.
	rateEarly := rc.Rate()
	assert.True(t, rateEarly > 0)

	// Wait for half the window. Rate should remain the same (total unchanged, divisor is fixed window).
	time.Sleep(window / 2)
	rateMid := rc.Rate()
	assert.Equal(t, rateEarly, rateMid, "rate should be stable within window when no new adds")

	// Wait for the rest of the window. Data expires, rate should be 0.
	time.Sleep(window/2 + 50*time.Millisecond)
	rateLate := rc.Rate()
	assert.Equal(t, float64(0), rateLate, "rate should be 0 after window expires")
}

func TestAverageRateCounter_SteadyRate(t *testing.T) {
	window := 400 * time.Millisecond
	bucketDuration := window / numBuckets // 20ms per bucket
	rc := NewAverageRateCounter(window)

	// Add data at a steady rate for a full window.
	bytesPerTick := int64(100)
	ticks := 20
	for i := 0; i < ticks; i++ {
		rc.Add(bytesPerTick)
		time.Sleep(bucketDuration)
	}

	// After a full window of steady adds:
	// total should be close to ticks * bytesPerTick = 2000
	// rate should be close to 2000 / 0.4s = 5000 bytes/s
	rate := rc.Rate()
	expectedRate := float64(ticks) * float64(bytesPerTick) / window.Seconds()
	// Allow 30% tolerance for timing jitter in CI.
	assert.InDelta(t, expectedRate, rate, expectedRate*0.3, "rate should approximate steady input rate")
}

func TestAverageRateCounter_ConcurrentAccess(t *testing.T) {
	rc := NewAverageRateCounter(200 * time.Millisecond)

	var wg sync.WaitGroup
	numGoroutines := 10
	addsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < addsPerGoroutine; j++ {
				rc.Add(10)
			}
		}()
	}

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

	rate := rc.Rate()
	assert.True(t, rate >= 0)
}

func TestAverageRateCounter_LargeValues(t *testing.T) {
	rc := NewAverageRateCounter(200 * time.Millisecond)

	rc.Add(1 << 30) // 1 GB
	rc.Add(1 << 30) // 1 GB

	rate := rc.Rate()
	assert.True(t, rate > 0)
}

func TestAverageRateCounter_LongPauseResetsCleanly(t *testing.T) {
	window := 200 * time.Millisecond
	rc := NewAverageRateCounter(window)

	rc.Add(5000)
	assert.True(t, rc.Rate() > 0)

	// Wait much longer than the window (5x).
	time.Sleep(5 * window)

	// All data should be expired. Rate and total should be 0.
	assert.Equal(t, float64(0), rc.Rate())
	assert.Equal(t, int64(0), rc.total)

	// Adding again should work normally.
	rc.Add(1000)
	assert.True(t, rc.Rate() > 0)
	assert.Equal(t, int64(1000), rc.total)
}
