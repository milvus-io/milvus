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
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Partial implementations refer to https://github.com/golang/time/blob/master/rate/rate_test.go

const (
	d = 100 * time.Millisecond
)

var (
	t0 = time.Now()
	t1 = t0.Add(time.Duration(1) * d)
	t2 = t0.Add(time.Duration(2) * d)
	t3 = t0.Add(time.Duration(3) * d)
	t4 = t0.Add(time.Duration(4) * d)
	t5 = t0.Add(time.Duration(5) * d)
	t9 = t0.Add(time.Duration(9) * d)
)

type allow struct {
	t            time.Time
	n            int
	ok           bool
	remainTokens float64
}

func (lim *Limiter) getTokens() float64 {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.tokens
}

func run(t *testing.T, lim *Limiter, allows []allow) {
	for i, a := range allows {
		ok := lim.AllowN(a.t, a.n)
		if ok != a.ok || int64(lim.getTokens()) != int64(a.remainTokens) {
			t.Errorf("step %d: lim.AllowN(%v, %v) = %v want %v, remainTokens = %v, wantTokens = %v",
				i, a.t, a.n, ok, a.ok, lim.getTokens(), a.remainTokens)
		}
	}
}

func runWithoutCheckToken(t *testing.T, lim *Limiter, allows []allow) {
	for i, a := range allows {
		ok := lim.AllowN(a.t, a.n)
		if ok != a.ok {
			t.Errorf("step %d: lim.AllowN(%v, %v) = %v want %v", i, a.t, a.n, ok, a.ok)
		}
	}
}

func TestLimit(t *testing.T) {
	t.Run("test limit", func(t *testing.T) {
		// test base
		now := time.Now()
		run(t, NewLimiter(100, 100), []allow{
			{now, 200, true, -100},
			{now, 100, false, -100},
			{now.Add(1 * time.Second), 300, true, -300},
			{now.Add(2 * time.Second), 1, false, -300},
			{now.Add(7 * time.Second), 50, true, 50},
			{now.Add(7 * time.Second), 60, true, -10},
			{now.Add(7 * time.Second), 10, false, -10},
		})

		// limit 10, burst 1
		run(t, NewLimiter(10, 1), []allow{
			{t0, 1, true, 0},
			{t0, 1, true, -1},
			{t0, 1, false, -1},
			{t1, 1, true, -1},
			{t1, 1, false, -1},
			{t1, 1, false, -1},
			{t2, 2, true, -2},
			{t2, 1, false, -2},
			{t2, 1, false, -2},
		})

		// limit 10, burst 3
		run(t, NewLimiter(10, 3), []allow{
			{t0, 2, true, 1},
			{t0, 2, true, -1},
			{t0, 1, false, -1},
			{t0, 1, false, -1},
			{t1, 4, true, -4},
			{t2, 1, false, -4},
			{t3, 1, false, -4},
			{t4, 1, false, -4},
			{t5, 1, true, -1},
			{t9, 3, true, 0},
			{t9, 3, true, -3},
		})

		// // start at t1
		run(t, NewLimiter(10, 3), []allow{
			{t1, 1, true, 2},
			{t0, 1, true, 1},
			{t0, 1, true, 0},
			{t0, 1, true, -1},
			{t0, 1, false, -1},
			{t1, 1, true, -1},
			{t1, 1, false, -1},
			{t1, 1, false, -1},
			{t2, 1, true, -1},
			{t2, 1, false, -1},
			{t2, 1, false, -1},
		})

		limit := Inf
		burst := math.MaxFloat64
		limiter := NewLimiter(limit, burst)
		runWithoutCheckToken(t, limiter, []allow{
			{t0, 1 * 1024 * 1024, true, 0},
			{t1, 1 * 1024 * 1024, true, 0},
			{t2, 1 * 1024 * 1024, true, 0},
			{t3, 1 * 1024 * 1024, true, 0},
			{t4, 1 * 1024 * 1024, true, 0},
			{t5, 1 * 1024 * 1024, true, 0},
		})
	})

	t.Run("test SetLimit", func(t *testing.T) {
		lim := NewLimiter(10, 10)

		run(t, lim, []allow{
			{t0, 5, true, 5},
			{t0, 1, true, 4},
			{t1, 1, true, 4},
		})
		lim.SetLimit(100)
		runWithoutCheckToken(t, lim, []allow{{t2, 10, true, 0}})
	})

	t.Run("test no truncation error", func(t *testing.T) {
		if !NewLimiter(0.7692307692307693, 1).AllowN(time.Now(), 1) {
			t.Fatal("expected true")
		}
	})

	t.Run("test zero limit", func(t *testing.T) {
		r := NewLimiter(0, 1)
		if !r.AllowN(time.Now(), 1) {
			t.Errorf("Limit(0, 1) want true when first used")
		}
		if r.AllowN(time.Now(), 1) {
			t.Errorf("Limit(0, 1) want false when already used")
		}
	})
}

// testTime is a fake time used for testing.
type testTime struct {
	mu     sync.Mutex
	cur    time.Time   // current fake time
	timers []testTimer // fake timers
}

// testTimer is a fake timer.
type testTimer struct {
	when time.Time
	ch   chan<- time.Time
}

// now returns the current fake time.
func (tt *testTime) now() time.Time {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	return tt.cur
}

// newTimer creates a fake timer. It returns the channel,
// a function to stop the timer (which we don't care about),
// and a function to advance to the next timer.
func (tt *testTime) newTimer(dur time.Duration) (<-chan time.Time, func() bool, func()) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	ch := make(chan time.Time, 1)
	timer := testTimer{
		when: tt.cur.Add(dur),
		ch:   ch,
	}
	tt.timers = append(tt.timers, timer)
	return ch, func() bool { return true }, tt.advanceToTimer
}

// since returns the fake time since the given time.
func (tt *testTime) since(t time.Time) time.Duration {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	return tt.cur.Sub(t)
}

// advance advances the fake time.
func (tt *testTime) advance(dur time.Duration) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	tt.advanceUnlocked(dur)
}

// advanceUnlock advances the fake time, assuming it is already locked.
func (tt *testTime) advanceUnlocked(dur time.Duration) {
	tt.cur = tt.cur.Add(dur)
	i := 0
	for i < len(tt.timers) {
		if tt.timers[i].when.After(tt.cur) {
			i++
		} else {
			tt.timers[i].ch <- tt.cur
			copy(tt.timers[i:], tt.timers[i+1:])
			tt.timers = tt.timers[:len(tt.timers)-1]
		}
	}
}

// advanceToTimer advances the time to the next timer.
func (tt *testTime) advanceToTimer() {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	if len(tt.timers) == 0 {
		panic("no timer")
	}
	when := tt.timers[0].when
	for _, timer := range tt.timers[1:] {
		if timer.when.Before(when) {
			when = timer.when
		}
	}
	tt.advanceUnlocked(when.Sub(tt.cur))
}

// makeTestTime hooks the testTimer into the package.
func makeTestTime(t *testing.T) *testTime {
	return &testTime{
		cur: time.Now(),
	}
}

func TestSimultaneousRequests(t *testing.T) {
	const (
		limit       = 1
		burst       = 5
		numRequests = 15
	)
	var (
		wg    sync.WaitGroup
		numOK = uint32(0)
	)

	// Very slow replenishing bucket.
	lim := NewLimiter(limit, burst)

	// Tries to take a token, atomically updates the counter and decreases the wait
	// group counter.
	f := func() {
		defer wg.Done()
		if ok := lim.AllowN(time.Now(), 1); ok {
			atomic.AddUint32(&numOK, 1)
		}
	}

	wg.Add(numRequests)
	for i := 0; i < numRequests; i++ {
		go f()
	}
	wg.Wait()
	want := burst + 1 // due to overdraft mechanism
	if numOK != uint32(want) {
		t.Errorf("numOK = %d, want %d", numOK, want)
	}
}

func TestLongRunningQPS(t *testing.T) {
	// The test runs for a few (fake) seconds executing many requests
	// and then checks that overall number of requests is reasonable.
	const (
		limit = 100
		burst = 100
	)
	var (
		numOK = int32(0)
		tt    = makeTestTime(t)
	)

	lim := NewLimiter(limit, burst)

	start := tt.now()
	end := start.Add(5 * time.Second)
	for tt.now().Before(end) {
		if ok := lim.AllowN(tt.now(), 1); ok {
			numOK++
		}

		// This will still offer ~500 requests per second, but won't consume
		// outrageous amount of CPU.
		tt.advance(2 * time.Millisecond)
	}
	elapsed := tt.since(start)
	ideal := burst + (limit * float64(elapsed) / float64(time.Second))

	// We should never get more requests than allowed.
	t.Log("numOK =", numOK, "want", int32(ideal), "ideal", ideal)
	if want := int32(ideal); numOK > want {
		t.Errorf("numOK = %d, want %d (ideal %f)", numOK, want, ideal)
	}
	// We should get very close to the number of requests allowed.
	t.Log("numOK =", numOK, "want", int32(0.999*ideal), "ideal", ideal)
	if want := int32(0.999 * ideal); numOK < want {
		t.Errorf("numOK = %d, want %d (ideal %f)", numOK, want, ideal)
	}
}

func BenchmarkLimiter_AllowN(b *testing.B) {
	lim := NewLimiter(1, 1)
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lim.AllowN(now, 1)
		}
	})
}

func TestLimiter_Cancel(t *testing.T) {
	// The test runs for a few (fake) seconds executing many requests
	// and then checks that overall number of requests is reasonable.
	const (
		limit = 100
		burst = 100
	)
	var (
		numOK = int32(0)
		tt    = makeTestTime(t)
	)

	lim := NewLimiter(limit, burst)

	start := tt.now()
	end := start.Add(5 * time.Second)
	i := 0
	cancelNum := 100
	for tt.now().Before(end) {
		if ok := lim.AllowN(tt.now(), 1); ok {
			numOK++
			// inject some cancellations
			if i <= cancelNum {
				lim.Cancel(1)
				numOK--
			}
		}

		// This will still offer ~500 requests per second, but won't consume
		// outrageous amount of CPU.
		tt.advance(100 * time.Microsecond)
		i++
	}
	elapsed := tt.since(start)
	ideal := burst + (limit * float64(elapsed) / float64(time.Second))

	// We should never get more requests than allowed.
	t.Log("numOK =", numOK, "want", int32(ideal), "ideal", ideal)
	if want := int32(ideal); numOK > want {
		t.Errorf("numOK = %d, want %d (ideal %f)", numOK, want, ideal)
	}
	// We should get very close to the number of requests allowed.
	t.Log("numOK =", numOK, "want", int32(0.999*ideal), "ideal", ideal)
	if want := int32(0.999 * ideal); numOK < want {
		t.Errorf("numOK = %d, want %d (ideal %f)", numOK, want, ideal)
	}
}
