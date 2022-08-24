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

const (
	DefaultWindow      = 10 * time.Second
	DefaultGranularity = 1 * time.Second
	DefaultAvgDuration = 3 * time.Second
)

// RateCollector helps to collect and calculate values (like throughput, QPS, TPS, etc...),
// It implements a sliding window with custom size and granularity to store values.
type RateCollector struct {
	sync.Mutex

	window      time.Duration
	granularity time.Duration
	position    int
	values      map[string][]float64

	last time.Time
}

// NewRateCollector is shorthand for newRateCollector(window, granularity, time.Now()).
func NewRateCollector(window time.Duration, granularity time.Duration) (*RateCollector, error) {
	return newRateCollector(window, granularity, time.Now())
}

// newRateCollector returns a new RateCollector with given window and granularity.
func newRateCollector(window time.Duration, granularity time.Duration, now time.Time) (*RateCollector, error) {
	if window == 0 || granularity == 0 {
		return nil, fmt.Errorf("create RateCollector failed, window or granularity cannot be 0, window = %d, granularity = %d", window, granularity)
	}
	if window < granularity || window%granularity != 0 {
		return nil, fmt.Errorf("create RateCollector failed, window has to be a multiplier of the granularity, window = %d, granularity = %d", window, granularity)
	}
	rc := &RateCollector{
		window:      window,
		granularity: granularity,
		position:    0,
		values:      make(map[string][]float64),
		last:        now,
	}
	return rc, nil
}

// Register init values of RateCollector for specified label.
func (r *RateCollector) Register(label string) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.values[label]; !ok {
		r.values[label] = make([]float64, int(r.window/r.granularity))
	}
}

// Deregister remove values of RateCollector for specified label.
func (r *RateCollector) Deregister(label string) {
	r.Lock()
	defer r.Unlock()
	delete(r.values, label)
}

// Add is shorthand for add(label, value, time.Now()).
func (r *RateCollector) Add(label string, value float64) {
	r.add(label, value, time.Now())
}

// add increases the current value of specified label.
func (r *RateCollector) add(label string, value float64, now time.Time) {
	r.Lock()
	defer r.Unlock()
	r.update(now)
	if _, ok := r.values[label]; ok {
		r.values[label][r.position] += value
	}
}

// Max is shorthand for max(label, time.Now()).
func (r *RateCollector) Max(label string, now time.Time) (float64, error) {
	return r.max(label, time.Now())
}

// max returns the maximal value in the window of specified label.
func (r *RateCollector) max(label string, now time.Time) (float64, error) {
	r.Lock()
	defer r.Unlock()
	r.update(now)
	if _, ok := r.values[label]; ok {
		max := float64(0)
		for _, v := range r.values[label] {
			if v > max {
				max = v
			}
		}
		return max, nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for label %s", label)
}

// Min is shorthand for min(label, time.Now()).
func (r *RateCollector) Min(label string, now time.Time) (float64, error) {
	return r.min(label, time.Now())
}

// min returns the minimal value in the window of specified label.
func (r *RateCollector) min(label string, now time.Time) (float64, error) {
	r.Lock()
	defer r.Unlock()
	r.update(now)
	if _, ok := r.values[label]; ok {
		min := math.MaxFloat64
		for _, v := range r.values[label] {
			if v < min {
				min = v
			}
		}
		return min, nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for label %s", label)
}

// Rate is shorthand for rate(label, duration, time.Now()).
func (r *RateCollector) Rate(label string, duration time.Duration) (float64, error) {
	return r.rate(label, duration, time.Now())
}

// rate returns the latest mean value of the specified duration.
func (r *RateCollector) rate(label string, duration time.Duration, now time.Time) (float64, error) {
	if duration > r.window {
		duration = r.window
	}
	size := int(duration / r.granularity)
	if size <= 0 {
		return 0, nil
	}

	r.Lock()
	defer r.Unlock()
	r.update(now)
	if _, ok := r.values[label]; ok {
		total := float64(0)
		tmp := r.position
		for i := 0; i < size; i++ {
			total += r.values[label][tmp]
			tmp--
			if tmp < 0 {
				tmp = int(r.window/r.granularity) - 1
			}
		}
		return total / float64(size), nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for label %s", label)
}

// update would update position and clear old values.
func (r *RateCollector) update(now time.Time) {
	duration := now.Sub(r.last)
	if duration/r.granularity == 0 {
		return
	}

	shift := func() {
		if r.position = r.position + 1; r.position >= int(r.window/r.granularity) {
			r.position = 0
		}
		for rt := range r.values {
			r.values[rt][r.position] = 0
		}
		r.last = r.last.Add(r.granularity)
	}

	for i := 0; i < int(duration/r.granularity); i++ {
		shift()
	}
}

// print is a helper function to print values.
func (r *RateCollector) print() {
	for k, v := range r.values {
		fmt.Printf("========== %s ==========\n", k)
		fmt.Println(v)
		fmt.Println("position=", r.position)
	}
}
