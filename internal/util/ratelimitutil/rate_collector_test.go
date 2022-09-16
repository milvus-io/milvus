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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateCollector(t *testing.T) {
	t.Run("test add and get", func(t *testing.T) {
		var (
			ts0   = time.Now()
			ts11  = ts0.Add(time.Duration(1.0 * float64(time.Second)))
			ts12  = ts0.Add(time.Duration(1.9 * float64(time.Second)))
			ts31  = ts0.Add(time.Duration(3.1 * float64(time.Second)))
			ts41  = ts0.Add(time.Duration(4.0 * float64(time.Second)))
			ts100 = ts0.Add(time.Duration(100.0 * float64(time.Second)))
		)

		rc, err := newRateCollector(DefaultWindow, DefaultGranularity, ts0)
		assert.NoError(t, err)
		label := "mock_label"
		rc.Register(label)
		defer rc.Deregister(label)

		rc.add(label, 10, ts11)
		rc.add(label, 20, ts12)

		v, err := rc.rate(label, 2*time.Second, ts12)
		assert.NoError(t, err)
		assert.Equal(t, float64(15), v)

		rc.add(label, 20, ts31)
		v, err = rc.rate(label, 2*time.Second, ts31)
		assert.NoError(t, err)
		assert.Equal(t, float64(10), v)
		v, err = rc.rate(label, 4*time.Second, ts31)
		assert.NoError(t, err)
		assert.Equal(t, 50.0/4.0, v)

		rc.add(label, 20, ts41)
		v, err = rc.rate(label, 1*time.Second, ts31)
		assert.NoError(t, err)
		assert.Equal(t, float64(20), v)
		v, err = rc.rate(label, 2*time.Second, ts31)
		assert.NoError(t, err)
		assert.Equal(t, float64(20), v)

		v, err = rc.rate(label, 2*time.Second, ts100)
		assert.NoError(t, err)
		assert.Equal(t, float64(0), v)
	})

	t.Run("test min max", func(t *testing.T) {
		var (
			ts0  = time.Now()
			ts11 = ts0.Add(time.Duration(1.0 * float64(time.Second)))
			ts12 = ts0.Add(time.Duration(1.9 * float64(time.Second)))
			ts31 = ts0.Add(time.Duration(3.1 * float64(time.Second)))
		)

		rc, err := newRateCollector(DefaultWindow, DefaultGranularity, ts0)
		assert.NoError(t, err)
		label := "mock_label"
		rc.Register(label)
		defer rc.Deregister(label)

		rc.add(label, 10, ts11)
		rc.add(label, 20, ts12)

		v, err := rc.min(label, ts31)
		assert.NoError(t, err)
		assert.Equal(t, float64(0), v)

		v, err = rc.max(label, ts31)
		assert.NoError(t, err)
		assert.Equal(t, float64(30), v)

		rc.print()
	})

	t.Run("long running", func(t *testing.T) {
		const testPeriod = 10

		tt := makeTestTime(t)
		start := tt.now()
		end := start.Add(testPeriod * time.Second)

		rc, err := newRateCollector(DefaultWindow, DefaultGranularity, start)
		assert.NoError(t, err)
		label := "mock_label"
		rc.Register(label)
		defer rc.Deregister(label)

		slots := make(map[int]float64)
		for tt.now().Before(end) {
			increase := rand.Float64()
			rc.add(label, increase, tt.now())

			slotIndex := int(tt.now().Sub(start) / time.Second)
			slots[slotIndex] += increase

			tt.advance(2 * time.Millisecond)
		}

		getSlotRate := func(duration int) float64 {
			total := 0.0
			for i := testPeriod - 1; i >= testPeriod-duration; i-- {
				total += slots[i]
			}
			return total / float64(duration)
		}

		for i := 1; i <= testPeriod-1; i++ {
			want := getSlotRate(i)
			actual, err := rc.rate(label, time.Duration(i)*time.Second, tt.now().Add(-time.Second/5))
			assert.NoError(t, err)
			assert.True(t, math.Abs(want-actual) < 0.000001)
		}
	})
}
