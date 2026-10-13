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

package index

import (
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWatermarkWaitsForEarlierWriter(t *testing.T) {
	w := newWatermark(0)
	a := w.enter() // will get tt=100, applies late
	b := w.enter() // will get tt=101, applies first
	w.end(b)
	w.tick(101) // TimeTick 101: every tt <= 101 is appended
	assert.EqualValues(t, 0, w.lastApplied(), "writer a is still in flight")
	w.end(a)
	assert.EqualValues(t, 101, w.lastApplied())
}

func TestWatermarkLaterEpochDoesNotBlockEarlier(t *testing.T) {
	w := newWatermark(5)
	w.tick(10)
	assert.EqualValues(t, 10, w.lastApplied())
	c := w.enter() // enters after tick 10, so its tt > 10
	w.tick(20)
	assert.EqualValues(t, 10, w.lastApplied())
	w.end(c)
	assert.EqualValues(t, 20, w.lastApplied())
}

func TestWatermarkIsMonotonic(t *testing.T) {
	w := newWatermark(50)
	w.tick(40) // an old tick after a restart seed
	assert.EqualValues(t, 50, w.lastApplied())
}

func TestWatermarkReleasesEpochs(t *testing.T) {
	w := newWatermark(0)
	for i := uint64(1); i <= 100; i++ {
		tok := w.enter()
		w.tick(i * 10)
		w.end(tok)
	}
	assert.EqualValues(t, 1000, w.lastApplied())
	assert.Empty(t, w.inflight)
	assert.Empty(t, w.closeAt)
}

func TestWatermarkEndTwicePanics(t *testing.T) {
	w := newWatermark(0)
	tok := w.enter()
	w.end(tok)
	assert.Panics(t, func() { w.end(tok) })
}

func TestWatermarkConcurrent(t *testing.T) {
	const writers = 8
	const rounds = 200
	w := newWatermark(0)

	var writersWG sync.WaitGroup
	for i := 0; i < writers; i++ {
		writersWG.Add(1)
		go func(seed int64) {
			defer writersWG.Done()
			r := rand.New(rand.NewSource(seed))
			for j := 0; j < rounds; j++ {
				tok := w.enter()
				if r.Intn(4) == 0 {
					time.Sleep(time.Duration(r.Intn(50)) * time.Microsecond)
				} else {
					runtime.Gosched()
				}
				w.end(tok)
			}
		}(int64(i))
	}

	stop := make(chan struct{})
	var lastTick atomic.Uint64
	var bgWG sync.WaitGroup
	bgWG.Add(2)
	go func() {
		defer bgWG.Done()
		tt := uint64(0)
		for {
			select {
			case <-stop:
				return
			default:
			}
			tt++
			w.tick(tt)
			lastTick.Store(tt)
			runtime.Gosched()
		}
	}()
	go func() {
		defer bgWG.Done()
		prev := uint64(0)
		for {
			select {
			case <-stop:
				return
			default:
			}
			cur := w.lastApplied()
			if cur < prev {
				t.Errorf("lastApplied went back from %d to %d", prev, cur)
				return
			}
			assert.LessOrEqual(t, cur, lastTick.Load()+1)
			prev = cur
			runtime.Gosched()
		}
	}()

	writersWG.Wait()
	close(stop)
	bgWG.Wait()

	final := lastTick.Load() + 1
	w.tick(final)
	assert.EqualValues(t, final, w.lastApplied())
	assert.Empty(t, w.inflight)
	assert.Empty(t, w.closeAt)
}
