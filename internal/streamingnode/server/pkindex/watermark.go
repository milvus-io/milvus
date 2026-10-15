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

package pkindex

import (
	"fmt"
	"sync"
)

// watermark computes lastAppliedTimeTick of one pchannel.
//
// The value is a contiguous prefix: every pkindex change of a WAL message
// with a time tick (tt) <= the value is written to the engine. It is not the
// tt of the newest applied message, because writers apply out of order.
//
// The pkindex interceptor runs above the timetick interceptor, so a writer
// calls enter before it gets a tt. The writer calls end after its engine
// write, or after it discards the change. A TimeTick message with value T is
// appended only after every message with tt <= T is appended, so every such
// writer entered before tick(T) is called.
//
// TimeTick messages split writers into epochs. tick(T) closes the current
// epoch with T. When an epoch and all earlier epochs have no writers in
// flight, the watermark moves to the tick of that epoch. A writer that
// enters before tick(T) but gets a tt > T only delays the watermark.
//
// The watermark never blocks a writer. It only delays the reported value.
type watermark struct {
	mu       sync.Mutex
	applied  uint64
	head     uint64            // oldest epoch that is not released
	cur      uint64            // epoch that new writers join
	inflight map[uint64]int64  // epoch -> writers in flight, only counts > 0
	closeAt  map[uint64]uint64 // closed epoch that is not released -> its tick
}

// writerToken identifies the epoch that a writer joined.
type writerToken uint64

// newWatermark returns a watermark that starts at seed.
func newWatermark(seed uint64) *watermark {
	return &watermark{
		applied:  seed,
		inflight: make(map[uint64]int64),
		closeAt:  make(map[uint64]uint64),
	}
}

// enter registers a writer before it gets a tt.
// The caller must call end exactly once with the returned token.
func (w *watermark) enter() writerToken {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.inflight[w.cur]++
	return writerToken(w.cur)
}

// end releases a writer after its engine write or its discard.
// It panics if the token has no writer in flight.
func (w *watermark) end(t writerToken) {
	w.mu.Lock()
	defer w.mu.Unlock()
	e := uint64(t)
	n, ok := w.inflight[e]
	if !ok || n <= 0 {
		panic(fmt.Sprintf("pkindex watermark: end called for epoch %d with no writer in flight, end must be called exactly once per enter", e))
	}
	if n == 1 {
		delete(w.inflight, e)
	} else {
		w.inflight[e] = n - 1
	}
	if e == w.head {
		w.advanceLocked()
	}
}

// tick closes the current epoch with tt.
// The caller calls it after the TimeTick message with tt returns from the append.
func (w *watermark) tick(tt uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closeAt[w.cur] = tt
	w.cur++
	w.advanceLocked()
}

// lastApplied returns the current watermark.
func (w *watermark) lastApplied() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.applied
}

// advanceLocked releases closed epochs from head while they have no writer in flight.
// The watermark only moves forward.
func (w *watermark) advanceLocked() {
	for w.head < w.cur && w.inflight[w.head] == 0 {
		if tt := w.closeAt[w.head]; tt > w.applied {
			w.applied = tt
		}
		delete(w.closeAt, w.head)
		w.head++
	}
}
