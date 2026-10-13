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

package snapshot

import "time"

// maxBackoffShift limits how many times the wait doubles.
const maxBackoffShift = 16

// Backoff is the retry state of the handovers of one vchannel. After each
// failure the wait doubles from Base, up to Max.
//
// Backoff is not safe for concurrent use. Only the one handover of the
// vchannel that runs at a time uses it.
type Backoff struct {
	Base, Max time.Duration
	failures  int
	next      time.Time
}

// Ready reports whether a handover can start at now.
func (b *Backoff) Ready(now time.Time) bool { return !now.Before(b.next) }

// Fail records a failed handover at now. The next one is ready after
// Base << min(failures, 16), and never later than Max.
func (b *Backoff) Fail(now time.Time) {
	shift := min(b.failures, maxBackoffShift)
	wait := b.Max
	// compare before the shift, so that an overflow also gives Max
	if b.Base <= b.Max>>shift {
		wait = b.Base << shift
	}
	b.failures++
	b.next = now.Add(wait)
}

// Succeed resets the state after a successful handover.
func (b *Backoff) Succeed() {
	b.failures = 0
	b.next = time.Time{}
}

// Failures returns the number of failures since the last success.
func (b *Backoff) Failures() int { return b.failures }
