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

package task

// Segment load timeout backoff.
//
// A load that outlives the task timeout is cancelled and started over from
// nothing, so a segment whose loads are slower than the timeout (heavy-tailed
// load latency, an overloaded node) is retried forever and never loads. Each
// load that ends on its deadline doubles the next attempt's timeout for that
// segment, up to 8x; a successful load resets it. Records expire after
// segmentBackoffTTL.

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	segmentBackoffMax = 3 // doublings: 8x
	segmentBackoffTTL = 30 * time.Minute
)

type segmentBackoff struct {
	doublings int
	at        time.Time
}

var segmentTimeouts = struct {
	sync.Mutex
	m map[int64]segmentBackoff
}{m: make(map[int64]segmentBackoff)}

// noteSegmentTaskDone records how a segment task ended.
func noteSegmentTaskDone(task *SegmentTask) {
	segID := task.SegmentID()
	segmentTimeouts.Lock()
	defer segmentTimeouts.Unlock()
	switch {
	case task.Status() == TaskStatusSucceeded:
		delete(segmentTimeouts.m, segID)
	case task.Status() == TaskStatusFailed && errors.Is(task.Err(), context.DeadlineExceeded):
		now := time.Now()
		b := segmentTimeouts.m[segID]
		if now.Sub(b.at) > segmentBackoffTTL {
			b.doublings = 0
		}
		b.doublings = min(b.doublings+1, segmentBackoffMax)
		b.at = now
		segmentTimeouts.m[segID] = b
		if len(segmentTimeouts.m) > 100_000 {
			for id, e := range segmentTimeouts.m {
				if now.Sub(e.at) > segmentBackoffTTL {
					delete(segmentTimeouts.m, id)
				}
			}
		}
	}
}

// SegmentTimeoutFactor is the multiplier for the next load of segmentID.
func SegmentTimeoutFactor(segmentID int64) time.Duration {
	segmentTimeouts.Lock()
	defer segmentTimeouts.Unlock()
	b, ok := segmentTimeouts.m[segmentID]
	if !ok || time.Since(b.at) > segmentBackoffTTL {
		return 1
	}
	return 1 << b.doublings
}
