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

// Task timeout ladder.
//
// A load or watch that outlives its task timeout is canceled and started
// over from nothing, so one that is slower than the timeout (an oversized
// segment, a delegator with a long catch-up, an overloaded node) would be
// retried forever and never finish. The first attempt runs under the
// configured timeout (queryCoord.segmentTaskTimeout / channelTaskTimeout,
// 1 minute: with the pacer keeping loads from queueing on the node, an
// attempt that does not finish within it is unusually slow). An attempt that
// ended on its deadline is retried under the retry timeout
// (queryCoord.segmentTaskRetryTimeout / channelTaskRetryTimeout, 5 minutes)
// until it succeeds, which resets the key. Records expire after backoffTTL.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const backoffTTL = 30 * time.Minute

// timeoutLadder remembers which keys (segments, channel/node pairs) last
// failed on their deadline.
type timeoutLadder struct {
	mu sync.Mutex
	m  map[string]time.Time // key -> time of the last deadline failure
}

var (
	segmentTimeouts = timeoutLadder{m: make(map[string]time.Time)}
	channelTimeouts = timeoutLadder{m: make(map[string]time.Time)}
)

// deadlineExceeded reports whether a task ended on its deadline: the
// scheduler cancels it with the context error, or the executor fails it with
// the RPC's error, which carries the gRPC DeadlineExceeded code.
func deadlineExceeded(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded
}

// note records how a task for key ended: a success clears it, a deadline
// failure (failed or canceled on the deadline) marks it, any other outcome
// leaves it.
func (l *timeoutLadder) note(key string, status Status, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case status == TaskStatusSucceeded:
		delete(l.m, key)
	case (status == TaskStatusFailed || status == TaskStatusCanceled) && deadlineExceeded(err):
		now := time.Now()
		l.m[key] = now
		if len(l.m) > 100_000 {
			for k, at := range l.m {
				if now.Sub(at) > backoffTTL {
					delete(l.m, k)
				}
			}
		}
	}
}

// timeout is the timeout for the next attempt of key: base for a first
// attempt, retry (never below base) after a deadline failure.
func (l *timeoutLadder) timeout(key string, base, retry time.Duration) time.Duration {
	if retry <= 0 {
		return base
	}
	l.mu.Lock()
	at, ok := l.m[key]
	l.mu.Unlock()
	if !ok || time.Since(at) > backoffTTL {
		return base
	}
	return max(base, retry)
}

func noteSegmentTaskDone(task *SegmentTask) {
	segmentTimeouts.note(fmt.Sprint(task.SegmentID()), task.Status(), task.Err())
}

// noteChannelTaskDone records the outcome of a channel task for the node it
// watched on.
func noteChannelTaskDone(task *ChannelTask) {
	for _, action := range task.Actions() {
		if action.Type() == ActionTypeGrow {
			channelTimeouts.note(fmt.Sprintf("%s/%d", task.Channel(), action.Node()), task.Status(), task.Err())
		}
	}
}

// SegmentTaskTimeout is the timeout for the next load of segmentID. The short
// first-attempt timeout assumes the pacer: it keeps loads from queueing on
// the node, so one that misses it is unusually slow. Without the pacer a
// burst of loads shares the node and most of them miss a short timeout, are
// canceled and started over, so every attempt then gets the retry timeout
// (the 5-minute timeout of earlier releases).
func SegmentTaskTimeout(segmentID int64, base time.Duration) time.Duration {
	cfg := &paramtable.Get().QueryCoordCfg
	retry := cfg.SegmentTaskRetryTimeout.GetAsDuration(time.Millisecond)
	if !cfg.LoadPacingEnabled.GetAsBool() {
		return max(base, retry)
	}
	return segmentTimeouts.timeout(fmt.Sprint(segmentID), base, retry)
}

// ChannelTaskTimeout is the timeout for the next watch of channel on node.
func ChannelTaskTimeout(channel string, node int64, base time.Duration) time.Duration {
	retry := paramtable.Get().QueryCoordCfg.ChannelTaskRetryTimeout.GetAsDuration(time.Millisecond)
	return channelTimeouts.timeout(fmt.Sprintf("%s/%d", channel, node), base, retry)
}
