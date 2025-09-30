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

package replicatestream

import (
	"context"
	"sync"

	message "github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// MsgQueue exposes the required operations. We include context support to meet the
// blocking + cancel requirements.
// The names mirror the user's suggestion with small adjustments for clarity.
type MsgQueue interface {
	// Enqueue appends a message. Blocks if capacity is full until space appears
	// (via CleanupConfirmedMessages) or ctx is canceled.
	Enqueue(ctx context.Context, msg message.ImmutableMessage) error

	// ReadNext returns the next message from the current read cursor and advances
	// the cursor by one. It does NOT delete the message from the queue storage.
	// Blocks when there are no readable messages (i.e., cursor is at tail) until
	// a new message is Enqueued or ctx is canceled.
	ReadNext(ctx context.Context) (message.ImmutableMessage, error)

	// SeekToHead moves the read cursor to the first not-yet-deleted message.
	SeekToHead()

	// CleanupConfirmedMessages permanently removes all messages whose Timetick()
	// <= lastConfirmedTimeTick. This may free capacity and wake Enqueue waiters.
	// Returns the messages that were cleaned up.
	CleanupConfirmedMessages(lastConfirmedTimeTick uint64) []message.ImmutableMessage

	// Len returns the number of stored (not yet deleted) messages.
	Len() int

	// Cap returns the maximum capacity of the queue.
	Cap() int
}

var _ MsgQueue = (*msgQueue)(nil)

// msgQueue is a bounded, in-memory implementation of MsgQueue.
// It uses a simple slice for storage and an integer read cursor that is always
// within [0, len(buf)].
type msgQueue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond

	buf     []message.ImmutableMessage
	readIdx int
	cap     int
	closed  bool
}

// NewMsgQueue creates a queue with a fixed capacity (>0).
func NewMsgQueue(capacity int) *msgQueue {
	if capacity <= 0 {
		panic("capacity must be > 0")
	}
	q := &msgQueue{cap: capacity}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)
	return q
}

func (q *msgQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.buf)
}

func (q *msgQueue) Cap() int { return q.cap }

// Enqueue implements a blocking producer. It respects ctx cancellation.
func (q *msgQueue) Enqueue(ctx context.Context, msg message.ImmutableMessage) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.buf) >= q.cap {
		if ctx.Err() != nil {
			return context.Canceled
		}
		q.waitWithContext(ctx, q.notFull)
		if ctx.Err() != nil {
			return context.Canceled
		}
	}

	// Optional runtime check: enforce non-decreasing timetick
	// if n := len(q.buf); n > 0 {
	//     if q.buf[n-1].Timetick() > msg.Timetick() {
	//         return fmt.Errorf("enqueue timetick order violation: last=%d new=%d", q.buf[n-1].Timetick(), msg.Timetick())
	//     }
	// }

	q.buf = append(q.buf, msg)

	// New data is available for readers.
	q.notEmpty.Signal()
	return nil
}

// ReadNext returns the next message at the read cursor. Does not delete it.
func (q *msgQueue) ReadNext(ctx context.Context) (message.ImmutableMessage, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.readIdx >= len(q.buf) { // no readable messages
		if ctx.Err() != nil {
			return nil, context.Canceled
		}
		q.waitWithContext(ctx, q.notEmpty)
		if ctx.Err() != nil {
			return nil, context.Canceled
		}
	}

	m := q.buf[q.readIdx]
	q.readIdx++ // advance read cursor only; storage remains intact

	// Readers advancing may not directly free capacity, but producers may still
	// be waiting due to full buffer; signal in case cleanup made space earlier.
	q.notFull.Signal()
	return m, nil
}

// SeekToHead resets the read cursor to the first existing element.
func (q *msgQueue) SeekToHead() {
	q.mu.Lock()
	q.readIdx = 0
	// Let potential consumers know there's readable data (if any exists).
	if len(q.buf) > 0 {
		q.notEmpty.Broadcast()
	}
	q.mu.Unlock()
}

// CleanupConfirmedMessages permanently drops messages with Timetick <= watermark.
// This frees capacity and may move the read cursor backward proportionally to the
// number of deleted messages (but clamped at 0).
// Returns the messages that were cleaned up.
func (q *msgQueue) CleanupConfirmedMessages(lastConfirmedTimeTick uint64) []message.ImmutableMessage {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.buf) == 0 {
		return nil
	}

	// Find first index whose Timetick() > lastConfirmedTimeTick
	cut := 0
	for cut < len(q.buf) && q.buf[cut].TimeTick() <= lastConfirmedTimeTick {
		cut++
	}

	var cleanedMessages []message.ImmutableMessage
	if cut > 0 {
		// Collect the messages that will be cleaned up
		cleanedMessages = make([]message.ImmutableMessage, cut)
		copy(cleanedMessages, q.buf[:cut])

		// Drop the prefix [0:cut)
		q.buf = q.buf[cut:]
		// Adjust read cursor relative to the new slice
		q.readIdx -= cut
		if q.readIdx < 0 {
			q.readIdx = 0
		}
		// Free space became available; wake blocked producers.
		q.notFull.Broadcast()
	}

	return cleanedMessages
}

// waitWithContext waits on cond while allowing ctx cancellation to wake it up.
// Implementation detail:
//   - cond.Wait() requires holding q.mu; Wait atomically unlocks and re-locks.
//   - We spawn a short-lived goroutine that Broadcasts on ctx.Done(). This is
//     safe and bounded: after Wait returns, we close a local channel to let the
//     goroutine exit immediately (even if ctx wasn't canceled).
func (q *msgQueue) waitWithContext(ctx context.Context, cond *sync.Cond) {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			q.mu.Lock()
			cond.Broadcast()
			q.mu.Unlock()
		case <-done:
		}
	}()
	cond.Wait() // releases q.mu while waiting; re-locks before returning
	close(done)
}
