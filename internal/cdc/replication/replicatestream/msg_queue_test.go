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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
)

func TestMsgQueue_BasicOperations(t *testing.T) {
	// Test basic queue operations
	queue := NewMsgQueue(3)
	assert.Equal(t, 3, queue.Cap())
	assert.Equal(t, 0, queue.Len())

	// Test enqueue and dequeue
	ctx := context.Background()
	msg1 := mock_message.NewMockImmutableMessage(t)
	msg1.EXPECT().TimeTick().Return(uint64(100)).Maybe()

	err := queue.Enqueue(ctx, msg1)
	assert.NoError(t, err)
	assert.Equal(t, 1, queue.Len())

	// Test dequeue
	dequeuedMsg, err := queue.ReadNext(ctx)
	assert.NoError(t, err)
	assert.Equal(t, msg1, dequeuedMsg)
	assert.Equal(t, 1, queue.Len()) // Length doesn't change after dequeue
}

func TestMsgQueue_EnqueueBlocking(t *testing.T) {
	// Test enqueue blocking when queue is full
	queue := NewMsgQueue(2)
	ctx := context.Background()

	// Fill the queue
	msg1 := mock_message.NewMockImmutableMessage(t)
	msg1.EXPECT().TimeTick().Return(uint64(100)).Maybe()
	msg2 := mock_message.NewMockImmutableMessage(t)
	msg2.EXPECT().TimeTick().Return(uint64(200)).Maybe()

	err := queue.Enqueue(ctx, msg1)
	assert.NoError(t, err)
	err = queue.Enqueue(ctx, msg2)
	assert.NoError(t, err)
	assert.Equal(t, 2, queue.Len())

	// Try to enqueue when full - should block
	msg3 := mock_message.NewMockImmutableMessage(t)
	msg3.EXPECT().TimeTick().Return(uint64(300)).Maybe()

	// Use a context with timeout to test blocking
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	err = queue.Enqueue(ctxWithTimeout, msg3)
	assert.Error(t, err)
	// Context timeout will cause context.Canceled error, not DeadlineExceeded
	assert.Equal(t, context.Canceled, err)
}

func TestMsgQueue_DequeueBlocking(t *testing.T) {
	// Test dequeue blocking when queue is empty
	queue := NewMsgQueue(2)
	ctx := context.Background()

	// Try to dequeue from empty queue - should block
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	_, err := queue.ReadNext(ctxWithTimeout)
	assert.Error(t, err)
	// Context timeout will cause context.Canceled error, not DeadlineExceeded
	assert.Equal(t, context.Canceled, err)
}

func TestMsgQueue_SeekToHead(t *testing.T) {
	// Test seek to head functionality
	queue := NewMsgQueue(3)
	ctx := context.Background()

	// Add messages
	msg1 := mock_message.NewMockImmutableMessage(t)
	msg1.EXPECT().TimeTick().Return(uint64(100)).Maybe()
	msg2 := mock_message.NewMockImmutableMessage(t)
	msg2.EXPECT().TimeTick().Return(uint64(200)).Maybe()

	err := queue.Enqueue(ctx, msg1)
	assert.NoError(t, err)
	err = queue.Enqueue(ctx, msg2)
	assert.NoError(t, err)

	// Dequeue first message
	dequeuedMsg, err := queue.ReadNext(ctx)
	assert.NoError(t, err)
	assert.Equal(t, msg1, dequeuedMsg)

	// Seek to head
	queue.SeekToHead()

	// Should be able to dequeue first message again
	dequeuedMsg, err = queue.ReadNext(ctx)
	assert.NoError(t, err)
	assert.Equal(t, msg1, dequeuedMsg)
}

func TestMsgQueue_CleanupConfirmedMessages(t *testing.T) {
	// Test cleanup functionality
	queue := NewMsgQueue(5)
	ctx := context.Background()

	// Add messages with different timeticks
	msg1 := mock_message.NewMockImmutableMessage(t)
	msg1.EXPECT().TimeTick().Return(uint64(100)).Maybe()
	msg2 := mock_message.NewMockImmutableMessage(t)
	msg2.EXPECT().TimeTick().Return(uint64(200)).Maybe()
	msg3 := mock_message.NewMockImmutableMessage(t)
	msg3.EXPECT().TimeTick().Return(uint64(300)).Maybe()

	err := queue.Enqueue(ctx, msg1)
	assert.NoError(t, err)
	err = queue.Enqueue(ctx, msg2)
	assert.NoError(t, err)
	err = queue.Enqueue(ctx, msg3)
	assert.NoError(t, err)

	assert.Equal(t, 3, queue.Len())

	// Cleanup messages with timetick <= 200
	cleanedMessages := queue.CleanupConfirmedMessages(200)
	assert.Equal(t, 1, queue.Len())
	assert.Equal(t, 2, len(cleanedMessages))
	assert.Equal(t, msg1, cleanedMessages[0])
	assert.Equal(t, msg2, cleanedMessages[1])

	// First two messages should be removed
	dequeuedMsg, err := queue.ReadNext(ctx)
	assert.NoError(t, err)
	assert.Equal(t, msg3, dequeuedMsg) // Only msg3 remains
}

func TestMsgQueue_CleanupWithReadCursor(t *testing.T) {
	// Test cleanup when read cursor is advanced
	queue := NewMsgQueue(5)
	ctx := context.Background()

	// Add messages
	msg1 := mock_message.NewMockImmutableMessage(t)
	msg1.EXPECT().TimeTick().Return(uint64(100)).Maybe()
	msg2 := mock_message.NewMockImmutableMessage(t)
	msg2.EXPECT().TimeTick().Return(uint64(200)).Maybe()
	msg3 := mock_message.NewMockImmutableMessage(t)
	msg3.EXPECT().TimeTick().Return(uint64(300)).Maybe()

	err := queue.Enqueue(ctx, msg1)
	assert.NoError(t, err)
	err = queue.Enqueue(ctx, msg2)
	assert.NoError(t, err)
	err = queue.Enqueue(ctx, msg3)
	assert.NoError(t, err)

	// Dequeue first message (advance read cursor)
	dequeuedMsg, err := queue.ReadNext(ctx)
	assert.NoError(t, err)
	assert.Equal(t, msg1, dequeuedMsg)
	assert.Equal(t, 1, queue.readIdx)

	// Cleanup messages with timetick <= 150
	cleanedMessages := queue.CleanupConfirmedMessages(150)
	assert.Equal(t, 2, queue.Len())   // msg1 removed, msg2 and msg3 remain
	assert.Equal(t, 0, queue.readIdx) // read cursor adjusted
	assert.Equal(t, 1, len(cleanedMessages))
	assert.Equal(t, msg1, cleanedMessages[0])
}

func TestMsgQueue_ContextCancellation(t *testing.T) {
	// Test context cancellation
	queue := NewMsgQueue(1)
	ctx, cancel := context.WithCancel(context.Background())

	// Fill the queue
	msg1 := mock_message.NewMockImmutableMessage(t)
	msg1.EXPECT().TimeTick().Return(uint64(100)).Maybe()
	err := queue.Enqueue(ctx, msg1)
	assert.NoError(t, err)

	// Try to enqueue when full
	msg2 := mock_message.NewMockImmutableMessage(t)
	msg2.EXPECT().TimeTick().Return(uint64(200)).Maybe()

	// Cancel context before enqueue
	cancel()
	err = queue.Enqueue(ctx, msg2)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestMsgQueue_NewMsgQueueValidation(t *testing.T) {
	// Test constructor validation
	assert.Panics(t, func() {
		NewMsgQueue(0)
	})

	assert.Panics(t, func() {
		NewMsgQueue(-1)
	})

	// Valid capacity
	queue := NewMsgQueue(1)
	assert.NotNil(t, queue)
	assert.Equal(t, 1, queue.Cap())
}

func TestMsgQueue_ConcurrentOperations(t *testing.T) {
	// Test concurrent enqueue and dequeue operations
	queue := NewMsgQueue(10)
	ctx := context.Background()
	numMessages := 100

	wg := sync.WaitGroup{}
	// Start producer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numMessages; i++ {
			msg := mock_message.NewMockImmutableMessage(t)
			msg.EXPECT().TimeTick().Return(uint64(i)).Maybe()
			err := queue.Enqueue(ctx, msg)
			assert.NoError(t, err)
		}
	}()

	// Start consumer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numMessages; i++ {
			dequeuedMsg, err := queue.ReadNext(ctx)
			assert.NoError(t, err)
			cleanedMessages := queue.CleanupConfirmedMessages(dequeuedMsg.TimeTick())
			assert.Equal(t, 1, len(cleanedMessages))
			assert.Equal(t, dequeuedMsg, cleanedMessages[0])
		}
	}()

	wg.Wait()
	assert.Equal(t, 0, queue.Len())
}

func TestMsgQueue_EdgeCases(t *testing.T) {
	// Test edge cases
	queue := NewMsgQueue(1)

	// Test with nil message (if allowed by interface)
	// This depends on the actual message.ImmutableMessage interface implementation
	// For now, we'll test with valid messages

	// Test cleanup on empty queue
	cleanedMessages := queue.CleanupConfirmedMessages(100)
	assert.Equal(t, 0, queue.Len())
	assert.Nil(t, cleanedMessages)

	// Test seek to head on empty queue
	queue.SeekToHead()
	assert.Equal(t, 0, queue.readIdx)
}
