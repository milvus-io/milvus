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

package producer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/ratelimit"
)

func TestProduceRateLimiter(t *testing.T) {
	rl := newProduceRateLimiter("test-channel")

	t.Run("InitialState", func(t *testing.T) {
		assert.Equal(t, rate.Limit(rate.Inf), rl.limiter.Limit())
		assert.Equal(t, "test-channel", rl.channel)
		assert.Equal(t, streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL, rl.state.State)
	})

	t.Run("UpdateRateLimitState_Slowdown", func(t *testing.T) {
		state := ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
			Rate:  1024 * 1024,
		}
		rl.UpdateRateLimitState(state)
		assert.Equal(t, rate.Limit(1024*1024), rl.limiter.Limit())
		assert.Equal(t, getDefaultBurst(), rl.limiter.Burst())
		assert.Equal(t, state, rl.state)
	})

	t.Run("UpdateRateLimitState_Reject", func(t *testing.T) {
		state := ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT,
			Rate:  0,
		}
		rl.UpdateRateLimitState(state)
		assert.Equal(t, rate.Limit(0), rl.limiter.Limit())
		assert.Equal(t, 0, rl.limiter.Burst())
		assert.Equal(t, state, rl.state)
	})

	t.Run("UpdateRateLimitState_Normal", func(t *testing.T) {
		state := ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL,
			Rate:  0,
		}
		rl.UpdateRateLimitState(state)
		assert.Equal(t, rate.Limit(rate.Inf), rl.limiter.Limit())
		assert.Equal(t, getDefaultBurst(), rl.limiter.Burst())
		assert.Equal(t, state, rl.state)
	})

	t.Run("RequestReservation", func(t *testing.T) {
		rl.UpdateRateLimitState(ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
			Rate:  100,
		})

		msg := mock_message.NewMockMutableMessage(t)
		msg.EXPECT().EstimateSize().Return(50)

		res, err := rl.RequestReservation(context.Background(), msg)
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.True(t, res.OK())
	})

	t.Run("WaitUntilAvailable", func(t *testing.T) {
		rl.UpdateRateLimitState(ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT,
			Rate:  0,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- rl.WaitUntilAvailable(ctx)
		}()

		// Wait a bit, then change state to Normal
		time.Sleep(50 * time.Millisecond)
		rl.UpdateRateLimitState(ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL,
			Rate:  0,
		})

		err := <-errCh
		assert.NoError(t, err)
	})

	t.Run("WaitUntilAvailable_ContextCanceled", func(t *testing.T) {
		rl.UpdateRateLimitState(ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT,
			Rate:  0,
		})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := rl.WaitUntilAvailable(ctx)
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("UpdateRateLimitState_SameState", func(t *testing.T) {
		state := ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
			Rate:  1024 * 1024,
		}
		rl.UpdateRateLimitState(state)

		// This should be a no-op, but we can't easily verify it other than it not panicking
		rl.UpdateRateLimitState(state)
		assert.Equal(t, state, rl.state)
	})

	t.Run("RequestReservation_MultipleMessages", func(t *testing.T) {
		rl.UpdateRateLimitState(ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
			Rate:  1000,
		})

		msg1 := mock_message.NewMockMutableMessage(t)
		msg1.EXPECT().EstimateSize().Return(50)
		msg2 := mock_message.NewMockMutableMessage(t)
		msg2.EXPECT().EstimateSize().Return(50)

		res, err := rl.RequestReservation(context.Background(), msg1, msg2)
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.True(t, res.OK())
	})

	t.Run("RequestReservation_RateLimit_ErrSlowDown", func(t *testing.T) {
		rl.limiter.SetLimit(0)
		rl.limiter.SetBurst(0)

		msg := mock_message.NewMockMutableMessage(t)
		msg.EXPECT().EstimateSize().Return(100)

		res, err := rl.RequestReservation(context.Background(), msg)
		assert.Error(t, err)
		assert.Nil(t, res)
		assert.Equal(t, ErrSlowDown, err)
	})
}

func TestProduceRateLimiter_ConcurrentUpdates(t *testing.T) {
	rl := newProduceRateLimiter("test-channel-concurrent")

	// Test concurrent state updates don't cause race conditions
	t.Run("ConcurrentStateUpdates", func(t *testing.T) {
		done := make(chan struct{})
		states := []ratelimit.RateLimitState{
			{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL, Rate: 0},
			{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 1024},
			{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT, Rate: 0},
		}

		// Start multiple goroutines updating state concurrently
		for i := 0; i < 10; i++ {
			go func(idx int) {
				for j := 0; j < 100; j++ {
					state := states[(idx+j)%len(states)]
					rl.UpdateRateLimitState(state)
				}
			}(i)
		}

		// Also start a goroutine that waits for available
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_ = rl.WaitUntilAvailable(ctx)
		}()

		// Wait a bit for all goroutines to complete
		time.Sleep(200 * time.Millisecond)
		close(done)
	})
}

func TestProduceRateLimiter_RapidStateChanges(t *testing.T) {
	rl := newProduceRateLimiter("test-channel-rapid")

	// Rapidly change states and verify the final state is correct
	for i := 0; i < 100; i++ {
		state := ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
			Rate:  int64(i * 100),
		}
		rl.UpdateRateLimitState(state)
	}

	// Final state should have Rate = 9900
	assert.Equal(t, int64(9900), rl.state.Rate)
	assert.Equal(t, streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, rl.state.State)
}

func TestProduceRateLimiter_WaitUntilAvailable_MultipleBroadcasts(t *testing.T) {
	rl := newProduceRateLimiter("test-channel-broadcast")

	// Set initial state to REJECT
	rl.UpdateRateLimitState(ratelimit.RateLimitState{
		State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT,
		Rate:  0,
	})

	// Start multiple waiters
	const numWaiters = 5
	errChs := make([]chan error, numWaiters)
	for i := 0; i < numWaiters; i++ {
		errChs[i] = make(chan error, 1)
		go func(ch chan error) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			ch <- rl.WaitUntilAvailable(ctx)
		}(errChs[i])
	}

	// Wait a bit, then change state to Normal - this should wake up all waiters
	time.Sleep(50 * time.Millisecond)
	rl.UpdateRateLimitState(ratelimit.RateLimitState{
		State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL,
		Rate:  0,
	})

	// All waiters should complete without error
	for i := 0; i < numWaiters; i++ {
		err := <-errChs[i]
		assert.NoError(t, err, "waiter %d should complete without error", i)
	}
}

func TestProduceRateLimiter_WaitUntilAvailable_StateTransitions(t *testing.T) {
	rl := newProduceRateLimiter("test-channel-transitions")

	t.Run("RejectToSlowdown", func(t *testing.T) {
		rl.UpdateRateLimitState(ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT,
			Rate:  0,
		})

		errCh := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			errCh <- rl.WaitUntilAvailable(ctx)
		}()

		time.Sleep(50 * time.Millisecond)
		// Transition to SLOWDOWN should also wake up waiters
		rl.UpdateRateLimitState(ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
			Rate:  1024,
		})

		err := <-errCh
		assert.NoError(t, err)
	})

	t.Run("AlreadyNormal", func(t *testing.T) {
		rl.UpdateRateLimitState(ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL,
			Rate:  0,
		})

		// Should return immediately
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err := rl.WaitUntilAvailable(ctx)
		assert.NoError(t, err)
	})

	t.Run("AlreadySlowdown", func(t *testing.T) {
		rl.UpdateRateLimitState(ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
			Rate:  1024,
		})

		// Should return immediately (not REJECT state)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err := rl.WaitUntilAvailable(ctx)
		assert.NoError(t, err)
	})
}
