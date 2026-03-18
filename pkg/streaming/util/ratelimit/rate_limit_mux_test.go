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

package ratelimit

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

func TestMuxRateLimitObserverRegistry(t *testing.T) {
	mux := NewMuxRateLimitObserverRegistry()
	observer := new(MockRateLimitObserver)

	// Register observer
	observer.On("UpdateRateLimitState", NewNormalRateLimitState()).Once()
	mux.Register(observer)
	assert.False(t, mux.IsRejected())

	// Notify source 1: Slowdown
	s1 := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 2000}
	observer.On("UpdateRateLimitState", s1).Once()
	mux.NotifySourceRateLimitState("source1", s1)
	assert.False(t, mux.IsRejected())
	observer.AssertExpectations(t)

	// Notify source 2: More restrictive Slowdown
	s2 := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 1000}
	observer.On("UpdateRateLimitState", s2).Once()
	mux.NotifySourceRateLimitState("source2", s2)
	assert.False(t, mux.IsRejected())
	observer.AssertExpectations(t)

	// Notify source 1: Less restrictive Slowdown (should still be s2)
	s1New := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 3000}
	// No observer update expected because the merged state is still s2
	mux.NotifySourceRateLimitState("source1", s1New)
	assert.False(t, mux.IsRejected())
	observer.AssertExpectations(t)

	// Notify source 3: Reject
	s3 := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT}
	observer.On("UpdateRateLimitState", s3).Once()
	mux.NotifySourceRateLimitState("source3", s3)
	assert.True(t, mux.IsRejected())
	observer.AssertExpectations(t)

	// Notify source 3: Normal (revert to s2)
	observer.On("UpdateRateLimitState", s2).Once()
	mux.NotifySourceRateLimitState("source3", NewNormalRateLimitState())
	assert.False(t, mux.IsRejected())
	observer.AssertExpectations(t)

	// Notify source 2: Normal (revert to s1_new)
	observer.On("UpdateRateLimitState", s1New).Once()
	mux.NotifySourceRateLimitState("source2", NewNormalRateLimitState())
	assert.False(t, mux.IsRejected())
	observer.AssertExpectations(t)

	// Notify source 1: Normal (back to normal)
	observer.On("UpdateRateLimitState", NewNormalRateLimitState()).Once()
	mux.NotifySourceRateLimitState("source1", NewNormalRateLimitState())
	assert.False(t, mux.IsRejected())
	observer.AssertExpectations(t)

	// Test panic for NotifyRateLimitStateChange
	assert.Panics(t, func() {
		mux.NotifyRateLimitStateChange(s1)
	})
}
