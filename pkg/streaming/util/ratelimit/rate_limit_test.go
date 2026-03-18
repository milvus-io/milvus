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
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

type MockRateLimitObserver struct {
	mock.Mock
}

func (m *MockRateLimitObserver) UpdateRateLimitState(state RateLimitState) {
	m.Called(state)
}

func TestRateLimitState(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		s1 := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL}
		assert.Equal(t, "WAL_RATE_LIMIT_STATE_NORMAL", s1.String())

		s2 := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT}
		assert.Equal(t, "WAL_RATE_LIMIT_STATE_REJECT", s2.String())

		s3 := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 1024 * 1024}
		assert.Equal(t, "WAL_RATE_LIMIT_STATE_SLOWDOWN:1.000MB/s", s3.String())
	})

	t.Run("MoreRestrictiveThan", func(t *testing.T) {
		normal := NewNormalRateLimitState()
		slowdown1 := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 2000}
		slowdown2 := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 1000}
		reject := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT}

		assert.True(t, slowdown1.MoreRestrictiveThan(normal))
		assert.True(t, slowdown2.MoreRestrictiveThan(slowdown1))
		assert.True(t, reject.MoreRestrictiveThan(slowdown2))
		assert.False(t, normal.MoreRestrictiveThan(slowdown1))
		assert.False(t, slowdown1.MoreRestrictiveThan(slowdown2))
		assert.False(t, slowdown2.MoreRestrictiveThan(reject))
	})
}

func TestRateLimitObserverRegistry(t *testing.T) {
	registry := NewRateLimitObserverRegistry()
	observer1 := new(MockRateLimitObserver)
	observer2 := new(MockRateLimitObserver)

	// Test Register
	observer1.On("UpdateRateLimitState", NewNormalRateLimitState()).Once()
	registry.Register(observer1)
	observer1.AssertExpectations(t)

	// Test Notify with same state (should not notify)
	registry.NotifyRateLimitStateChange(NewNormalRateLimitState())
	observer1.AssertExpectations(t)

	// Test Notify with new state
	newState := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN, Rate: 1000}
	observer1.On("UpdateRateLimitState", newState).Once()
	registry.NotifyRateLimitStateChange(newState)
	observer1.AssertExpectations(t)

	// Test Register with latest state
	observer2.On("UpdateRateLimitState", newState).Once()
	registry.Register(observer2)
	observer2.AssertExpectations(t)

	// Test Notify multiple observers
	finalState := RateLimitState{State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT}
	observer1.On("UpdateRateLimitState", finalState).Once()
	observer2.On("UpdateRateLimitState", finalState).Once()
	registry.NotifyRateLimitStateChange(finalState)
	observer1.AssertExpectations(t)
	observer2.AssertExpectations(t)

	// Test Unregister
	registry.Unregister(observer1)
	newState2 := NewNormalRateLimitState()
	observer2.On("UpdateRateLimitState", newState2).Once()
	registry.NotifyRateLimitStateChange(newState2)
	observer1.AssertExpectations(t) // Should not be called again
	observer2.AssertExpectations(t)
}
