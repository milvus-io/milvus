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
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

// NewMuxRateLimitObserverRegistry creates a new MuxRateLimitObserverRegistryImpl.
func NewMuxRateLimitObserverRegistry() *MuxRateLimitObserverRegistryImpl {
	return &MuxRateLimitObserverRegistryImpl{
		RateLimitObserverRegistryImpl: NewRateLimitObserverRegistry(),
		isRejected:                    atomic.NewBool(false),
		sourceStates:                  make(map[string]RateLimitState),
	}
}

// MuxRateLimitObserverRegistryImpl is the implementation of RateLimitObserverRegistry
// that supports merging rate limit states from multiple sources.
// It selects the most restrictive rate limit state and forwards it to all observers.
//
// Rate limit intensity order (most to least restrictive):
// 1. REJECT - completely reject all requests
// 2. SLOWDOWN - limit rate (lower rate = more restrictive)
// 3. NORMAL - no rate limiting
type MuxRateLimitObserverRegistryImpl struct {
	*RateLimitObserverRegistryImpl
	isRejected   *atomic.Bool
	sourceStates map[string]RateLimitState // map from source name to its state
}

// IsRejected returns true if the rate limit state is rejected.
func (m *MuxRateLimitObserverRegistryImpl) IsRejected() bool {
	return m.isRejected.Load()
}

// NotifySourceRateLimitState updates the rate limit state for a specific source.
// The merged state (most restrictive) will be forwarded to all observers.
func (m *MuxRateLimitObserverRegistryImpl) NotifySourceRateLimitState(source string, state RateLimitState) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Update or remove the source state
	if state.State == streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL {
		// NORMAL state means no rate limiting from this source, remove it
		delete(m.sourceStates, source)
	} else {
		m.sourceStates[source] = state
	}

	// Compute the merged state
	targetState := m.computeMergedStateLocked()
	m.RateLimitObserverRegistryImpl.notifyRateLimitStateChange(targetState)
	if targetState.State == streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT {
		m.isRejected.Store(true)
	} else {
		m.isRejected.Store(false)
	}
}

// NotifyRateLimitStateChange updates the rate limit state for all sources.
func (m *MuxRateLimitObserverRegistryImpl) NotifyRateLimitStateChange(state RateLimitState) {
	panic("not implemented")
}

// computeMergedStateLocked computes the merged rate limit state from all sources.
// Must be called with mu held.
// Returns the most restrictive state among all sources.
func (m *MuxRateLimitObserverRegistryImpl) computeMergedStateLocked() RateLimitState {
	if len(m.sourceStates) == 0 {
		// No active rate limiting sources
		return NewNormalRateLimitState()
	}

	// Find the most restrictive state
	targetState := NewNormalRateLimitState()
	for _, state := range m.sourceStates {
		if state.MoreRestrictiveThan(targetState) {
			targetState = state
		}
	}
	return targetState
}
