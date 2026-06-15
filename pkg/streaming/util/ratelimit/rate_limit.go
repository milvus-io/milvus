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
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

// NewNormalRateLimitState creates a new normal rate limit state.
func NewNormalRateLimitState() RateLimitState {
	return RateLimitState{
		State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_NORMAL,
		Rate:  0,
	}
}

// RateLimitState is the state of the rate limit.
type RateLimitState struct {
	State streamingpb.WALRateLimitState
	Rate  int64
}

// String returns the string representation of the rate limit state.
func (r *RateLimitState) String() string {
	if r.State == streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN {
		return fmt.Sprintf("%s:%.3fMB/s", r.State, float64(r.Rate)/1024.0/1024.0)
	}
	return r.State.String()
}

// MoreRestrictiveThan returns true if the rate limit state is more restrictive than the other.
func (r RateLimitState) MoreRestrictiveThan(other RateLimitState) bool {
	return r.State > other.State || (r.State == other.State && r.Rate < other.Rate)
}

// RateLimitObserver is the interface that wraps the basic rate limiting methods.
type RateLimitObserver interface {
	UpdateRateLimitState(state RateLimitState)
}

// RateLimitObserverRegistry is the interface that wraps the basic rate limit notifier registry methods.
type RateLimitObserverRegistry interface {
	// Register registers a rate limit observer.
	Register(observer RateLimitObserver)

	// Unregister unregisters a rate limit observer.
	Unregister(observer RateLimitObserver)
}

// NewRateLimitObserverRegistry creates a new RateLimitObserverRegistry.
func NewRateLimitObserverRegistry() *RateLimitObserverRegistryImpl {
	return &RateLimitObserverRegistryImpl{
		mu:          sync.Mutex{},
		latestState: NewNormalRateLimitState(),
		observers:   make(map[RateLimitObserver]struct{}),
	}
}

// RateLimitObserverRegistryImpl is the implementation of the RateLimitObserverRegistry interface.
type RateLimitObserverRegistryImpl struct {
	mu          sync.Mutex
	latestState RateLimitState
	observers   map[RateLimitObserver]struct{}
}

// Register registers a rate limit observer.
func (r *RateLimitObserverRegistryImpl) Register(observer RateLimitObserver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observers[observer] = struct{}{}
	// notify the observer with the latest rate limit.
	observer.UpdateRateLimitState(r.latestState)
}

// Unregister unregisters a rate limit observer.
func (r *RateLimitObserverRegistryImpl) Unregister(observer RateLimitObserver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.observers, observer)
}

// NotifyRateLimitStateChange notifies the rate limit state change to all observers.
func (r *RateLimitObserverRegistryImpl) NotifyRateLimitStateChange(state RateLimitState) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.notifyRateLimitStateChange(state)
}

func (r *RateLimitObserverRegistryImpl) notifyRateLimitStateChange(state RateLimitState) {
	if r.latestState == state {
		return
	}
	r.latestState = state
	for observer := range r.observers {
		observer.UpdateRateLimitState(state)
	}
}
