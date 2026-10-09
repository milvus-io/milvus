// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package readiness

import "sync"

// Notifications keeps change signals only for collections with active waiters.
// Signals carry no metadata: a waiter must reread authoritative state after it
// wakes. Release permanently invalidates existing subscriptions, including when
// a new load starts before a waiter gets scheduled again.
type Notifications struct {
	mu          sync.Mutex
	collections map[int64]*subscriptionState
	done        chan struct{}
	closed      bool
}

type subscriptionState struct {
	changed  chan struct{}
	released bool
	waiters  int
}

type Subscription struct {
	owner        *Notifications
	collectionID int64
	state        *subscriptionState
	once         sync.Once
}

func NewNotifications() *Notifications {
	return &Notifications{collections: make(map[int64]*subscriptionState), done: make(chan struct{})}
}

func (n *Notifications) Subscribe(collectionID int64) *Subscription {
	n.mu.Lock()
	defer n.mu.Unlock()
	state := n.collections[collectionID]
	if state == nil {
		state = &subscriptionState{changed: make(chan struct{})}
		if !n.closed {
			n.collections[collectionID] = state
		}
	}
	state.waiters++
	return &Subscription{owner: n, collectionID: collectionID, state: state}
}

// Observe returns the signal to capture BEFORE reading current collection state.
// A concurrent change closes that signal even if it precedes the actual wait.
func (s *Subscription) Observe() (changed <-chan struct{}, released bool) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	return s.state.changed, s.state.released
}

func (s *Subscription) Close() {
	s.once.Do(func() {
		s.owner.mu.Lock()
		defer s.owner.mu.Unlock()
		s.state.waiters--
		if s.state.waiters == 0 && s.owner.collections[s.collectionID] == s.state {
			delete(s.owner.collections, s.collectionID)
		}
	})
}

// Notify is nonblocking and safe to call from a state-machine callback.
func (n *Notifications) Notify(collectionID int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if state := n.collections[collectionID]; state != nil {
		close(state.changed)
		state.changed = make(chan struct{})
	}
}

func (n *Notifications) Release(collectionID int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if state := n.collections[collectionID]; state != nil {
		state.released = true
		close(state.changed)
		delete(n.collections, collectionID)
	}
}

func (n *Notifications) Done() <-chan struct{} { return n.done }

func (n *Notifications) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.closed {
		n.closed = true
		close(n.done)
		clear(n.collections)
	}
}
