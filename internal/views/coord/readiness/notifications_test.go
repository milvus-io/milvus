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

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNotificationBeforeBlockingIsNotLost(t *testing.T) {
	n := NewNotifications()
	s := n.Subscribe(1)
	defer s.Close()
	changed, released := s.Observe()
	require.False(t, released)
	n.Notify(2)
	select {
	case <-changed:
		t.Fatal("another collection woke this waiter")
	default:
	}
	n.Notify(1)
	select {
	case <-changed:
	default:
		t.Fatal("notification between checking state and blocking was lost")
	}
	next, _ := s.Observe()
	require.NotEqual(t, changed, next)
}

func TestReleasePermanentlyInvalidatesExistingWaiters(t *testing.T) {
	n := NewNotifications()
	old := n.Subscribe(1)
	other := n.Subscribe(1)
	oldSignal, _ := old.Observe()
	otherSignal, _ := other.Observe()
	require.Equal(t, oldSignal, otherSignal)
	n.Release(1)
	fresh := n.Subscribe(1)
	defer fresh.Close()
	n.Notify(1)
	_, released := old.Observe()
	require.True(t, released, "reload must not erase the release observed by old waiters")
	_, released = fresh.Observe()
	require.False(t, released)
	old.Close()
	old.Close()
	other.Close()
	require.Len(t, n.collections, 1, "old waiters must not remove a new subscription")
	fresh.Close()
	require.Empty(t, n.collections)
}

func TestNotificationsRetainOnlyActiveCollections(t *testing.T) {
	n := NewNotifications()
	for id := int64(0); id < 3; id++ {
		n.Notify(id)
		require.Empty(t, n.collections, "notifications must not retain collections without waiters")
		n.Release(id)
		require.Empty(t, n.collections)
		s := n.Subscribe(id)
		require.Len(t, n.collections, 1)
		s.Close()
		require.Empty(t, n.collections)
	}
	require.Empty(t, n.collections)
	n.Close()
	n.Close()
	select {
	case <-n.Done():
	default:
		t.Fatal("shutdown did not wake waiters")
	}
	n.Subscribe(1).Close()
	require.Empty(t, n.collections)
}

func TestConcurrentNotificationReleaseAndUnsubscribe(t *testing.T) {
	n := NewNotifications()
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Go(func() {
			for j := 0; j < 1000; j++ {
				s := n.Subscribe(1)
				s.Observe()
				n.Notify(1)
				n.Release(1)
				s.Close()
			}
		})
	}
	workers.Wait()
	require.Empty(t, n.collections)
}
