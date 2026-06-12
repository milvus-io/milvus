// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardclient

import (
	"testing"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestChannelBlacklist(t *testing.T) {
	paramtable.Init()

	t.Run("test add and get blacklisted nodes", func(t *testing.T) {
		// Set blacklist duration to 1 second for testing
		paramtable.Get().Save(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key, "1s")
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key)

		blacklist := NewChannelBlacklist()
		channel := "test_channel"

		// Add nodes to blacklist
		blacklist.Add(channel, 1)
		blacklist.Add(channel, 2)

		// Get blacklisted nodes
		nodes := blacklist.GetBlacklistedNodes(channel)
		if len(nodes) != 2 {
			t.Errorf("expected 2 blacklisted nodes, got %d", len(nodes))
		}

		// Check that both nodes are in the list
		nodeSet := make(map[int64]bool)
		for _, n := range nodes {
			nodeSet[n] = true
		}
		if !nodeSet[1] || !nodeSet[2] {
			t.Errorf("expected nodes 1 and 2 to be blacklisted, got %v", nodes)
		}
	})

	t.Run("test blacklist expiration", func(t *testing.T) {
		// Set blacklist duration to 100ms for testing
		paramtable.Get().Save(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key, "100ms")
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key)

		blacklist := NewChannelBlacklist()
		channel := "test_channel"

		// Add node to blacklist
		blacklist.Add(channel, 1)

		// Node should be blacklisted
		nodes := blacklist.GetBlacklistedNodes(channel)
		if len(nodes) != 1 {
			t.Errorf("expected 1 blacklisted node, got %d", len(nodes))
		}

		// Wait for expiration
		time.Sleep(150 * time.Millisecond)

		// Node should no longer be blacklisted
		nodes = blacklist.GetBlacklistedNodes(channel)
		if len(nodes) != 0 {
			t.Errorf("expected 0 blacklisted nodes after expiration, got %d", len(nodes))
		}
	})

	t.Run("test clear blacklist", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key, "1s")
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key)

		blacklist := NewChannelBlacklist()
		channel := "test_channel"

		// Add nodes to blacklist
		blacklist.Add(channel, 1)
		blacklist.Add(channel, 2)

		// Clear blacklist
		blacklist.Clear(channel)

		// Should have no blacklisted nodes
		nodes := blacklist.GetBlacklistedNodes(channel)
		if len(nodes) != 0 {
			t.Errorf("expected 0 blacklisted nodes after clear, got %d", len(nodes))
		}
	})

	t.Run("test blacklist disabled when duration is 0", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key, "0s")
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key)

		blacklist := NewChannelBlacklist()
		channel := "test_channel"

		// Add node to blacklist - should be a no-op
		blacklist.Add(channel, 1)

		// Should have no blacklisted nodes
		nodes := blacklist.GetBlacklistedNodes(channel)
		if len(nodes) != 0 {
			t.Errorf("expected 0 blacklisted nodes when disabled, got %d", len(nodes))
		}
	})

	t.Run("test different channels have separate blacklists", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key, "1s")
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key)

		blacklist := NewChannelBlacklist()

		// Add different nodes to different channels
		blacklist.Add("channel1", 1)
		blacklist.Add("channel2", 2)

		// Check channel1
		nodes1 := blacklist.GetBlacklistedNodes("channel1")
		if len(nodes1) != 1 || nodes1[0] != 1 {
			t.Errorf("expected node 1 in channel1, got %v", nodes1)
		}

		// Check channel2
		nodes2 := blacklist.GetBlacklistedNodes("channel2")
		if len(nodes2) != 1 || nodes2[0] != 2 {
			t.Errorf("expected node 2 in channel2, got %v", nodes2)
		}
	})

	t.Run("test cleanup removes expired entries", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key, "50ms")
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key)

		blacklist := NewChannelBlacklist()
		channel := "test_channel"

		// Add node to blacklist
		blacklist.Add(channel, 1)

		// Wait for expiration
		time.Sleep(100 * time.Millisecond)

		// Manually trigger cleanup
		blacklist.cleanup()

		// The internal map should be cleaned up
		blacklist.mu.RLock()
		_, exists := blacklist.blacklist[channel]
		blacklist.mu.RUnlock()

		if exists {
			t.Errorf("expected channel to be removed from blacklist after cleanup")
		}
	})

	t.Run("test start and close cleanup loop", func(t *testing.T) {
		blacklist := NewChannelBlacklist()
		blacklist.Start()
		// Give some time for goroutine to start
		time.Sleep(10 * time.Millisecond)
		blacklist.Close()
		// Should not panic or hang
	})

	t.Run("test get non-existent channel", func(t *testing.T) {
		blacklist := NewChannelBlacklist()

		// Get blacklisted nodes for a channel that was never added
		nodes := blacklist.GetBlacklistedNodes("non_existent_channel")
		if nodes != nil {
			t.Errorf("expected nil for non-existent channel, got %v", nodes)
		}
	})

	t.Run("test add same node updates expiration", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key, "200ms")
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.Key)

		blacklist := NewChannelBlacklist()
		channel := "test_channel"

		// Add node to blacklist
		blacklist.Add(channel, 1)

		// Wait 100ms
		time.Sleep(100 * time.Millisecond)

		// Add same node again - should refresh expiration
		blacklist.Add(channel, 1)

		// Wait another 150ms (total 250ms from first add, but only 150ms from second add)
		time.Sleep(150 * time.Millisecond)

		// Node should still be blacklisted because expiration was refreshed
		nodes := blacklist.GetBlacklistedNodes(channel)
		if len(nodes) != 1 {
			t.Errorf("expected 1 blacklisted node after refresh, got %d", len(nodes))
		}
	})

	t.Run("test close can be called multiple times", func(t *testing.T) {
		blacklist := NewChannelBlacklist()
		blacklist.Start()
		time.Sleep(10 * time.Millisecond)

		// Close multiple times - should not panic due to closeOnce
		blacklist.Close()
		blacklist.Close()
		blacklist.Close()
		// Should not panic
	})

	t.Run("test cleanup loop with default interval", func(t *testing.T) {
		// Set cleanup interval to 0 to trigger default value
		paramtable.Get().Save(paramtable.Get().ProxyCfg.ReplicaBlacklistCleanupInterval.Key, "0s")
		defer paramtable.Get().Reset(paramtable.Get().ProxyCfg.ReplicaBlacklistCleanupInterval.Key)

		blacklist := NewChannelBlacklist()
		blacklist.Start()
		// Give some time for goroutine to start with default interval
		time.Sleep(10 * time.Millisecond)
		blacklist.Close()
		// Should not panic or hang - default interval (10s) should be used
	})
}
