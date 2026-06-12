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
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// ChannelBlacklist manages blacklisted nodes per channel with expiration.
// When a query fails on a delegator, the node is added to the blacklist
// for the corresponding channel. The node will be automatically removed
// from the blacklist after the configured duration (default 30s).
type ChannelBlacklist struct {
	mu sync.RWMutex
	// channel -> nodeID -> expireAt
	blacklist map[string]map[int64]time.Time

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewChannelBlacklist creates a new ChannelBlacklist
func NewChannelBlacklist() *ChannelBlacklist {
	return &ChannelBlacklist{
		blacklist: make(map[string]map[int64]time.Time),
		closeCh:   make(chan struct{}),
	}
}

// Start starts the background cleanup loop
func (b *ChannelBlacklist) Start() {
	b.wg.Add(1)
	go b.cleanupLoop()
}

// Close stops the background cleanup loop
func (b *ChannelBlacklist) Close() {
	b.closeOnce.Do(func() {
		close(b.closeCh)
		b.wg.Wait()
	})
}

// cleanupLoop periodically removes expired entries from the blacklist
func (b *ChannelBlacklist) cleanupLoop() {
	defer b.wg.Done()

	interval := paramtable.Get().ProxyCfg.ReplicaBlacklistCleanupInterval.GetAsDurationByParse()
	if interval <= 0 {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Info("Start blacklist cleanup loop")
	for {
		select {
		case <-b.closeCh:
			log.Info("Blacklist cleanup loop exit")
			return
		case <-ticker.C:
			b.cleanup()
		}
	}
}

// cleanup removes expired entries from the blacklist
func (b *ChannelBlacklist) cleanup() {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	for channel, nodes := range b.blacklist {
		for nodeID, expireAt := range nodes {
			if now.After(expireAt) {
				delete(nodes, nodeID)
			}
		}
		// Remove empty channel entries
		if len(nodes) == 0 {
			delete(b.blacklist, channel)
		}
	}
}

// Add adds a node to the blacklist for a specific channel.
// The node will be blacklisted for the duration configured by
// proxy.replicaBlacklistDuration (default 30s).
// If duration is <= 0, the blacklist is disabled and this is a no-op.
func (b *ChannelBlacklist) Add(channel string, nodeID int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	duration := paramtable.Get().ProxyCfg.ReplicaBlacklistDuration.GetAsDurationByParse()
	if duration <= 0 {
		// blacklist is disabled
		return
	}

	if _, ok := b.blacklist[channel]; !ok {
		b.blacklist[channel] = make(map[int64]time.Time)
	}
	b.blacklist[channel][nodeID] = time.Now().Add(duration)
}

// GetBlacklistedNodes returns unexpired blacklisted node IDs for a channel.
// Returns nil if no nodes are blacklisted for the channel.
func (b *ChannelBlacklist) GetBlacklistedNodes(channel string) []int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	nodes, ok := b.blacklist[channel]
	if !ok {
		return nil
	}

	now := time.Now()
	result := make([]int64, 0, len(nodes))
	for nodeID, expireAt := range nodes {
		if now.Before(expireAt) {
			result = append(result, nodeID)
		}
	}
	return result
}

// Clear clears blacklist for a specific channel.
// This is called when all replicas are excluded and we need to retry.
func (b *ChannelBlacklist) Clear(channel string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.blacklist, channel)
}
