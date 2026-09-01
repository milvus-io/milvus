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

package meta

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// ShardSplitStateCache answers whether a collection is mid shard-split and which
// of its vchannels is a fenced split source, by reading the collection's
// per-shard routing info (shard_infos) from the coordinator. The result is
// cached with a short TTL so the checkers can consult it every cycle without an
// RPC per check; splits are rare and the source set only changes at the start
// and end of a split window.
type ShardSplitStateCache struct {
	broker Broker
	ttl    time.Duration

	mu     sync.Mutex
	states map[int64]*shardSplitEntry
}

type shardSplitEntry struct {
	// channelStates maps each of the collection's vchannels to its shard state.
	//
	// State is all querycoord needs, and all the collection meta carries: which
	// sources a target was carved from is provenance with the split task's
	// lifetime, and lives there rather than on the collection.
	channelStates map[string]schemapb.ShardState
	fetchedAt     time.Time
}

func (e *shardSplitEntry) channelsInState(state schemapb.ShardState) []string {
	var out []string
	for channel, s := range e.channelStates {
		if s == state {
			out = append(out, channel)
		}
	}
	return out
}

// NewShardSplitStateCache builds a cache backed by broker.DescribeCollection.
func NewShardSplitStateCache(broker Broker, ttl time.Duration) *ShardSplitStateCache {
	return &ShardSplitStateCache{
		broker: broker,
		ttl:    ttl,
		states: make(map[int64]*shardSplitEntry),
	}
}

// IsShardSplitting reports whether a split is in progress for the collection:
// either a shard is a fenced split source (ShardSplitting), or a source has been
// dropped at adoption but not yet released and cleaned up (ShardDropped). Balance
// stays frozen across both windows so the balancer never moves a source channel
// mid-handoff — which would tear down its in-process children or re-spawn orphan
// ones on another node. The freeze lifts once datacoord removes the dropped
// source from the collection meta, marking the split fully complete.
func (c *ShardSplitStateCache) IsShardSplitting(ctx context.Context, collectionID int64) bool {
	return len(c.SplittingSourceChannels(ctx, collectionID)) > 0 ||
		len(c.DroppedSourceChannels(ctx, collectionID)) > 0
}

// SplittingSourceChannels returns the collection's vchannels that are fenced
// split sources (ShardState_ShardSplitting) — balance is frozen for them.
func (c *ShardSplitStateCache) SplittingSourceChannels(ctx context.Context, collectionID int64) []string {
	return c.channelsInState(ctx, collectionID, schemapb.ShardState_ShardSplitting)
}

// CreatingTargetChannels returns the collection's split target vchannels that
// are not yet adopted (ShardState_ShardCreating) — querycoord must NOT watch
// them yet; they are fronted in-process by the source delegator.
func (c *ShardSplitStateCache) CreatingTargetChannels(ctx context.Context, collectionID int64) []string {
	return c.channelsInState(ctx, collectionID, schemapb.ShardState_ShardCreating)
}

// LiveChannels returns the collection's vchannels that currently serve reads --
// the Normal ones and the split targets still being adopted. It is what a caller
// asks when it needs "is the post-split topology up" without knowing which
// target replaced which source.
func (c *ShardSplitStateCache) LiveChannels(ctx context.Context, collectionID int64) []string {
	entry := c.entryFor(ctx, collectionID)
	if entry == nil {
		return nil
	}
	var out []string
	for channel, state := range entry.channelStates {
		switch state {
		case schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardCreating:
			out = append(out, channel)
		}
	}
	return out
}

// DroppedSourceChannels returns the collection's split source vchannels that
// have been released after adoption (ShardState_ShardDropped) — querycoord must
// release them.
func (c *ShardSplitStateCache) DroppedSourceChannels(ctx context.Context, collectionID int64) []string {
	return c.channelsInState(ctx, collectionID, schemapb.ShardState_ShardDropped)
}

// channelsInState returns the collection's vchannels in the given shard state.
func (c *ShardSplitStateCache) channelsInState(ctx context.Context, collectionID int64, state schemapb.ShardState) []string {
	if entry := c.entryFor(ctx, collectionID); entry != nil {
		return entry.channelsInState(state)
	}
	return nil
}

// entryFor returns the cached split-state entry for a collection, refreshing it
// when older than the TTL; on a refresh error it falls back to the last known
// entry so a transient coordinator error does not flap the freeze.
func (c *ShardSplitStateCache) entryFor(ctx context.Context, collectionID int64) *shardSplitEntry {
	c.mu.Lock()
	entry, ok := c.states[collectionID]
	fresh := ok && time.Since(entry.fetchedAt) < c.ttl
	c.mu.Unlock()
	if fresh {
		return entry
	}

	fetched, err := c.fetch(ctx, collectionID)
	if err != nil {
		if ok {
			return entry
		}
		return nil
	}
	c.mu.Lock()
	c.states[collectionID] = fetched
	c.mu.Unlock()
	return fetched
}

// Invalidate drops the cached state for a collection so the next query refetches
// immediately — used to lift the freeze the moment a split completes rather than
// waiting out the TTL.
func (c *ShardSplitStateCache) Invalidate(collectionID int64) {
	c.mu.Lock()
	delete(c.states, collectionID)
	c.mu.Unlock()
}

func (c *ShardSplitStateCache) fetch(ctx context.Context, collectionID int64) (*shardSplitEntry, error) {
	resp, err := c.broker.DescribeCollection(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	vchannels := resp.GetVirtualChannelNames()
	states := make(map[string]schemapb.ShardState, len(vchannels))
	// shard_infos is parallel to virtual_channel_names.
	for i, info := range resp.GetShardInfos() {
		if i < len(vchannels) {
			states[vchannels[i]] = info.GetState()
		}
	}
	return &shardSplitEntry{channelStates: states, fetchedAt: time.Now()}, nil
}
