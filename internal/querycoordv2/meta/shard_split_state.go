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
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	channelStates ShardStates
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

// IsShardSplitting reports whether a shard of the collection is a fenced split
// source (ShardSplitting), i.e. whether a split window is open. It is not the
// whole balance freeze: adoption ends the window, but the freeze holds until the
// current target stops listing the retired source (see
// BalanceChecker.frozenForShardSplit).
func (c *ShardSplitStateCache) IsShardSplitting(ctx context.Context, collectionID int64) bool {
	return len(c.SplittingSourceChannels(ctx, collectionID)) > 0
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

// CreatingTargetChannelsAsOf reports the collection's not-yet-adopted split
// target vchannels (ShardState_ShardCreating), as read no earlier than after.
// ok is false when the freshest entry the cache can produce -- including a
// fallback (entryFor, ReadShardStates) -- was fetched before after: such an
// entry predates whatever after marks (typically a next-target pull) and
// cannot speak to the state at or after it, so its "nothing Creating" must not
// be read as "adoption already happened". Callers needing a liveness check on
// a specific pull must use this instead of CreatingTargetChannels, which
// answers from whatever entry is cached regardless of its age relative to any
// pull.
func (c *ShardSplitStateCache) CreatingTargetChannelsAsOf(ctx context.Context, collectionID int64, after time.Time) (channels []string, ok bool) {
	entry := c.entryFor(ctx, collectionID)
	if entry == nil || entry.fetchedAt.Before(after) {
		return nil, false
	}
	return entry.channelsInState(schemapb.ShardState_ShardCreating), true
}

// ChannelStates returns the collection's per-vchannel shard states as one cached
// read, refreshing past the TTL like every other query on this cache. ok is
// false only when the cache holds nothing for the collection and cannot read it.
//
// It exists for callers that must reason about two states together -- "is this
// channel still a not-yet-adopted target AND is its fenced source still listed"
// -- which two separate queries cannot answer, because a TTL refresh may land
// between them and split the answer across two different reads. The map is a
// copy, so a caller may hold it.
func (c *ShardSplitStateCache) ChannelStates(ctx context.Context, collectionID int64) (ShardStates, bool) {
	entry := c.entryFor(ctx, collectionID)
	if entry == nil {
		return nil, false
	}
	states := make(ShardStates, len(entry.channelStates))
	for channel, state := range entry.channelStates {
		states[channel] = state
	}
	return states, true
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

// ShardStateSnapshot is one read of a collection's per-vchannel shard states,
// taken right before a next-target pull so the pull can mark its split window
// targets. It is immutable.
type ShardStateSnapshot struct {
	entry *shardSplitEntry
}

// SplitWindowTargets returns the channels of a next-target pull that must be
// marked as split window targets, given that this snapshot was read BEFORE the
// pull listed pulledChannels.
//
// The rule is a complement: a pulled channel is marked UNLESS this read saw it
// Normal, Splitting or Dropped. Shard states only move forward and none of those
// three returns to Creating, so such a channel cannot be a not-yet-adopted split
// target in the later pull. Every other pulled channel is marked:
//   - one this read saw Creating: still Creating in the pull, or adopted in
//     between (an over-mark);
//   - one this read did not list at all: a target fenced after the read (the
//     fence race), which the pull lists in the window.
//
// So for ANY earlier read the mark never misses a target that is still Creating
// in the pull; the only error is an over-mark, which holds the snapshot back
// until the window-end refresh re-pulls it. A collection that is not splitting
// lists the same channels in both reads, all Normal, and marks nothing.
func (s *ShardStateSnapshot) SplitWindowTargets(pulledChannels []string) []string {
	var marked []string
	for _, channel := range pulledChannels {
		state, listed := s.entry.channelStates[channel]
		if listed {
			switch state {
			case schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardDropped:
				continue
			}
		}
		marked = append(marked, channel)
	}
	return marked
}

// ReadShardStates reads the collection's shard states fresh, ignoring the TTL,
// and stores them for later cached queries. The freshest read over-marks the
// least, which is why it does not settle for a TTL-valid entry.
//
// When the fresh read fails it falls back to the last entry the cache holds for
// the collection: SplitWindowTargets never misses a target on ANY earlier read,
// so an old entry only over-marks (and a collection that is not splitting still
// marks nothing). The read error is returned only when the cache holds no entry.
func (c *ShardSplitStateCache) ReadShardStates(ctx context.Context, collectionID int64) (*ShardStateSnapshot, error) {
	fetched, err := c.fetch(ctx, collectionID)
	if err != nil {
		c.mu.Lock()
		cached, ok := c.states[collectionID]
		c.mu.Unlock()
		if !ok {
			return nil, err
		}
		mlog.Warn(ctx, "failed to read shard states fresh, fall back to the last cached read",
			mlog.FieldCollectionID(collectionID),
			mlog.Duration("cachedAge", time.Since(cached.fetchedAt)),
			mlog.Err(err))
		return &ShardStateSnapshot{entry: cached}, nil
	}
	c.mu.Lock()
	c.states[collectionID] = fetched
	c.mu.Unlock()
	return &ShardStateSnapshot{entry: fetched}, nil
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
	return &shardSplitEntry{channelStates: ShardStatesOf(resp), fetchedAt: time.Now()}, nil
}

// ShardStates maps each vchannel a collection lists to its shard state, as one
// DescribeCollection answered it.
type ShardStates map[string]schemapb.ShardState

// ShardStatesOf reads the per-vchannel shard states off a DescribeCollection
// response.
func ShardStatesOf(resp *milvuspb.DescribeCollectionResponse) ShardStates {
	vchannels := resp.GetVirtualChannelNames()
	states := make(ShardStates, len(vchannels))
	// shard_infos is parallel to virtual_channel_names. A listed vchannel without
	// one is a legacy shard, Normal, as rootcoord itself defaults it: it must
	// still count as listed, or SplitWindowTargets would mark it.
	infos := resp.GetShardInfos()
	for i, vchannel := range vchannels {
		state := schemapb.ShardState_ShardNormal
		if i < len(infos) {
			state = infos[i].GetState()
		}
		states[vchannel] = state
	}
	return states
}

// Lists reports whether the collection lists the vchannel.
//
// A collection is never described with no vchannel at all, so an empty listing
// carries no routing information -- a caller built without one, such as a test
// double -- and is read as listing everything.
func (s ShardStates) Lists(vchannel string) bool {
	if len(s) == 0 {
		return true
	}
	_, listed := s[vchannel]
	return listed
}

// Splitting reports whether one of the listed vchannels is a fenced split
// source.
func (s ShardStates) Splitting() bool {
	for _, state := range s {
		if state == schemapb.ShardState_ShardSplitting {
			return true
		}
	}
	return false
}

// Delisted returns the given channels the collection no longer lists. Among the
// channels of a collection's target these are shard split sources retired by an
// adoption the target predates: delisting is the only way a shard leaves the
// collection (see routing.CheckNoListedDroppedShard).
func (s ShardStates) Delisted(channels map[string]*DmChannel) []string {
	var out []string
	for name := range channels {
		if !s.Lists(name) {
			out = append(out, name)
		}
	}
	return out
}

// CheckWatchable refuses a watch of a vchannel the collection no longer lists,
// or of a split target not yet adopted.
//
// Such a vchannel is a shard split source that an adoption retired. The next
// target pulled inside the split window still lists it until the window-end
// re-pull, and the current target until the flip, so a checker working from
// either would re-watch it the moment its delegator is gone. The rebuilt
// delegator would serve the source's key range without the in-process children
// that carry the targets' writes: adoption delisted the source, so its rebuild
// re-derives no target to front. The flip releases it instead.
//
// A not-yet-adopted split target (ShardCreating) is fronted in-process by its
// source's delegator until adoption; a watch before that would build a second
// delegator for it, or adopt the source's child early.
//
// Both refusals are System errors: nothing in any request forces them, and it
// is the stale view that has to catch up. The second is transient and says so.
func (s ShardStates) CheckWatchable(vchannel string) error {
	if !s.Lists(vchannel) {
		return merr.WrapErrChannelNotFound(vchannel,
			"the collection no longer lists it: a retired shard split source is released at the flip, never re-watched")
	}
	if s[vchannel] == schemapb.ShardState_ShardCreating {
		return merr.WrapErrServiceUnavailable("shard split target not adopted yet",
			fmt.Sprintf("vchannel %s is fronted by its split source until adoption", vchannel))
	}
	return nil
}
