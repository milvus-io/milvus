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
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ShardSplitStateCache answers whether a collection is mid shard-split and which
// of its vchannels is a fenced split source, by reading the collection's
// per-shard routing info (shard_infos) from the coordinator. The result is
// cached with a short TTL so the checkers can consult it every cycle without an
// RPC per check; splits are rare and the source set only changes at the start
// and end of a split window.
//
// Readers that miss the TTL together share one read (single flight), and a read
// only ever replaces an older one: the read issued last wins, whenever it
// returns.
type ShardSplitStateCache struct {
	broker Broker
	ttl    time.Duration

	flight conc.Singleflight[*shardSplitEntry]

	mu     sync.Mutex
	states map[int64]*shardSplitEntry
	// invalidatedAt is when a collection was last invalidated: an entry read
	// before it is stale whatever its age, but still serves as the fallback of
	// a failed refresh.
	invalidatedAt map[int64]time.Time
}

type shardSplitEntry struct {
	// channelStates maps each of the collection's vchannels to its shard state.
	//
	// State is all querycoord needs, and all the collection meta carries: which
	// sources a target was carved from is provenance with the split task's
	// lifetime, and lives there rather than on the collection.
	channelStates ShardStates
	// fetchedAt is when the read was ISSUED, so an entry speaks to the state at
	// or after it (CreatingTargetChannelsAsOf) and orders reads by when they
	// were taken, not by when they happened to return.
	fetchedAt time.Time
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
		broker:        broker,
		ttl:           ttl,
		states:        make(map[int64]*shardSplitEntry),
		invalidatedAt: make(map[int64]time.Time),
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
// when older than the TTL or invalidated since it was read; on a refresh error
// it falls back to the last known entry so a transient coordinator error does
// not flap the freeze. Concurrent refreshes of one collection share one read;
// one that starts after an Invalidate does not join a read issued before it.
func (c *ShardSplitStateCache) entryFor(ctx context.Context, collectionID int64) *shardSplitEntry {
	c.mu.Lock()
	entry, ok := c.states[collectionID]
	invalidatedAt := c.invalidatedAt[collectionID]
	fresh := ok && !entry.fetchedAt.Before(invalidatedAt) && time.Since(entry.fetchedAt) < c.ttl
	c.mu.Unlock()
	if fresh {
		return entry
	}

	key := fmt.Sprintf("%d@%d", collectionID, invalidatedAt.UnixNano())
	fetched, err, _ := c.flight.Do(key, func() (*shardSplitEntry, error) {
		return c.fetch(ctx, collectionID)
	})
	if err != nil {
		if ok {
			return entry
		}
		return nil
	}
	return c.store(collectionID, fetched)
}

// store keeps the newer of the cached entry and fetched, by when each read was
// issued, and returns the one kept.
func (c *ShardSplitStateCache) store(collectionID int64, fetched *shardSplitEntry) *shardSplitEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cached, ok := c.states[collectionID]; ok && !fetched.fetchedAt.After(cached.fetchedAt) {
		return cached
	}
	c.states[collectionID] = fetched
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
	// a read issued after this one may have returned first; it is at least as
	// fresh, and just as much a read taken before the pull.
	return &ShardStateSnapshot{entry: c.store(collectionID, fetched)}, nil
}

// Invalidate makes the next query of a collection read its shard states afresh
// instead of waiting out the TTL -- used when something shows the cached read
// is out of date. The cached read stays the fallback of a failed refresh:
// forgetting it would turn a transient coordinator error into "states
// unknown", which freezes everything and stops every watch of the collection.
func (c *ShardSplitStateCache) Invalidate(collectionID int64) {
	c.mu.Lock()
	c.invalidatedAt[collectionID] = time.Now()
	c.mu.Unlock()
}

func (c *ShardSplitStateCache) fetch(ctx context.Context, collectionID int64) (*shardSplitEntry, error) {
	issuedAt := time.Now()
	resp, err := c.broker.DescribeCollection(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	return &shardSplitEntry{channelStates: ShardStatesOf(resp), fetchedAt: issuedAt}, nil
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

// CheckShardSplitMovable reports whether a shard split forbids moving any of
// the collection's channels or segments between nodes right now: nil when it
// does not, else a retriable System error naming why. It is
// EvalShardSplitFreeze(...).CheckCollection(); the rules are on
// ShardSplitFreeze. A nil cache disables the rule.
func CheckShardSplitMovable(ctx context.Context, cache *ShardSplitStateCache, targetMgr TargetManagerInterface, collectionID int64) error {
	return EvalShardSplitFreeze(ctx, cache, targetMgr, collectionID).CheckCollection()
}

// ShardSplitFreeze is one evaluation of what shard splits forbid moving in a
// collection. It is the one rule behind every refusal to move a channel or a
// segment during a split: the balance freeze (normal and stopping), the
// executor's last-word check on a channel move, and the refusal of manual moves
// (LoadBalance, TransferSegment, TransferChannel).
//
// Moving a split source rebuilds its delegator on another node without the
// in-process children it fronts, which is wrong for as long as reads can route
// to it; moving a split target before the flip either builds a delegator the
// source still fronts, or tears down the one the adoption converted in place.
// So the split's family -- its sources and its targets -- stays where it is:
//
//   - a channel the next target marks as a split window target. The mark is
//     taken from a fresh state read before the pull, so it gives a fence away
//     even while the states below still show a read from before it;
//   - a fenced source (ShardSplitting) and a not-yet-adopted target
//     (ShardCreating): the split window;
//   - a vchannel the current target lists but the collection no longer does: a
//     source retired by an adoption, which reads still route to until the
//     current target flips past it; and, while there is one, every channel of
//     the next target the current target does not list yet -- the adopted
//     targets waiting for that flip.
//
// Without a read of the shard states none of this can be ruled out, and
// nothing of the collection moves. Neither does anything when the states are
// behind the marks -- a marked channel they do not show Creating: the fence
// that opened that window postdates the read, so the read cannot name its
// source. Every other channel, and every segment attributed to one, may move.
type ShardSplitFreeze struct {
	collectionID int64
	// everything is set when nothing of the collection may move: the shard
	// states could not be read, or are behind the window marks.
	everything error
	// family are the channels of the collection's splits: sources and targets.
	family typeutil.Set[string]
}

// EvalShardSplitFreeze evaluates the freeze of a collection from the cached
// shard states. A nil cache disables the rule: nothing is frozen.
func EvalShardSplitFreeze(ctx context.Context, cache *ShardSplitStateCache, targetMgr TargetManagerInterface, collectionID int64) *ShardSplitFreeze {
	if cache == nil {
		return &ShardSplitFreeze{collectionID: collectionID}
	}
	states, ok := cache.ChannelStates(ctx, collectionID)
	if !ok {
		return &ShardSplitFreeze{
			collectionID: collectionID,
			everything: merr.WrapErrServiceUnavailable("shard split state unknown",
				fmt.Sprintf("collection %d: its shard states could not be read", collectionID)),
		}
	}
	return NewShardSplitFreeze(ctx, targetMgr, collectionID, states)
}

// NewShardSplitFreeze evaluates the freeze of a collection from the given shard
// states, for a caller that has just read them fresh.
func NewShardSplitFreeze(ctx context.Context, targetMgr TargetManagerInterface, collectionID int64, states ShardStates) *ShardSplitFreeze {
	family := typeutil.NewSet[string]()
	for channel := range targetMgr.GetSplitWindowTargets(ctx, collectionID, NextTarget) {
		if states[channel] != schemapb.ShardState_ShardCreating {
			return &ShardSplitFreeze{
				collectionID: collectionID,
				everything: merr.WrapErrServiceUnavailable("shard split states behind the window marks",
					fmt.Sprintf("collection %d: the next target marks %s, which the shard states do not show as a split target", collectionID, channel)),
			}
		}
		family.Insert(channel)
	}
	for channel, state := range states {
		if state == schemapb.ShardState_ShardSplitting || state == schemapb.ShardState_ShardCreating {
			family.Insert(channel)
		}
	}
	current := targetMgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget)
	if retired := states.Delisted(current); len(retired) > 0 {
		family.Insert(retired...)
		for channel := range targetMgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget) {
			if _, flipped := current[channel]; !flipped {
				family.Insert(channel)
			}
		}
	}
	return &ShardSplitFreeze{collectionID: collectionID, family: family}
}

// CheckCollection returns nil when no shard split freezes anything of the
// collection, else a retriable System error naming why.
func (f *ShardSplitFreeze) CheckCollection() error {
	if f.everything != nil {
		return f.everything
	}
	if len(f.family) > 0 {
		return merr.WrapErrServiceUnavailable("shard split in progress",
			fmt.Sprintf("collection %d: split sources and targets %v must stay where they are", f.collectionID, f.family.Collect()))
	}
	return nil
}

// CheckChannel returns nil when the channel -- its delegator, or a segment
// attributed to it -- may move, else a retriable System error naming why.
func (f *ShardSplitFreeze) CheckChannel(channel string) error {
	if f.everything != nil {
		return f.everything
	}
	if f.family.Contain(channel) {
		return merr.WrapErrServiceUnavailable("shard split in progress",
			fmt.Sprintf("collection %d: channel %s is a split source or target and must stay where it is", f.collectionID, channel))
	}
	return nil
}
