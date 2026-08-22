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

package routing

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Table is the single routing entry point: it maps one write (a namespace, or a
// primary key) to the vchannel that owns it, whatever routing rule the
// collection uses.
//
// A collection's shards each carry a routing predicate in their meta
// (schemapb.CollectionShardInfo.routing):
//
//   - none — legacy collections with no predicate: plain hash(pk) % shardNum;
//   - RangeRouting — namespace collections, a byte-comparable key range per
//     shard, refined by a namespace shard split;
//   - HashRouting — primary-key collections after a hash split, a
//     hash(pk) % modulus == remainder bucket per shard.
//
// Derive builds the right internal table from the meta, so callers do not
// branch on routing mode; they build a Table once per routing version and call
// Route.
type Table struct {
	mode schemapb.RoutingMode

	// Exactly one of these is non-nil, matching mode.
	rangeTable *RangeRoutingTable
	hashTable  *HashRoutingTable

	// channels is kept for the legacy path (no predicate in meta) and for
	// NumShards.
	channels []string
}

// Shard is one shard's routing meta as Derive consumes it: the vchannel plus
// the predicate read from its CollectionShardInfo.
type Shard struct {
	Vchannel string
	// Ranges is set for range routing (a shard may own more than one disjoint
	// range after a carve-out).
	Ranges []*schemapb.RoutingKeyRange
	// Buckets is set for hash routing.
	Buckets []HashBucket
}

// Derive builds the routing table of a collection from its routing mode and the
// per-shard routing meta.
//
// Shards must already be filtered to the ones that currently accept writes: the
// caller drops a fenced split source and a released one, whose key space the
// split targets have taken over. Derive then validates that what remains covers
// the key space exactly — a gap or an overlap is an error, never a silent
// mis-route.
//
// A nil, error-free result means "no explicit routing meta": the collection
// routes by the legacy hash(pk) % len(channels), and Route falls back to it.
func Derive(mode schemapb.RoutingMode, channels []string, shards []Shard) (*Table, error) {
	t := &Table{mode: mode, channels: append([]string(nil), channels...)}

	switch mode {
	case schemapb.RoutingMode_RoutingModeRange:
		ranges := make([]RangeShard, 0, len(shards))
		for _, s := range shards {
			for _, r := range s.Ranges {
				ranges = append(ranges, RangeShard{
					Lower:    r.GetLower(),
					Upper:    r.GetUpper(),
					Vchannel: s.Vchannel,
				})
			}
		}
		rt, err := DeriveRange(ranges)
		if err != nil {
			return nil, err
		}
		t.rangeTable = rt
		return t, nil

	case schemapb.RoutingMode_RoutingModeHash:
		// A hash-routed collection carries explicit buckets only after its first
		// split; before that its meta has no predicate and the legacy modulo
		// applies unchanged.
		if !hasHashPredicate(shards) {
			return t, nil
		}
		hashShards := make([]HashShard, 0, len(shards))
		for _, s := range shards {
			hashShards = append(hashShards, HashShard{
				Vchannel: s.Vchannel,
				Buckets:  s.Buckets,
			})
		}
		ht, err := DeriveHash(hashShards)
		if err != nil {
			return nil, err
		}
		t.hashTable = ht
		return t, nil

	default:
		return nil, errors.Newf("unsupported routing mode %v", mode)
	}
}

// hasHashPredicate reports whether any shard carries an explicit hash
// predicate, i.e. whether the collection has been split at least once.
func hasHashPredicate(shards []Shard) bool {
	for _, s := range shards {
		if len(s.Buckets) > 0 {
			return true
		}
	}
	return false
}

// Key is one write's routing input. Exactly one of its forms is meaningful per
// collection: a namespace collection routes by Namespace, a primary-key
// collection by the pk's Hash.
type Key struct {
	// Namespace is the routing key of a namespace collection.
	Namespace string
	// Hash is the primary key's hash, for hash-routed collections.
	Hash uint64
}

// Route returns the vchannel owning the given key, or an error when the key
// resolves to no shard (which means the routing meta is inconsistent, since a
// valid table tiles the key space).
func (t *Table) Route(key Key) (string, error) {
	if t == nil {
		// A nil table is how a caller carries "the routing meta was malformed
		// and I refused to derive it". Answering with an error rather than
		// panicking keeps that a rejected write instead of a crashed proxy.
		return "", errors.New("no routing table")
	}
	switch {
	case t.rangeTable != nil:
		vchannel := t.rangeTable.LookupNamespace(key.Namespace)
		if vchannel == "" {
			return "", errors.Newf("namespace %q resolved to no shard", key.Namespace)
		}
		return vchannel, nil

	case t.hashTable != nil:
		vchannel := t.hashTable.Lookup(key.Hash)
		if vchannel == "" {
			return "", errors.Newf("hash %d resolved to no shard", key.Hash)
		}
		return vchannel, nil

	default:
		// Legacy: hash(pk) % shardNum over the channel list.
		if len(t.channels) == 0 {
			return "", errors.New("routing table has no channels")
		}
		return t.channels[key.Hash%uint64(len(t.channels))], nil
	}
}

// LookupNamespace returns the vchannel owning a namespace, or "" when it
// resolves to none. A convenience wrapper over Route for the namespace case,
// where callers that only need the channel do not want to unwrap an error.
func (t *Table) LookupNamespace(namespace string) string {
	vchannel, err := t.Route(Key{Namespace: namespace})
	if err != nil {
		return ""
	}
	return vchannel
}

// Mode returns the collection's routing mode.
func (t *Table) Mode() schemapb.RoutingMode { return t.mode }

// IsExplicitHash reports whether the table routes primary keys by explicit hash
// buckets, i.e. whether the collection has been split at least once. Nil-safe,
// so a caller holding a collection with no routing meta need not branch.
func (t *Table) IsExplicitHash() bool { return t != nil && t.hashTable != nil }

// IsExplicit reports whether the table routes by an explicit per-shard
// predicate rather than by the legacy modulo. Callers that only need to know
// "must I narrow the channel set?" test this.
func (t *Table) IsExplicit() bool { return t.rangeTable != nil || t.hashTable != nil }

// NumShards returns the number of shards the collection routes over.
func (t *Table) NumShards() int {
	switch {
	case t.rangeTable != nil:
		return t.rangeTable.NumShards()
	default:
		return len(t.channels)
	}
}

// ShardsFromMeta converts the per-shard routing meta of a DescribeCollection
// response into Derive's input, keeping only the shards that currently accept
// writes.
//
// ShardNormal (serving) and ShardCreating (a split target, already created and
// writable) participate; the fenced split source (ShardSplitting) and the
// released one (ShardDropped) are excluded, because their key space now belongs
// to the targets. Excluding them is what keeps the remainder an exact cover.
func ShardsFromMeta(vchannels []string, infos []*schemapb.CollectionShardInfo) ([]Shard, error) {
	if len(infos) != len(vchannels) {
		return nil, errors.Newf("routing shard info count %d mismatches vchannel count %d",
			len(infos), len(vchannels))
	}
	shards := make([]Shard, 0, len(vchannels))
	for i, vchannel := range vchannels {
		switch infos[i].GetState() {
		case schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardCreating:
		default:
			continue
		}
		shard := Shard{
			Vchannel: vchannel,
			Ranges:   infos[i].GetRangeRouting().GetRanges(),
		}
		for _, b := range infos[i].GetHashRouting().GetBuckets() {
			shard.Buckets = append(shard.Buckets, HashBucket{
				Modulus:   b.GetModulus(),
				Remainder: b.GetRemainder(),
			})
		}
		shards = append(shards, shard)
	}
	return shards, nil
}

// rawHashes returns each primary key's raw hash — the same value the split's
// rewrite partitioner hashes with, so a key the write path sends to a shard is
// the key the rewrite would put there. Nil for an unsupported id type, matching
// typeutil.HashPK2Channels.
//
// The width matters: the hash is a uint32 widened to uint64, NOT reduced modulo
// anything. Reducing first (as the legacy path does, by the shard count) would
// destroy the bits the buckets are cut on.
func rawHashes(pks *schemapb.IDs) []uint64 {
	var out []uint64
	switch pks.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		for _, pk := range pks.GetIntId().GetData() {
			h, err := typeutil.Hash32Int64(pk)
			if err != nil {
				return nil
			}
			out = append(out, uint64(h))
		}
	case *schemapb.IDs_StrId:
		for _, pk := range pks.GetStrId().GetData() {
			out = append(out, uint64(typeutil.HashString2Uint32(pk)))
		}
	}
	return out
}

// RouteInsert maps a batch of primary keys to the vchannels owning them,
// returning vchannel -> row offsets and the per-row shard index that
// InsertMsg.HashValues carries.
//
// A collection that carries no explicit predicate routes by the legacy
// hash(pk) % len(channels), bit for bit. One that does — a primary-key
// collection that has been split — routes by its buckets, and MUST: after a
// doubling the surviving shard owns {2,1} while the two new ones own {4,0} and
// {4,2}, and no modulo over a channel list reproduces that. Routing such a
// collection by position sends keys to a shard that does not own them, and
// sends some of them to a fenced split source, which rejects the write.
//
// An unowned hash is an error rather than a guess: a table derived by Derive
// tiles the key space, so a miss means the routing meta is inconsistent and
// placing the row anywhere would corrupt the collection quietly.
// channels is the channel set the caller resolved for this write; it is what
// the legacy modulo is taken over, so a caller that narrowed it keeps that
// behaviour exactly.
func (t *Table) RouteInsert(pks *schemapb.IDs, channels []string) (map[string][]int, []uint32, error) {
	if t == nil || t.hashTable == nil {
		// Legacy modulo, including the range-routed case: a namespace
		// collection routes its inserts by namespace, not by primary key, and
		// never reaches here with an explicit table.
		offsets, hashValues := DeriveCompat(channels).RouteInsert(pks)
		return offsets, hashValues, nil
	}

	hashes := rawHashes(pks)
	if len(hashes) == 0 {
		return nil, nil, nil
	}
	// The shard index is still reported modulo the channel count so the field
	// keeps its shape for consumers that only use it as an opaque tag; the
	// placement below does not depend on it.
	numChannels := len(channels)
	offsets := make(map[string][]int, numChannels)
	hashValues := make([]uint32, 0, len(hashes))
	for i, hash := range hashes {
		vchannel, ok := t.hashTable.LookupOK(hash)
		if !ok {
			return nil, nil, errors.Newf("hash %d resolved to no shard", hash)
		}
		offsets[vchannel] = append(offsets[vchannel], i)
		if numChannels > 0 {
			hashValues = append(hashValues, uint32(hash%uint64(numChannels)))
		} else {
			hashValues = append(hashValues, 0)
		}
	}
	return offsets, hashValues, nil
}

// RouteDelete maps a batch of primary keys to the index, within channels, of
// the vchannel owning each one — the form the delete repacker consumes.
//
// Same rule as RouteInsert: an explicit table decides, and a collection without
// one keeps the legacy modulo bit for bit. A delete that went to the wrong
// shard would not delete anything, and one that went to a fenced split source
// would be rejected outright.
func (t *Table) RouteDelete(pks *schemapb.IDs, channels []string) ([]uint32, error) {
	if !t.IsExplicitHash() {
		return DeriveCompat(channels).HashPKs(pks), nil
	}
	index := make(map[string]uint32, len(channels))
	for i, channel := range channels {
		index[channel] = uint32(i)
	}
	hashes := rawHashes(pks)
	out := make([]uint32, 0, len(hashes))
	for _, hash := range hashes {
		vchannel, ok := t.hashTable.LookupOK(hash)
		if !ok {
			return nil, errors.Newf("hash %d resolved to no shard", hash)
		}
		position, ok := index[vchannel]
		if !ok {
			// The routing table names a shard the caller did not resolve — the
			// two views of the topology disagree, and guessing an index would
			// send the tombstone to an unrelated shard.
			return nil, errors.Newf("shard %q owns the key but is not in the request's channel set", vchannel)
		}
		out = append(out, position)
	}
	return out, nil
}
