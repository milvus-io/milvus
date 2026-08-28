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

// Table is the single routing entry point: it maps one write to the vchannel
// that owns it.
//
// Every collection routes the same way — the hash of its routing key, modulo the
// collection's routing modulus. What differs is only which value gets hashed,
// which the collection's shard_by expression names; by the time a write reaches
// this package that value is already a hash, so the table has one rule.
//
// A collection that has never been split carries no routing in its meta, and the
// table falls back to hash % len(channels) — the legacy placement, bit for bit.
// After the first split each shard carries the residues it owns, and those
// decide.
type Table struct {
	// hashTable is nil for a collection with no routing meta, i.e. one that has
	// never been split.
	hashTable *HashRoutingTable

	// channels is the collection's vchannel list, used by the legacy path and by
	// NumShards.
	channels []string
}

// Shard is one shard's routing meta as Derive consumes it: the vchannel plus the
// residues read from its CollectionShardInfo.
type Shard struct {
	Vchannel string
	Buckets  []uint64
}

// Derive builds the routing table of a collection from its routing modulus and
// per-shard residues.
//
// Shards must already be filtered to the ones that currently accept writes: the
// caller drops a fenced split source and a released one, whose key space the
// split targets have taken over. Derive then validates that what remains covers
// the key space exactly — a gap or an overlap is an error, never a silent
// mis-route.
//
// A collection with no residues in its meta has never been split; Derive returns
// a table that routes by the legacy modulo over channels.
func Derive(modulus uint64, channels []string, shards []Shard) (*Table, error) {
	t := &Table{channels: append([]string(nil), channels...)}
	if !hasRouting(shards) {
		return t, nil
	}

	hashShards := make([]HashShard, 0, len(shards))
	for _, s := range shards {
		hashShards = append(hashShards, HashShard{Vchannel: s.Vchannel, Buckets: s.Buckets})
	}
	ht, err := DeriveHash(modulus, hashShards)
	if err != nil {
		return nil, err
	}
	t.hashTable = ht
	return t, nil
}

// hasRouting reports whether any shard carries residues, i.e. whether the
// collection has been split at least once.
func hasRouting(shards []Shard) bool {
	for _, s := range shards {
		if len(s.Buckets) > 0 {
			return true
		}
	}
	return false
}

// Route returns the vchannel owning the given routing-key hash, or an error when
// the hash resolves to no shard — which means the routing meta is inconsistent,
// since a valid table tiles the key space.
func (t *Table) Route(hash uint64) (string, error) {
	if t == nil {
		// A nil table is how a caller carries "the routing meta was malformed and
		// I refused to derive it". Answering with an error rather than panicking
		// keeps that a rejected write instead of a crashed proxy.
		return "", errors.New("no routing table")
	}
	if t.hashTable != nil {
		vchannel, ok := t.hashTable.LookupOK(hash)
		if !ok {
			return "", errors.Newf("hash %d resolved to no shard", hash)
		}
		return vchannel, nil
	}
	if len(t.channels) == 0 {
		return "", errors.New("routing table has no channels")
	}
	return t.channels[hash%uint64(len(t.channels))], nil
}

// IsExplicit reports whether the table routes by the per-shard residues in the
// meta rather than by the legacy modulo, i.e. whether the collection has been
// split at least once. Nil-safe, so a caller holding a collection with no
// routing meta need not branch.
func (t *Table) IsExplicit() bool { return t != nil && t.hashTable != nil }

// NumShards returns the number of shards the collection routes over.
func (t *Table) NumShards() int { return len(t.channels) }

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
		shards = append(shards, Shard{
			Vchannel: vchannel,
			Buckets:  infos[i].GetHashRouting().GetBuckets(),
		})
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
// destroy the bits the residues are cut on.
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
// A collection that carries no routing meta routes by the legacy
// hash(pk) % len(channels), bit for bit. One that does — a collection that has
// been split — routes by its residues, and MUST: after a doubling the surviving
// shard owns {1} at modulus 2 while the two new ones own {0} and {2} at modulus
// 4, and no modulo over a channel list reproduces that. Routing such a
// collection by position sends keys to a shard that does not own them, and sends
// some of them to a fenced split source, which rejects the write.
//
// An unowned hash is an error rather than a guess: a table derived by Derive
// tiles the key space, so a miss means the routing meta is inconsistent and
// placing the row anywhere would corrupt the collection quietly.
//
// channels is the channel set the caller resolved for this write; it is what the
// legacy modulo is taken over, so a caller that narrowed it keeps that behaviour
// exactly.
func (t *Table) RouteInsert(pks *schemapb.IDs, channels []string) (map[string][]int, []uint32, error) {
	if !t.IsExplicit() {
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

// RouteDelete maps a batch of primary keys to the index, within channels, of the
// vchannel owning each one — the form the delete repacker consumes.
//
// Same rule as RouteInsert: routing meta decides, and a collection without it
// keeps the legacy modulo bit for bit. A delete that went to the wrong shard
// would not delete anything, and one that went to a fenced split source would be
// rejected outright.
func (t *Table) RouteDelete(pks *schemapb.IDs, channels []string) ([]uint32, error) {
	if !t.IsExplicit() {
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
			// The routing table names a shard the caller did not resolve — the two
			// views of the topology disagree, and guessing an index would send the
			// tombstone to an unrelated shard.
			return nil, errors.Newf("shard %q owns the key but is not in the request's channel set", vchannel)
		}
		out = append(out, position)
	}
	return out, nil
}
