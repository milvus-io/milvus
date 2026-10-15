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

// Package routing validates a collection's shard routing meta.
//
// A row is placed by one number, its routing value, taken modulo the
// collection's routing modulus; the shard owning that residue owns the row. A
// shard split rewrites that ownership, so every routing post-image it commits
// must still tile the key space: every residue below the modulus owned by
// exactly one writable shard. ShardsFromMeta and Derive are that check.
//
// A collection that has never been split carries no residues and a modulus of
// zero. It is not a second rule: its table is the residue table with one
// residue per shard in vchannel order, which is the legacy hash % shardNum
// placement bit for bit.
//
// The write path that routes rows by residue is not on this branch; it lands
// with the proxy's residue routing (design doc §11).
package routing

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Shard is one shard's routing meta as Derive consumes it: the vchannel plus the
// residues read from its CollectionShardInfo.
type Shard struct {
	Vchannel string
	Buckets  []uint64
}

// Derive builds the residue table of a collection from its routing modulus and
// per-shard residues, refusing a topology that does not tile the key space.
//
// Shards must already be filtered to the ones that currently accept writes, which
// is what ShardsFromMeta does: a fenced split source and a released one own no
// keys, and their key space belongs to the targets. Derive then validates that
// what remains covers the key space exactly — a gap or an overlap is an error,
// never a silent mis-route.
//
// The modulus is what says whether the collection has been split, not the
// presence of residues. Zero means never split, and the table is built from the
// channel order; non-zero means the meta must say which residues each shard owns,
// and a topology that does not is rejected rather than quietly downgraded to the
// legacy modulo — which, over a vchannel list that a split has already grown,
// would re-place every row in the collection.
func Derive(modulus uint64, channels []string, shards []Shard) (*ResidueTable, error) {
	if len(channels) == 0 {
		// The legacy rule divides by the channel count and the explicit rule
		// indexes into it; neither has an answer here.
		return nil, merr.WrapErrServiceInternal("routing table needs at least one vchannel")
	}

	explicit := false
	for _, s := range shards {
		if len(s.Buckets) > 0 {
			explicit = true
			break
		}
	}

	switch {
	case modulus == 0 && explicit:
		return nil, merr.WrapErrServiceInternal("shards carry residues but the collection reports no routing modulus")
	case modulus != 0 && !explicit:
		return nil, merr.WrapErrServiceInternalMsg(
			"collection reports routing modulus %d but no shard carries a residue", modulus)
	case modulus == 0:
		// The legacy rule is "shard i owns residue i at modulus len(channels)",
		// so a shorter shard list means the caller declared some vchannel
		// non-writable while the collection reports no modulus -- meta that
		// cannot be produced by a real split, since retiring a shard is what
		// writes residues in the first place. Deriving anyway would route the
		// excluded shard's residue straight back to it. A caller with no shard
		// info at all is the ordinary never-split case and is left alone.
		if len(shards) != 0 && len(shards) != len(channels) {
			return nil, merr.WrapErrServiceInternalMsg(
				"collection reports no routing modulus but only %d of %d vchannels own keys",
				len(shards), len(channels))
		}
		// Same length is not the same set. A shard list that names a vchannel
		// the collection does not carry is malformed on this branch exactly as
		// on the explicit one; deriving from the channel list alone would just
		// hide it.
		if len(shards) != 0 {
			if err := refuseShardsOutside(channels, shards); err != nil {
				return nil, err
			}
		}
		return deriveCompat(channels)
	}

	// A shard the collection's own channel list does not carry cannot be routed
	// to by anyone, and no meta refresh changes that: refused, non-retriably.
	if err := refuseShardsOutside(channels, shards); err != nil {
		return nil, err
	}
	return deriveHash(modulus, shards)
}

// refuseShardsOutside rejects, non-retriably, a shard whose vchannel the
// collection does not carry, and a vchannel listed twice.
func refuseShardsOutside(channels []string, shards []Shard) error {
	known := make(map[string]struct{}, len(channels))
	for _, ch := range channels {
		known[ch] = struct{}{}
	}
	seen := make(map[string]struct{}, len(shards))
	for _, s := range shards {
		if _, ok := known[s.Vchannel]; !ok {
			return merr.WrapErrServiceInternalMsg(
				"routing shard %q is not in the collection's vchannel list", s.Vchannel)
		}
		// A subset of the same length is not the same set: [v0, v0] against
		// [v0, v1] would pass the length check on the compat branch and on the
		// explicit branch would let one vchannel claim two residue sets under
		// two entries.
		if _, dup := seen[s.Vchannel]; dup {
			return merr.WrapErrServiceInternalMsg(
				"routing shard %q appears twice in the collection's shard list", s.Vchannel)
		}
		seen[s.Vchannel] = struct{}{}
	}
	return nil
}

// ShardsFromMeta converts the per-shard routing meta of a collection into
// Derive's input, keeping only the shards that currently accept writes.
//
// ShardNormal (serving) and ShardCreating (a split target, already created and
// writable) participate; the fenced split source (ShardSplitting) and the
// released one (ShardDropped) are excluded, because their key space now belongs
// to the targets. Excluding them is what keeps the remainder an exact cover.
//
// A state this build does not know may own keys, so it fails rather than being
// dropped: dropping it silently re-routes whatever it owned, and mapping it onto
// the zero value would make a shard that takes no writes look like one that does.
func ShardsFromMeta(vchannels []string, infos []*schemapb.CollectionShardInfo) ([]Shard, error) {
	if len(infos) == 0 {
		// Not a mismatch: this is the never-split shape. The proto documents an
		// empty shard_infos as "the peer predates shard split". Derive already
		// reads no shard info as the legacy assignment; answer the same way here.
		return nil, nil
	}
	if len(infos) != len(vchannels) {
		return nil, merr.WrapErrServiceInternalMsg("routing shard info count %d mismatches vchannel count %d",
			len(infos), len(vchannels))
	}
	shards := make([]Shard, 0, len(vchannels))
	for i, vchannel := range vchannels {
		// The two arrays are parallel by convention, and nothing downstream can
		// catch a violation: a PERMUTED infos still tiles [0, M) exactly, so
		// Derive accepts it and every residue ends up bound to a shard that does
		// not own it. The name is the only thing that can detect it, so check it
		// wherever the producer set it. An empty name is tolerated: it is the
		// persisted shape of a collection created before the field existed.
		if name := infos[i].GetVchannelName(); name != "" && name != vchannel {
			return nil, merr.WrapErrServiceInternalMsg(
				"routing shard info %d names vchannel %q but the vchannel list has %q at that position",
				i, name, vchannel)
		}
		switch state := infos[i].GetState(); state {
		case schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardCreating:
			// A Creating shard is admitted because a split's targets are
			// write-routable from the routing commit onward, before they are
			// serviceable for reads. That relies on an invariant the commit
			// owns: a Creating entry is published WITH its residues, in the same
			// transaction. One published without them makes Derive refuse the
			// whole table.
			shards = append(shards, Shard{
				Vchannel: vchannel,
				Buckets:  infos[i].GetHashRouting().GetBuckets(),
			})
		case schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardDropped:
			// Owns no keys; the targets carved from it own them now.
		default:
			return nil, merr.WrapErrServiceInternalMsg("shard %q reports shard state %d, which this build does not know",
				vchannel, int32(state))
		}
	}
	return shards, nil
}
