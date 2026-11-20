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
// Package routing is the model of how the rows of a collection are placed on
// its shards, and of how a shard split changes that placement.
//
// A row is placed by one number: the hash of its routing key modulo the
// routing modulus M of the collection. Each shard owns a set of residues
// modulo M, and the sets of all shards partition [0, M).
//
// A collection that was never split stores no residues and a modulus of zero.
// Its table has modulus N, the number of shards, and shard i owns residue i.
// This is the placement of typeutil.HashPK2Channels.
//
// A shard split replaces one shard by two new ones. The two new shards share
// the residues of the old one. If the old shard owns a single residue r, the
// modulus doubles first: r becomes {r, r+M}, and the residues of every other
// shard are expressed again at 2M. No key of any other shard moves.
//
// A shard that was split is not a shard of the routing table any more. The
// collection keeps one lineage record for it, see Lineage.
package routing

import (
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MaxModulus bounds the routing modulus, so that malformed metadata cannot
// allocate a huge table. schemapb documents the same bound.
const MaxModulus = 1 << 15

// Shard is the routing metadata of one shard: its vchannel and the residues
// it owns.
type Shard struct {
	VChannel string
	Buckets  []uint64
}

// Lineage records that a shard was split. It is written once, when the split
// is applied, and never changed.
type Lineage struct {
	// SourceVChannel is the shard that was split.
	SourceVChannel string
	// TargetVChannels are the shards that own its keys after the split.
	TargetVChannels []string
	// SwitchTimeTick is the time tick of the SplitShard message on the source
	// vchannel, in the WAL of the cluster that holds this record.
	SwitchTimeTick uint64
	// SplitTaskID is the id of the split.
	SplitTaskID int64
}

// Routing is the routing metadata of a collection.
type Routing struct {
	// Modulus is the routing modulus. Zero on a collection that was never
	// split.
	Modulus uint64
	// ShardBy is the routing key expression. Empty if it was never declared.
	ShardBy string
	// Shards are the shards that accept DML, in the order of the vchannel list
	// of the collection. Buckets are empty if and only if Modulus is zero.
	Shards []Shard
}

// Clone returns a deep copy.
func (r Routing) Clone() Routing {
	shards := make([]Shard, 0, len(r.Shards))
	for _, s := range r.Shards {
		shards = append(shards, Shard{VChannel: s.VChannel, Buckets: slices.Clone(s.Buckets)})
	}
	return Routing{Modulus: r.Modulus, ShardBy: r.ShardBy, Shards: shards}
}

// VChannels returns the vchannels of the shards, in order.
func (r Routing) VChannels() []string {
	vchannels := make([]string, 0, len(r.Shards))
	for _, s := range r.Shards {
		vchannels = append(vchannels, s.VChannel)
	}
	return vchannels
}

// Explicit returns the same routing with a non-zero modulus and with residues
// on every shard. A routing that was never split becomes modulus N with
// residue i on shard i. It fails if the routing does not tile the key space.
func (r Routing) Explicit() (Routing, error) {
	table, err := r.Table()
	if err != nil {
		return Routing{}, err
	}
	explicit := Routing{Modulus: table.modulus, ShardBy: r.ShardBy, Shards: make([]Shard, 0, len(r.Shards))}
	for _, s := range r.Shards {
		explicit.Shards = append(explicit.Shards, Shard{VChannel: s.VChannel, Buckets: table.BucketsOf(s.VChannel)})
	}
	return explicit, nil
}

// ShardInfos returns the routing as the shard infos of a message or of a
// DescribeCollection response.
func (r Routing) ShardInfos() []*schemapb.CollectionShardInfo {
	infos := make([]*schemapb.CollectionShardInfo, 0, len(r.Shards))
	for _, s := range r.Shards {
		info := &schemapb.CollectionShardInfo{
			State:        schemapb.ShardState_ShardNormal,
			VchannelName: s.VChannel,
		}
		if len(s.Buckets) > 0 {
			info.Routing = &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: slices.Clone(s.Buckets)},
			}
		}
		infos = append(infos, info)
	}
	return infos
}

// RoutingFromShardInfos builds a routing from shard infos that name their
// vchannel. Only a shard in state ShardNormal accepts DML.
//
// A shard info in state ShardSplitting is a split source as DescribeCollection
// lists it: it owns no key any more and is skipped, so a caller may pass the
// shard infos of a response as they are. Any other state is refused, because a
// routing never lists a shard that owns no key.
func RoutingFromShardInfos(modulus uint64, shardBy string, infos []*schemapb.CollectionShardInfo) (Routing, error) {
	r := Routing{Modulus: modulus, ShardBy: shardBy, Shards: make([]Shard, 0, len(infos))}
	for i, info := range infos {
		if info.GetVchannelName() == "" {
			return Routing{}, merr.WrapErrServiceInternalMsg("routing shard info %d carries no vchannel name", i)
		}
		if info.GetState() == schemapb.ShardState_ShardSplitting {
			continue
		}
		if info.GetState() != schemapb.ShardState_ShardNormal {
			return Routing{}, merr.WrapErrServiceInternalMsg("routing shard %s is in state %s, only a normal shard owns keys",
				info.GetVchannelName(), info.GetState().String())
		}
		r.Shards = append(r.Shards, Shard{
			VChannel: info.GetVchannelName(),
			Buckets:  slices.Clone(info.GetHashRouting().GetBuckets()),
		})
	}
	return r, nil
}

// Table is a validated assignment of residues to shards.
type Table struct {
	modulus uint64
	// slots[r] is the vchannel that owns residue r. Every slot is filled.
	slots []string
}

// Table validates the routing and builds its table. Every residue below the
// modulus must be owned by exactly one shard. A gap or an overlap is an error,
// never a silent misroute.
//
// The modulus says whether the collection was split, not the presence of
// residues. With a modulus of zero no shard may carry residues, and the table
// is the legacy one. With a non-zero modulus every shard must carry residues.
func (r Routing) Table() (*Table, error) {
	if len(r.Shards) == 0 {
		return nil, merr.WrapErrServiceInternal("a routing needs at least one shard")
	}
	seen := make(map[string]struct{}, len(r.Shards))
	for i, s := range r.Shards {
		if s.VChannel == "" {
			return nil, merr.WrapErrServiceInternalMsg("routing shard %d carries no vchannel name", i)
		}
		if _, ok := seen[s.VChannel]; ok {
			return nil, merr.WrapErrServiceInternalMsg("routing shard %s is listed twice", s.VChannel)
		}
		seen[s.VChannel] = struct{}{}
	}

	if r.Modulus == 0 {
		for _, s := range r.Shards {
			if len(s.Buckets) > 0 {
				return nil, merr.WrapErrServiceInternalMsg("shard %s carries residues but the collection has no routing modulus", s.VChannel)
			}
		}
		return &Table{modulus: uint64(len(r.Shards)), slots: r.VChannels()}, nil
	}
	if r.Modulus > MaxModulus {
		return nil, merr.WrapErrServiceInternalMsg("routing modulus %d exceeds the bound %d", r.Modulus, MaxModulus)
	}

	slots := make([]string, r.Modulus)
	for _, s := range r.Shards {
		if len(s.Buckets) == 0 {
			return nil, merr.WrapErrServiceInternalMsg("shard %s owns no residue at routing modulus %d", s.VChannel, r.Modulus)
		}
		for _, bucket := range s.Buckets {
			if bucket >= r.Modulus {
				return nil, merr.WrapErrServiceInternalMsg("shard %s owns residue %d, which is not below the routing modulus %d",
					s.VChannel, bucket, r.Modulus)
			}
			if owner := slots[bucket]; owner != "" {
				return nil, merr.WrapErrServiceInternalMsg("residue %d (mod %d) is owned by shard %s and by shard %s",
					bucket, r.Modulus, owner, s.VChannel)
			}
			slots[bucket] = s.VChannel
		}
	}
	for bucket, owner := range slots {
		if owner == "" {
			return nil, merr.WrapErrServiceInternalMsg("residue %d (mod %d) is owned by no shard", bucket, r.Modulus)
		}
	}
	return &Table{modulus: r.Modulus, slots: slots}, nil
}

// Modulus returns the modulus of the table. It is never zero.
func (t *Table) Modulus() uint64 {
	return t.modulus
}

// Lookup returns the vchannel that owns a residue. It reports false for a
// residue that is not below the modulus.
func (t *Table) Lookup(residue uint64) (string, bool) {
	if residue >= t.modulus {
		return "", false
	}
	return t.slots[residue], true
}

// BucketsOf returns the residues a vchannel owns, in increasing order.
func (t *Table) BucketsOf(vchannel string) []uint64 {
	var buckets []uint64
	for residue, owner := range t.slots {
		if owner == vchannel {
			buckets = append(buckets, uint64(residue))
		}
	}
	return buckets
}
