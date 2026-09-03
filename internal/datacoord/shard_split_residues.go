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

package datacoord

import (
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// shardResidues is a collection's routing state expressed the one way the rest
// of the split machinery needs it: a modulus, and the residues each routable
// shard owns against it.
//
// It exists because a collection that has never been split says the same thing
// implicitly — shard i of the N routable shards owns residue i at modulus N,
// which IS the legacy hash%N assignment — and every caller would otherwise have
// to branch on which of the two forms it is looking at. That equivalence is what
// lets an existing collection be split with no migration.
type shardResidues struct {
	// modulus is what every residue below is taken against. Always > 0.
	modulus uint64
	// byVChannel maps a routable vchannel to the residues it owns, sorted.
	byVChannel map[string][]uint64
}

// residuesOf reads a collection's routing state.
//
// Only the ROUTABLE shards appear. That matters for the legacy case: after an
// earlier split the vchannel list still carries the retired sources, and taking
// the modulus from its length would hand out a modulus the collection never
// routed by.
func residuesOf(collection *collectionInfo) (*shardResidues, error) {
	infos := make([]*schemapb.CollectionShardInfo, len(collection.VChannelNames))
	for i, vchannel := range collection.VChannelNames {
		infos[i] = collection.ShardInfos[vchannel]
	}
	shards, err := routing.ShardsFromMeta(collection.VChannelNames, infos)
	if err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return nil, merr.WrapErrServiceInternalMsg(
			"collection %d has no routable shard", collection.ID)
	}

	out := &shardResidues{
		modulus:    collection.RoutingModulus,
		byVChannel: make(map[string][]uint64, len(shards)),
	}

	explicit := false
	for _, shard := range shards {
		if len(shard.Buckets) > 0 {
			explicit = true
			break
		}
	}
	if !explicit {
		// Never split. The convention -- shard i owns residue i at modulus N --
		// has exactly one implementation, in the routing package, and it is keyed
		// on the order the vchannel list defines; re-deriving it here from a map
		// would produce a permutation nothing downstream can detect.
		vchannels := make([]string, 0, len(shards))
		for _, shard := range shards {
			vchannels = append(vchannels, shard.Vchannel)
		}
		modulus, legacy, err := routing.LegacyShards(vchannels)
		if err != nil {
			return nil, err
		}
		out.modulus = modulus
		for _, shard := range legacy {
			out.byVChannel[shard.Vchannel] = shard.Buckets
		}
		return out, nil
	}

	if out.modulus == 0 {
		return nil, merr.WrapErrServiceInternalMsg(
			"collection %d carries residues but no routing modulus", collection.ID)
	}
	for _, shard := range shards {
		if len(shard.Buckets) == 0 {
			// A shard with no residues among shards that have them is a hole:
			// it can never be written to and can never be split. Refuse rather
			// than plan a split against a topology that is already broken.
			return nil, merr.WrapErrServiceInternalMsg(
				"shard %q of collection %d owns no residue", shard.Vchannel, collection.ID)
		}
		residues := append([]uint64(nil), shard.Buckets...)
		sort.Slice(residues, func(i, j int) bool { return residues[i] < residues[j] })
		out.byVChannel[shard.Vchannel] = residues
	}
	return out, nil
}

// of returns the residues a shard owns.
func (r *shardResidues) of(vchannel string) ([]uint64, error) {
	residues, ok := r.byVChannel[vchannel]
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg(
			"vchannel %q is not a routable shard of its collection", vchannel)
	}
	return residues, nil
}

// ownerOf returns the vchannel owning a residue. Exactly one shard does, since
// the residues tile [0, modulus).
func (r *shardResidues) ownerOf(residue uint64) (string, bool) {
	for vchannel, residues := range r.byVChannel {
		for _, own := range residues {
			if own == residue {
				return vchannel, true
			}
		}
	}
	return "", false
}
