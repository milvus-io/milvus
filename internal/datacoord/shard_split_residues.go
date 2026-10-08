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
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The residue arithmetic of a primary-key shard split (design doc §3.1).
//
// A collection carries one routing modulus M and every writable shard owns a
// set of residues modulo M; the sets tile [0, M). A split halves one shard's
// set. When the shard still owns several residues the set is divided and M
// does not move. A shard down to a single residue r has nothing left to divide:
// M doubles and r becomes {r} and {r+M}, the same keys cut on one more hash
// bit. A doubling is collection-wide: every shard the split leaves alone is
// re-expressed at the new modulus in the same commit, or half of its keys
// would route nowhere.
//
// A never-split collection is the same thing implicitly -- M is its vchannel
// count and shard i owns residue i -- which is exactly how routing.TableFromMeta
// reads it, so the first split of a never-split shard is an ordinary doubling.

// shardResidues is a collection's routing state as the split planner needs it:
// the modulus, and the residues each writable shard owns against it.
type shardResidues struct {
	// modulus is what every residue below is taken against. Always > 0.
	modulus uint64
	// byVChannel maps a writable vchannel to the residues it owns, sorted.
	byVChannel map[string][]uint64
}

// residuesOf reads a collection's routing state through the one derivation the
// write path uses (routing.TableFromMeta), so the planner cannot see a
// topology the proxy would route differently. A topology that does not tile the
// key space is refused.
func residuesOf(coll *model.Collection) (*shardResidues, error) {
	table, err := routing.TableFromMeta(coll.VirtualChannelNames, shardInfosOf(coll), coll.RoutingModulus)
	if err != nil {
		return nil, merr.Wrapf(err, "read the routing of collection %d", coll.CollectionID)
	}
	out := &shardResidues{modulus: table.Modulus(), byVChannel: make(map[string][]uint64)}
	for r := uint64(0); r < table.Modulus(); r++ {
		// Derive refuses a gap, so every residue has an owner.
		owner, _ := table.Lookup(r)
		out.byVChannel[owner] = append(out.byVChannel[owner], r)
	}
	return out, nil
}

// shardInfosOf lists the collection's shard infos parallel to its vchannels. A
// vchannel without one is a legacy Normal shard.
func shardInfosOf(coll *model.Collection) []*schemapb.CollectionShardInfo {
	infos := make([]*schemapb.CollectionShardInfo, len(coll.VirtualChannelNames))
	for i, vchannel := range coll.VirtualChannelNames {
		if info, ok := coll.ShardInfos[vchannel]; ok && info != nil {
			infos[i] = info.ToPB()
			infos[i].VchannelName = vchannel
			continue
		}
		infos[i] = &schemapb.CollectionShardInfo{VchannelName: vchannel}
	}
	return infos
}

// of returns the residues a writable shard owns.
func (r *shardResidues) of(vchannel string) ([]uint64, error) {
	residues, ok := r.byVChannel[vchannel]
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("vchannel %s is not a writable shard of its collection", vchannel)
	}
	return residues, nil
}

// ownerOf returns the vchannel owning a residue.
func (r *shardResidues) ownerOf(residue uint64) (string, bool) {
	for vchannel, residues := range r.byVChannel {
		if slices.Contains(residues, residue) {
			return vchannel, true
		}
	}
	return "", false
}

// maxSplitModulus mirrors the routing package's cap (schemapb's "the modulus
// stays under 2^15"): a split that would double past it is refused before
// anything is planned, rather than by the builder after allocation.
const maxSplitModulus = 1 << 15

// planSplitResidues halves the residues own a shard owns at modulus, returning
// the two halves and the collection's modulus after the split.
//
// A set of several residues is cut in the middle of its sorted order and the
// modulus stays; an odd set leaves the extra residue on the left. The cut is
// deterministic, so a retried plan is the plan it retries. A single residue r
// doubles the modulus into {r} and {r+modulus}.
func planSplitResidues(modulus uint64, own []uint64) (left, right []uint64, after uint64, err error) {
	if modulus == 0 || modulus > maxSplitModulus {
		return nil, nil, 0, merr.WrapErrServiceInternalMsg("cannot split a shard at routing modulus %d", modulus)
	}
	if len(own) == 0 {
		return nil, nil, 0, merr.WrapErrServiceInternalMsg("cannot split a shard that owns no residue")
	}
	sorted := slices.Clone(own)
	slices.Sort(sorted)
	for i, r := range sorted {
		if r >= modulus || (i > 0 && sorted[i-1] == r) {
			return nil, nil, 0, merr.WrapErrServiceInternalMsg("cannot split a shard owning residues %v at modulus %d", own, modulus)
		}
	}
	if len(sorted) == 1 {
		if modulus*2 > maxSplitModulus {
			return nil, nil, 0, merr.WrapErrServiceInternalMsg(
				"cannot split the single residue %d of a shard: doubling modulus %d exceeds the cap %d",
				sorted[0], modulus, maxSplitModulus)
		}
		return []uint64{sorted[0]}, []uint64{sorted[0] + modulus}, modulus * 2, nil
	}
	half := (len(sorted) + 1) / 2
	return sorted[:half:half], sorted[half:], modulus, nil
}

// rebaseResidues re-expresses residues taken against from as their equivalent
// against to, a multiple of from: residue r covers r, r+from, ... below to.
// Sorted.
func rebaseResidues(residues []uint64, from, to uint64) ([]uint64, error) {
	if from == 0 || to == 0 || to%from != 0 {
		return nil, merr.WrapErrServiceInternalMsg("cannot rebase residues from modulus %d onto %d", from, to)
	}
	out := make([]uint64, 0, uint64(len(residues))*(to/from))
	for _, r := range residues {
		for v := r; v < to; v += from {
			out = append(out, v)
		}
	}
	slices.Sort(out)
	return out, nil
}
