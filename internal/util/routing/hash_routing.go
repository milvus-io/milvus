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
	"sort"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// HashShard is one shard's ownership: its vchannel and the residues it owns,
// taken modulo the collection's routing modulus.
type HashShard struct {
	Vchannel string
	Buckets  []uint64
}

// ResidueTable resolves a routing value to the vchannel owning it.
//
// A collection carries one routing modulus M, and each shard owns a set of
// residues modulo M. The sets partition [0, M), so a lookup is a single array
// index:
//
//	route(value) = slots[value % M]
//
// M is not the shard count. A never-split N-shard collection is M = N with one
// residue per shard, which is exactly the legacy hash%N placement; afterwards a
// split either divides a residue set at the current M or, when the shard owns a
// single residue, doubles M (see PlanSplit). The table is rebuilt from the
// collection meta whenever the routing changes.
//
// Named for what it holds rather than how it is looked up, because
// pkg/common.HashRoutingTable is a different, mutable structure that routes by
// a modulo over a live member list.
type ResidueTable struct {
	// modulus is M. Always > 0 in a valid table.
	modulus uint64
	// slots[r] is the vchannel owning the values whose residue modulo M is r. A
	// table from DeriveHash has every slot filled — it rejects a shard set
	// leaving one uncovered. A table from DeriveHashPartial may leave slots
	// empty, for a shard set that deliberately covers only part of the space.
	slots []string
}

// DeriveHash builds the residue table of a collection.
//
// It validates that the shards tile the value space exactly: every residue below
// modulus must be claimed by exactly one shard. A gap (some value routes nowhere)
// or an overlap (some value routes to two shards) is rejected, so malformed
// routing meta fails loudly instead of silently mis-placing writes.
func DeriveHash(modulus uint64, shards []HashShard) (*ResidueTable, error) {
	table, err := DeriveHashPartial(modulus, shards)
	if err != nil {
		return nil, err
	}
	for r, vchannel := range table.slots {
		if vchannel == "" {
			return nil, merr.WrapErrServiceInternalMsg("hash routing gap: residue %d (mod %d) is unowned", r, modulus)
		}
	}
	return table, nil
}

// DeriveHashPartial builds a table from shards that need only be mutually
// disjoint, not a cover of the whole value space. Residues no shard claims stay
// unowned, and LookupOK reports them as such.
//
// This is the shape a shard split's targets have while the split is in flight:
// the two targets tile exactly the values of their source and deliberately claim
// nothing else. Rejecting that as a gap — which a whole-space cover requires —
// would be wrong; what must still be rejected is an overlap, which would send
// one value to two shards.
func DeriveHashPartial(modulus uint64, shards []HashShard) (*ResidueTable, error) {
	if err := checkModulus(modulus); err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return nil, merr.WrapErrServiceInternal("hash routing table needs at least one shard")
	}

	slots := make([]string, modulus)
	for _, s := range shards {
		if s.Vchannel == "" {
			// The empty name is the marker for "this residue is unowned", so a
			// shard carrying it would be indistinguishable from a gap: its
			// residues would read back as unowned and every other shard's claim
			// on them would go unchecked.
			return nil, merr.WrapErrServiceInternal("a routing shard carries no vchannel name")
		}
		if len(s.Buckets) == 0 {
			// A shard owning no residue can never be written to and can never be
			// split, but still counts as a shard everywhere else — it is a silent
			// dead shard, not a harmless one. Reject it here so the meta that
			// produced it is fixed instead of being served.
			return nil, merr.WrapErrServiceInternalMsg("shard %q owns no residue", s.Vchannel)
		}
		for _, r := range s.Buckets {
			if r >= modulus {
				return nil, merr.WrapErrServiceInternalMsg("shard %q owns residue %d, which is not below the modulus %d",
					s.Vchannel, r, modulus)
			}
			if owner := slots[r]; owner != "" {
				// Distinguish the two shapes: one shard listing a residue twice is
				// a malformed shard entry, while two shards claiming it is a
				// malformed topology. Reporting both as `"a" and "a"` sends the
				// reader looking for a second shard that does not exist.
				if owner == s.Vchannel {
					return nil, merr.WrapErrServiceInternalMsg("shard %q lists residue %d (mod %d) twice",
						s.Vchannel, r, modulus)
				}
				return nil, merr.WrapErrServiceInternalMsg("hash routing overlap at residue %d (mod %d): shards %q and %q",
					r, modulus, owner, s.Vchannel)
			}
			slots[r] = s.Vchannel
		}
	}
	return &ResidueTable{modulus: modulus, slots: slots}, nil
}

// deriveCompat builds the table of a never-split collection: modulus
// len(channels), shard i owning residue i alone. That is exactly the legacy
// typeutil.HashPK2Channels placement, so a collection whose meta carries no
// routing routes through the same lookup as one that does — there is no second
// code path that could drift from it.
func deriveCompat(channels []string) (*ResidueTable, error) {
	modulus, shards, err := LegacyShards(channels)
	if err != nil {
		return nil, err
	}
	hashShards := make([]HashShard, 0, len(shards))
	for _, s := range shards {
		hashShards = append(hashShards, HashShard(s))
	}
	return DeriveHash(modulus, hashShards)
}

// NumSlots returns the routing modulus M.
func (t *ResidueTable) NumSlots() uint64 { return t.modulus }

// LookupOK returns the vchannel owning the given routing value, and whether any
// shard claims it. On a table from DeriveHash every value has an owner; on a
// partial table an unowned residue reports false. Callers must use the second
// return rather than testing the first against "": a value belonging to no shard
// is a malformed plan, and guessing an owner would silently misplace rows.
func (t *ResidueTable) LookupOK(value uint64) (string, bool) {
	vchannel := t.slots[value%t.modulus]
	return vchannel, vchannel != ""
}

// SplitPlan is the routing change one shard split makes.
type SplitPlan struct {
	// Modulus is the collection's routing modulus after the split. It is
	// unchanged unless the split had to double it.
	Modulus uint64
	// Left and Right are the residue sets of the two shards the split produces,
	// expressed at Modulus. Together they cover exactly what the source covered.
	Left  []uint64
	Right []uint64
}

// DoublesModulus reports whether the plan raises the collection's modulus, in
// which case every shard the split did not touch must be carried onto the new
// modulus with Rebase before the table is re-derived.
func (p *SplitPlan) DoublesModulus(before uint64) bool { return p.Modulus != before }

// PlanSplit halves the value space of one shard.
//
// A shard owning several residues is halved by dividing that set in two, and the
// collection's modulus does not move — this is the common case, since a split
// deep in a collection's history is splitting a shard that still owns a whole
// band of residues. Only a shard down to a single residue r has nothing left to
// divide: there the modulus doubles to 2M and r becomes {r} and {r+M}, which is
// the same value space cut on one more hash bit.
//
// A doubling is a collection-wide change: at 2M every other shard's residue set
// must be re-expressed with Rebase, or it would claim only half of what it owns
// and the rest of the value space would route nowhere. Callers apply both in the
// single atomic collection-meta update that commits the split.
func PlanSplit(modulus uint64, buckets []uint64) (*SplitPlan, error) {
	if err := checkModulus(modulus); err != nil {
		return nil, err
	}
	sorted, err := normalizeBuckets(buckets, modulus)
	if err != nil {
		return nil, err
	}

	if len(sorted) == 1 {
		doubled := modulus * 2
		if err := checkModulus(doubled); err != nil {
			return nil, merr.Wrapf(err, "cannot split the single residue %d of a shard", sorted[0])
		}
		return &SplitPlan{
			Modulus: doubled,
			Left:    []uint64{sorted[0]},
			Right:   []uint64{sorted[0] + modulus},
		}, nil
	}

	// A sound hash spreads values evenly over residues, so the halves are
	// balanced in proportion to how many residues each gets; an odd set cannot be
	// cut evenly and the left half keeps the extra one. Cutting the sorted set in
	// the middle makes the choice deterministic, which keeps a retried plan
	// identical to the one it retries.
	half := (len(sorted) + 1) / 2
	return &SplitPlan{
		Modulus: modulus,
		Left:    sorted[:half:half],
		Right:   sorted[half:],
	}, nil
}

// Rebase re-expresses a residue set from oldModulus onto newModulus, which must
// be a multiple of it. Residue r modulo M covers exactly {r, r+M, r+2M, ...}
// below the new modulus, so the shard keeps the same values under the new
// arithmetic.
func Rebase(buckets []uint64, oldModulus, newModulus uint64) ([]uint64, error) {
	if err := checkModulus(oldModulus); err != nil {
		return nil, err
	}
	if err := checkModulus(newModulus); err != nil {
		return nil, err
	}
	if newModulus%oldModulus != 0 {
		return nil, merr.WrapErrServiceInternalMsg("cannot rebase from modulus %d onto %d: %d is not a multiple of %d",
			oldModulus, newModulus, newModulus, oldModulus)
	}
	sorted, err := normalizeBuckets(buckets, oldModulus)
	if err != nil {
		return nil, err
	}

	factor := newModulus / oldModulus
	out := make([]uint64, 0, uint64(len(sorted))*factor)
	for _, r := range sorted {
		for v := r; v < newModulus; v += oldModulus {
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// maxModulus caps the routing modulus so malformed or pathological meta cannot
// allocate an enormous slot array.
//
// It is above the reachable value with room to spare: a collection holds at most
// one vchannel per pchannel, and the modulus only doubles when the shard being
// halved is down to a single residue, so schemapb's own bound — "the modulus
// stays under 2^15" — is what this matches. A larger number in the meta is
// corruption, and rejecting it costs a split that could not have happened.
const maxModulus = 1 << 15

func checkModulus(modulus uint64) error {
	if modulus == 0 {
		return merr.WrapErrServiceInternal("routing modulus must be positive")
	}
	if modulus > maxModulus {
		return merr.WrapErrServiceInternalMsg("routing modulus %d exceeds the cap %d", modulus, maxModulus)
	}
	return nil
}

// normalizeBuckets validates a residue set and returns it sorted, so callers
// that compare or divide sets do not depend on the order the meta happened to
// store them in.
func normalizeBuckets(buckets []uint64, modulus uint64) ([]uint64, error) {
	if len(buckets) == 0 {
		return nil, merr.WrapErrServiceInternal("shard owns no residue")
	}
	sorted := append([]uint64(nil), buckets...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for i, r := range sorted {
		if r >= modulus {
			return nil, merr.WrapErrServiceInternalMsg("residue %d is not below the modulus %d", r, modulus)
		}
		if i > 0 && sorted[i-1] == r {
			return nil, merr.WrapErrServiceInternalMsg("residue %d is listed twice", r)
		}
	}
	return sorted, nil
}
