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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ResidueTable is a validated residue assignment of a collection.
//
// A collection carries one routing modulus M, and each shard owns a set of
// residues modulo M. The sets partition [0, M): slots[r] is the vchannel owning
// the values whose residue modulo M is r, and every slot is filled.
//
// M is not the shard count. A never-split N-shard collection is M = N with one
// residue per shard, which is exactly the legacy hash%N placement; afterwards a
// split either divides a residue set at the current M or doubles M.
type ResidueTable struct {
	// modulus is M. Always > 0 in a valid table.
	modulus uint64
	// slots[r] is the vchannel owning residue r.
	slots []string
}

// deriveHash builds the residue table of a split collection.
//
// It validates that the shards tile the value space exactly: every residue below
// modulus must be claimed by exactly one shard. A gap (some value routes nowhere)
// or an overlap (some value routes to two shards) is rejected, so malformed
// routing meta fails loudly instead of silently mis-placing writes.
func deriveHash(modulus uint64, shards []Shard) (*ResidueTable, error) {
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
			// shard carrying it would be indistinguishable from a gap.
			return nil, merr.WrapErrServiceInternal("a routing shard carries no vchannel name")
		}
		if len(s.Buckets) == 0 {
			// A shard owning no residue can never be written to and can never be
			// split, but still counts as a shard everywhere else — it is a silent
			// dead shard, not a harmless one.
			return nil, merr.WrapErrServiceInternalMsg("shard %q owns no residue", s.Vchannel)
		}
		for _, r := range s.Buckets {
			if r >= modulus {
				return nil, merr.WrapErrServiceInternalMsg("shard %q owns residue %d, which is not below the modulus %d",
					s.Vchannel, r, modulus)
			}
			if owner := slots[r]; owner != "" {
				// One shard listing a residue twice is a malformed shard entry,
				// while two shards claiming it is a malformed topology.
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
	for r, vchannel := range slots {
		if vchannel == "" {
			return nil, merr.WrapErrServiceInternalMsg("hash routing gap: residue %d (mod %d) is unowned", r, modulus)
		}
	}
	return &ResidueTable{modulus: modulus, slots: slots}, nil
}

// deriveCompat builds the table of a never-split collection: modulus
// len(channels), shard i owning residue i alone. That is exactly the legacy
// typeutil.HashPK2Channels placement.
func deriveCompat(channels []string) (*ResidueTable, error) {
	shards := make([]Shard, 0, len(channels))
	for i, vchannel := range channels {
		if vchannel == "" {
			return nil, merr.WrapErrServiceInternalMsg("vchannel %d carries no name", i)
		}
		shards = append(shards, Shard{Vchannel: vchannel, Buckets: []uint64{uint64(i)}})
	}
	if err := refuseShardsOutside(channels, shards); err != nil {
		return nil, err
	}
	return deriveHash(uint64(len(channels)), shards)
}

// maxModulus caps the routing modulus so malformed or pathological meta cannot
// allocate an enormous slot array. It matches schemapb's own bound — "the
// modulus stays under 2^15". A larger number in the meta is corruption.
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
