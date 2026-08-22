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
)

// HashBucket is one shard's hash predicate: it owns the keys whose hash
// satisfies hash % Modulus == Remainder. An unsplit N-shard collection is
// exactly {Modulus: N, Remainder: k} for shard k, so the legacy hash%N routing
// is expressible with no metadata rewrite (design §2, §3.1).
type HashBucket struct {
	Modulus   uint64
	Remainder uint64
}

// HashShard describes one shard's ownership under hash routing: its vchannel
// and the hash buckets it owns.
type HashShard struct {
	Vchannel string
	Buckets  []HashBucket
}

// HashRoutingTable routes a primary key to a vchannel by a flat hash-bucket
// lookup.
//
// The buckets of the shards may have different moduli after a sequence of
// doubling splits (N, 2N, 4N, ...). They are normalized onto a single modulus
// M = lcm(all moduli) — always a power-of-two multiple of the original shard
// count — so the lookup is one array index instead of a scan over predicates:
//
//	route(pk): slots[hash % M]
//
// M is bounded by the collection's shard-count cap, and the table is re-derived
// from the collection meta on every routing-version change.
type HashRoutingTable struct {
	// modulus is M, the normalized modulus. Always > 0 for a valid table.
	modulus uint64
	// slots[i] is the vchannel owning the keys with hash % M == i. A table built
	// by DeriveHash has every slot filled — it rejects a shard set that leaves
	// one uncovered. A table built by DeriveHashPartial may leave slots empty,
	// for a shard set that deliberately covers only part of the key space.
	slots []string
}

// DeriveHash builds a HashRoutingTable from the shards of a hash-routed
// collection.
//
// It validates that the buckets tile the key space exactly: normalized onto
// M = lcm(moduli), every residue must be claimed by exactly one shard. A gap
// (some key routes nowhere) or an overlap (some key routes to two shards) is
// rejected, so a malformed routing meta fails loudly instead of silently
// mis-routing writes.
func DeriveHash(shards []HashShard) (*HashRoutingTable, error) {
	table, err := DeriveHashPartial(shards)
	if err != nil {
		return nil, err
	}
	// Every slot must be owned by someone, or some key would route nowhere.
	for r, ch := range table.slots {
		if ch == "" {
			return nil, errors.Newf("hash routing gap: residue %d (mod %d) is unowned", r, table.modulus)
		}
	}
	return table, nil
}

// DeriveHashPartial builds a HashRoutingTable from shards that need only be
// mutually disjoint, not a cover of the whole key space. Residues no shard
// claims stay unowned, and LookupOK reports them.
//
// This is the shape a shard split's targets have while the split is in flight:
// a doubling's two targets, say {4,0} and {4,2}, tile exactly the keys of their
// source's {2,0} bucket and deliberately claim nothing else. Rejecting that as a
// gap — which is what a whole-space cover requires — would be wrong; what must
// still be rejected is an overlap, since that would send one key to two shards.
func DeriveHashPartial(shards []HashShard) (*HashRoutingTable, error) {
	if len(shards) == 0 {
		return nil, errors.New("hash routing table needs at least one shard")
	}

	// M = lcm of every bucket modulus.
	m := uint64(1)
	for _, s := range shards {
		for _, b := range s.Buckets {
			if b.Modulus == 0 {
				return nil, errors.Newf("shard %q has a zero-modulus hash bucket", s.Vchannel)
			}
			if b.Remainder >= b.Modulus {
				return nil, errors.Newf("shard %q has bucket remainder %d >= modulus %d",
					s.Vchannel, b.Remainder, b.Modulus)
			}
			var err error
			if m, err = lcm(m, b.Modulus); err != nil {
				return nil, errors.Wrapf(err, "shard %q modulus %d", s.Vchannel, b.Modulus)
			}
		}
	}
	if m > maxNormalizedModulus {
		return nil, errors.Newf("normalized hash modulus %d exceeds the cap %d", m, maxNormalizedModulus)
	}

	slots := make([]string, m)
	for _, s := range shards {
		for _, b := range s.Buckets {
			// Expand bucket (modulus, remainder) onto the normalized modulus:
			// every residue r < M with r % modulus == remainder belongs to it.
			for r := b.Remainder; r < m; r += b.Modulus {
				if slots[r] != "" {
					return nil, errors.Newf(
						"hash routing overlap at residue %d (mod %d): shards %q and %q",
						r, m, slots[r], s.Vchannel)
				}
				slots[r] = s.Vchannel
			}
		}
	}

	return &HashRoutingTable{modulus: m, slots: slots}, nil
}

// DeriveHashCompat builds the table of a never-split collection: shard i owns
// {Modulus: len(channels), Remainder: i}, i.e. exactly the legacy
// typeutil.HashPK2Channels behaviour. Used for collections whose meta carries no
// explicit routing predicate yet.
func DeriveHashCompat(channels []string) (*HashRoutingTable, error) {
	shards := make([]HashShard, 0, len(channels))
	for i, ch := range channels {
		shards = append(shards, HashShard{
			Vchannel: ch,
			Buckets:  []HashBucket{{Modulus: uint64(len(channels)), Remainder: uint64(i)}},
		})
	}
	return DeriveHash(shards)
}

// NumSlots returns the normalized modulus M.
func (t *HashRoutingTable) NumSlots() uint64 { return t.modulus }

// Lookup returns the vchannel owning the given key hash. On a table from
// DeriveHash every hash has an owner; on a partial table an unowned residue
// returns "", which LookupOK distinguishes explicitly.
func (t *HashRoutingTable) Lookup(rawHash uint64) string {
	return t.slots[rawHash%t.modulus]
}

// LookupOK returns the vchannel owning the given key hash, and whether any shard
// claims it. Callers over a partial table must use this rather than testing
// Lookup against "": a key that belongs to no target is a malformed plan, and
// guessing an owner would silently misplace rows.
func (t *HashRoutingTable) LookupOK(rawHash uint64) (string, bool) {
	ch := t.slots[rawHash%t.modulus]
	return ch, ch != ""
}

// SplitBuckets returns the two buckets a doubling split of b produces: the same
// remainder at twice the modulus, and that remainder shifted by the old modulus.
// Together they cover exactly the keys b covered, cut on the next hash bit
// (design §3.1).
func SplitBuckets(b HashBucket) (HashBucket, HashBucket) {
	return HashBucket{Modulus: b.Modulus * 2, Remainder: b.Remainder},
		HashBucket{Modulus: b.Modulus * 2, Remainder: b.Remainder + b.Modulus}
}

// maxNormalizedModulus caps the normalized modulus M so a malformed or
// pathological meta cannot allocate an enormous slot array. It is far above the
// reachable shard count (which is itself capped by the pchannel count).
const maxNormalizedModulus = 1 << 20

// lcm returns the least common multiple of a and b, erroring on overflow.
func lcm(a, b uint64) (uint64, error) {
	g := gcd(a, b)
	q := a / g
	if q != 0 && b > maxNormalizedModulus/q {
		return 0, errors.Newf("hash modulus lcm(%d, %d) overflows the cap", a, b)
	}
	return q * b, nil
}

func gcd(a, b uint64) uint64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
