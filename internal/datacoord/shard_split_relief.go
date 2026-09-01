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
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// The runaway-doubling guard.
//
// A doubling relieves a shard by cutting its key space on the next hash bit.
// That works because primary keys are unique and the hash spreads them, so each
// half takes roughly half the rows. It stops working if the SAME key is
// inserted enough times to dominate the shard — Milvus does not enforce
// uniqueness on insert — because every copy of that key has the same hash and
// lands on the same half. The shard is rewritten in full, one half comes out
// holding everything, it is still over the threshold, and the trigger splits it
// again. Nothing is relieved and a full rewrite burns every round, forever.
//
// The test is made from live state rather than from remembered history: a
// doubling produces a sibling pair, residues r and r+M/2 at modulus M, so if the
// last one relieved nothing then this shard's sibling half is nearly empty. Deciding it
// from the sibling rather than from "how big was my parent" is what makes it
// survive task reaping, GC and a coordinator restart — there is nothing to
// remember.
//
// Design: docs/design-docs/design_docs/20260610-shard_split.md §10.2.

// siblingResidue returns the residue the doubling that produced r must have
// split off alongside it, and whether the shard has such a doubling in its
// ancestry at all.
//
// A doubling from M/2 to M turns residue r into {r} and {r + M/2}, so the two
// halves of one doubling are always M/2 apart. An odd modulus has no doubling in
// its ancestry -- the collection was rehashed to an odd shard count, and its
// shards were carved from every source at once rather than cut from a parent --
// so there is no sibling to compare against.
func siblingResidue(modulus, r uint64) (uint64, bool) {
	if modulus < 2 || modulus%2 != 0 {
		return 0, false
	}
	return (r + modulus/2) % modulus, true
}

// siblingHalfSize returns the size of the shard holding the sibling residue.
//
// One collection-wide modulus is what makes this a lookup rather than the
// interval arithmetic a per-shard modulus needed: every shard's residues are
// taken against the same modulus, the residues tile it, so exactly one shard
// owns the sibling. Zero when no shard does, which the caller reads as "the
// other half is empty".
func (m *shardSplitManager) siblingHalfSize(residues *shardResidues, sibling uint64) int64 {
	vchannel, ok := residues.ownerOf(sibling)
	if !ok {
		return 0
	}
	return m.collectShardStats(vchannel).size
}

// doublingRelievedNothing reports whether the doubling that produced this shard
// left it holding essentially all of its parent's data, which means doubling it
// again will do the same.
//
// Returns false — allow the split — whenever the question cannot be answered:
// a shard with no doubling ancestry, a bucket that cannot be read, an empty
// shard. The guard exists to stop a loop that burns a full rewrite per round,
// not to second-guess a split that has a plausible reason to run.
func (m *shardSplitManager) doublingRelievedNothing(
	collection *collectionInfo,
	vchannel string,
	size int64,
) bool {
	if size <= 0 {
		return false
	}
	minShrinkRatio := Params.DataCoordCfg.ShardSplitMinSiblingRatio.GetAsFloat()
	if minShrinkRatio <= 0 {
		return false // guard disabled
	}
	residues, err := residuesOf(collection)
	if err != nil {
		return false
	}
	own, err := residues.of(vchannel)
	if err != nil {
		return false
	}
	if len(own) != 1 {
		// A shard still owning several residues is halved by dividing that set,
		// which neither doubles the modulus nor rewrites at a new hash bit. The
		// runaway this guard exists for -- a full rewrite per round, forever --
		// is specific to the doubling case, so a set-halving is left alone.
		return false
	}
	sibling, ok := siblingResidue(residues.modulus, own[0])
	if !ok {
		return false
	}
	siblingSize := m.siblingHalfSize(residues, sibling)
	if siblingSize == 0 {
		// Nothing at all on the other half. Either the previous doubling put
		// everything here, or the sibling shard is missing from the topology;
		// both are reasons not to spend another full rewrite proving it.
		return true
	}
	return float64(siblingSize) < minShrinkRatio*float64(size)
}

// refuseUnrelievableDoubling reports whether the shard must not be split again,
// logging the reason at a rate limit so the condition is visible without
// flooding: it is re-evaluated on every detection tick and does not clear on its
// own.
func (m *shardSplitManager) refuseUnrelievableDoubling(
	logger *mlog.Logger,
	collection *collectionInfo,
	vchannel string,
	size int64,
) bool {
	if !m.doublingRelievedNothing(collection, vchannel, size) {
		return false
	}
	logger.RatedWarn(m.ctx, 300,
		"refusing to double a shard its last doubling did not relieve; "+
			"its sibling half is nearly empty, which a unique primary key cannot produce — "+
			"look for one key inserted many times",
		mlog.Int64("collectionID", collection.ID),
		mlog.String("vchannel", vchannel),
		mlog.Int64("size", size))
	return true
}
