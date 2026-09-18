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
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The runaway-doubling guard.
//
// A doubling relieves a shard by cutting its keys on the next hash bit. That
// works because primary keys are spread by the hash, so each half takes about
// half the rows. It stops working if the SAME key is inserted enough times to
// dominate the shard -- Milvus does not enforce uniqueness on insert -- because
// every copy hashes the same and lands on the same half. The shard is rewritten
// in full, one half comes out holding everything, it is still over the
// threshold, and the trigger doubles it again, forever.
//
// The test is made from live state, not remembered history: a doubling
// produces a sibling pair, residues r and r+M/2 at modulus M, so if the last
// one relieved nothing, this shard's sibling half is nearly empty. Deciding it
// from the sibling is what makes it survive a coordinator restart: there is
// nothing to remember.

// siblingResidue returns the residue the doubling that produced r split off
// alongside it: the halves of one doubling from M/2 to M are M/2 apart.
func siblingResidue(modulus, r uint64) (uint64, bool) {
	if modulus < 2 || modulus%2 != 0 {
		return 0, false
	}
	return (r + modulus/2) % modulus, true
}

// doublingRelievedNothing reports whether the doubling that produced this shard
// left it holding essentially all of its parent's data, which means doubling it
// again would do the same.
//
// It allows the split whenever the question has no answer: the guard disabled,
// an empty shard, a never-split collection (its modulus is its shard count, and
// no doubling produced any of its shards), a shard that still owns several
// residues (it is halved by dividing that set, not by a doubling).
func (m *shardSplitManager) doublingRelievedNothing(coll *splitCollection, residues *shardResidues, own []uint64, size int64) bool {
	minRatio := paramtable.Get().DataCoordCfg.ShardSplitMinSiblingRatio.GetAsFloat()
	if minRatio <= 0 || size <= 0 || coll.RoutingModulus == 0 || len(own) != 1 {
		return false
	}
	sibling, ok := siblingResidue(residues.modulus, own[0])
	if !ok {
		return false
	}
	owner, ok := residues.ownerOf(sibling)
	if !ok {
		return true
	}
	siblingSize := m.collectShardStats(owner).size
	return float64(siblingSize) < minRatio*float64(size)
}
