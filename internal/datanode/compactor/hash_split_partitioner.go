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

package compactor

import (
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The row-routing predicate of a HashSplitCompaction: it decides, for one row's
// primary key, which of the shard-split targets that row belongs to.
//
// A collection placed by primary key has segments that straddle every split
// boundary, so the rewrite repartitions each source segment row by row (design
// doc §6.3). Each target owns a set of residues modulo the post-split routing
// modulus, and a row goes to the target owning its primary key's residue --
// routing.PKResidue, the very function the write path places the row's inserts
// and deletes by, so a rewritten row lands on the shard every later write of its
// key is routed to.
//
// The predicate is a pure function of the pk and the plan, so a re-dispatch of a
// lost plan reproduces exactly the same partition, which the rewrite's
// crash-idempotency relies on.

// hashSplitPartitioner routes a primary key to one of a split's targets.
type hashSplitPartitioner struct {
	// modulus is the post-split routing modulus the residues are taken against.
	modulus uint64
	// vchannels is parallel to the plan's targets: index i is the target whose
	// rows go to writer i.
	vchannels []string
	// owner maps a residue to the index of the target owning it. It is a
	// PARTIAL table: a split's targets tile only their source's residues, and
	// the rows of the input segment are exactly the keys of those residues.
	owner map[uint64]int
}

// newHashSplitPartitioner builds the partitioner from a plan's split targets.
//
// It refuses a target set that cannot route every key of the input to exactly
// one target -- no modulus, a target owning nothing, a residue not below the
// modulus, a residue two targets claim, a vchannel named twice -- so a bad plan
// fails the compaction instead of silently dropping or duplicating rows.
func newHashSplitPartitioner(modulus uint64, targets []*datapb.SplitShardTaskTarget) (*hashSplitPartitioner, error) {
	if len(targets) < 2 {
		return nil, merr.WrapErrServiceInternalMsg("a hash split rewrite needs at least 2 targets, got %d", len(targets))
	}
	if modulus == 0 {
		return nil, merr.WrapErrServiceInternalMsg("a hash split rewrite carries no routing modulus")
	}
	p := &hashSplitPartitioner{
		modulus:   modulus,
		vchannels: make([]string, 0, len(targets)),
		owner:     make(map[uint64]int),
	}
	seen := make(map[string]struct{}, len(targets))
	for i, target := range targets {
		vchannel := target.GetVchannel()
		if vchannel == "" {
			return nil, merr.WrapErrServiceInternalMsg("hash split target %d names no vchannel", i)
		}
		if _, dup := seen[vchannel]; dup {
			return nil, merr.WrapErrServiceInternalMsg("target vchannel %q appears twice in the plan", vchannel)
		}
		seen[vchannel] = struct{}{}
		if len(target.GetBuckets()) == 0 {
			return nil, merr.WrapErrServiceInternalMsg("hash split target %q owns no residue", vchannel)
		}
		for _, residue := range target.GetBuckets() {
			if residue >= modulus {
				return nil, merr.WrapErrServiceInternalMsg(
					"hash split target %q owns residue %d, which is not below the modulus %d", vchannel, residue, modulus)
			}
			if prev, taken := p.owner[residue]; taken {
				return nil, merr.WrapErrServiceInternalMsg(
					"hash split targets overlap at residue %d (mod %d): %q and %q", residue, modulus, p.vchannels[prev], vchannel)
			}
			p.owner[residue] = i
		}
		p.vchannels = append(p.vchannels, vchannel)
	}
	return p, nil
}

// Route returns the index of the target owning a primary key, an int64 or a
// string. It errors rather than guessing when no target claims the key's
// residue, so a malformed plan cannot silently misplace rows.
func (p *hashSplitPartitioner) Route(pk any) (int, error) {
	residue, err := routing.PKResidue(pk, p.modulus)
	if err != nil {
		return 0, err
	}
	idx, ok := p.owner[residue]
	if !ok {
		return 0, merr.WrapErrServiceInternalMsg(
			"primary key %v (residue %d mod %d) matches none of the split targets", pk, residue, p.modulus)
	}
	return idx, nil
}

// TargetVChannel returns the vchannel of target i, the channel its output
// segments are written to.
func (p *hashSplitPartitioner) TargetVChannel(i int) string { return p.vchannels[i] }

// NumTargets returns the number of targets.
func (p *hashSplitPartitioner) NumTargets() int { return len(p.vchannels) }

// hashSplitSink receives the rows the rewrite routed to one target. The
// production sink is the target's MultiSegmentWriter.
type hashSplitSink interface {
	Write(r storage.Record) error
}
