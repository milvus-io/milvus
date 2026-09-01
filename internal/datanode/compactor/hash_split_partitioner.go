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
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The row-routing predicate of a HashSplitCompaction: it decides, for one row's
// primary key, which of the two shard-split targets that row belongs to.
//
// A hash-routed collection's segments straddle every split boundary, so the
// rewrite must repartition each source segment row by row (design
// docs/design-docs/design_docs/20260610-shard_split.md §6.5).
// The two targets are the halves of a doubling: each owns a hash bucket
// hash(pk) % modulus == remainder, the two differing only in the next hash bit,
// so every key of the source lands in exactly one of them.
//
// The predicate is a pure function of the pk and the targets, so a re-dispatch
// of a lost plan reproduces exactly the same partition — the property the
// rewrite's crash-idempotency relies on.

// hashSplitPartitioner routes a primary key to one of a split's targets.
type hashSplitPartitioner struct {
	// vchannels is parallel to the plan's output segments: index i is the target
	// whose rows go to outputs[i].
	vchannels []string
	// table maps a key hash to an owning target vchannel in one array index.
	// Built as a PARTIAL table: a doubling's targets tile only their source's
	// bucket, not the whole key space, and the rows of the input segment are
	// exactly the keys of that bucket.
	table *routing.ResidueTable
	// index maps a target vchannel back to its position in vchannels.
	index map[string]int
}

// newHashSplitPartitioner builds the partitioner from a plan's split targets.
//
// It rejects a malformed target set — one that cannot route every key of the
// input to exactly one target — so a bad plan fails the compaction instead of
// silently dropping or duplicating rows. The tiling check is the routing
// package's own, the same one the cluster's write-path routing table is
// validated by, so a plan that would disagree with live routing cannot run.
func newHashSplitPartitioner(modulus uint64, targets []*datapb.SplitShardTaskTarget) (*hashSplitPartitioner, error) {
	if len(targets) < 2 {
		return nil, merr.WrapErrParameterInvalidMsg(
			"a hash split rewrite needs at least 2 targets, got %d", len(targets))
	}
	p := &hashSplitPartitioner{
		vchannels: make([]string, 0, len(targets)),
		index:     make(map[string]int, len(targets)),
	}
	shards := make([]routing.HashShard, 0, len(targets))
	for i, t := range targets {
		vchannel := t.GetVchannel()
		if _, dup := p.index[vchannel]; dup {
			return nil, merr.WrapErrParameterInvalidMsg(
				"target vchannel %q appears twice in the plan", vchannel)
		}
		p.vchannels = append(p.vchannels, vchannel)
		p.index[vchannel] = i
		shards = append(shards, routing.HashShard{
			Vchannel: vchannel,
			Buckets:  t.GetBuckets(),
		})
	}

	// Partial, not whole: the targets of one split cover exactly their source's
	// residues and deliberately claim nothing else, so the rest of the key space
	// is expected to be unowned here. What must still be rejected is an overlap,
	// which would write one row into two output segments.
	table, err := routing.DeriveHashPartial(modulus, shards)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("malformed hash split targets: %s", err.Error())
	}
	p.table = table
	return p, nil
}

// RouteInt64 returns the index of the target owning the given int64 primary key.
func (p *hashSplitPartitioner) RouteInt64(pk int64) (int, error) {
	h, err := typeutil.Hash32Int64(pk)
	if err != nil {
		return 0, err
	}
	return p.routeHash(uint64(h))
}

// RouteVarChar returns the index of the target owning the given varchar key.
func (p *hashSplitPartitioner) RouteVarChar(pk string) (int, error) {
	return p.routeHash(uint64(typeutil.HashString2Uint32(pk)))
}

// routeHash applies the hash buckets. It errors rather than guessing when no
// target claims the hash, so a malformed plan cannot silently misplace rows.
func (p *hashSplitPartitioner) routeHash(hash uint64) (int, error) {
	vchannel, ok := p.table.LookupOK(hash)
	if !ok {
		return 0, merr.WrapErrServiceInternalMsg(
			"hash %d matches none of the split targets", hash)
	}
	return p.index[vchannel], nil
}

// TargetVChannel returns the vchannel of target i, the channel its output
// segment is written to.
func (p *hashSplitPartitioner) TargetVChannel(i int) string { return p.vchannels[i] }

// NumTargets returns the number of targets: 2 for a doubling, M for a rehash
// to M shards.
func (p *hashSplitPartitioner) NumTargets() int { return len(p.vchannels) }
