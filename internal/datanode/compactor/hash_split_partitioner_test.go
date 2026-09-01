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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func doublingTargets(modulus, remA, remB uint64) []*datapb.SplitShardTaskTarget {
	_ = modulus
	return []*datapb.SplitShardTaskTarget{
		{Vchannel: "target-a", Buckets: []uint64{remA}},
		{Vchannel: "target-b", Buckets: []uint64{remB}},
	}
}

func TestDoublingRoutesEveryKeyToExactlyOneTarget(t *testing.T) {
	// Source bucket {mod 2, rem 0} doubled into {4,0} and {4,2}.
	p, err := newHashSplitPartitioner(4, doublingTargets(4, 0, 2))
	require.NoError(t, err)
	assert.Equal(t, 2, p.NumTargets())
	assert.Equal(t, "target-a", p.TargetVChannel(0))
	assert.Equal(t, "target-b", p.TargetVChannel(1))

	// Every int64 key the source owned lands in exactly one half, and the half
	// agrees with the bucket predicate.
	routed := map[int]int{}
	for pk := int64(0); pk < 2000; pk++ {
		h, err := typeutil.Hash32Int64(pk)
		require.NoError(t, err)
		if uint64(h)%2 != 0 {
			continue // not this source shard's key
		}
		idx, err := p.RouteInt64(pk)
		require.NoError(t, err)
		routed[idx]++
		if uint64(h)%4 == 0 {
			assert.Equal(t, 0, idx, "pk %d hash %d", pk, h)
		} else {
			assert.Equal(t, 1, idx, "pk %d hash %d", pk, h)
		}
	}
	// Both halves get a meaningful share (balanced by construction).
	assert.Greater(t, routed[0], 0)
	assert.Greater(t, routed[1], 0)
}

func TestDoublingVarCharKeys(t *testing.T) {
	p, err := newHashSplitPartitioner(2, doublingTargets(2, 0, 1))
	require.NoError(t, err)
	for i := range 500 {
		pk := fmt.Sprintf("key-%d", i)
		idx, err := p.RouteVarChar(pk)
		require.NoError(t, err)
		want := int(uint64(typeutil.HashString2Uint32(pk)) % 2)
		assert.Equal(t, want, idx, "pk %s", pk)
	}
}

func TestDoublingIsDeterministic(t *testing.T) {
	// The rewrite's crash-idempotency relies on the partition being a pure
	// function of the pk: a re-dispatched plan must reproduce it exactly.
	p1, err := newHashSplitPartitioner(8, doublingTargets(8, 3, 7))
	require.NoError(t, err)
	p2, err := newHashSplitPartitioner(8, doublingTargets(8, 3, 7))
	require.NoError(t, err)
	for pk := int64(0); pk < 500; pk++ {
		h, err := typeutil.Hash32Int64(pk)
		require.NoError(t, err)
		if uint64(h)%4 != 3 {
			continue
		}
		a, err := p1.RouteInt64(pk)
		require.NoError(t, err)
		b, err := p2.RouteInt64(pk)
		require.NoError(t, err)
		assert.Equal(t, a, b, "pk %d must route identically across plans", pk)
	}
}

func TestPartitionerRejectsMalformedTargets(t *testing.T) {
	cases := []struct {
		name    string
		modulus uint64
		targets []*datapb.SplitShardTaskTarget
		errStr  string
	}{
		{
			name:    "fewer than two targets",
			modulus: 2,
			targets: []*datapb.SplitShardTaskTarget{{Vchannel: "a", Buckets: []uint64{0}}},
			errStr:  "at least 2 targets",
		},
		{
			name:    "target owning no residue",
			modulus: 2,
			targets: []*datapb.SplitShardTaskTarget{
				{Vchannel: "a", Buckets: []uint64{0}},
				{Vchannel: "b"},
			},
			errStr: "owns no residue",
		},
		{
			name:    "residue out of range",
			modulus: 2,
			targets: []*datapb.SplitShardTaskTarget{
				{Vchannel: "a", Buckets: []uint64{5}},
				{Vchannel: "b", Buckets: []uint64{1}},
			},
			errStr: "not below the modulus",
		},
		{
			// Both claim residue 4, so a key hashing there would be written to
			// two output segments and counted twice.
			name:    "targets that overlap",
			modulus: 8,
			targets: []*datapb.SplitShardTaskTarget{
				{Vchannel: "a", Buckets: []uint64{0, 4}},
				{Vchannel: "b", Buckets: []uint64{4}},
			},
			errStr: "overlap",
		},
		{
			name:    "targets with identical residues",
			modulus: 4,
			targets: []*datapb.SplitShardTaskTarget{
				{Vchannel: "a", Buckets: []uint64{1}},
				{Vchannel: "b", Buckets: []uint64{1}},
			},
			errStr: "overlap",
		},
		{
			name:    "the same target vchannel twice",
			modulus: 2,
			targets: []*datapb.SplitShardTaskTarget{
				{Vchannel: "a", Buckets: []uint64{0}},
				{Vchannel: "a", Buckets: []uint64{1}},
			},
			errStr: "appears twice",
		},
		{
			name:    "no modulus to read the residues against",
			modulus: 0,
			targets: []*datapb.SplitShardTaskTarget{
				{Vchannel: "a", Buckets: []uint64{0}},
				{Vchannel: "b", Buckets: []uint64{1}},
			},
			errStr: "modulus",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newHashSplitPartitioner(tc.modulus, tc.targets)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errStr)
		})
	}
}

func TestRouteHashErrorsWhenNoTargetClaims(t *testing.T) {
	// A doubling whose two residue sets do not cover the source's keys: a key
	// hashing to neither must be reported, not guessed.
	p, err := newHashSplitPartitioner(4, doublingTargets(4, 0, 2))
	require.NoError(t, err)
	_, err = p.routeHash(1) // 1%4 == 1, claimed by neither target
	require.Error(t, err)
	assert.Contains(t, err.Error(), "matches none of the split targets")
}

func TestPartitionerRoutesAcrossManyTargets(t *testing.T) {
	// A rehash to an arbitrary shard count gives the plan M targets, not two:
	// every key of the input segment goes to whichever of the M buckets claims
	// it, and together they cover the whole key space.
	const m = 5
	targets := make([]*datapb.SplitShardTaskTarget, 0, m)
	for r := 0; r < m; r++ {
		targets = append(targets, &datapb.SplitShardTaskTarget{
			Vchannel: fmt.Sprintf("vch%d", r),
			Buckets:  []uint64{uint64(r)},
		})
	}
	p, err := newHashSplitPartitioner(m, targets)
	require.NoError(t, err)
	assert.Equal(t, m, p.NumTargets())

	// Every hash lands on the target whose remainder it matches, and the index
	// the partitioner returns is the plan's output-segment index.
	for hash := uint64(0); hash < 50; hash++ {
		idx, err := p.routeHash(hash)
		require.NoError(t, err)
		assert.Equal(t, int(hash%m), idx)
		assert.Equal(t, fmt.Sprintf("vch%d", hash%m), p.TargetVChannel(idx))
	}
}

func TestPartitionerHandlesTargetsOwningSeveralResidues(t *testing.T) {
	// One collection-wide modulus does not mean one residue per target: a target
	// carved by dividing a residue SET owns several, and the partitioner must
	// route every one of them to it.
	p, err := newHashSplitPartitioner(4, []*datapb.SplitShardTaskTarget{
		{Vchannel: "odd", Buckets: []uint64{1, 3}},
		{Vchannel: "ev0", Buckets: []uint64{0}},
		{Vchannel: "ev2", Buckets: []uint64{2}},
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		hash     uint64
		vchannel string
	}{
		{0, "ev0"},
		{1, "odd"},
		{2, "ev2"},
		{3, "odd"},
		{4, "ev0"},
		{5, "odd"},
		{6, "ev2"},
		{7, "odd"},
	} {
		idx, err := p.routeHash(tc.hash)
		require.NoError(t, err)
		assert.Equal(t, tc.vchannel, p.TargetVChannel(idx), "hash %d", tc.hash)
	}
}
