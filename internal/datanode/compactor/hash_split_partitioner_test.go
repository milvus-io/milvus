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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func doublingTargets(remA, remB uint64) []*datapb.SplitShardTaskTarget {
	return []*datapb.SplitShardTaskTarget{
		{Vchannel: "target-a", Buckets: []uint64{remA}},
		{Vchannel: "target-b", Buckets: []uint64{remB}},
	}
}

func TestDoublingRoutesEveryKeyToExactlyOneTarget(t *testing.T) {
	// Source residue {mod 2, 0} doubled into {4: 0} and {4: 2}.
	p, err := newHashSplitPartitioner(4, doublingTargets(0, 2))
	require.NoError(t, err)
	assert.Equal(t, 2, p.NumTargets())
	assert.Equal(t, "target-a", p.TargetVChannel(0))
	assert.Equal(t, "target-b", p.TargetVChannel(1))

	routed := map[int]int{}
	for pk := int64(0); pk < 2000; pk++ {
		h, err := typeutil.Hash32Int64(pk)
		require.NoError(t, err)
		if uint64(h)%2 != 0 {
			continue // not this source shard's key
		}
		idx, err := p.Route(pk)
		require.NoError(t, err)
		routed[idx]++
		if uint64(h)%4 == 0 {
			assert.Equal(t, 0, idx, "pk %d hash %d", pk, h)
		} else {
			assert.Equal(t, 1, idx, "pk %d hash %d", pk, h)
		}
	}
	assert.Greater(t, routed[0], 0)
	assert.Greater(t, routed[1], 0)
}

func TestDoublingVarCharKeys(t *testing.T) {
	p, err := newHashSplitPartitioner(2, doublingTargets(0, 1))
	require.NoError(t, err)
	for i := range 500 {
		pk := fmt.Sprintf("key-%d", i)
		idx, err := p.Route(pk)
		require.NoError(t, err)
		want := int(uint64(typeutil.HashString2Uint32(pk)) % 2)
		assert.Equal(t, want, idx, "pk %s", pk)
	}
}

func TestDoublingIsDeterministic(t *testing.T) {
	// The rewrite's crash-idempotency relies on the partition being a pure
	// function of the pk: a re-dispatched plan must reproduce it exactly.
	p1, err := newHashSplitPartitioner(8, doublingTargets(3, 7))
	require.NoError(t, err)
	p2, err := newHashSplitPartitioner(8, doublingTargets(3, 7))
	require.NoError(t, err)
	for pk := int64(0); pk < 500; pk++ {
		h, err := typeutil.Hash32Int64(pk)
		require.NoError(t, err)
		if uint64(h)%4 != 3 {
			continue
		}
		a, err := p1.Route(pk)
		require.NoError(t, err)
		b, err := p2.Route(pk)
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
			name:    "target naming no vchannel",
			modulus: 2,
			targets: []*datapb.SplitShardTaskTarget{
				{Vchannel: "a", Buckets: []uint64{0}},
				{Buckets: []uint64{1}},
			},
			errStr: "names no vchannel",
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
			// Both claim residue 4, so a key there would be written to two
			// output segments and counted twice.
			name:    "targets that overlap",
			modulus: 8,
			targets: []*datapb.SplitShardTaskTarget{
				{Vchannel: "a", Buckets: []uint64{0, 4}},
				{Vchannel: "b", Buckets: []uint64{4}},
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
			targets: doublingTargets(0, 1),
			errStr:  "modulus",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newHashSplitPartitioner(tc.modulus, tc.targets)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrServiceInternal)
			assert.Contains(t, err.Error(), tc.errStr)
		})
	}
}

func TestPartitionerErrorsWhenNoTargetClaims(t *testing.T) {
	// Targets claiming only the even residues of modulus 4: a key whose residue
	// is odd belongs to neither and must be reported, not guessed.
	p, err := newHashSplitPartitioner(4, doublingTargets(0, 2))
	require.NoError(t, err)
	for pk := int64(0); pk < 100; pk++ {
		r, err := routing.PKResidueInt64(pk, 4)
		require.NoError(t, err)
		if r%2 == 0 {
			continue
		}
		_, err = p.Route(pk)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "matches none of the split targets")
		return
	}
	t.Fatal("no odd-residue key among the first 100")
}

func TestPartitionerRefusesAKeyOfAnotherType(t *testing.T) {
	p, err := newHashSplitPartitioner(2, doublingTargets(0, 1))
	require.NoError(t, err)
	_, err = p.Route(int32(1))
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestPartitionerHandlesTargetsOwningSeveralResidues(t *testing.T) {
	// A target carved by dividing a residue SET owns several residues, and the
	// partitioner must route every one of them to it.
	p, err := newHashSplitPartitioner(8, []*datapb.SplitShardTaskTarget{
		{Vchannel: "low", Buckets: []uint64{1, 3}},
		{Vchannel: "high", Buckets: []uint64{5, 7}},
	})
	require.NoError(t, err)
	for pk := int64(0); pk < 1000; pk++ {
		r, err := routing.PKResidueInt64(pk, 8)
		require.NoError(t, err)
		idx, err := p.Route(pk)
		switch r {
		case 1, 3:
			require.NoError(t, err)
			assert.Equal(t, "low", p.TargetVChannel(idx))
		case 5, 7:
			require.NoError(t, err)
			assert.Equal(t, "high", p.TargetVChannel(idx))
		default:
			assert.Error(t, err, "pk %d residue %d", pk, r)
		}
	}
}

// postSplitShape is one split as both sides see it: the plan's targets, and the
// collection's whole post-split routing meta the write path derives its table
// from.
type postSplitShape struct {
	name     string
	modulus  uint64
	targets  []*datapb.SplitShardTaskTarget
	channels []string
	infos    []*schemapb.CollectionShardInfo
	// sourceOwned reports whether a pk's pre-split residue belonged to the
	// source, i.e. whether a row with it could be in the input segment at all.
	sourceOwned func(pk any) bool
}

func hashInfo(vchannel string, residues ...uint64) *schemapb.CollectionShardInfo {
	return &schemapb.CollectionShardInfo{
		VchannelName: vchannel,
		State:        schemapb.ShardState_ShardNormal,
		Routing:      &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: residues}},
	}
}

func postSplitShapes(t *testing.T) []postSplitShape {
	residueOf := func(pk any, modulus uint64) uint64 {
		r, err := routing.PKResidue(pk, modulus)
		require.NoError(t, err)
		return r
	}
	return []postSplitShape{
		{
			// A one-shard collection's first split doubles M from 1 to 2.
			name:    "the only shard at modulus 2",
			modulus: 2,
			targets: []*datapb.SplitShardTaskTarget{
				{Vchannel: "t0", Buckets: []uint64{0}},
				{Vchannel: "t1", Buckets: []uint64{1}},
			},
			channels:    []string{"t0", "t1"},
			infos:       []*schemapb.CollectionShardInfo{hashInfo("t0", 0), hashInfo("t1", 1)},
			sourceOwned: func(any) bool { return true },
		},
		{
			// The odd shard of a two-shard collection splits: M doubles to 4 and
			// the untouched even shard is re-expressed as {0, 2}.
			name:    "the odd shard of two at modulus 4",
			modulus: 4,
			targets: []*datapb.SplitShardTaskTarget{
				{Vchannel: "odd1", Buckets: []uint64{1}},
				{Vchannel: "odd3", Buckets: []uint64{3}},
			},
			channels:    []string{"even", "odd1", "odd3"},
			infos:       []*schemapb.CollectionShardInfo{hashInfo("even", 0, 2), hashInfo("odd1", 1), hashInfo("odd3", 3)},
			sourceOwned: func(pk any) bool { return residueOf(pk, 2) == 1 },
		},
		{
			// A shard owning four residues splits its set at the same M.
			name:    "a set of four divided at modulus 8",
			modulus: 8,
			targets: []*datapb.SplitShardTaskTarget{
				{Vchannel: "lo", Buckets: []uint64{0, 2}},
				{Vchannel: "hi", Buckets: []uint64{4, 6}},
			},
			channels:    []string{"odd", "lo", "hi"},
			infos:       []*schemapb.CollectionShardInfo{hashInfo("odd", 1, 3, 5, 7), hashInfo("lo", 0, 2), hashInfo("hi", 4, 6)},
			sourceOwned: func(pk any) bool { return residueOf(pk, 2) == 0 },
		},
	}
}

// TestPartitionerAgreesWithTheWritePath is the parity the whole rewrite rests
// on: every row of the input lands on exactly the target the post-split routing
// table -- the one the proxy routes that key's inserts and deletes by -- names.
func TestPartitionerAgreesWithTheWritePath(t *testing.T) {
	for _, shape := range postSplitShapes(t) {
		t.Run(shape.name, func(t *testing.T) {
			table, err := routing.TableFromMeta(shape.channels, shape.infos, shape.modulus)
			require.NoError(t, err)
			p, err := newHashSplitPartitioner(shape.modulus, shape.targets)
			require.NoError(t, err)

			keys := make([]any, 0, 4000)
			for i := int64(0); i < 2000; i++ {
				keys = append(keys, i*7919, fmt.Sprintf("pk-%d", i))
			}
			perTarget := map[string]int{}
			for _, pk := range keys {
				if !shape.sourceOwned(pk) {
					continue
				}
				idx, err := p.Route(pk)
				require.NoError(t, err)
				want, err := table.VChannelOfPK(pk)
				require.NoError(t, err)
				assert.Equal(t, want, p.TargetVChannel(idx), "pk %v", pk)
				perTarget[want]++
			}
			assert.Len(t, perTarget, 2, "both targets must own keys for the check to mean anything")
		})
	}
}
