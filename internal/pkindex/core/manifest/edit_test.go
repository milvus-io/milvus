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

package manifest

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
)

func claimed(t *testing.T, term int64) *pkindexpb.Manifest {
	m, changed, err := ApplyClaim(nil, Claim{CollectionID: 1, VChannel: "v", PChannelTerm: term, PKDataType: schemapb.DataType_Int64, SSTBasePath: "b"})
	require.NoError(t, err)
	require.True(t, changed)
	return m
}

func ssts(ids ...int64) []*pkindexpb.SSTRef {
	out := make([]*pkindexpb.SSTRef, 0, len(ids))
	for _, id := range ids {
		out = append(out, &pkindexpb.SSTRef{Id: id})
	}
	return out
}

func ids(m *pkindexpb.Manifest) []int64 {
	out := []int64{}
	for _, s := range m.GetSsts() {
		out = append(out, s.GetId())
	}
	return out
}

func TestApplyClaim(t *testing.T) {
	m := claimed(t, 5)
	assert.EqualValues(t, 1, m.GetVersion())
	assert.EqualValues(t, 5, m.GetPchannelTerm())
	assert.Equal(t, "b", m.GetSstBasePath())

	same, changed, err := ApplyClaim(m, Claim{PChannelTerm: 5, PKDataType: schemapb.DataType_Int64})
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Same(t, m, same)

	up, changed, err := ApplyClaim(m, Claim{PChannelTerm: 6, PKDataType: schemapb.DataType_Int64})
	require.NoError(t, err)
	assert.True(t, changed)
	assert.EqualValues(t, 2, up.GetVersion())
	assert.EqualValues(t, 6, up.GetPchannelTerm())
	assert.EqualValues(t, 5, m.GetPchannelTerm(), "input must not change")

	_, _, err = ApplyClaim(up, Claim{PChannelTerm: 5, PKDataType: schemapb.DataType_Int64})
	assert.True(t, errors.Is(err, ErrFenced))
	_, _, err = ApplyClaim(up, Claim{PChannelTerm: 7, PKDataType: schemapb.DataType_VarChar})
	assert.True(t, errors.Is(err, ErrSchemaMismatch))
	_, _, err = ApplyClaim(up, Claim{PChannelTerm: 5, PKDataType: schemapb.DataType_VarChar})
	assert.True(t, errors.Is(err, ErrFenced), "a stale owner learns it is fenced before the type check")
}

func TestApplyFlush(t *testing.T) {
	m := claimed(t, 5)
	_, _, err := ApplyFlush(nil, Flush{PChannelTerm: 5})
	assert.True(t, errors.Is(err, ErrNotClaimed))

	m1, changed, err := ApplyFlush(m, Flush{PChannelTerm: 5, AddSSTs: ssts(2, 1), CheckpointTimeTick: 100})
	require.NoError(t, err)
	require.True(t, changed)
	assert.Equal(t, []int64{2, 1}, ids(m1))
	assert.EqualValues(t, 100, m1.GetCheckpointTimeTick())

	m2, _, err := ApplyFlush(m1, Flush{PChannelTerm: 5, AddSSTs: ssts(3), CheckpointTimeTick: 200})
	require.NoError(t, err)
	assert.Equal(t, []int64{3, 2, 1}, ids(m2), "new SSTs go first")

	// a retry of an applied flush returns the manifest unchanged
	again, changed, err := ApplyFlush(m2, Flush{PChannelTerm: 5, AddSSTs: ssts(3), CheckpointTimeTick: 200})
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Same(t, m2, again)

	// the same term with an old checkpoint is an applied retry, whatever its SSTs are
	old, changed, err := ApplyFlush(m2, Flush{PChannelTerm: 5, AddSSTs: ssts(9), CheckpointTimeTick: 150})
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Same(t, m2, old)
	_, _, err = ApplyFlush(m2, Flush{PChannelTerm: 5, AddSSTs: ssts(1), CheckpointTimeTick: 300})
	assert.True(t, errors.Is(err, ErrDuplicateSST))
	_, _, err = ApplyFlush(m2, Flush{PChannelTerm: 4, AddSSTs: ssts(9), CheckpointTimeTick: 300})
	assert.True(t, errors.Is(err, ErrFenced))

	up, _, err := ApplyFlush(m2, Flush{PChannelTerm: 6, AddSSTs: ssts(9), CheckpointTimeTick: 300})
	require.NoError(t, err)
	assert.EqualValues(t, 6, up.GetPchannelTerm(), "a higher term is accepted and recorded")
	assert.EqualValues(t, 5, m2.GetPchannelTerm(), "input must not change")
	assert.Equal(t, []int64{3, 2, 1}, ids(m2), "input must not change")
}

func TestApplyFlushRetryAfterCompaction(t *testing.T) {
	m, _, err := ApplyFlush(claimed(t, 5), Flush{PChannelTerm: 5, AddSSTs: ssts(2, 1), CheckpointTimeTick: 100})
	require.NoError(t, err)
	c, _, err := ApplyCompact(m, Compact{Inputs: []int64{2, 1}, Outputs: ssts(10)})
	require.NoError(t, err)

	again, changed, err := ApplyFlush(c, Flush{PChannelTerm: 5, AddSSTs: ssts(2, 1), CheckpointTimeTick: 100})
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Same(t, c, again)
}

func TestApplyFlushHigherTermOldCheckpoint(t *testing.T) {
	m, _, err := ApplyFlush(claimed(t, 5), Flush{PChannelTerm: 5, AddSSTs: ssts(1), CheckpointTimeTick: 100})
	require.NoError(t, err)
	for _, tick := range []uint64{100, 50} {
		_, _, err = ApplyFlush(m, Flush{PChannelTerm: 6, AddSSTs: ssts(1), CheckpointTimeTick: tick})
		assert.True(t, errors.Is(err, ErrCheckpointRegressed), "%d", tick)
	}
}

func TestApplyFlushCheckpointOnly(t *testing.T) {
	m, _, err := ApplyFlush(claimed(t, 5), Flush{PChannelTerm: 5, AddSSTs: ssts(1), CheckpointTimeTick: 100})
	require.NoError(t, err)

	adv, changed, err := ApplyFlush(m, Flush{PChannelTerm: 5, CheckpointTimeTick: 200})
	require.NoError(t, err)
	assert.True(t, changed)
	assert.EqualValues(t, m.GetVersion()+1, adv.GetVersion())
	assert.EqualValues(t, 200, adv.GetCheckpointTimeTick())
	assert.Equal(t, []int64{1}, ids(adv))

	// an empty flush that does not move the checkpoint is an applied retry
	same, changed, err := ApplyFlush(adv, Flush{PChannelTerm: 5, CheckpointTimeTick: 200})
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Same(t, adv, same)
}

func TestApplyFlushDuplicateInsideAddSSTs(t *testing.T) {
	m := claimed(t, 5)
	_, _, err := ApplyFlush(m, Flush{PChannelTerm: 5, AddSSTs: ssts(7, 7), CheckpointTimeTick: 100})
	assert.True(t, errors.Is(err, ErrDuplicateSST))
}

func TestApplyCompact(t *testing.T) {
	m, _, err := ApplyFlush(claimed(t, 1), Flush{PChannelTerm: 1, AddSSTs: ssts(4, 3, 2, 1), CheckpointTimeTick: 10})
	require.NoError(t, err)

	c, changed, err := ApplyCompact(m, Compact{Inputs: []int64{2, 1}, Outputs: ssts(10)})
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, []int64{4, 3, 10}, ids(c))
	assert.EqualValues(t, m.GetVersion()+1, c.GetVersion())
	assert.EqualValues(t, 10, c.GetCheckpointTimeTick())
	assert.Equal(t, []int64{4, 3, 2, 1}, ids(m), "input must not change")

	for _, in := range [][]int64{{3, 1}, {1, 2}, {5}, {}} {
		_, _, err := ApplyCompact(m, Compact{Inputs: in, Outputs: ssts(10)})
		assert.True(t, errors.Is(err, ErrStaleEdit), "%v", in)
	}
	_, _, err = ApplyCompact(m, Compact{Inputs: []int64{2, 1}, Outputs: ssts(4)})
	assert.True(t, errors.Is(err, ErrDuplicateSST))
	_, _, err = ApplyCompact(nil, Compact{Inputs: []int64{1}, Outputs: ssts(10)})
	assert.True(t, errors.Is(err, ErrNotClaimed))
}

func TestApplyCompactDuplicateInsideOutputs(t *testing.T) {
	m, _, err := ApplyFlush(claimed(t, 1), Flush{PChannelTerm: 1, AddSSTs: ssts(2, 1), CheckpointTimeTick: 10})
	require.NoError(t, err)
	_, _, err = ApplyCompact(m, Compact{Inputs: []int64{2, 1}, Outputs: ssts(10, 10)})
	assert.True(t, errors.Is(err, ErrDuplicateSST))
}

// Flush and Compact commute: applying all compactions before all flushes, or
// interleaving them in any way that keeps each kind's own order, gives the same
// manifest.
func TestFlushAndCompactCommute(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for round := 0; round < 500; round++ {
		n := 2 + r.Intn(8)
		base := make([]int64, n)
		for i := range base {
			base[i] = int64(n - i)
		}
		start, _, err := ApplyFlush(claimed(t, 1), Flush{PChannelTerm: 1, AddSSTs: ssts(base...), CheckpointTimeTick: 10})
		require.NoError(t, err)

		nextID := int64(1000)
		fresh := func(count int) []*pkindexpb.SSTRef {
			out := make([]*pkindexpb.SSTRef, 0, count)
			for i := 0; i < count; i++ {
				out = append(out, &pkindexpb.SSTRef{Id: nextID})
				nextID++
			}
			return out
		}

		flushes := make([]Flush, 1+r.Intn(4))
		for i := range flushes {
			flushes[i] = Flush{PChannelTerm: 1, AddSSTs: fresh(r.Intn(3)), CheckpointTimeTick: uint64(20 + 10*i)}
		}

		// compactions take disjoint contiguous runs of the base list
		var compacts []Compact
		for pos, want := 0, 1+r.Intn(3); pos < n && len(compacts) < want; {
			from := pos + r.Intn(n-pos)
			to := from + 1 + r.Intn(n-from)
			compacts = append(compacts, Compact{Inputs: base[from:to], Outputs: fresh(1 + r.Intn(2))})
			pos = to + 1
		}

		compactFirst := start
		for _, c := range compacts {
			compactFirst, _, err = ApplyCompact(compactFirst, c)
			require.NoError(t, err)
		}
		for _, f := range flushes {
			compactFirst, _, err = ApplyFlush(compactFirst, f)
			require.NoError(t, err)
		}

		mixed := start
		for fi, ci := 0, 0; fi < len(flushes) || ci < len(compacts); {
			if ci == len(compacts) || (fi < len(flushes) && r.Intn(2) == 0) {
				mixed, _, err = ApplyFlush(mixed, flushes[fi])
				fi++
			} else {
				mixed, _, err = ApplyCompact(mixed, compacts[ci])
				ci++
			}
			require.NoError(t, err)
		}

		assert.True(t, proto.Equal(compactFirst, mixed), "round %d: %v vs %v", round, ids(compactFirst), ids(mixed))
	}
}
