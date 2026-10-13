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

// flush returns a flush of vchannel "v" of collection 1 with an Int64
// primary key and SST base path "b".
func flush(term int64, tick uint64, ids ...int64) Flush {
	return Flush{
		CollectionID: 1, VChannel: "v", SSTBasePath: "b", PKDataType: schemapb.DataType_Int64,
		PChannelTerm: term, AddSSTs: ssts(ids...), CheckpointTimeTick: tick,
	}
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

func TestApplyPChannelClaim(t *testing.T) {
	first, changed, err := ApplyPChannelClaim(nil, "p", 5)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.EqualValues(t, 5, first.GetPchannelTerm())

	same, changed, err := ApplyPChannelClaim(first, "p", 5)
	require.NoError(t, err)
	assert.False(t, changed, "the same term is idempotent")
	assert.Same(t, first, same)

	up, changed, err := ApplyPChannelClaim(first, "p", 6)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.EqualValues(t, 6, up.GetPchannelTerm())
	assert.EqualValues(t, 5, first.GetPchannelTerm(), "input must not change")

	_, _, err = ApplyPChannelClaim(up, "p", 5)
	assert.True(t, errors.Is(err, ErrFenced))
}

func TestCheckFlushTerm(t *testing.T) {
	rec := &pkindexpb.PChannelClaim{PchannelTerm: 5}
	assert.NoError(t, CheckFlushTerm(rec, "p", 5))
	assert.True(t, errors.Is(CheckFlushTerm(rec, "p", 4), ErrFenced))
	assert.True(t, errors.Is(CheckFlushTerm(rec, "p", 6), ErrNotClaimed))
	assert.True(t, errors.Is(CheckFlushTerm(nil, "p", 5), ErrNotClaimed))
}

// The first flush of a vchannel creates version 1 from the fields of the flush.
func TestApplyFlushCreatesFirstVersion(t *testing.T) {
	m, changed, err := ApplyFlush(nil, flush(5, 100, 2, 1))
	require.NoError(t, err)
	require.True(t, changed)
	assert.EqualValues(t, 1, m.GetVersion())
	assert.EqualValues(t, 1, m.GetCollectionId())
	assert.Equal(t, "v", m.GetVchannel())
	assert.Equal(t, "b", m.GetSstBasePath())
	assert.Equal(t, schemapb.DataType_Int64, m.GetPkDataType())
	assert.EqualValues(t, 5, m.GetLastFlushTerm())
	assert.EqualValues(t, 100, m.GetCheckpointTimeTick())
	assert.Equal(t, []int64{2, 1}, ids(m))
}

func TestApplyFlush(t *testing.T) {
	m1, changed, err := ApplyFlush(nil, flush(5, 100, 2, 1))
	require.NoError(t, err)
	require.True(t, changed)

	m2, _, err := ApplyFlush(m1, flush(5, 200, 3))
	require.NoError(t, err)
	assert.Equal(t, []int64{3, 2, 1}, ids(m2), "new SSTs go first")
	assert.EqualValues(t, 2, m2.GetVersion())

	// a retry of an applied flush returns the manifest unchanged
	again, changed, err := ApplyFlush(m2, flush(5, 200, 3))
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Same(t, m2, again)

	// the last flush term with an old checkpoint is an applied retry, whatever its SSTs are
	old, changed, err := ApplyFlush(m2, flush(5, 150, 9))
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Same(t, m2, old)
	_, _, err = ApplyFlush(m2, flush(5, 300, 1))
	assert.True(t, errors.Is(err, ErrDuplicateSST))

	other := flush(6, 300, 9)
	other.PKDataType = schemapb.DataType_VarChar
	_, _, err = ApplyFlush(m2, other)
	assert.True(t, errors.Is(err, ErrSchemaMismatch))

	up, _, err := ApplyFlush(m2, flush(6, 300, 9))
	require.NoError(t, err)
	assert.EqualValues(t, 6, up.GetLastFlushTerm(), "the flush term becomes the last flush term")
	assert.EqualValues(t, 5, m2.GetLastFlushTerm(), "input must not change")
	assert.Equal(t, []int64{3, 2, 1}, ids(m2), "input must not change")
}

func TestApplyFlushRetryAfterCompaction(t *testing.T) {
	m, _, err := ApplyFlush(nil, flush(5, 100, 2, 1))
	require.NoError(t, err)
	c, _, err := ApplyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: ssts(10)})
	require.NoError(t, err)

	again, changed, err := ApplyFlush(c, flush(5, 100, 2, 1))
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Same(t, c, again)
}

// A flush whose checkpoint does not move forward and whose term is not the
// last flush term is a regression, not a retry.
func TestApplyFlushOtherTermOldCheckpoint(t *testing.T) {
	m, _, err := ApplyFlush(nil, flush(5, 100, 1))
	require.NoError(t, err)
	for _, term := range []int64{4, 6} {
		for _, tick := range []uint64{100, 50} {
			_, _, err = ApplyFlush(m, flush(term, tick, 1))
			assert.True(t, errors.Is(err, ErrCheckpointRegressed), "term %d tick %d", term, tick)
		}
	}
}

func TestApplyFlushCheckpointOnly(t *testing.T) {
	m, _, err := ApplyFlush(nil, flush(5, 100, 1))
	require.NoError(t, err)

	adv, changed, err := ApplyFlush(m, flush(5, 200))
	require.NoError(t, err)
	assert.True(t, changed)
	assert.EqualValues(t, m.GetVersion()+1, adv.GetVersion())
	assert.EqualValues(t, 200, adv.GetCheckpointTimeTick())
	assert.Equal(t, []int64{1}, ids(adv))

	// an empty flush that does not move the checkpoint is an applied retry
	same, changed, err := ApplyFlush(adv, flush(5, 200))
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Same(t, adv, same)
}

func TestApplyFlushDuplicateInsideAddSSTs(t *testing.T) {
	_, _, err := ApplyFlush(nil, flush(5, 100, 7, 7))
	assert.True(t, errors.Is(err, ErrDuplicateSST))
}

func TestApplyCompaction(t *testing.T) {
	m, _, err := ApplyFlush(nil, flush(1, 10, 4, 3, 2, 1))
	require.NoError(t, err)

	c, changed, err := ApplyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: ssts(10)})
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, []int64{4, 3, 10}, ids(c))
	assert.EqualValues(t, m.GetVersion()+1, c.GetVersion())
	assert.EqualValues(t, 10, c.GetCheckpointTimeTick())
	assert.Equal(t, []int64{4, 3, 2, 1}, ids(m), "input must not change")

	for _, in := range [][]int64{{3, 1}, {1, 2}, {5}, {}} {
		_, _, err := ApplyCompaction(m, Compaction{Inputs: in, Outputs: ssts(10)})
		assert.True(t, errors.Is(err, ErrStaleEdit), "%v", in)
	}
	_, _, err = ApplyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: ssts(4)})
	assert.True(t, errors.Is(err, ErrDuplicateSST))
	_, _, err = ApplyCompaction(nil, Compaction{Inputs: []int64{1}, Outputs: ssts(10)})
	assert.True(t, errors.Is(err, ErrNotClaimed))
}

// A retry of an applied compaction finds none of its inputs and all of its
// outputs in the list: it returns cur unchanged.
func TestApplyCompactionRetryOfApplied(t *testing.T) {
	m, _, err := ApplyFlush(nil, flush(1, 10, 4, 3, 2, 1))
	require.NoError(t, err)
	edit := Compaction{Inputs: []int64{2, 1}, Outputs: ssts(11, 10)}
	c, changed, err := ApplyCompaction(m, edit)
	require.NoError(t, err)
	require.True(t, changed)

	again, changed, err := ApplyCompaction(c, edit)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Same(t, c, again)

	// a flush after the compaction does not change the result
	f, _, err := ApplyFlush(c, flush(1, 20, 20))
	require.NoError(t, err)
	again, changed, err = ApplyCompaction(f, edit)
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Same(t, f, again)
}

// Every other mismatch of the inputs and the outputs is a stale edit.
func TestApplyCompactionMixedIsStale(t *testing.T) {
	m, _, err := ApplyFlush(nil, flush(1, 10, 4, 3, 2, 1))
	require.NoError(t, err)
	// list after this compaction: 4 3 11 10
	c, _, err := ApplyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: ssts(11, 10)})
	require.NoError(t, err)

	cases := map[string]Compaction{
		"some inputs present, outputs present":  {Inputs: []int64{3, 2}, Outputs: ssts(11, 10)},
		"some inputs present, outputs absent":   {Inputs: []int64{3, 2}, Outputs: ssts(30)},
		"inputs present not contiguous":         {Inputs: []int64{4, 11}, Outputs: ssts(30)},
		"inputs absent, outputs absent":         {Inputs: []int64{2, 1}, Outputs: ssts(30)},
		"inputs absent, outputs partly present": {Inputs: []int64{2, 1}, Outputs: ssts(11, 30)},
		"no inputs, outputs present":            {Inputs: []int64{}, Outputs: ssts(11, 10)},
	}
	for name, edit := range cases {
		_, _, err := ApplyCompaction(c, edit)
		assert.True(t, errors.Is(err, ErrStaleEdit), name)
	}
}

func TestApplyCompactionDuplicateInsideOutputs(t *testing.T) {
	m, _, err := ApplyFlush(nil, flush(1, 10, 2, 1))
	require.NoError(t, err)
	_, _, err = ApplyCompaction(m, Compaction{Inputs: []int64{2, 1}, Outputs: ssts(10, 10)})
	assert.True(t, errors.Is(err, ErrDuplicateSST))
}

// Flush and Compaction commute: applying all compactions before all flushes, or
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
		start, _, err := ApplyFlush(nil, flush(1, 10, base...))
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
			flushes[i] = flush(1, uint64(20+10*i))
			flushes[i].AddSSTs = fresh(r.Intn(3))
		}

		// compactions take disjoint contiguous runs of the base list
		var compacts []Compaction
		for pos, want := 0, 1+r.Intn(3); pos < n && len(compacts) < want; {
			from := pos + r.Intn(n-pos)
			to := from + 1 + r.Intn(n-from)
			compacts = append(compacts, Compaction{Inputs: base[from:to], Outputs: fresh(1 + r.Intn(2))})
			pos = to + 1
		}

		compactFirst := start
		for _, c := range compacts {
			compactFirst, _, err = ApplyCompaction(compactFirst, c)
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
				mixed, _, err = ApplyCompaction(mixed, compacts[ci])
				ci++
			}
			require.NoError(t, err)
		}

		assert.True(t, proto.Equal(compactFirst, mixed), "round %d: %v vs %v", round, ids(compactFirst), ids(mixed))
	}
}
