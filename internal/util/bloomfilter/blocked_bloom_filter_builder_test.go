// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bloomfilter

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const testFP = 0.001

func newTestBFBuilder(t *testing.T, capacityHint uint) *BlockedBloomFilterBuilder {
	bf, err := NewBlockedBloomFilterBuilder(capacityHint, testFP)
	require.NoError(t, err)
	return bf
}

func int64Key(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func TestNewBlockedBloomFilterBuilder(t *testing.T) {
	for _, fp := range []float64{-0.1, 0, 1, 1.5} {
		t.Run(fmt.Sprintf("invalid fp %v", fp), func(t *testing.T) {
			bf, err := NewBlockedBloomFilterBuilder(100, fp)
			require.Nil(t, bf)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}

	t.Run("type is blocked", func(t *testing.T) {
		builder := newTestBFBuilder(t, 100)
		builder.Add(int64Key(1))
		bf, err := builder.Build()
		require.NoError(t, err)
		require.Equal(t, BlockedBF, bf.Type())
	})
}

func TestBlockedBloomFilterBuilderBuild(t *testing.T) {
	t.Run("no data", func(t *testing.T) {
		bf := newTestBFBuilder(t, 4)

		built, err := bf.Build()

		require.Nil(t, built)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	})

	for _, tc := range []struct {
		n    int
		hint uint
	}{
		{n: 1, hint: 0},
		{n: 1, hint: 4},
		{n: 5, hint: 4},
		{n: 1000, hint: 4},
		{n: 1000, hint: 100000},
		{n: builderHashBlockSize, hint: 0},
	} {
		n := tc.n
		t.Run(fmt.Sprintf("same as blocked bloom filter with %d rows and hint %d", n, tc.hint), func(t *testing.T) {
			deferred := newTestBFBuilder(t, tc.hint)
			for i := 0; i < n; i++ {
				deferred.Add(int64Key(int64(i)))
				deferred.AddString(fmt.Sprintf("pk-%d", i))
			}
			// Both Add and AddString count as rows, so the capacity is 2n.
			want := newBlockedBloomFilter(uint(2*n), testFP)
			for i := 0; i < n; i++ {
				want.Add(int64Key(int64(i)))
				want.AddString(fmt.Sprintf("pk-%d", i))
			}

			built, err := deferred.Build()
			require.NoError(t, err)
			got, err := built.MarshalJSON()
			require.NoError(t, err)
			wantJSON, err := want.MarshalJSON()
			require.NoError(t, err)

			require.Equal(t, wantJSON, got)
		})
	}

	t.Run("decoded filter contains added data", func(t *testing.T) {
		bf := newTestBFBuilder(t, 4)
		for i := 0; i < 100; i++ {
			bf.Add(int64Key(int64(i)))
			bf.AddString(fmt.Sprintf("pk-%d", i))
		}
		built, err := bf.Build()
		require.NoError(t, err)
		data, err := built.MarshalJSON()
		require.NoError(t, err)

		decoded, err := UnmarshalJSON(data, BlockedBF)

		require.NoError(t, err)
		require.Equal(t, BlockedBF, decoded.Type())
		require.Equal(t, newBlockedBloomFilter(200, testFP).Cap(), decoded.Cap())
		for i := 0; i < 100; i++ {
			require.True(t, decoded.Test(int64Key(int64(i))))
			require.True(t, decoded.TestString(fmt.Sprintf("pk-%d", i)))
			require.True(t, decoded.TestLocations(Locations(int64Key(int64(i)), decoded.K(), BlockedBF)))
		}
	})

	t.Run("build twice and continue adding", func(t *testing.T) {
		bf := newTestBFBuilder(t, 4)
		for i := 0; i < 10; i++ {
			bf.Add(int64Key(int64(i)))
		}

		first, err := bf.Build()
		require.NoError(t, err)
		firstJSON, err := first.MarshalJSON()
		require.NoError(t, err)
		second, err := bf.Build()
		require.NoError(t, err)
		secondJSON, err := second.MarshalJSON()
		require.NoError(t, err)

		require.Equal(t, firstJSON, secondJSON)

		for i := 10; i < 1000; i++ {
			bf.Add(int64Key(int64(i)))
		}
		expanded, err := bf.Build()
		require.NoError(t, err)
		require.Equal(t, newBlockedBloomFilter(1000, testFP).Cap(), expanded.Cap())
		for i := 0; i < 1000; i++ {
			require.True(t, expanded.Test(int64Key(int64(i))))
		}
		unchanged, err := first.MarshalJSON()
		require.NoError(t, err)
		require.Equal(t, firstJSON, unchanged)
	})
}

func TestBlockedBloomFilterBuilderReserve(t *testing.T) {
	bf := newTestBFBuilder(t, 1000000)
	require.NoError(t, bf.Reserve(100000))
	require.Zero(t, bf.hashes.Len())
	require.Error(t, bf.Reserve(-1))
	for i := 0; i < 100000; i++ {
		bf.Add(int64Key(int64(i)))
	}
	blocks := 0
	for block := range bf.hashes.Blocks() {
		blocks++
		require.Len(t, block, 100000)
	}
	require.Equal(t, 1, blocks)
}
