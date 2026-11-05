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

package storage

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestBM25StatsLegacyWireCompatibility(t *testing.T) {
	// A flat version-0 blob, including duplicate, zero and negative DF records.
	records := [][2]uint32{{0, 2}, {math.MaxUint32, 4}, {17, 0}, {42, math.MaxUint32}, {0, 3}}
	var legacy bytes.Buffer
	for _, field := range []any{int32(0), int64(9), int64(27)} {
		require.NoError(t, binary.Write(&legacy, common.Endian, field))
	}
	for _, record := range records {
		require.NoError(t, binary.Write(&legacy, common.Endian, record))
	}
	want := map[uint32]int32{0: 5, math.MaxUint32: 4, 17: 0, 42: -1}
	for _, streaming := range []bool{false, true} {
		stats := NewBM25Stats()
		if streaming {
			require.NoError(t, stats.DeserializeFromReader(bytes.NewReader(legacy.Bytes())))
		} else {
			require.NoError(t, stats.Deserialize(legacy.Bytes()))
		}
		require.Equal(t, want, flattenBM25Counts(stats.rowsWithToken))
		require.Equal(t, int64(9), stats.NumRow())
		require.Equal(t, int64(27), stats.NumToken())
		data, err := stats.Serialize()
		require.NoError(t, err)
		var writer bytes.Buffer
		require.NoError(t, stats.SerializeToWriter(&writer))
		// Decode each output with the old flat-record contract, independently of BM25Stats.
		for _, blob := range [][]byte{data, writer.Bytes()} {
			reader := bytes.NewReader(blob)
			var version int32
			var rows, tokens int64
			require.NoError(t, binary.Read(reader, common.Endian, &version))
			require.NoError(t, binary.Read(reader, common.Endian, &rows))
			require.NoError(t, binary.Read(reader, common.Endian, &tokens))
			require.Zero(t, version)
			require.Equal(t, int64(9), rows)
			require.Equal(t, int64(27), tokens)
			counts := map[uint32]int32{}
			for {
				var record [2]uint32
				err := binary.Read(reader, common.Endian, &record)
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				_, duplicate := counts[record[0]]
				require.False(t, duplicate)
				counts[record[0]] = int32(record[1])
			}
			require.Equal(t, want, counts)
		}
	}
}

func TestBM25StatsShardedArithmetic(t *testing.T) {
	const vocabulary = 1 << 17
	a, b := NewBM25Stats(), NewBM25Stats()
	expected := make(map[uint32]int32, vocabulary*2)
	for i := uint32(0); i < vocabulary; i++ {
		key := i * 2246822519
		a.Append(map[uint32]float32{key: 2})
		expected[key]++
		key = (i + vocabulary/2) * 2246822519
		b.AppendBytes(typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{key: 3}))
		expected[key]++
	}
	a.Merge(b)
	require.Equal(t, expected, flattenBM25Counts(a.rowsWithToken))
	require.Equal(t, int64(2*vocabulary), a.NumRow())
	require.Equal(t, int64(5*vocabulary), a.NumToken())

	clone := a.Clone()
	a.Minus(b)
	for i := uint32(0); i < vocabulary; i++ {
		key := (i + vocabulary/2) * 2246822519
		expected[key]--
		if expected[key] == 0 {
			delete(expected, key)
		}
	}
	require.Equal(t, expected, flattenBM25Counts(a.rowsWithToken))
	require.Equal(t, int64(vocabulary), a.NumRow())
	require.Equal(t, int64(2*vocabulary), a.NumToken())
	// Both directions must own their maps after cloning.
	clone.Minus(a)
	require.Equal(t, flattenBM25Counts(b.rowsWithToken), flattenBM25Counts(clone.rowsWithToken))
	require.Equal(t, expected, flattenBM25Counts(a.rowsWithToken))
}
