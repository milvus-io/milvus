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

package milvusclient

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/roaringfilter"
)

func TestNewRoaringBitmapBlobAcceptsSignedIntegerSlices(t *testing.T) {
	tests := map[string]any{
		"int":   []int{-1, 0, 1, 42},
		"int8":  []int8{math.MinInt8, -1, 0, math.MaxInt8},
		"int16": []int16{math.MinInt16, -1, 0, math.MaxInt16},
		"int32": []int32{math.MinInt32, -1, 0, math.MaxInt32},
		"int64": []int64{math.MinInt64, -1, 0, math.MaxInt64},
	}

	for name, members := range tests {
		t.Run(name, func(t *testing.T) {
			blob, err := NewRoaringBitmapBlob(members)
			require.NoError(t, err)
			filter, err := roaringfilter.Parse(blob)
			require.NoError(t, err)
			require.Equal(t, uint64(4), filter.Cardinality())
			require.True(t, filter.ContainsInt64(-1))
			require.True(t, filter.ContainsInt64(0))
		})
	}
}

func TestNewRoaringBitmapBlobDeduplicatesMembers(t *testing.T) {
	blob, err := NewRoaringBitmapBlob([]int64{-1, -1, 42, 42})
	require.NoError(t, err)

	filter, err := roaringfilter.Parse(blob)
	require.NoError(t, err)
	require.Equal(t, uint64(2), filter.Cardinality())
	require.True(t, filter.ContainsInt64(-1))
	require.True(t, filter.ContainsInt64(42))
}

func TestNewRoaringBitmapBlobRejectsUnsupportedMemberTypes(t *testing.T) {
	for _, members := range []any{
		[]uint64{1},
		[]float64{1},
		[]string{"1"},
		[]bool{true},
	} {
		_, err := NewRoaringBitmapBlob(members)
		require.Error(t, err, "members type %T must be rejected", members)
	}
}

func TestRoaringBitmapBlobTemplateMarshaling(t *testing.T) {
	blob, err := NewRoaringBitmapBlob([]int64{-1, 0, 42})
	require.NoError(t, err)

	value, err := any2TmplValue(blob)
	require.NoError(t, err)
	bytesValue, ok := value.GetVal().(*schemapb.TemplateValue_BytesVal)
	require.True(t, ok, "RoaringBitmapBlob must marshal as native protobuf bytes")
	require.Equal(t, []byte(blob), bytesValue.BytesVal)
}
