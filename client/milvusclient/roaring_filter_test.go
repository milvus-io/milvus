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
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/membership/roaringfilter"
)

// decodeBlob deserializes the portable Roaring64 body of an MRB1 blob. The
// structural validator lives on the server (pkg/util/roaringfilter), so these
// SDK tests decode directly rather than through a client-side copy of it.
func decodeBlob(t *testing.T, blob RoaringBitmapBlob) *roaring64.Bitmap {
	t.Helper()
	// The envelope is Build's output too, and the structural validator lives on
	// the server, so assert the header here rather than decoding straight past it.
	require.Equal(t, roaringfilter.Magic, string(blob[:4]))
	require.Equal(t, roaringfilter.Version, binary.LittleEndian.Uint16(blob[4:6]))
	require.Equal(t, roaringfilter.FormatPortableRoaring64, binary.LittleEndian.Uint16(blob[6:8]))
	require.Zero(t, binary.LittleEndian.Uint64(blob[24:32]), "reserved must be zero")

	bitmap := roaring64.New()
	consumed, err := bitmap.ReadFrom(bytes.NewReader(blob[roaringfilter.HeaderSize:]))
	require.NoError(t, err)
	require.Equal(t, int64(len(blob)-roaringfilter.HeaderSize), consumed,
		"body must be consumed exactly, with no trailing bytes")
	return bitmap
}

func containsSigned(bitmap *roaring64.Bitmap, member int64) bool {
	return bitmap.Contains(uint64(member))
}

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
			bitmap := decodeBlob(t, blob)
			require.Equal(t, uint64(4), bitmap.GetCardinality())
			require.True(t, containsSigned(bitmap, -1))
			require.True(t, containsSigned(bitmap, 0))
		})
	}
}

func TestNewRoaringBitmapBlobDeduplicatesMembers(t *testing.T) {
	blob, err := NewRoaringBitmapBlob([]int64{-1, -1, 42, 42})
	require.NoError(t, err)

	bitmap := decodeBlob(t, blob)
	require.Equal(t, uint64(2), bitmap.GetCardinality())
	require.True(t, containsSigned(bitmap, -1))
	require.True(t, containsSigned(bitmap, 42))
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

var benchmarkRoaringBitmapBlobSink RoaringBitmapBlob

func makeSignedMembers[T ~int32 | ~int64](n int, shuffled bool) []T {
	members := make([]T, n)
	for i := range members {
		value := i
		if shuffled {
			value = (i*2053)%n - n/2
		}
		members[i] = T(value)
	}
	return members
}

func roaringBitmapBlobAllocsPerRun(members any) (float64, error) {
	var err error
	allocs := testing.AllocsPerRun(20, func() {
		benchmarkRoaringBitmapBlobSink, err = NewRoaringBitmapBlob(members)
	})
	return allocs, err
}

func TestNewRoaringBitmapBlobNarrowAllocationParity(t *testing.T) {
	for _, shuffled := range []bool{false, true} {
		name := "ordered"
		if shuffled {
			name = "shuffled"
		}
		t.Run(name, func(t *testing.T) {
			int32Members := makeSignedMembers[int32](4096, shuffled)
			int64Members := makeSignedMembers[int64](4096, shuffled)
			int32Allocs, err := roaringBitmapBlobAllocsPerRun(int32Members)
			require.NoError(t, err)
			int64Allocs, err := roaringBitmapBlobAllocsPerRun(int64Members)
			require.NoError(t, err)
			require.Equal(t, int64Allocs, int32Allocs,
				"a narrow input must not allocate an intermediate []int64")
		})
	}
}

func benchmarkNewRoaringBitmapBlob(b *testing.B, members any) {
	b.Helper()
	blob, err := NewRoaringBitmapBlob(members)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkRoaringBitmapBlobSink = blob
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		blob, err := NewRoaringBitmapBlob(members)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkRoaringBitmapBlobSink = blob
	}
}

func BenchmarkNewRoaringBitmapBlobInt32Shuffled(b *testing.B) {
	members := makeSignedMembers[int32](64*1024, true)
	benchmarkNewRoaringBitmapBlob(b, members)
}

func BenchmarkNewRoaringBitmapBlobInt64Shuffled(b *testing.B) {
	members := makeSignedMembers[int64](64*1024, true)
	benchmarkNewRoaringBitmapBlob(b, members)
}
