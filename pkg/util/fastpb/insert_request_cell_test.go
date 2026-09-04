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

package fastpb

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// encodeCell runs the arithmetic path end to end and returns the bytes it
// produces, mirroring how appendArrayArray drives it: classify, measure, then
// replay the measured payload through the writer.
func encodeCell(t *testing.T, cell *schemapb.ScalarField) ([]byte, bool) {
	t.Helper()
	plan, ok := classifyScalarCell(cell)
	if !ok {
		return nil, false
	}
	plan.payload = scalarCellPayload(cell, plan)

	size := plan.scalarCellSize()
	w := newInsertViewMarshalWriter(make([]byte, 0, size), nil)
	require.NoError(t, appendScalarCell(w, cell, plan))
	require.NoError(t, w.err)
	require.Equal(t, size, w.n, "scalarCellSize disagrees with the bytes written")
	return w.out, true
}

// assertCellMatchesProto is the core contract: for every cell the arithmetic
// path claims, its size must equal proto.Size and its bytes must equal
// proto.Marshal. A mismatch in size corrupts the enclosing length prefix, so
// this is checked separately from the bytes.
func assertCellMatchesProto(t *testing.T, cell *schemapb.ScalarField) {
	t.Helper()
	got, ok := encodeCell(t, cell)
	if !ok {
		t.Skip("cell is not on the arithmetic path")
	}

	want, err := proto.Marshal(cell)
	require.NoError(t, err)
	assert.Equal(t, len(want), len(got), "size mismatch: proto=%d arithmetic=%d", len(want), len(got))
	assert.Equal(t, want, got, "bytes differ from proto.Marshal")

	// Decoding what we wrote must also reproduce the original message. Equal
	// byte length with different content would otherwise slip through if both
	// assertions above were ever relaxed.
	var round schemapb.ScalarField
	require.NoError(t, proto.Unmarshal(got, &round))
	assert.True(t, proto.Equal(cell, &round), "round trip differs: want=%v got=%v", cell, &round)
}

func TestScalarCell_AllOneofs(t *testing.T) {
	cells := map[string]*schemapb.ScalarField{
		"bool": {Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false, true}}}},
		"int": {Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{
			// Negative int32 is sign-extended to a 10-byte varint.
			Data: []int32{0, 1, -1, math.MaxInt32, math.MinInt32, 127, 128},
		}}},
		"long": {Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{
			Data: []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 127, 128, 16383, 16384},
		}}},
		"float": {Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{
			Data: []float32{0, -0, 1.5, float32(math.Inf(1)), float32(math.Inf(-1))},
		}}},
		"double": {Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{
			Data: []float64{0, -0, 1.5, math.Inf(1), math.Inf(-1)},
		}}},
		"string": {Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{
			Data: []string{"", "a", "utf8 ✓ 中文", string(make([]byte, 300))},
		}}},
		"bytes": {Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{
			Data: [][]byte{nil, {}, {0x00}, make([]byte, 300)},
		}}},
		"json": {Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{
			Data: [][]byte{[]byte(`{"a":1}`), {}},
		}}},
		"geometry": {Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{
			Data: [][]byte{{0x01, 0x02}},
		}}},
		"timestamptz": {Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{
			Data: []int64{0, -1, math.MaxInt64},
		}}},
		"geometry_wkt": {Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{
			Data: []string{"POINT(1 2)", ""},
		}}},
		"mol": {Data: &schemapb.ScalarField_MolData{MolData: &schemapb.MolArray{Data: [][]byte{{0xff}}}}},
		"mol_smiles": {Data: &schemapb.ScalarField_MolSmilesData{MolSmilesData: &schemapb.MolSmilesArray{
			Data: []string{"CCO"},
		}}},
		"date": {Data: &schemapb.ScalarField_DateData{DateData: &schemapb.DateArray{
			Data: []int32{0, -1, math.MaxInt32},
		}}},
		"time": {Data: &schemapb.ScalarField_TimeData{TimeData: &schemapb.TimeArray{
			Data: []int64{0, -1, math.MaxInt64},
		}}},
	}

	for name, cell := range cells {
		t.Run(name, func(t *testing.T) {
			assertCellMatchesProto(t, cell)
		})
	}
}

// TestScalarCell_EmptyArrays pins the proto3 rule the arithmetic path has to
// reproduce by hand: an empty packed field is omitted entirely, leaving an
// empty array message, while the oneof itself is still written.
func TestScalarCell_EmptyArrays(t *testing.T) {
	cells := map[string]*schemapb.ScalarField{
		"empty bool":   {Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{}}},
		"empty long":   {Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{}}},
		"empty float":  {Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{}}},
		"empty string": {Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}}},
		"empty bytes":  {Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{}}},
		"one empty string": {Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{
			Data: []string{""},
		}}},
		"one empty bytes": {Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{
			Data: [][]byte{{}},
		}}},
	}

	for name, cell := range cells {
		t.Run(name, func(t *testing.T) {
			assertCellMatchesProto(t, cell)
		})
	}
}

func TestScalarCell_NilAndEmptyCell(t *testing.T) {
	t.Run("nil cell", func(t *testing.T) {
		plan, ok := classifyScalarCell(nil)
		require.True(t, ok)
		assert.Equal(t, scalarCellEmpty, plan.kind)
		assert.Equal(t, 0, plan.scalarCellSize())
	})

	t.Run("no oneof set", func(t *testing.T) {
		cell := &schemapb.ScalarField{}
		assertCellMatchesProto(t, cell)
		plan, ok := classifyScalarCell(cell)
		require.True(t, ok)
		assert.Equal(t, scalarCellEmpty, plan.kind)
	})
}

// TestScalarCell_FallbackCases covers every input the arithmetic path must
// refuse. Refusing is the safe outcome: the caller keeps using proto.Marshal
// on both the sizing and the writing side, so the two stay consistent.
func TestScalarCell_FallbackCases(t *testing.T) {
	t.Run("unknown fields", func(t *testing.T) {
		cell := &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
			LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
		}}
		unknown := protowire.AppendTag(nil, 999, protowire.VarintType)
		unknown = protowire.AppendVarint(unknown, 7)
		cell.ProtoReflect().SetUnknown(protoreflect.RawFields(unknown))

		_, ok := classifyScalarCell(cell)
		assert.False(t, ok, "a cell with unknown fields must not take the arithmetic path")
	})

	t.Run("packed nested unknown fields", func(t *testing.T) {
		array := &schemapb.LongArray{Data: []int64{1, 2, 3}}
		unknown := protowire.AppendTag(nil, 999, protowire.VarintType)
		unknown = protowire.AppendVarint(unknown, 11)
		array.ProtoReflect().SetUnknown(protoreflect.RawFields(unknown))
		cell := &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: array}}

		_, ok := classifyScalarCell(cell)
		assert.False(t, ok, "a packed nested message with unknown fields must use protobuf")
	})

	t.Run("repeated nested unknown fields", func(t *testing.T) {
		array := &schemapb.StringArray{Data: []string{"a", "b"}}
		unknown := protowire.AppendTag(nil, 998, protowire.BytesType)
		unknown = protowire.AppendBytes(unknown, []byte("future"))
		array.ProtoReflect().SetUnknown(protoreflect.RawFields(unknown))
		cell := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: array}}

		_, ok := classifyScalarCell(cell)
		assert.False(t, ok, "a repeated nested message with unknown fields must use protobuf")
	})

	t.Run("nested array", func(t *testing.T) {
		cell := &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{Data: []*schemapb.ScalarField{{}}},
		}}
		_, ok := classifyScalarCell(cell)
		assert.False(t, ok, "nested ArrayData must not take the arithmetic path")
	})

	t.Run("set but nil array", func(t *testing.T) {
		cell := &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: nil}}
		_, ok := classifyScalarCell(cell)
		assert.False(t, ok, "a set-but-nil oneof wrapper must not take the arithmetic path")
	})
}

func TestAppendArrayCell_ReplaysPayloadToken(t *testing.T) {
	unknown := protowire.AppendTag(nil, 999, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 7)
	fallbackArray := &schemapb.StringArray{Data: []string{"future"}}
	fallbackArray.ProtoReflect().SetUnknown(protoreflect.RawFields(unknown))

	for _, tc := range []struct {
		name  string
		cell  *schemapb.ScalarField
		token int
	}{
		{
			name: "arithmetic",
			cell: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: []int64{1, 128, -1}},
			}},
		},
		{
			name: "protobuf fallback",
			cell: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{
				StringData: fallbackArray,
			}},
			token: scalarCellProtoFallbackPayload,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.token != scalarCellProtoFallbackPayload {
				plan, ok := classifyScalarCell(tc.cell)
				require.True(t, ok)
				tc.token = scalarCellPayload(tc.cell, plan)
			}
			want, err := proto.Marshal(&schemapb.ArrayArray{Data: []*schemapb.ScalarField{tc.cell}})
			require.NoError(t, err)

			w := newInsertViewMarshalWriter(make([]byte, 0, len(want)), nil)
			require.NoError(t, appendArrayCell(w, tc.cell, tc.token))
			require.NoError(t, w.err)
			assert.Equal(t, 0, w.planIndex)
			assert.Equal(t, want, w.out)
		})
	}
}

// TestScalarCell_InvalidUTF8 pins that an array cell now passes invalid UTF-8
// through instead of failing. proto.Marshal, the encoder this path replaced,
// rejects it; the arithmetic encoder treats proto3 strings as trusted internal
// input, which is what the top-level varchar path already did. The bytes it
// writes are exactly what proto.Marshal would have produced had it not refused.
func TestScalarCell_InvalidUTF8(t *testing.T) {
	for name, invalid := range map[string]string{
		"bare continuation": string([]byte{0xff, 0xfe}),
		"truncated":         string([]byte{0xe4, 0xb8}),
		"surrogate":         string([]byte{0xed, 0xa0, 0x80}),
		"overlong nul":      string([]byte{0xc0, 0x80}),
	} {
		t.Run(name, func(t *testing.T) {
			cell := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{Data: []string{invalid}},
			}}

			_, err := proto.Marshal(cell)
			require.Error(t, err, "proto.Marshal is expected to reject invalid UTF-8")

			got, ok := encodeCell(t, cell)
			require.True(t, ok)

			// proto.Unmarshal validates UTF-8 too, so compare against the wire
			// bytes the official encoder would have emitted:
			// ScalarField.string_data (6) -> StringArray.data (1) -> raw string.
			inner := protowire.AppendTag(nil, 1, protowire.BytesType)
			inner = protowire.AppendString(inner, invalid)
			want := protowire.AppendTag(nil, 6, protowire.BytesType)
			want = protowire.AppendBytes(want, inner)
			assert.Equal(t, want, got)
		})
	}
}

// TestScalarCell_ValidUTF8Boundaries guards the check above against being
// tightened by accident: these are all legal and must still encode.
func TestScalarCell_ValidUTF8Boundaries(t *testing.T) {
	cell := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{
		StringData: &schemapb.StringArray{Data: []string{
			"",
			"\x00",
			"\x7f",
			"\u0080",
			"߿",
			"ࠀ",
			"￿",
			"\U0001f600",
		}},
	}}
	assertCellMatchesProto(t, cell)
}

// TestScalarCell_RandomizedAgainstProto is the broad net: random shapes and
// values across every supported oneof, each compared byte for byte against the
// official encoder.
func TestScalarCell_RandomizedAgainstProto(t *testing.T) {
	rng := rand.New(rand.NewSource(20260810))

	randInt64 := func() int64 {
		// Cover every varint width, including the 10-byte negative case.
		switch rng.Intn(5) {
		case 0:
			return 0
		case 1:
			return int64(rng.Intn(128))
		case 2:
			return int64(rng.Intn(1 << 20))
		case 3:
			return -int64(rng.Intn(1 << 20))
		default:
			return rng.Int63()
		}
	}
	randBytes := func() []byte {
		b := make([]byte, rng.Intn(40))
		rng.Read(b)
		return b
	}
	// proto.Marshal rejects invalid UTF-8 in a string field, so the differential
	// comparison has to feed it valid text. The arithmetic path's behavior on
	// invalid UTF-8 is pinned separately in TestScalarCell_InvalidUTF8.
	randString := func() string {
		runes := make([]rune, rng.Intn(20))
		for j := range runes {
			runes[j] = rune(rng.Intn(0x2FFF-0x20) + 0x20)
		}
		return string(runes)
	}

	for i := 0; i < 2000; i++ {
		n := rng.Intn(12)
		var cell *schemapb.ScalarField
		switch rng.Intn(8) {
		case 0:
			data := make([]bool, n)
			for j := range data {
				data[j] = rng.Intn(2) == 1
			}
			cell = &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: data}}}
		case 1:
			data := make([]int32, n)
			for j := range data {
				data[j] = int32(randInt64())
			}
			cell = &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}}}
		case 2:
			data := make([]int64, n)
			for j := range data {
				data[j] = randInt64()
			}
			cell = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}}}
		case 3:
			data := make([]float32, n)
			for j := range data {
				data[j] = rng.Float32()
			}
			cell = &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: data}}}
		case 4:
			data := make([]float64, n)
			for j := range data {
				data[j] = rng.Float64()
			}
			cell = &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: data}}}
		case 5:
			data := make([]string, n)
			for j := range data {
				data[j] = randString()
			}
			cell = &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data}}}
		case 6:
			data := make([][]byte, n)
			for j := range data {
				data[j] = randBytes()
			}
			cell = &schemapb.ScalarField{Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: data}}}
		default:
			data := make([][]byte, n)
			for j := range data {
				data[j] = randBytes()
			}
			cell = &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: data}}}
		}

		got, ok := encodeCell(t, cell)
		require.True(t, ok)
		want, err := proto.Marshal(cell)
		require.NoError(t, err)
		require.Equal(t, want, got, "iteration %d: cell=%v", i, cell)
	}
}
