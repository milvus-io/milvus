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
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// TestInsertRequestViewParity holds every message the cursor produces to the
// encoding it replaces: decoding it must yield exactly the message that
// materializing the same rows through AppendFieldData and marshaling it would
// have produced. It runs the whole corpus of field shapes against several
// message budgets, so the split boundaries move around and each shape is
// checked at more than one selection.
//
// The two encodings are wire-equivalent, not byte-identical, and the difference
// is the order fields go out in. Inside a FieldData this encoder writes strictly
// by field number -- type, field_name, the scalars/vectors oneof, field_id --
// while Go's marshaler emits a oneof payload after the plain fields that follow
// it by number, so it writes field_id before the oneof. Both decode to the same
// InsertRequest, which is the contract a consumer depends on. Do not tighten
// this to bytes.Equal without reordering the encoder to match.
//
// The digest is logged, not asserted. It is there to compare two builds by
// hand; a golden value would have to be regenerated for changes that are not
// regressions, such as a new field in the corpus.
//
// The corpus deliberately avoids invalid UTF-8, which is the one intentional
// behavior change: proto.Marshal errors on it, so there would be no oracle to
// compare against.
func TestInsertRequestViewParity(t *testing.T) {
	template := insertViewTemplate()
	digest := sha256.New()
	totalMessages := 0
	totalBytes := 0

	for _, tc := range parityCorpus() {
		t.Run(tc.name, func(t *testing.T) {
			// Several budgets so message boundaries land in different
			// places; 0 means unbounded, which yields a single message.
			for _, budget := range []int{0, 512, 4 << 10, 64 << 10} {
				rows := make([]int, int(tc.source.GetNumRows()))
				for i := range rows {
					rows[i] = i
				}

				cursor, err := NewInsertRequestViewCursor(tc.source)
				require.NoError(t, err)
				for start := 0; start < len(rows); {
					encoder, consumed, err := cursor.NextEncoder(template, rows[start:], budget)
					require.NoError(t, err)
					size, err := encoder.EncodedSize()
					require.NoError(t, err)
					payload := make([]byte, size)
					written, err := encoder.MarshalTo(payload)
					require.NoError(t, err)
					require.Equal(t, size, written)

					var decoded msgpb.InsertRequest
					require.NoError(t, proto.Unmarshal(payload, &decoded))
					require.Equal(t, uint64(consumed), decoded.GetNumRows())

					selection := rows[start : start+consumed]
					expected := materializeWithAppendFieldData(template, tc.source, selection)
					require.True(t, proto.Equal(expected, &decoded),
						"encoding drifted from the materialized oracle: budget=%d selection=%v",
						budget, selection)
					require.Equal(t, proto.Size(expected), size,
						"encoded size drifted: budget=%d selection=%v", budget, selection)

					digest.Write(payload)
					totalMessages++
					totalBytes += size
					start += consumed
				}
			}
		})
	}

	t.Logf("PARITY messages=%d bytes=%d sha256=%s",
		totalMessages, totalBytes, hex.EncodeToString(digest.Sum(nil)))
}

type parityCase struct {
	name   string
	source *msgpb.InsertRequest
}

func parityCorpus() []parityCase {
	rng := rand.New(rand.NewSource(4242))
	const rowCount = 400

	rowIDs := make([]int64, rowCount)
	timestamps := make([]uint64, rowCount)
	for row := range rowIDs {
		rowIDs[row] = int64(row) * 7
		timestamps[row] = uint64(row) * 13
	}
	base := func(fields ...*schemapb.FieldData) *msgpb.InsertRequest {
		return &msgpb.InsertRequest{
			NumRows:    rowCount,
			RowIDs:     rowIDs,
			Timestamps: timestamps,
			FieldsData: fields,
		}
	}

	// Array cells with varied element counts and value magnitudes, so the
	// packed payload hits every varint width including the 10-byte negative.
	arrayCells := make([]*schemapb.ScalarField, rowCount)
	stringCells := make([]*schemapb.ScalarField, rowCount)
	emptyishCells := make([]*schemapb.ScalarField, rowCount)
	nestedArrayCells := make([]*schemapb.ScalarField, rowCount)
	for row := range arrayCells {
		n := rng.Intn(16)
		values := make([]int64, n)
		for i := range values {
			switch rng.Intn(4) {
			case 0:
				values[i] = 0
			case 1:
				values[i] = int64(rng.Intn(1 << 14))
			case 2:
				values[i] = -int64(rng.Intn(1 << 14))
			default:
				values[i] = rng.Int63()
			}
		}
		arrayCells[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
			LongData: &schemapb.LongArray{Data: values},
		}}

		texts := make([]string, rng.Intn(5))
		for i := range texts {
			texts[i] = "cell-" + strconv.Itoa(row) + "-" + strconv.Itoa(i)
		}
		stringCells[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{
			StringData: &schemapb.StringArray{Data: texts},
		}}

		// Mix in the empty-array and no-oneof shapes, whose proto3 encoding
		// the arithmetic path has to reproduce by hand.
		switch row % 3 {
		case 0:
			emptyishCells[row] = &schemapb.ScalarField{}
		case 1:
			emptyishCells[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{},
			}}
		default:
			emptyishCells[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{
				BoolData: &schemapb.BoolArray{Data: []bool{row%2 == 0}},
			}}
		}

		// Recursive ARRAY support landed after the original encoder branch.
		// Nested cells deliberately take the protobuf fallback path, which must
		// still preserve the complete cell while selecting outer rows.
		nestedArrayCells[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_Array,
				Data: []*schemapb.ScalarField{
					{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
						ElementType: schemapb.DataType_Int64,
						Data: []*schemapb.ScalarField{{Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{int64(row), int64(-row)}},
						}}},
					}}},
				},
			},
		}}
	}

	longs := make([]int64, rowCount)
	texts := make([]string, rowCount)
	blobs := make([][]byte, rowCount)
	floats := make([]float32, 0, rowCount*4)
	sparse := make([][]byte, rowCount)
	valid := make([]bool, rowCount)
	for row := range longs {
		longs[row] = rng.Int63() * int64(1-2*(row%2))
		texts[row] = "row-" + strconv.Itoa(row)
		blobs[row] = []byte{byte(row), byte(row >> 8)}
		floats = append(floats, float32(row), 1.5, -2.25, 0)
		entry := make([]byte, 8*(1+row%3))
		for i := 0; i < len(entry); i += 8 {
			entry[i] = byte(i / 8)
		}
		sparse[row] = entry
		valid[row] = row%4 != 0
	}

	nullableVector := &schemapb.FieldData{
		Type: schemapb.DataType_FloatVector, FieldId: 400, FieldName: "nullable_vec",
		ValidData: valid,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 2,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{
				Data: func() []float32 {
					out := make([]float32, 0, rowCount*2)
					for row := 0; row < rowCount; row++ {
						if valid[row] {
							out = append(out, float32(row), float32(-row))
						}
					}
					return out
				}(),
			}},
		}},
	}
	dynamicJSON := scalarField(202, schemapb.DataType_JSON, &schemapb.ScalarField_JsonData{
		JsonData: &schemapb.JSONArray{Data: blobs},
	})
	dynamicJSON.FieldName = "$meta"
	dynamicJSON.IsDynamic = true

	return []parityCase{
		{name: "array_int64", source: base(
			scalarField(100, schemapb.DataType_Array, &schemapb.ScalarField_ArrayData{
				ArrayData: &schemapb.ArrayArray{Data: arrayCells, ElementType: schemapb.DataType_Int64},
			}))},
		{name: "array_varchar", source: base(
			scalarField(101, schemapb.DataType_Array, &schemapb.ScalarField_ArrayData{
				ArrayData: &schemapb.ArrayArray{Data: stringCells, ElementType: schemapb.DataType_VarChar},
			}))},
		{name: "array_empty_shapes", source: base(
			scalarField(102, schemapb.DataType_Array, &schemapb.ScalarField_ArrayData{
				ArrayData: &schemapb.ArrayArray{Data: emptyishCells, ElementType: schemapb.DataType_Bool},
			}))},
		{name: "recursively_nested_array", source: base(
			scalarField(103, schemapb.DataType_Array, &schemapb.ScalarField_ArrayData{
				ArrayData: &schemapb.ArrayArray{Data: nestedArrayCells, ElementType: schemapb.DataType_Array},
			}))},
		{name: "mixed_columns", source: base(
			scalarField(200, schemapb.DataType_Int64, &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: longs},
			}),
			scalarField(201, schemapb.DataType_VarChar, &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{Data: texts},
			}),
			dynamicJSON,
			scalarField(203, schemapb.DataType_Array, &schemapb.ScalarField_ArrayData{
				ArrayData: &schemapb.ArrayArray{Data: arrayCells, ElementType: schemapb.DataType_Int64},
			}),
			&schemapb.FieldData{
				Type: schemapb.DataType_FloatVector, FieldId: 204, FieldName: "vec",
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim:  4,
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: floats}},
				}},
			},
		)},
		{name: "array_every_element_type", source: base(arrayFieldsForEveryElementType(rowCount)...)},
		{name: "sparse_and_nullable_vector", source: base(
			&schemapb.FieldData{
				Type: schemapb.DataType_SparseFloatVector, FieldId: 300, FieldName: "sparse",
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: &schemapb.SparseFloatArray{Contents: sparse},
					},
				}},
			},
			nullableVector,
		)},
	}
}

// arrayFieldsForEveryElementType builds one array column per supported cell
// type. The cell-level differential test covers each type against
// proto.Marshal directly, but only these exercise the full path -- sizing,
// the enclosing ArrayArray length prefix, and message splitting -- for every
// element type rather than just the int64 and string ones used elsewhere.
func arrayFieldsForEveryElementType(rowCount int) []*schemapb.FieldData {
	cell := func(row int, kind int) *schemapb.ScalarField {
		// Vary element count per row, including 0, so empty packed payloads
		// and empty repeated lists both appear.
		n := row % 4
		switch kind {
		case 0:
			d := make([]bool, n)
			for i := range d {
				d[i] = (row+i)%2 == 0
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: d}}}
		case 1:
			d := make([]int32, n)
			for i := range d {
				d[i] = int32(row*3+i) * int32(1-2*(i%2)) // mix signs: negatives are 10-byte varints
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: d}}}
		case 2:
			d := make([]int64, n)
			for i := range d {
				d[i] = int64(row*7+i) * int64(1-2*(i%2))
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: d}}}
		case 3:
			d := make([]float32, n)
			for i := range d {
				d[i] = float32(row) + float32(i)/4
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: d}}}
		case 4:
			d := make([]float64, n)
			for i := range d {
				d[i] = float64(row) - float64(i)/8
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: d}}}
		case 5:
			d := make([]string, n)
			for i := range d {
				d[i] = "s" + strconv.Itoa(row) + "_" + strconv.Itoa(i)
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: d}}}
		case 6:
			d := make([][]byte, n)
			for i := range d {
				d[i] = []byte{byte(row), byte(i)}
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: d}}}
		case 7:
			d := make([][]byte, n)
			for i := range d {
				d[i] = []byte(`{"r":` + strconv.Itoa(row) + `}`)
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: d}}}
		case 8:
			d := make([][]byte, n)
			for i := range d {
				d[i] = []byte{0x01, byte(row), byte(i)}
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: d}}}
		case 9:
			d := make([]int64, n)
			for i := range d {
				d[i] = int64(row)*1_000_000 + int64(i)
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: d}}}
		case 10:
			d := make([]string, n)
			for i := range d {
				d[i] = "POINT(" + strconv.Itoa(row) + " " + strconv.Itoa(i) + ")"
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: d}}}
		case 11:
			d := make([][]byte, n)
			for i := range d {
				d[i] = []byte{0xde, byte(row), byte(i)}
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_MolData{MolData: &schemapb.MolArray{Data: d}}}
		case 12:
			d := make([]string, n)
			for i := range d {
				d[i] = "CC" + strconv.Itoa(row%9) + "O"
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_MolSmilesData{MolSmilesData: &schemapb.MolSmilesArray{Data: d}}}
		case 13:
			d := make([]int32, n)
			for i := range d {
				d[i] = int32(row*17 + i)
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_DateData{DateData: &schemapb.DateArray{Data: d}}}
		default:
			d := make([]int64, n)
			for i := range d {
				d[i] = int64(row*31+i) * int64(1-2*(i%2))
			}
			return &schemapb.ScalarField{Data: &schemapb.ScalarField_TimeData{TimeData: &schemapb.TimeArray{Data: d}}}
		}
	}

	elementTypes := []schemapb.DataType{
		schemapb.DataType_Bool, schemapb.DataType_Int32, schemapb.DataType_Int64,
		schemapb.DataType_Float, schemapb.DataType_Double, schemapb.DataType_VarChar,
		schemapb.DataType_String, schemapb.DataType_JSON, schemapb.DataType_Geometry,
		schemapb.DataType_Timestamptz, schemapb.DataType_Geometry, schemapb.DataType_Mol,
		schemapb.DataType_Mol, schemapb.DataType_Date, schemapb.DataType_Time,
	}

	fields := make([]*schemapb.FieldData, 0, len(elementTypes))
	for kind, elementType := range elementTypes {
		cells := make([]*schemapb.ScalarField, rowCount)
		for row := range cells {
			cells[row] = cell(row, kind)
		}
		fields = append(fields, scalarField(int64(500+kind), schemapb.DataType_Array,
			&schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				Data: cells, ElementType: elementType,
			}}))
	}
	return fields
}
