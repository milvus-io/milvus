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

package queryutil

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// The maxOutputSize guard must trip at the same payload on both transports.
// On the Arrow path the user columns are not in FieldsData, so a calculator
// that only reads FieldsData reports the size of the system columns alone --
// 8 bytes against a real 536 for the schema below. That does not just make the
// guard loose, it makes it useless: the guard exists to refuse BEFORE
// materializing, and an undercount defers the refusal to the next hop, after
// the node has built the whole result and marshaled it.
func TestRowSizeCountsArrowColumns(t *testing.T) {
	mem := memory.NewGoAllocator()
	const dim = 128

	// What the protobuf path hands the reduce: every column in FieldsData.
	protoResult := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{
			longCol(1, []int64{1, 2}),      // Timestamp (system)
			longCol(109, []int64{10, 11}),  // pk     8
			intCol(101, []int32{1, 2}),     // int8   4 (widened)
			floatCol(104, []float32{1, 2}), // float  4
			floatVecCol(107, dim, 2),       // vector 512
		},
	}
	// What the Arrow path hands it: system columns only, user columns in the
	// record, exactly as the diagnostic on the real pipeline shows.
	arrowResult := &internalpb.RetrieveResults{
		FieldsData: []*schemapb.FieldData{longCol(1, []int64{1, 2})},
	}
	rec := userColumnRecord(t, mem, dim)
	defer rec.Release()

	wantPerRow := int64(8 + 8 + 4 + 4 + dim*4)

	protoCalc := newRowSizeCalculator(protoResult)
	arrowCalc := newRowSizeCalculator(arrowResult).withArrowRecord(rec)

	for row := int64(0); row < 2; row++ {
		assert.Equal(t, wantPerRow, protoCalc.rowSize(row),
			"protobuf row size changed; the expectation below is derived from it")
		assert.Equal(t, wantPerRow, arrowCalc.rowSize(row),
			"arrow row %d undercounts: the guard would pass %dx too much payload",
			row, wantPerRow/max64(arrowCalc.rowSize(row), 1))
	}

	// Without the record the calculator sees the system column only. Asserted
	// so the test fails if withArrowRecord silently becomes a no-op.
	blind := newRowSizeCalculator(arrowResult)
	assert.Equal(t, int64(8), blind.rowSize(0),
		"the blind calculator should see only the Timestamp column")
}

// TestRowSizeCountsArrowVarLenColumns covers the columns whose width is not
// constant, which are read from the Arrow offsets rather than a type width.
func TestRowSizeCountsArrowVarLenColumns(t *testing.T) {
	mem := memory.NewGoAllocator()

	sb := array.NewStringBuilder(mem)
	defer sb.Release()
	sb.AppendValues([]string{"a", "abcdefghij", ""}, nil)
	strs := sb.NewStringArray()
	defer strs.Release()

	md := arrow.NewMetadata(nil, nil)
	schema := arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.BinaryTypes.String}}, &md)
	rec := array.NewRecord(schema, []arrow.Array{strs}, 3)
	defer rec.Release()

	calc := newRowSizeCalculator(&internalpb.RetrieveResults{}).withArrowRecord(rec)
	assert.Equal(t, int64(1), calc.rowSize(0))
	assert.Equal(t, int64(10), calc.rowSize(1))
	assert.Equal(t, int64(0), calc.rowSize(2))
	// Out of range must not panic; it contributes nothing, matching the
	// protobuf accounting's behavior for a short column.
	assert.Equal(t, int64(0), calc.rowSize(99))
}

// TestReduceGuardTripsOnArrowPath drives the operator itself: the same payload
// must be refused whether it arrives as FieldsData or as an Arrow record.
func TestReduceGuardTripsOnArrowPath(t *testing.T) {
	mem := memory.NewGoAllocator()
	const dim = 128
	const perRow = 8 + 8 + 4 + 4 + dim*4

	// A budget that two rows exceed and one row does not.
	for _, tc := range []struct {
		name      string
		maxOutput int64
		wantErr   bool
	}{
		{"under budget", perRow * 4, false},
		{"over budget", perRow + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, path := range []string{"protobuf", "arrow"} {
				t.Run(path, func(t *testing.T) {
					var input *internalpb.RetrieveResults
					var records []arrow.Record
					var rec arrow.Record
					if path == "protobuf" {
						input = &internalpb.RetrieveResults{
							Ids: intIDs(10, 11),
							FieldsData: []*schemapb.FieldData{
								longCol(1, []int64{1, 2}),
								longCol(109, []int64{10, 11}),
								intCol(101, []int32{1, 2}),
								floatCol(104, []float32{1, 2}),
								floatVecCol(107, dim, 2),
							},
						}
					} else {
						input = &internalpb.RetrieveResults{
							Ids:        intIDs(10, 11),
							FieldsData: []*schemapb.FieldData{longCol(1, []int64{1, 2})},
						}
						rec = userColumnRecord(t, mem, dim)
						defer rec.Release()
						records = []arrow.Record{rec}
					}

					op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, tc.maxOutput, 0, nil).
						withArrowRecords(records, &ArrowSelection{})
					_, err := op.Run(context.Background(), nil, []*internalpb.RetrieveResults{input})
					if tc.wantErr {
						require.Error(t, err, "%s path must refuse an over-budget result", path)
						assert.Contains(t, err.Error(), "maxOutputSize")
					} else {
						require.NoError(t, err)
					}
				})
			}
		})
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func intIDs(vals ...int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: vals}}}
}

func longCol(id int64, vals []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: schemapb.DataType_Int64, FieldId: id,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: vals}},
		}},
	}
}

func intCol(id int64, vals []int32) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: schemapb.DataType_Int8, FieldId: id,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: vals}},
		}},
	}
}

func floatCol(id int64, vals []float32) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: schemapb.DataType_Float, FieldId: id,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: vals}},
		}},
	}
}

func floatVecCol(id int64, dim, rows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: schemapb.DataType_FloatVector, FieldId: id,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: int64(dim),
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: make([]float32, dim*rows)},
			},
		}},
	}
}

// userColumnRecord mirrors what the Arrow export produces for the schema above:
// pk int64, int8 widened to int32, float32, and the vector as
// fixed_size_binary(dim*4).
func userColumnRecord(t *testing.T, mem memory.Allocator, dim int) arrow.Record {
	t.Helper()
	md := arrow.NewMetadata(
		[]string{"milvus.field_order", "milvus.valid_data_fields"},
		[]string{"1,109,101,104,107", ""})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "i8", Type: arrow.PrimitiveTypes.Int32},
		{Name: "f", Type: arrow.PrimitiveTypes.Float32},
		{Name: "vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: dim * 4}},
	}, &md)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 11}, nil)
	b.Field(1).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	b.Field(2).(*array.Float32Builder).AppendValues([]float32{1, 2}, nil)
	vb := b.Field(3).(*array.FixedSizeBinaryBuilder)
	vb.Append(make([]byte, dim*4))
	vb.Append(make([]byte, dim*4))
	return b.NewRecord()
}

// TestRowSizeChargesNullableScalarLikeProtobuf pins the one case where the two
// transports' accounting can silently diverge.
//
// calcFieldElementSizeWithCompactIndex returns the fixed width for a scalar
// UNCONDITIONALLY -- its compactIdx/validity test lives inside the GetVectors()
// branch and is only populated for compact-nullable VECTORS. So a nullable
// scalar's null row costs 8 bytes on the protobuf path. If the Arrow calculator
// skips it, the guard trips at a different row count on the two transports,
// which is exactly what withArrowRecord exists to prevent.
//
// A nullable VECTOR is the opposite case and must still charge 0, because there
// the protobuf side does too.
func TestRowSizeChargesNullableScalarLikeProtobuf(t *testing.T) {
	mem := memory.NewGoAllocator()

	const dim = 4
	md := arrow.NewMetadata(
		[]string{"milvus.field_order", "milvus.valid_data_fields"},
		[]string{"1,200,201", "200,201"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "nullable_i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "nullable_vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: dim * 4}, Nullable: true},
	}, &md)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	// Row 0 valid, row 1 null, in both columns.
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{7, 0}, []bool{true, false})
	vb := b.Field(1).(*array.FixedSizeBinaryBuilder)
	vb.Append(make([]byte, dim*4))
	vb.AppendNull()
	rec := b.NewRecord()
	defer rec.Release()

	res := &internalpb.RetrieveResults{
		Ids:        intIDs(10, 11),
		FieldsData: []*schemapb.FieldData{longCol(1, []int64{100, 101})},
	}
	c := newRowSizeCalculator(res).withArrowRecord(rec)

	// Row 0: timestamp 8 + nullable int64 8 + vector 16 = 32.
	assert.EqualValues(t, 8+8+dim*4, c.rowSize(0))
	// Row 1: timestamp 8 + nullable int64 8 (STILL charged, as protobuf does)
	// + vector 0 (skipped, as protobuf does) = 16.
	assert.EqualValues(t, 8+8, c.rowSize(1),
		"a null SCALAR row must still be charged its width -- the protobuf "+
			"scalar branch never tests validity -- while a null VECTOR row is "+
			"skipped on both paths")
}
