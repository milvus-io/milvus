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

// Unit tests for MaterializeArrowSelection, which previously had none.
//
// The end-to-end differential tests in querynodev2/segments/arrowe2e cover the
// happy path well, but they need a built libmilvus_core, so they cannot run
// here and they only ever feed well-formed input. Every GUARD in the
// materializer -- duplicate field ids, column-count mismatch, out-of-range row
// refs, a field id absent from the schema, field_order cardinality -- was
// therefore untested, and those are exactly the branches that turn a routing or
// alignment bug into an error instead of silent corruption.
//
// These run against a synthetic arrow.Record, so they need no cgo. That is the
// point of keeping this package cgo-free.
package queryutil

import (
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

// userRecord builds what the C++ export hands over: the user columns only, with
// the two schema metadata keys naming every column of the original
// fields_data in order (system columns included).
func userRecord(t *testing.T, mem memory.Allocator, fieldOrder, validDataFields string) arrow.Record {
	t.Helper()
	md := arrow.NewMetadata(
		[]string{"milvus.field_order", "milvus.valid_data_fields"},
		[]string{fieldOrder, validDataFields})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "f", Type: arrow.PrimitiveTypes.Float32},
	}, &md)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 11, 12}, nil)
	b.Field(1).(*array.Float32Builder).AppendValues([]float32{1, 2, 3}, nil)
	return b.NewRecord()
}

// header is the protobuf side: the system columns the export left behind.
func header(ids ...int64) *segcorepb.RetrieveResults {
	out := &segcorepb.RetrieveResults{}
	for _, id := range ids {
		out.FieldsData = append(out.FieldsData, &schemapb.FieldData{
			Type:    schemapb.DataType_Int64,
			FieldId: id,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			}},
		})
	}
	return out
}

func testSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, Name: "Timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: 109, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 104, Name: "f", DataType: schemapb.DataType_Float},
	}}
}

func TestMaterializeArrowSelection(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("writes the selected rows in field_order", func(t *testing.T) {
		rec := userRecord(t, mem, "1,109,104", "")
		defer rec.Release()
		h := header(1)
		sel := &ArrowSelection{
			Records: []arrow.Record{rec},
			// Deliberately out of order and a strict subset: the output must
			// follow the selection, not the record.
			Rows: []rowRef{{resultIdx: 0, rowIdx: 2}, {resultIdx: 0, rowIdx: 0}},
		}
		require.NoError(t, MaterializeArrowSelection(h, sel, testSchema()))

		require.Len(t, h.FieldsData, 3)
		assert.Equal(t, []int64{1, 109, 104},
			[]int64{h.FieldsData[0].GetFieldId(), h.FieldsData[1].GetFieldId(), h.FieldsData[2].GetFieldId()},
			"columns must be reassembled in field_order")
		assert.Equal(t, []int64{12, 10},
			h.FieldsData[1].GetScalars().GetLongData().GetData())
		assert.Equal(t, []float32{3, 1},
			h.FieldsData[2].GetScalars().GetFloatData().GetData())
		// FieldName must stay empty so the two transports are interchangeable.
		assert.Empty(t, h.FieldsData[2].GetFieldName())
	})

	t.Run("nil selection is a no-op", func(t *testing.T) {
		h := header(1)
		require.NoError(t, MaterializeArrowSelection(h, nil, testSchema()))
		assert.Len(t, h.FieldsData, 1)
		require.NoError(t, MaterializeArrowSelection(h, &ArrowSelection{}, testSchema()))
		assert.Len(t, h.FieldsData, 1)
	})

	for _, tc := range []struct {
		name string
		rec  func() arrow.Record
		hdr  *segcorepb.RetrieveResults
		rows []rowRef
		want string
	}{
		{
			// Aggregation columns all carry field id 0, so they are identified
			// positionally and cannot be reassembled by id.
			name: "duplicate field id is refused",
			rec:  func() arrow.Record { return userRecord(t, mem, "0,0,104", "") },
			hdr:  header(1), rows: []rowRef{{0, 0}},
			want: "duplicate field id",
		},
		{
			name: "row index past the record is refused",
			rec:  func() arrow.Record { return userRecord(t, mem, "1,109,104", "") },
			hdr:  header(1), rows: []rowRef{{resultIdx: 0, rowIdx: 99}},
			want: "out of range",
		},
		{
			name: "result index past the record slice is refused",
			rec:  func() arrow.Record { return userRecord(t, mem, "1,109,104", "") },
			hdr:  header(1), rows: []rowRef{{resultIdx: 7, rowIdx: 0}},
			want: "references result",
		},
		{
			// field_order lists 4 ids but only 3 columns exist.
			name: "field_order cardinality mismatch is refused",
			rec:  func() arrow.Record { return userRecord(t, mem, "1,109,104,999", "") },
			hdr:  header(1), rows: []rowRef{{0, 0}},
			want: "user field ids",
		},
		{
			name: "missing field_order metadata is refused",
			rec: func() arrow.Record {
				md := arrow.NewMetadata(nil, nil)
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
				}, &md)
				b := array.NewRecordBuilder(mem, schema)
				defer b.Release()
				b.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
				return b.NewRecord()
			},
			hdr: header(1), rows: []rowRef{{0, 0}},
			want: "milvus.field_order",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := tc.rec()
			defer rec.Release()
			sel := &ArrowSelection{Records: []arrow.Record{rec}, Rows: tc.rows}
			err := MaterializeArrowSelection(tc.hdr, sel, testSchema())
			require.Error(t, err, "guard did not fire")
			assert.Contains(t, err.Error(), tc.want)
		})
	}

	t.Run("short record is refused, not panicked on", func(t *testing.T) {
		full := userRecord(t, mem, "1,109,104", "")
		defer full.Release()
		md := arrow.NewMetadata(
			[]string{"milvus.field_order", "milvus.valid_data_fields"},
			[]string{"1,109,104", ""})
		narrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		}, &md)
		nb := array.NewRecordBuilder(mem, narrowSchema)
		defer nb.Release()
		nb.Field(0).(*array.Int64Builder).AppendValues([]int64{5, 6, 7}, nil)
		narrow := nb.NewRecord()
		defer narrow.Release()

		sel := &ArrowSelection{
			Records: []arrow.Record{full, narrow},
			Rows:    []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 1, rowIdx: 0}},
		}
		err := MaterializeArrowSelection(header(1), sel, testSchema())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column count mismatch")
	})
}

// TestMaterializeRejectsDivergentRecordMetadata pins the cross-record check.
//
// The materializer reads field_order and valid_data_fields from the TEMPLATE
// (the first non-nil record) and applies that reading to every record's rows.
// Nothing in the tree produces disagreeing records today -- both keys are
// schema-derived per request -- so this covers the guard, not a reachable bug.
// It is worth having because column count agreement, which is checked
// separately, does not imply metadata agreement, and a wrong reading strips or
// invents a validity bitmap silently rather than erroring.
func TestMaterializeRejectsDivergentRecordMetadata(t *testing.T) {
	mem := memory.NewGoAllocator()

	build := func(validDataFields string) arrow.Record {
		md := arrow.NewMetadata(
			[]string{"milvus.field_order", "milvus.valid_data_fields"},
			[]string{"1,100", validDataFields})
		sch := arrow.NewSchema([]arrow.Field{
			{Name: "c0", Type: arrow.PrimitiveTypes.Int64},
		}, &md)
		b := array.NewRecordBuilder(mem, sch)
		defer b.Release()
		b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
		return b.NewRecord()
	}

	// Same column count, same field_order, DIFFERENT valid_data_fields.
	a, c := build("100"), build("")
	defer a.Release()
	defer c.Release()

	sel := &ArrowSelection{
		Records: []arrow.Record{a, c},
		Rows:    []rowRef{{resultIdx: 0, rowIdx: 0}, {resultIdx: 1, rowIdx: 1}},
	}
	err := MaterializeArrowSelection(header(1), sel, testSchema())
	require.Error(t, err)
	require.Contains(t, err.Error(), "disagrees with the template")
}

// vectorRecord builds a one-column FloatVector record with an explicit byte
// width, so a test can make the width disagree with the schema's dim.
//
// nulls makes the column nullable, which is what pushes it onto the GATHER path
// -- the one a width mismatch used to reach unchecked.
func vectorRecord(t *testing.T, mem memory.Allocator, byteWidth int, nulls bool) arrow.Record {
	t.Helper()
	md := arrow.NewMetadata(
		[]string{"milvus.field_order", "milvus.valid_data_fields"},
		[]string{"1,200", map[bool]string{true: "200", false: ""}[nulls]})
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: byteWidth}, Nullable: nulls},
	}, &md)
	b := array.NewRecordBuilder(mem, sch)
	defer b.Release()
	vb := b.Field(0).(*array.FixedSizeBinaryBuilder)
	vb.Append(make([]byte, byteWidth))
	if nulls {
		vb.AppendNull()
	} else {
		vb.Append(make([]byte, byteWidth))
	}
	return b.NewRecord()
}

func floatVecSchema(dim int64) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, Name: "Timestamp", DataType: schemapb.DataType_Int64},
		{
			FieldID: 200, Name: "vec", DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: "dim", Value: strconv.FormatInt(dim, 10)},
			},
		},
	}}
}

// TestMaterializeRejectsVectorWidthDimMismatch pins that a vector whose Arrow
// byte width disagrees with its schema dim is an ERROR, on BOTH paths.
//
// An earlier revision only made materializeColumn decline such a column
// (ok=false). That was not a guard: it routed the column to gatherColumns, and
// that path does not compare width to dim either -- resolveVectorDim takes the
// dim from the schema, then compactFloatVector allocates numRows*dim floats and
// copies numRows*width bytes, so copy() silently truncates or zero-fills, and
// its nullable branch writes at stride dim while reading at stride width.
// Declining produced the same inconsistent column by a longer route.
//
// The nullable subtest is the important one: it is the case that actually
// reaches the gather.
func TestMaterializeRejectsVectorWidthDimMismatch(t *testing.T) {
	mem := memory.NewGoAllocator()

	for _, tc := range []struct {
		name  string
		nulls bool
	}{
		{"fast path", false},
		{"gather path", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 4 floats of payload, but the schema claims dim 8.
			rec := vectorRecord(t, mem, 4*4, tc.nulls)
			defer rec.Release()

			sel := &ArrowSelection{
				Records: []arrow.Record{rec},
				Rows:    []rowRef{{resultIdx: 0, rowIdx: 0}},
			}
			err := MaterializeArrowSelection(header(1), sel, floatVecSchema(8))
			require.Error(t, err, "a width/dim disagreement must not be silently materialized")
			require.Contains(t, err.Error(), "byte width")
		})
	}

	t.Run("agreeing width is accepted", func(t *testing.T) {
		rec := vectorRecord(t, mem, 8*4, false)
		defer rec.Release()
		sel := &ArrowSelection{
			Records: []arrow.Record{rec},
			Rows:    []rowRef{{resultIdx: 0, rowIdx: 0}},
		}
		require.NoError(t, MaterializeArrowSelection(header(1), sel, floatVecSchema(8)))
	})
}
