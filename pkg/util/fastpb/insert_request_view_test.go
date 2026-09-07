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
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	msgpb "github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestInsertRequestViewEncoder_DifferentialAllRepackTypes(t *testing.T) {
	template := insertViewTemplate()
	// The encoder owns the row-bearing fields, even if a caller accidentally
	// leaves stale values in the metadata template.
	template.Timestamps = []uint64{999}
	template.RowIDs = []int64{999}
	template.FieldsData = []*schemapb.FieldData{{FieldId: 999}}
	template.NumRows = 999
	template.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 7))

	source := insertViewAllRepackTypesSource()
	for _, rows := range [][]int{
		{0},
		{0, 2, 4},
		{1, 4}, // all-null selection for compact vector fields
		{0, 1, 2, 3, 4},
	} {
		t.Run(rowsName(rows), func(t *testing.T) {
			assertViewMatchesMaterialized(t, template, source, rows)
		})
	}
}

func TestInsertRequestViewEncoder_DifferentialRandomSelections(t *testing.T) {
	r := rand.New(rand.NewSource(0x52269))
	template := insertViewTemplate()
	for iteration := 0; iteration < 500; iteration++ {
		rowCount := 1 + r.Intn(32)
		source := randomInsertViewSource(r, rowCount)
		rows := make([]int, 0, rowCount)
		for row := 0; row < rowCount; row++ {
			if r.Intn(3) != 0 {
				rows = append(rows, row)
			}
		}
		if len(rows) == 0 {
			rows = append(rows, r.Intn(rowCount))
		}
		assertViewMatchesMaterialized(t, template, source, rows)
	}
}

func TestInsertRequestViewCursor_AggregatesPackedScalarsExactly(t *testing.T) {
	allTypes := insertViewAllRepackTypesSource()
	source := &msgpb.InsertRequest{
		NumRows:    allTypes.GetNumRows(),
		RowIDs:     allTypes.GetRowIDs(),
		Timestamps: allTypes.GetTimestamps(),
		FieldsData: []*schemapb.FieldData{
			allTypes.GetFieldsData()[0], // bool, including ValidData
			allTypes.GetFieldsData()[1], // int32 varints
			allTypes.GetFieldsData()[2], // int64 varints
			allTypes.GetFieldsData()[3], // fixed32
			allTypes.GetFieldsData()[4], // fixed64
			allTypes.GetFieldsData()[8], // timestamptz varints
		},
	}
	rows := []int{0, 1, 2, 3, 4}
	template := insertViewTemplate()
	expected := materializeWithAppendFieldData(template, source, rows)

	cursor, err := NewInsertRequestViewCursor(source)
	require.NoError(t, err)
	encoder, consumed, err := cursor.NextEncoder(template, rows, 1<<20)
	require.NoError(t, err)
	require.Equal(t, len(rows), consumed)

	size, err := encoder.EncodedSize()
	require.NoError(t, err)
	actualBytes := make([]byte, size)
	_, err = encoder.MarshalTo(actualBytes)
	require.NoError(t, err)
	require.Equal(t, proto.Size(expected), size)

	actual := &msgpb.InsertRequest{}
	require.NoError(t, proto.Unmarshal(actualBytes, actual))
	assert.True(t, proto.Equal(expected, actual))
}

func TestInsertRequestViewCursor_AggregatesSimpleVectorsExactly(t *testing.T) {
	const (
		rowCount = 64
		dim      = 16
	)
	rows, rowIDs, timestamps := benchmarkInsertRows(rowCount)
	floatValues := make([]float32, rowCount*dim)
	byteValues := make([]byte, rowCount*dim)
	binaryValues := make([]byte, rowCount*dim/8)
	for i := range floatValues {
		floatValues[i] = float32(i)
	}
	for i := range byteValues {
		byteValues[i] = byte(i)
	}
	for i := range binaryValues {
		binaryValues[i] = byte(i)
	}

	wideBytes := make([]byte, rowCount*dim*2)
	fields := map[string]*schemapb.FieldData{
		"binary":   vectorField(1, schemapb.DataType_BinaryVector, dim, &schemapb.VectorField_BinaryVector{BinaryVector: binaryValues}, nil),
		"float":    vectorField(1, schemapb.DataType_FloatVector, dim, &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: floatValues}}, nil),
		"float16":  vectorField(1, schemapb.DataType_Float16Vector, dim, &schemapb.VectorField_Float16Vector{Float16Vector: wideBytes}, nil),
		"bfloat16": vectorField(1, schemapb.DataType_BFloat16Vector, dim, &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: wideBytes}, nil),
		"int8":     vectorField(1, schemapb.DataType_Int8Vector, dim, &schemapb.VectorField_Int8Vector{Int8Vector: byteValues}, nil),
	}

	for name, field := range fields {
		t.Run(name, func(t *testing.T) {
			source := &msgpb.InsertRequest{NumRows: rowCount, RowIDs: rowIDs, Timestamps: timestamps, FieldsData: []*schemapb.FieldData{field}}
			limit := proto.Size(materializeWithAppendFieldData(insertViewTemplate(), source, rows[:17]))
			assertCursorExactSplits(t, insertViewTemplate(), source, rows, limit)
		})
	}
}

func TestInsertRequestViewCursor_AggregatesSparseVectorExactly(t *testing.T) {
	const rowCount = 64
	rows, rowIDs, timestamps := benchmarkInsertRows(rowCount)
	contents := make([][]byte, rowCount)
	for row := range contents {
		contents[row] = sparseTestRow(uint32(row + 1))
	}
	source := &msgpb.InsertRequest{
		NumRows: rowCount, RowIDs: rowIDs, Timestamps: timestamps,
		FieldsData: []*schemapb.FieldData{vectorField(1, schemapb.DataType_SparseFloatVector, 1024,
			&schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: 1024, Contents: contents}}, nil)},
	}
	limit := proto.Size(materializeWithAppendFieldData(insertViewTemplate(), source, rows[:17]))
	assertCursorExactSplits(t, insertViewTemplate(), source, rows, limit)
}

// boundedVariableSimplePrefix sizes one 64-row block at a time and replays only
// the block that crosses the limit, so the seam between two blocks is where a
// checkpoint/rollback mistake would show up: a prefix that ends exactly on the
// block edge, the first row of the following block, and a split that happens
// several blocks in. A budget equal to the size of prefix+1 rows must roll that
// row over (the limit is exclusive); one more byte of budget must take it.
func assertBoundedPrefixBlockSeams(t *testing.T, source *msgpb.InsertRequest, rows []int, prefixes []int) {
	t.Helper()
	template := insertViewTemplate()
	sizeAt := func(prefix int) int {
		return proto.Size(materializeWithAppendFieldData(template, source, rows[:prefix]))
	}
	consumedWithLimit := func(limit int) int {
		cursor, err := NewInsertRequestViewCursor(source)
		require.NoError(t, err)
		_, consumed, err := cursor.NextEncoder(template, rows, limit)
		require.NoError(t, err)
		return consumed
	}
	for _, prefix := range prefixes {
		require.Less(t, prefix+1, len(rows))
		t.Run("prefix"+strconv.Itoa(prefix), func(t *testing.T) {
			limit := sizeAt(prefix + 1)
			assert.Equal(t, prefix, consumedWithLimit(limit), "equal-size prefix must roll over")
			assertCursorExactSplits(t, template, source, rows, limit)

			assert.Equal(t, prefix+1, consumedWithLimit(limit+1), "one more byte must take one more row")
			assertCursorExactSplits(t, template, source, rows, limit+1)
		})
	}
}

func TestInsertRequestViewCursor_RepeatedScalarPrefixBlockSeams(t *testing.T) {
	const rowCount = 200
	rows, rowIDs, timestamps := benchmarkInsertRows(rowCount)
	stringsData := make([]string, rowCount)
	jsonData := make([][]byte, rowCount)
	for row := 0; row < rowCount; row++ {
		stringsData[row] = strings.Repeat("s", row%131)
		jsonData[row] = []byte(`{"row":` + strconv.Itoa(row) + `,"payload":"` + strings.Repeat("j", row%257) + `"}`)
	}
	source := &msgpb.InsertRequest{
		NumRows: rowCount, RowIDs: rowIDs, Timestamps: timestamps,
		FieldsData: []*schemapb.FieldData{
			scalarField(1, schemapb.DataType_VarChar,
				&schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: stringsData}}),
			scalarField(2, schemapb.DataType_JSON,
				&schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: jsonData}}),
		},
	}
	assertBoundedPrefixBlockSeams(t, source, rows, []int{1, 62, 63, 64, 65, 66, 127, 128, 129, 130})
}

func TestInsertRequestViewCursor_SparsePrefixBlockSeams(t *testing.T) {
	const rowCount = 200
	rows, rowIDs, timestamps := benchmarkInsertRows(rowCount)
	contents := make([][]byte, rowCount)
	for row := range contents {
		// The sparse index grows with the row, so the selection's max dimension
		// keeps moving and has to be rolled back with the rest of the block.
		contents[row] = sparseTestRow(uint32(row + 1))
	}
	source := &msgpb.InsertRequest{
		NumRows: rowCount, RowIDs: rowIDs, Timestamps: timestamps,
		FieldsData: []*schemapb.FieldData{vectorField(1, schemapb.DataType_SparseFloatVector, int64(rowCount+1),
			&schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
				Dim: int64(rowCount + 1), Contents: contents,
			}}, nil)},
	}
	assertBoundedPrefixBlockSeams(t, source, rows, []int{1, 63, 64, 65, 126, 127, 128, 129})
}

func TestInsertRequestViewCursor_AggregatesRepeatedScalarsExactly(t *testing.T) {
	const rowCount = 64
	rows, rowIDs, timestamps := benchmarkInsertRows(rowCount)
	stringsData := make([]string, rowCount)
	jsonData := make([][]byte, rowCount)
	valid := make([]bool, rowCount)
	for row := 0; row < rowCount; row++ {
		stringsData[row] = strings.Repeat("s", row%131)
		jsonData[row] = []byte(`{"row":` + strconv.Itoa(row) + `,"payload":"` + strings.Repeat("j", row%257) + `"}`)
		valid[row] = row%3 != 0
	}
	jsonField := scalarField(2, schemapb.DataType_JSON,
		&schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: jsonData}})
	jsonField.ValidData = valid
	source := &msgpb.InsertRequest{
		NumRows: rowCount, RowIDs: rowIDs, Timestamps: timestamps,
		FieldsData: []*schemapb.FieldData{
			scalarField(1, schemapb.DataType_VarChar,
				&schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: stringsData}}),
			jsonField,
		},
	}
	limit := proto.Size(materializeWithAppendFieldData(insertViewTemplate(), source, rows[:17]))
	assertCursorExactSplits(t, insertViewTemplate(), source, rows, limit)
}

func TestInsertRequestViewEncoder_VectorArrayCellPlans(t *testing.T) {
	unknown := protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 7)
	fallback := &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{3, 4}}}}
	fallback.ProtoReflect().SetUnknown(unknown)
	cells := []*schemapb.VectorField{
		{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}},
		fallback,
		nil,
		{Dim: 2},
	}
	source := &msgpb.InsertRequest{
		NumRows: 4, RowIDs: []int64{1, 2, 3, 4}, Timestamps: []uint64{10, 20, 30, 40},
		FieldsData: []*schemapb.FieldData{vectorField(1, schemapb.DataType_ArrayOfVector, 2,
			&schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: 2, ElementType: schemapb.DataType_FloatVector, Data: cells}}, nil)},
	}
	assertViewMatchesMaterialized(t, insertViewTemplate(), source, []int{0, 1, 2, 3})
}

func TestInsertRequestViewEncoder_DifferentialNilRepeatedMessages(t *testing.T) {
	template := insertViewTemplate()
	source := insertViewAllRepackTypesSource()
	source.GetFieldsData()[6].GetScalars().GetArrayData().Data[2] = nil
	source.GetFieldsData()[17].GetVectors().GetVectorArray().Data[2] = nil
	assertViewMatchesMaterialized(t, template, source, []int{2})
}

func TestInsertRequestViewEncoder_ArrayNestedUnknownFields(t *testing.T) {
	packedUnknown := protowire.AppendTag(nil, 999, protowire.VarintType)
	packedUnknown = protowire.AppendVarint(packedUnknown, 17)
	packed := &schemapb.LongArray{Data: []int64{1, 128, -1}}
	packed.ProtoReflect().SetUnknown(packedUnknown)

	repeatedUnknown := protowire.AppendTag(nil, 998, protowire.BytesType)
	repeatedUnknown = protowire.AppendBytes(repeatedUnknown, []byte("future-string-array-field"))
	repeated := &schemapb.StringArray{Data: []string{"a", "b"}}
	repeated.ProtoReflect().SetUnknown(repeatedUnknown)

	source := &msgpb.InsertRequest{
		NumRows:    2,
		RowIDs:     []int64{1, 2},
		Timestamps: []uint64{10, 20},
		FieldsData: []*schemapb.FieldData{
			scalarField(1, schemapb.DataType_Array, &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_Int64,
				Data: []*schemapb.ScalarField{
					{Data: &schemapb.ScalarField_LongData{LongData: packed}},
					{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{2}}}},
				},
			}}),
			scalarField(2, schemapb.DataType_Array, &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_VarChar,
				Data: []*schemapb.ScalarField{
					{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"known"}}}},
					{Data: &schemapb.ScalarField_StringData{StringData: repeated}},
				},
			}}),
		},
	}

	rows := []int{0, 1}
	expected := materializeWithAppendFieldData(insertViewTemplate(), source, rows)

	encoder, err := NewInsertRequestViewEncoder(insertViewTemplate(), source, rows)
	require.NoError(t, err)
	size, err := encoder.EncodedSize()
	require.NoError(t, err)
	actualBytes := make([]byte, size)
	_, err = encoder.MarshalTo(actualBytes)
	require.NoError(t, err)
	assert.Equal(t, proto.Size(expected), len(actualBytes))
	replayedBytes := make([]byte, size)
	_, err = encoder.MarshalTo(replayedBytes)
	require.NoError(t, err)
	assert.Equal(t, actualBytes, replayedBytes)

	decoded := &msgpb.InsertRequest{}
	require.NoError(t, proto.Unmarshal(actualBytes, decoded))
	assert.True(t, proto.Equal(expected, decoded))
	assert.True(t, bytes.Equal(packedUnknown, decoded.GetFieldsData()[0].GetScalars().GetArrayData().GetData()[0].GetLongData().ProtoReflect().GetUnknown()))
	assert.True(t, bytes.Equal(repeatedUnknown, decoded.GetFieldsData()[1].GetScalars().GetArrayData().GetData()[1].GetStringData().ProtoReflect().GetUnknown()))
}

// TestInsertRequestViewEncoder_ArrayCellValidDataFallback pins the fix for a
// real proto change, not a synthetic one: schemapb.ScalarField and
// schemapb.VectorField each gained a ValidData field (element-level nullability
// within one cell) that has no producer anywhere in this repo yet. Because
// proto reflection recognizes it, GetUnknown() no longer catches it -- the
// arithmetic cell path only writes the oneof value, so a cell carrying it would
// silently lose that data. classifyScalarCell / classifyVectorArrayCell must
// treat a non-empty ValidData the same as an unrecognized field: fall back to
// the protobuf path, which serializes every field it does not know to drop.
func TestInsertRequestViewEncoder_ArrayCellValidDataFallback(t *testing.T) {
	scalarCell := &schemapb.ScalarField{
		Data:      &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}},
		ValidData: []bool{true, false},
	}
	vectorCell := &schemapb.VectorField{
		Dim:       2,
		Data:      &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}},
		ValidData: []bool{true},
	}

	source := &msgpb.InsertRequest{
		NumRows:    1,
		RowIDs:     []int64{1},
		Timestamps: []uint64{10},
		FieldsData: []*schemapb.FieldData{
			scalarField(1, schemapb.DataType_Array, &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_Int64,
				Data:        []*schemapb.ScalarField{scalarCell},
			}}),
			vectorField(2, schemapb.DataType_ArrayOfVector, 2,
				&schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
					Dim: 2, ElementType: schemapb.DataType_FloatVector,
					Data: []*schemapb.VectorField{vectorCell},
				}}, nil),
		},
	}

	rows := []int{0}
	expected := materializeWithAppendFieldData(insertViewTemplate(), source, rows)

	encoder, err := NewInsertRequestViewEncoder(insertViewTemplate(), source, rows)
	require.NoError(t, err)
	size, err := encoder.EncodedSize()
	require.NoError(t, err)
	actualBytes := make([]byte, size)
	_, err = encoder.MarshalTo(actualBytes)
	require.NoError(t, err)
	assert.Equal(t, proto.Size(expected), len(actualBytes))

	decoded := &msgpb.InsertRequest{}
	require.NoError(t, proto.Unmarshal(actualBytes, decoded))
	assert.True(t, proto.Equal(expected, decoded))
	assert.Equal(t, []bool{true, false},
		decoded.GetFieldsData()[0].GetScalars().GetArrayData().GetData()[0].GetValidData(),
		"ScalarField.ValidData must survive the cell, not be silently dropped")
	assert.Equal(t, []bool{true},
		decoded.GetFieldsData()[1].GetVectors().GetVectorArray().GetData()[0].GetValidData(),
		"VectorField.ValidData must survive the cell, not be silently dropped")
}

// TestInsertRequestViewEncoder_TopLevelFieldSpecificValidData covers the
// field-specific ScalarField.ValidData (field 17) / VectorField.ValidData
// (field 9) location that #52203 moved top-level row nullability to, with the
// legacy FieldData.ValidData left empty. The encoder must resolve validity
// with the same precedence as typeutil.GetFieldDataValidData and re-emit it at
// the field-specific location the AppendFieldData oracle now writes,
// row-selected like every other column. The vector payload is compacted: only
// non-null rows carry vector data.
func TestInsertRequestViewEncoder_TopLevelFieldSpecificValidData(t *testing.T) {
	source := &msgpb.InsertRequest{
		NumRows:    3,
		RowIDs:     []int64{1, 2, 3},
		Timestamps: []uint64{10, 20, 30},
		FieldsData: []*schemapb.FieldData{
			scalarField(1, schemapb.DataType_Int64, &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}),
			vectorField(2, schemapb.DataType_FloatVector, 2,
				&schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 5, 6}}}, nil),
		},
	}
	source.FieldsData[0].GetScalars().ValidData = []bool{true, false, true}
	// Compacted vector payload: rows 0 and 2 are valid and own the two vectors.
	source.FieldsData[1].GetVectors().ValidData = []bool{true, false, true}

	rows := []int{0, 1, 2}
	expected := materializeWithAppendFieldData(insertViewTemplate(), source, rows)

	encoder, err := NewInsertRequestViewEncoder(insertViewTemplate(), source, rows)
	require.NoError(t, err)
	size, err := encoder.EncodedSize()
	require.NoError(t, err)
	actualBytes := make([]byte, size)
	_, err = encoder.MarshalTo(actualBytes)
	require.NoError(t, err)
	assert.Equal(t, proto.Size(expected), len(actualBytes))

	decoded := &msgpb.InsertRequest{}
	require.NoError(t, proto.Unmarshal(actualBytes, decoded))
	assert.True(t, proto.Equal(expected, decoded))
	assert.Empty(t, decoded.GetFieldsData()[0].GetValidData(),
		"legacy FieldData.ValidData must stay empty; validity lives at the field-specific location")
	assert.Equal(t, []bool{true, false, true},
		decoded.GetFieldsData()[0].GetScalars().GetValidData(),
		"ScalarField.ValidData must survive at the field-specific location")
	assert.Equal(t, []bool{true, false, true},
		decoded.GetFieldsData()[1].GetVectors().GetValidData(),
		"VectorField.ValidData must survive at the field-specific location")

	// Row selection with nulls: keep rows 1 (null) and 2 (valid).
	subRows := []int{1, 2}
	subExpected := materializeWithAppendFieldData(insertViewTemplate(), source, subRows)
	subDecoded := encodeAndDecodeInsertView(t, insertViewTemplate(), source, subRows)
	assert.True(t, proto.Equal(subExpected, subDecoded))
	assert.Equal(t, []bool{false, true}, subDecoded.GetFieldsData()[1].GetVectors().GetValidData())
}

func TestInsertRequestViewEncoder_ExtendedScalarOneofs(t *testing.T) {
	// Bytes/Mol/MolSmiles/Date/Time are current ScalarField oneofs, but the old
	// AppendFieldData repack helper does not handle them. Keep this explicit
	// expected-object test separate from the old-path differential oracle above.
	rows := []int{0, 2, 3}
	source := &msgpb.InsertRequest{
		NumRows:    4,
		RowIDs:     []int64{1, 2, 3, 4},
		Timestamps: []uint64{11, 12, 13, 14},
		FieldsData: []*schemapb.FieldData{
			scalarField(100, schemapb.DataType_None, &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: [][]byte{{1}, {}, {3, 3}, {4}}}}),
			scalarField(101, schemapb.DataType_None, &schemapb.ScalarField_MolData{MolData: &schemapb.MolArray{Data: [][]byte{{10}, {20}, {30}, {40}}}}),
			scalarField(102, schemapb.DataType_Text, &schemapb.ScalarField_MolSmilesData{MolSmilesData: &schemapb.MolSmilesArray{Data: []string{"C", "CC", "CCC", ""}}}),
			scalarField(103, schemapb.DataType_Date, &schemapb.ScalarField_DateData{DateData: &schemapb.DateArray{Data: []int32{-1, 0, 1, math.MaxInt32}}}),
			scalarField(104, schemapb.DataType_Time, &schemapb.ScalarField_TimeData{TimeData: &schemapb.TimeArray{Data: []int64{-1, 0, 1, math.MaxInt64}}}),
		},
	}

	expected := materializeExtendedScalars(insertViewTemplate(), source, rows)
	got := encodeAndDecodeInsertView(t, insertViewTemplate(), source, rows)
	require.True(t, proto.Equal(expected, got), "extended scalar selection mismatch\nexpected: %v\nactual:   %v", expected, got)

	packedSource := &msgpb.InsertRequest{
		NumRows:    source.GetNumRows(),
		RowIDs:     source.GetRowIDs(),
		Timestamps: source.GetTimestamps(),
		FieldsData: source.GetFieldsData()[3:],
	}
	packedExpected := materializeExtendedScalars(insertViewTemplate(), packedSource, rows)
	packedGot := encodeAndDecodeInsertView(t, insertViewTemplate(), packedSource, rows)
	require.True(t, proto.Equal(packedExpected, packedGot), "packed date/time selection mismatch\nexpected: %v\nactual:   %v", packedExpected, packedGot)
}

func TestInsertRequestViewCursor_SequentialViews(t *testing.T) {
	template := insertViewTemplate()
	source := insertViewAllRepackTypesSource()
	cursor, err := NewInsertRequestViewCursor(source)
	require.NoError(t, err)

	for _, rows := range [][]int{{0, 2}, {3, 4}} {
		encoder, err := cursor.newEncoder(template, rows)
		require.NoError(t, err)
		size, err := encoder.EncodedSize()
		require.NoError(t, err)
		payload := make([]byte, size)
		_, err = encoder.MarshalTo(payload)
		require.NoError(t, err)
		got := &msgpb.InsertRequest{}
		require.NoError(t, proto.Unmarshal(payload, got))
		expected := materializeWithAppendFieldData(template, source, rows)
		require.True(t, proto.Equal(expected, got))
	}

	_, err = cursor.newEncoder(template, []int{4})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	_, err = NewInsertRequestViewCursor(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	t.Run("requires synchronous consumption", func(t *testing.T) {
		cursor, err := NewInsertRequestViewCursor(source)
		require.NoError(t, err)
		encoder, err := cursor.newEncoder(template, []int{0})
		require.NoError(t, err)
		_, err = cursor.newEncoder(template, []int{2})
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)

		size, err := encoder.EncodedSize()
		require.NoError(t, err)
		_, err = encoder.MarshalTo(make([]byte, size))
		require.NoError(t, err)
		_, err = cursor.newEncoder(template, []int{2})
		require.NoError(t, err)
	})

	t.Run("first view starts after row zero", func(t *testing.T) {
		cursor, err := NewInsertRequestViewCursor(source)
		require.NoError(t, err)
		for _, rows := range [][]int{{2}, {4}} {
			encoder, err := cursor.newEncoder(template, rows)
			require.NoError(t, err)
			size, err := encoder.EncodedSize()
			require.NoError(t, err)
			payload := make([]byte, size)
			_, err = encoder.MarshalTo(payload)
			require.NoError(t, err)

			got := &msgpb.InsertRequest{}
			require.NoError(t, proto.Unmarshal(payload, got))
			expected := materializeWithAppendFieldData(template, source, rows)
			require.True(t, proto.Equal(expected, got))
		}
	})

	t.Run("consumed encoder cannot release a newer encoder", func(t *testing.T) {
		cursor, err := NewInsertRequestViewCursor(source)
		require.NoError(t, err)

		first, err := cursor.newEncoder(template, []int{0})
		require.NoError(t, err)
		firstSize, err := first.EncodedSize()
		require.NoError(t, err)
		firstCopy := *first
		_, err = first.MarshalTo(make([]byte, firstSize))
		require.NoError(t, err)

		second, err := cursor.newEncoder(template, []int{2})
		require.NoError(t, err)
		_, err = firstCopy.MarshalTo(make([]byte, firstSize))
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)

		_, err = cursor.newEncoder(template, []int{3})
		require.Error(t, err, "the consumed encoder must not release the newer encoder's scratch")
		assert.ErrorIs(t, err, merr.ErrServiceInternal)

		secondSize, err := second.EncodedSize()
		require.NoError(t, err)
		_, err = second.MarshalTo(make([]byte, secondSize))
		require.NoError(t, err)
		_, err = cursor.newEncoder(template, []int{3})
		require.NoError(t, err)
	})

	t.Run("failed encoder creation does not advance compact prefix", func(t *testing.T) {
		cursor, err := NewInsertRequestViewCursor(source)
		require.NoError(t, err)

		_, err = cursor.newEncoder(nil, []int{2})
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)

		var encoder *InsertRequestViewEncoder
		require.NotPanics(t, func() {
			encoder, err = cursor.newEncoder(template, []int{0})
		})
		require.NoError(t, err)
		size, err := encoder.EncodedSize()
		require.NoError(t, err)
		payload := make([]byte, size)
		_, err = encoder.MarshalTo(payload)
		require.NoError(t, err)

		got := &msgpb.InsertRequest{}
		require.NoError(t, proto.Unmarshal(payload, got))
		expected := materializeWithAppendFieldData(template, source, []int{0})
		require.True(t, proto.Equal(expected, got))
	})
}

func TestInsertRequestViewCursor_AggregateRejectsShortValidData(t *testing.T) {
	rows := []int{0, 1}
	for _, tc := range []struct {
		name  string
		field *schemapb.FieldData
	}{
		{
			name: "payload-less field",
			field: &schemapb.FieldData{
				Type: schemapb.DataType_Bool, FieldName: "empty", FieldId: 100,
				ValidData: []bool{true},
			},
		},
		{
			name: "empty vector",
			field: &schemapb.FieldData{
				Type: schemapb.DataType_FloatVector, FieldName: "empty-vector", FieldId: 101,
				ValidData: []bool{true},
				Field:     &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 4}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &msgpb.InsertRequest{
				NumRows: 2, RowIDs: []int64{1, 2}, Timestamps: []uint64{10, 20},
				FieldsData: []*schemapb.FieldData{tc.field},
			}
			cursor, err := NewInsertRequestViewCursor(source)
			require.NoError(t, err)
			require.NotPanics(t, func() {
				_, _, err = cursor.NextEncoder(insertViewTemplate(), rows, 1<<20)
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}
}

func TestInsertRequestViewCursor_ExactPrefixSizing(t *testing.T) {
	const rowCount = 129
	rowIDs := make([]int64, rowCount)
	timestamps := make([]uint64, rowCount)
	longs := make([]int64, rowCount)
	stringsData := make([]string, rowCount)
	for row := 0; row < rowCount; row++ {
		rowIDs[row] = int64(row)
		timestamps[row] = uint64(row)
		longs[row] = int64(row)
	}
	for row, value := range []uint64{0, 127, 128, 16_383, 16_384} {
		rowIDs[row] = int64(value)
		timestamps[row] = value
		longs[row] = int64(value)
	}
	stringsData[1] = strings.Repeat("a", 127)
	stringsData[2] = strings.Repeat("b", 128)
	stringsData[3] = strings.Repeat("c", 16_383)
	stringsData[4] = strings.Repeat("d", 16_384)
	source := &msgpb.InsertRequest{
		NumRows:    rowCount,
		RowIDs:     rowIDs,
		Timestamps: timestamps,
		FieldsData: []*schemapb.FieldData{
			scalarField(1, schemapb.DataType_Int64, &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: longs}}),
			scalarField(2, schemapb.DataType_VarChar, &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: stringsData}}),
		},
	}
	template := insertViewTemplate()
	rows := make([]int, rowCount)
	for row := range rows {
		rows[row] = row
	}

	for _, prefix := range []int{1, 2, 3, 4, 5, 126, 127, 128, 129} {
		cursor, err := NewInsertRequestViewCursor(source)
		require.NoError(t, err)
		encoder, consumed, err := cursor.NextEncoder(template, rows[:prefix], 0)
		require.NoError(t, err)
		assert.Equal(t, prefix, consumed)
		expected := materializeWithAppendFieldData(template, source, rows[:prefix])
		size, err := encoder.EncodedSize()
		require.NoError(t, err)
		assert.Equal(t, proto.Size(expected), size, "prefix=%d", prefix)
		payload := make([]byte, size)
		written, err := encoder.MarshalTo(payload)
		require.NoError(t, err)
		assert.Equal(t, size, written)
	}
}

func TestInsertRequestViewCursor_ExactSplitPendingRow(t *testing.T) {
	unknown := protowire.AppendTag(nil, 999, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 17)
	fallbackArray := &schemapb.LongArray{Data: []int64{1, 128, -1}}
	fallbackArray.ProtoReflect().SetUnknown(unknown)

	for _, tc := range []struct {
		name string
		cell *schemapb.ScalarField
	}{
		{
			name: "arithmetic payload token",
			cell: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: []int64{1, 128, -1}},
			}},
		},
		{
			name: "protobuf fallback token",
			cell: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
				LongData: fallbackArray,
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			template := insertViewTemplate()
			source := insertViewAllRepackTypesSource()
			// Row 2 is the first rejected/pending row below. Its ARRAY token must
			// survive into the next view without re-sizing the cell. Keep
			// ArrayOfVector nil to retain zero-length nested-message coverage.
			source.GetFieldsData()[6].GetScalars().GetArrayData().Data[2] = tc.cell
			source.GetFieldsData()[17].GetVectors().GetVectorArray().Data[2] = nil
			oracleSource := proto.Clone(source).(*msgpb.InsertRequest)
			rows := []int{0, 1, 2, 3, 4}
			limit := proto.Size(materializeWithAppendFieldData(template, oracleSource, rows[:3]))

			cursor, err := NewInsertRequestViewCursor(source)
			require.NoError(t, err)
			start := 0
			var selections [][]int
			for start < len(rows) {
				encoder, consumed, err := cursor.NextEncoder(template, rows[start:], limit)
				require.NoError(t, err)
				selection := rows[start : start+consumed]
				selections = append(selections, append([]int(nil), selection...))

				size, err := encoder.EncodedSize()
				require.NoError(t, err)
				payload := make([]byte, size)
				_, err = encoder.MarshalTo(payload)
				require.NoError(t, err)
				got := &msgpb.InsertRequest{}
				require.NoError(t, proto.Unmarshal(payload, got))
				expected := materializeWithAppendFieldData(template, oracleSource, selection)
				require.True(t, proto.Equal(expected, got), "selection=%v", selection)
				start += consumed
			}
			assert.Equal(t, []int{0, 1}, selections[0], "candidate equal to the limit must roll over")
			assert.Equal(t, rows, flattenRowSelections(selections))
		})
	}
}

func TestInsertRequestViewCursor_SingleOversizedPendingRow(t *testing.T) {
	values := []string{"small", strings.Repeat("x", 4096), "small"}
	source := &msgpb.InsertRequest{
		NumRows:    3,
		RowIDs:     []int64{1, 2, 3},
		Timestamps: []uint64{1, 2, 3},
		FieldsData: []*schemapb.FieldData{
			scalarField(1, schemapb.DataType_VarChar, &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}),
		},
	}
	template := insertViewTemplate()
	rows := []int{0, 1, 2}
	smallSize := proto.Size(materializeWithAppendFieldData(template, source, rows[:1]))
	hugeSize := proto.Size(materializeWithAppendFieldData(template, source, rows[1:2]))
	limit := smallSize + 32
	require.Less(t, limit, hugeSize)

	cursor, err := NewInsertRequestViewCursor(source)
	require.NoError(t, err)
	start := 0
	var selections [][]int
	var sizes []int
	for start < len(rows) {
		encoder, consumed, err := cursor.NextEncoder(template, rows[start:], limit)
		require.NoError(t, err)
		selection := rows[start : start+consumed]
		selections = append(selections, append([]int(nil), selection...))
		size, err := encoder.EncodedSize()
		require.NoError(t, err)
		sizes = append(sizes, size)
		_, err = encoder.MarshalTo(make([]byte, size))
		require.NoError(t, err)
		start += consumed
	}
	assert.Equal(t, [][]int{{0}, {1}, {2}}, selections)
	assert.Less(t, sizes[0], limit)
	assert.Greater(t, sizes[1], limit)
	assert.Less(t, sizes[2], limit)
}

func TestInsertRequestViewCursor_DifferentialRandomExactSplits(t *testing.T) {
	r := rand.New(rand.NewSource(0x52269_51))
	template := insertViewTemplate()
	for iteration := 0; iteration < 100; iteration++ {
		rowCount := 2 + r.Intn(31)
		source := randomInsertViewSource(r, rowCount)
		rows := make([]int, 0, rowCount)
		for row := 0; row < rowCount; row++ {
			if r.Intn(4) != 0 {
				rows = append(rows, row)
			}
		}
		if len(rows) == 0 {
			rows = append(rows, r.Intn(rowCount))
		}
		oracleSource := proto.Clone(source).(*msgpb.InsertRequest)
		fullSize := proto.Size(materializeWithAppendFieldData(template, oracleSource, rows))
		limit := 1 + r.Intn(fullSize)

		cursor, err := NewInsertRequestViewCursor(source)
		require.NoError(t, err)
		start := 0
		for start < len(rows) {
			encoder, consumed, err := cursor.NextEncoder(template, rows[start:], limit)
			require.NoError(t, err)
			selection := rows[start : start+consumed]
			size, err := encoder.EncodedSize()
			require.NoError(t, err)
			if consumed > 1 {
				assert.Less(t, size, limit)
			}
			payload := make([]byte, size)
			_, err = encoder.MarshalTo(payload)
			require.NoError(t, err)
			got := &msgpb.InsertRequest{}
			require.NoError(t, proto.Unmarshal(payload, got))
			expected := materializeWithAppendFieldData(template, oracleSource, selection)
			require.True(t, proto.Equal(expected, got), "iteration=%d selection=%v limit=%d", iteration, selection, limit)
			if next := start + consumed; next < len(rows) {
				candidate := append(append([]int(nil), selection...), rows[next])
				candidateSize := proto.Size(materializeWithAppendFieldData(template, oracleSource, candidate))
				require.GreaterOrEqual(t, candidateSize, limit,
					"iteration=%d selection=%v next=%d limit=%d", iteration, selection, rows[next], limit)
			}
			start += consumed
		}
	}
}

func TestInsertRequestViewEncoder_AllNullHighDimVectorDoesNotMaterialize(t *testing.T) {
	const (
		rowCount = 10_000
		dim      = 32_768
	)
	rows := make([]int, rowCount)
	rowIDs := make([]int64, rowCount)
	timestamps := make([]uint64, rowCount)
	valid := make([]bool, rowCount)
	for row := range rows {
		rows[row] = row
		rowIDs[row] = int64(row)
	}
	source := &msgpb.InsertRequest{
		NumRows:    rowCount,
		RowIDs:     rowIDs,
		Timestamps: timestamps,
		FieldsData: []*schemapb.FieldData{{
			Type:      schemapb.DataType_FloatVector,
			FieldName: "embedding",
			FieldId:   100,
			ValidData: valid,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{},
				},
			}},
		}},
	}

	encoder, err := NewInsertRequestViewEncoder(insertViewTemplate(), source, rows)
	require.NoError(t, err)
	size, err := encoder.EncodedSize()
	require.NoError(t, err)
	assert.Less(t, size, 1<<20, "all-null vector payload must not scale with dim*rowCount")

	decoded := encodeAndDecodeInsertView(t, insertViewTemplate(), source, rows)
	require.Len(t, decoded.GetFieldsData()[0].GetVectors().GetValidData(), rowCount)
	assert.Nil(t, decoded.GetFieldsData()[0].GetVectors().GetFloatVector())
}

func TestInsertRequestViewEncoder_InternalContractErrors(t *testing.T) {
	validSource := &msgpb.InsertRequest{
		NumRows:    2,
		RowIDs:     []int64{1, 2},
		Timestamps: []uint64{10, 20},
		FieldsData: []*schemapb.FieldData{
			scalarField(1, schemapb.DataType_Int64, &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}),
		},
	}

	cases := []struct {
		name     string
		template *msgpb.InsertRequest
		source   *msgpb.InsertRequest
		rows     []int
	}{
		{name: "nil template", source: validSource, rows: []int{0}},
		{name: "nil source", template: insertViewTemplate(), rows: []int{0}},
		{name: "empty selection", template: insertViewTemplate(), source: validSource},
		{name: "negative row", template: insertViewTemplate(), source: validSource, rows: []int{-1}},
		{name: "duplicate row", template: insertViewTemplate(), source: validSource, rows: []int{0, 0}},
		{name: "descending rows", template: insertViewTemplate(), source: validSource, rows: []int{1, 0}},
		{name: "row ID out of range", template: insertViewTemplate(), source: validSource, rows: []int{2}},
		{name: "row based source", template: insertViewTemplate(), source: &msgpb.InsertRequest{RowData: []*commonpb.Blob{{Value: []byte{1}}}}, rows: nil},
		{name: "struct arrays", template: insertViewTemplate(), source: &msgpb.InsertRequest{
			NumRows: 1, RowIDs: []int64{1}, Timestamps: []uint64{1},
			FieldsData: []*schemapb.FieldData{{FieldId: 1, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{}}}},
		}, rows: []int{0}},
		{name: "short scalar", template: insertViewTemplate(), source: &msgpb.InsertRequest{
			NumRows: 2, RowIDs: []int64{1, 2}, Timestamps: []uint64{1, 2},
			FieldsData: []*schemapb.FieldData{scalarField(1, schemapb.DataType_Int64, &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}})},
		}, rows: []int{1}},
		{name: "short repeated scalar", template: insertViewTemplate(), source: &msgpb.InsertRequest{
			NumRows: 2, RowIDs: []int64{1, 2}, Timestamps: []uint64{1, 2},
			FieldsData: []*schemapb.FieldData{scalarField(1, schemapb.DataType_JSON, &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{}`)}}})},
		}, rows: []int{1}},
		{name: "short compact vector", template: insertViewTemplate(), source: &msgpb.InsertRequest{
			NumRows: 2, RowIDs: []int64{1, 2}, Timestamps: []uint64{1, 2},
			FieldsData: []*schemapb.FieldData{{
				Type: schemapb.DataType_FloatVector, FieldId: 1, ValidData: []bool{true, true},
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}}},
			}},
		}, rows: []int{1}},
		{name: "bad binary dim", template: insertViewTemplate(), source: &msgpb.InsertRequest{
			NumRows: 1, RowIDs: []int64{1}, Timestamps: []uint64{1},
			FieldsData: []*schemapb.FieldData{{
				Type: schemapb.DataType_BinaryVector, FieldId: 1,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 7, Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1}}}},
			}},
		}, rows: []int{0}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewInsertRequestViewEncoder(tc.template, tc.source, tc.rows)
			require.Error(t, err)
			assert.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}
}

func TestInsertRequestViewEncoder_MarshalContract(t *testing.T) {
	source := insertViewAllRepackTypesSource()
	encoder, err := NewInsertRequestViewEncoder(insertViewTemplate(), source, []int{0, 2})
	require.NoError(t, err)
	size, err := encoder.EncodedSize()
	require.NoError(t, err)
	first := make([]byte, size)
	_, err = encoder.MarshalTo(first)
	require.NoError(t, err)
	second := make([]byte, size)
	_, err = encoder.MarshalTo(second)
	require.NoError(t, err)
	assert.Equal(t, first, second, "standalone ARRAY size plans must be replayable")

	_, err = encoder.MarshalTo(make([]byte, size-1))
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	// Borrowed inputs must remain immutable for the standalone encoder's entire
	// lifetime. This particular size-changing contract violation is detected and
	// returned as a typed internal error instead of silently returning a payload
	// outside the caller's buffer; cached-size mutation behavior is otherwise
	// intentionally undefined by protobuf.
	source.FieldsData[5].GetScalars().GetStringData().Data[0] = "a much longer value"
	_, err = encoder.MarshalTo(make([]byte, size))
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestNestedMessageMarshalOptionsCachedSizeContract(t *testing.T) {
	sizeOptions := nestedMessageMarshalOptions(false)
	cachedOptions := nestedMessageMarshalOptions(true)

	assert.True(t, sizeOptions.AllowPartial)
	assert.True(t, cachedOptions.UseCachedSize)
	cachedOptions.UseCachedSize = false
	assert.Equal(t, sizeOptions, cachedOptions,
		"cached marshal options must differ from sizing only by UseCachedSize")
}

func TestInsertRequestViewEncoder_InvalidUTF8(t *testing.T) {
	invalid := string([]byte{0xff})
	baseSource := func(field *schemapb.FieldData) *msgpb.InsertRequest {
		return &msgpb.InsertRequest{
			NumRows:    1,
			RowIDs:     []int64{1},
			Timestamps: []uint64{10},
			FieldsData: []*schemapb.FieldData{field},
		}
	}

	t.Run("top-level varchar is treated as trusted", func(t *testing.T) {
		source := baseSource(scalarField(1, schemapb.DataType_VarChar, &schemapb.ScalarField_StringData{
			StringData: &schemapb.StringArray{Data: []string{invalid}},
		}))
		encoder, err := NewInsertRequestViewEncoder(insertViewTemplate(), source, []int{0})
		require.NoError(t, err)
		size, err := encoder.EncodedSize()
		require.NoError(t, err)
		payload := make([]byte, size)
		written, err := encoder.MarshalTo(payload)
		require.NoError(t, err)
		assert.Equal(t, size, written)
		assert.True(t, bytes.Contains(payload, []byte{0xff}))
	})

	// An array cell used to be written by proto.Marshal, which validates UTF-8
	// and fails. The arithmetic encoder that replaced it treats proto3 strings
	// as trusted internal input, so nested strings now behave like the
	// top-level varchar above: passed through untouched.
	t.Run("nested array string is treated as trusted", func(t *testing.T) {
		source := baseSource(scalarField(1, schemapb.DataType_Array, &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{
				Data: []*schemapb.ScalarField{{Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{invalid}},
				}}},
				ElementType: schemapb.DataType_VarChar,
			},
		}))
		encoder, err := NewInsertRequestViewEncoder(insertViewTemplate(), source, []int{0})
		require.NoError(t, err)
		size, err := encoder.EncodedSize()
		require.NoError(t, err)
		payload := make([]byte, size)
		written, err := encoder.MarshalTo(payload)
		require.NoError(t, err)
		assert.Equal(t, size, written)
		assert.True(t, bytes.Contains(payload, []byte{0xff}))
	})
}

func assertViewMatchesMaterialized(t *testing.T, template, source *msgpb.InsertRequest, rows []int) {
	t.Helper()
	expected := materializeWithAppendFieldData(template, source, rows)
	got := encodeAndDecodeInsertView(t, template, source, rows)
	require.True(t, proto.Equal(expected, got), "view encoding mismatch\nrows:     %v\nexpected: %v\nactual:   %v", rows, expected, got)
	assert.Equal(t, proto.Size(expected), proto.Size(got))
}

func assertCursorExactSplits(t *testing.T, template, source *msgpb.InsertRequest, rows []int, limit int) {
	t.Helper()
	oracleSource := proto.Clone(source).(*msgpb.InsertRequest)
	cursor, err := NewInsertRequestViewCursor(source)
	require.NoError(t, err)
	for start := 0; start < len(rows); {
		encoder, consumed, err := cursor.NextEncoder(template, rows[start:], limit)
		require.NoError(t, err)
		selection := rows[start : start+consumed]
		size, err := encoder.EncodedSize()
		require.NoError(t, err)
		if consumed > 1 {
			require.Less(t, size, limit)
		}
		payload := make([]byte, size)
		_, err = encoder.MarshalTo(payload)
		require.NoError(t, err)
		actual := &msgpb.InsertRequest{}
		require.NoError(t, proto.Unmarshal(payload, actual))
		expected := materializeWithAppendFieldData(template, oracleSource, selection)
		require.True(t, proto.Equal(expected, actual), "selection=%v", selection)
		if next := start + consumed; next < len(rows) {
			candidate := append(append([]int(nil), selection...), rows[next])
			candidateSize := proto.Size(materializeWithAppendFieldData(template, oracleSource, candidate))
			require.GreaterOrEqual(t, candidateSize, limit, "selection=%v next=%d", selection, rows[next])
		}
		start += consumed
	}
}

func encodeAndDecodeInsertView(t *testing.T, template, source *msgpb.InsertRequest, rows []int) *msgpb.InsertRequest {
	t.Helper()
	encoder, err := NewInsertRequestViewEncoder(template, source, rows)
	require.NoError(t, err)
	size, err := encoder.EncodedSize()
	require.NoError(t, err)
	dst := make([]byte, size+8)
	for i := size; i < len(dst); i++ {
		dst[i] = 0xA5
	}
	n, err := encoder.MarshalTo(dst)
	require.NoError(t, err)
	require.Equal(t, size, n)
	for i := size; i < len(dst); i++ {
		assert.Equal(t, byte(0xA5), dst[i], "MarshalTo wrote past EncodedSize")
	}
	got := &msgpb.InsertRequest{}
	require.NoError(t, proto.Unmarshal(dst[:n], got))
	return got
}

func materializeWithAppendFieldData(template, source *msgpb.InsertRequest, rows []int) *msgpb.InsertRequest {
	expected := proto.Clone(template).(*msgpb.InsertRequest)
	expected.Timestamps = make([]uint64, 0, len(rows))
	expected.RowIDs = make([]int64, 0, len(rows))
	expected.RowData = nil
	expected.FieldsData = make([]*schemapb.FieldData, len(source.GetFieldsData()))
	expected.NumRows = uint64(len(rows))

	idxComputer := typeutil.NewFieldDataIdxComputer(source.GetFieldsData())
	for _, row := range rows {
		fieldIndices := idxComputer.Compute(int64(row))
		typeutil.AppendFieldData(expected.FieldsData, source.GetFieldsData(), int64(row), fieldIndices...)
		expected.Timestamps = append(expected.Timestamps, source.GetTimestamps()[row])
		expected.RowIDs = append(expected.RowIDs, source.GetRowIDs()[row])
	}
	return expected
}

func materializeExtendedScalars(template, source *msgpb.InsertRequest, rows []int) *msgpb.InsertRequest {
	expected := proto.Clone(template).(*msgpb.InsertRequest)
	expected.Timestamps = make([]uint64, 0, len(rows))
	expected.RowIDs = make([]int64, 0, len(rows))
	expected.RowData = nil
	expected.FieldsData = make([]*schemapb.FieldData, 0, len(source.GetFieldsData()))
	expected.NumRows = uint64(len(rows))
	for _, row := range rows {
		expected.Timestamps = append(expected.Timestamps, source.GetTimestamps()[row])
		expected.RowIDs = append(expected.RowIDs, source.GetRowIDs()[row])
	}
	for _, sourceField := range source.GetFieldsData() {
		field := &schemapb.FieldData{
			Type: sourceField.GetType(), FieldName: sourceField.GetFieldName(), FieldId: sourceField.GetFieldId(), IsDynamic: sourceField.GetIsDynamic(),
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{}},
		}
		for _, row := range rows {
			if len(sourceField.GetValidData()) > 0 {
				field.ValidData = append(field.ValidData, sourceField.GetValidData()[row])
			}
		}
		switch value := sourceField.GetScalars().Data.(type) {
		case *schemapb.ScalarField_BytesData:
			selected := &schemapb.BytesArray{}
			for _, row := range rows {
				selected.Data = append(selected.Data, value.BytesData.GetData()[row])
			}
			field.GetScalars().Data = &schemapb.ScalarField_BytesData{BytesData: selected}
		case *schemapb.ScalarField_MolData:
			selected := &schemapb.MolArray{}
			for _, row := range rows {
				selected.Data = append(selected.Data, value.MolData.GetData()[row])
			}
			field.GetScalars().Data = &schemapb.ScalarField_MolData{MolData: selected}
		case *schemapb.ScalarField_MolSmilesData:
			selected := &schemapb.MolSmilesArray{}
			for _, row := range rows {
				selected.Data = append(selected.Data, value.MolSmilesData.GetData()[row])
			}
			field.GetScalars().Data = &schemapb.ScalarField_MolSmilesData{MolSmilesData: selected}
		case *schemapb.ScalarField_DateData:
			selected := &schemapb.DateArray{}
			for _, row := range rows {
				selected.Data = append(selected.Data, value.DateData.GetData()[row])
			}
			field.GetScalars().Data = &schemapb.ScalarField_DateData{DateData: selected}
		case *schemapb.ScalarField_TimeData:
			selected := &schemapb.TimeArray{}
			for _, row := range rows {
				selected.Data = append(selected.Data, value.TimeData.GetData()[row])
			}
			field.GetScalars().Data = &schemapb.ScalarField_TimeData{TimeData: selected}
		default:
			panic("unexpected extended scalar")
		}
		expected.FieldsData = append(expected.FieldsData, field)
	}
	return expected
}

func insertViewTemplate() *msgpb.InsertRequest {
	namespace := "tenant-a"
	return &msgpb.InsertRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Insert,
			MsgID:     100,
			Timestamp: 200,
			SourceID:  300,
		},
		ShardName:      "by-dev-rootcoord-dml_0_123v0",
		DbName:         "db",
		CollectionName: "collection",
		PartitionName:  "partition",
		DbID:           10,
		CollectionID:   20,
		PartitionID:    30,
		SegmentID:      40,
		Version:        msgpb.InsertDataVersion_ColumnBased,
		Namespace:      &namespace,
	}
}

func insertViewAllRepackTypesSource() *msgpb.InsertRequest {
	const rows = 5
	valid := []bool{true, false, true, true, false}
	arrayRows := make([]*schemapb.ScalarField, rows)
	vectorArrayRows := make([]*schemapb.VectorField, rows)
	for row := 0; row < rows; row++ {
		arrayRows[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{int64(row), int64(row + 10)}}}}
		vectorData := []float32{float32(row), float32(row) + 0.5}
		if !valid[row] {
			vectorData = nil // ArrayOfVector keeps a placeholder for null rows.
		}
		vectorArrayRows[row] = &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vectorData}}}
	}

	return &msgpb.InsertRequest{
		NumRows:    rows,
		RowIDs:     []int64{-1, 0, 1, 2, math.MaxInt64},
		Timestamps: []uint64{0, 1, 127, 128, math.MaxUint64},
		FieldsData: []*schemapb.FieldData{
			{Type: schemapb.DataType_Bool, FieldName: "bool", FieldId: 1, ValidData: valid, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true, false, true, false, true}}}}}},
			scalarField(2, schemapb.DataType_Int32, &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{-1, 0, 1, 128, math.MaxInt32}}}),
			scalarField(3, schemapb.DataType_Int64, &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{-1, 0, 1, 128, math.MaxInt64}}}),
			scalarField(4, schemapb.DataType_Float, &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{-1.5, 0, 1.5, float32(math.Inf(1)), float32(math.NaN())}}}),
			scalarField(5, schemapb.DataType_Double, &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{-1.5, 0, 1.5, math.Inf(-1), math.NaN()}}}),
			scalarField(6, schemapb.DataType_VarChar, &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"", "ascii", "向量", "x", "last"}}}),
			scalarField(7, schemapb.DataType_Array, &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: arrayRows, ElementType: schemapb.DataType_Int64}}),
			scalarField(8, schemapb.DataType_JSON, &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: [][]byte{[]byte(`{}`), []byte(`null`), []byte(`{"a":2}`), {}, []byte(`[]`)}}}),
			scalarField(9, schemapb.DataType_Timestamptz, &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{-1, 0, 1, 2, 3}}}),
			scalarField(10, schemapb.DataType_Geometry, &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: [][]byte{{1}, {2}, {3}, {}, {5}}}}),
			scalarField(11, schemapb.DataType_VarChar, &schemapb.ScalarField_GeometryWktData{GeometryWktData: &schemapb.GeometryWktArray{Data: []string{"POINT(0 0)", "", "POINT(2 2)", "POINT(3 3)", "POINT(4 4)"}}}),
			{Type: schemapb.DataType_BinaryVector, FieldName: "binary", FieldId: 12, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 16, Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}}}}},
			{Type: schemapb.DataType_FloatVector, FieldName: "float-vector", FieldId: 13, ValidData: valid, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{10, 11, 20, 21, 30, 31}}}}}},
			{Type: schemapb.DataType_Float16Vector, FieldName: "float16", FieldId: 14, ValidData: valid, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}}}}},
			{Type: schemapb.DataType_BFloat16Vector, FieldName: "bfloat16", FieldId: 15, ValidData: valid, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}}}}},
			{Type: schemapb.DataType_SparseFloatVector, FieldName: "sparse", FieldId: 16, ValidData: valid, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 99, Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: 100, Contents: [][]byte{sparseTestRow(1), sparseTestRow(9), sparseTestRow(4)}}}}}},
			{Type: schemapb.DataType_Int8Vector, FieldName: "int8", FieldId: 17, ValidData: valid, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 3, 4, 5, 6}}}}},
			{Type: schemapb.DataType_ArrayOfVector, FieldName: "array-of-vector", FieldId: 18, ValidData: valid, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: 2, ElementType: schemapb.DataType_FloatVector, Data: vectorArrayRows}}}}},
		},
	}
}

func randomInsertViewSource(r *rand.Rand, rows int) *msgpb.InsertRequest {
	valid := make([]bool, rows)
	floatData := make([]float32, 0, rows*4)
	binaryData := make([]byte, 0, rows*2)
	sparseData := make([][]byte, 0, rows)
	for row := 0; row < rows; row++ {
		valid[row] = r.Intn(3) != 0
		if valid[row] {
			for i := 0; i < 4; i++ {
				floatData = append(floatData, r.Float32())
			}
			binaryData = append(binaryData, byte(r.Uint32()), byte(r.Uint32()))
			sparseData = append(sparseData, sparseTestRow(uint32(r.Intn(256))))
		}
	}
	rowIDs := make([]int64, rows)
	timestamps := make([]uint64, rows)
	ints := make([]int64, rows)
	strings := make([]string, rows)
	vectorArrayRows := make([]*schemapb.VectorField, rows)
	for row := 0; row < rows; row++ {
		rowIDs[row] = int64(r.Uint64())
		timestamps[row] = r.Uint64()
		ints[row] = int64(r.Uint64())
		strings[row] = rowsName([]int{row, r.Intn(1000)})
		vectorArrayRows[row] = &schemapb.VectorField{Dim: 1, Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{byte(r.Uint32())}}}
	}
	return &msgpb.InsertRequest{
		NumRows: uint64(rows), RowIDs: rowIDs, Timestamps: timestamps,
		FieldsData: []*schemapb.FieldData{
			scalarField(1, schemapb.DataType_Int64, &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ints}}),
			scalarField(2, schemapb.DataType_VarChar, &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: strings}}),
			{Type: schemapb.DataType_FloatVector, FieldId: 3, ValidData: valid, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 4, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: floatData}}}}},
			{Type: schemapb.DataType_BinaryVector, FieldId: 4, ValidData: valid, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 16, Data: &schemapb.VectorField_BinaryVector{BinaryVector: binaryData}}}},
			{Type: schemapb.DataType_SparseFloatVector, FieldId: 5, ValidData: valid, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: 256, Contents: sparseData}}}}},
			{Type: schemapb.DataType_ArrayOfVector, FieldId: 6, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 1, Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: 1, ElementType: schemapb.DataType_Int8Vector, Data: vectorArrayRows}}}}},
		},
	}
}

func scalarField(id int64, dataType schemapb.DataType, data any) *schemapb.FieldData {
	scalar := &schemapb.ScalarField{}
	switch value := data.(type) {
	case *schemapb.ScalarField_BoolData:
		scalar.Data = value
	case *schemapb.ScalarField_IntData:
		scalar.Data = value
	case *schemapb.ScalarField_LongData:
		scalar.Data = value
	case *schemapb.ScalarField_FloatData:
		scalar.Data = value
	case *schemapb.ScalarField_DoubleData:
		scalar.Data = value
	case *schemapb.ScalarField_StringData:
		scalar.Data = value
	case *schemapb.ScalarField_BytesData:
		scalar.Data = value
	case *schemapb.ScalarField_ArrayData:
		scalar.Data = value
	case *schemapb.ScalarField_JsonData:
		scalar.Data = value
	case *schemapb.ScalarField_GeometryData:
		scalar.Data = value
	case *schemapb.ScalarField_TimestamptzData:
		scalar.Data = value
	case *schemapb.ScalarField_GeometryWktData:
		scalar.Data = value
	case *schemapb.ScalarField_MolData:
		scalar.Data = value
	case *schemapb.ScalarField_MolSmilesData:
		scalar.Data = value
	case *schemapb.ScalarField_DateData:
		scalar.Data = value
	case *schemapb.ScalarField_TimeData:
		scalar.Data = value
	default:
		panic("unsupported scalar test data")
	}
	return &schemapb.FieldData{
		Type: dataType, FieldName: rowsName([]int{int(id)}), FieldId: id,
		Field: &schemapb.FieldData_Scalars{Scalars: scalar},
	}
}

func sparseTestRow(index uint32) []byte {
	row := make([]byte, 8)
	binary.LittleEndian.PutUint32(row, index)
	binary.LittleEndian.PutUint32(row[4:], math.Float32bits(float32(index)+0.5))
	return row
}

func rowsName(rows []int) string {
	if len(rows) == 0 {
		return "empty"
	}
	result := "rows"
	for _, row := range rows {
		result += "_" + string(rune('a'+row%26))
	}
	return result
}

func flattenRowSelections(selections [][]int) []int {
	var rows []int
	for _, selection := range selections {
		rows = append(rows, selection...)
	}
	return rows
}
