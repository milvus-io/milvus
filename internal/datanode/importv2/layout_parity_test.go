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

package importv2

import (
	"bytes"
	"context"
	stdcsv "encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proxy/fieldvalidator"
	"github.com/milvus-io/milvus/internal/storage"
	csvreader "github.com/milvus-io/milvus/internal/util/importutilv2/csv"
	jsonreader "github.com/milvus-io/milvus/internal/util/importutilv2/json"
	parquetreader "github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The layout parity test checks that bulk import stores struct sub-fields in
// exactly the layout insert stores them in. The same logical rows go through
//   - the insert path: compact gRPC FieldData -> fieldvalidator.Validate
//     (dense expansion) -> storage.ColumnBasedInsertMsgToInsertData, and
//   - the import path: a JSON, CSV or Parquet file -> the format reader ->
//     CheckRowsEqual + NormalizeStructArrayFields (what preimport/import run),
//
// and the two InsertData must agree row by row (proto.Equal) and produce the
// same Arrow record through storage.BuildRecord, the serializer both paths
// share.

const (
	parityDim          = 4
	parityStructID     = int64(200)
	parityPKID         = int64(100)
	parityVecID        = int64(101)
	parityExtraID      = int64(211) // element-nullable sub-field added after the files were written
	parityDroppedID    = int64(299) // sub-field present in the files but not in the target schema
	parityDroppedVecID = int64(298) // dropped vector sub-field, a non-primitive Parquet child
	parityNullKey      = "NULL"
)

// parityElem is one struct element: raw sub-field name -> logical value.
// nil is a null element; a missing key is an omitted (null) element.
// Scalars are int64 / float32 / bool / string, a nested inner array is []any,
// a vector is []float32.
type parityElem = map[string]any

type parityRow struct {
	pk  int64
	vec []float32
	s   []parityElem // nil: null struct row
}

func parityCapacity(capacity int) []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: strconv.Itoa(capacity)}}
}

func parityScalarSubField(id int64, name string, elementType schemapb.DataType, elementNullable bool) *schemapb.FieldSchema {
	params := parityCapacity(8)
	if typeutil.IsStringType(elementType) {
		params = append(params, &commonpb.KeyValuePair{Key: common.MaxLengthKey, Value: "16"})
	}
	return &schemapb.FieldSchema{
		FieldID:         id,
		Name:            typeutil.ConcatStructFieldName("s", name),
		DataType:        schemapb.DataType_Array,
		ElementType:     elementType,
		ElementNullable: elementNullable,
		Nullable:        true,
		TypeParams:      params,
	}
}

func parityNestedSubField(id int64, name string, leaf schemapb.DataType, elementNullable, leafNullable bool) *schemapb.FieldSchema {
	var leafParams []*commonpb.KeyValuePair
	if typeutil.IsStringType(leaf) {
		leafParams = []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "8"}}
	}
	return &schemapb.FieldSchema{
		FieldID:         id,
		Name:            typeutil.ConcatStructFieldName("s", name),
		DataType:        schemapb.DataType_Array,
		ElementType:     schemapb.DataType_Array,
		ElementNullable: elementNullable,
		Nullable:        true,
		TypeSchema: &schemapb.TypeSchema{
			Nullable:   true, // matches the field's row-level nullable
			TypeParams: parityCapacity(8),
			Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
				Nullable:   elementNullable,
				TypeParams: parityCapacity(4),
				Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
					Nullable:   leafNullable,
					TypeParams: leafParams,
					Kind:       &schemapb.TypeSchema_LeafType{LeafType: leaf},
				}},
			}},
		},
	}
}

func parityVectorSubField(id int64, name string, elementNullable bool) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID:         id,
		Name:            typeutil.ConcatStructFieldName("s", name),
		DataType:        schemapb.DataType_ArrayOfVector,
		ElementType:     schemapb.DataType_FloatVector,
		ElementNullable: elementNullable,
		Nullable:        true,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: strconv.Itoa(parityDim)},
			{Key: common.MaxCapacityKey, Value: "8"},
		},
	}
}

func parityBaseSubFields() []*schemapb.FieldSchema {
	return []*schemapb.FieldSchema{
		parityScalarSubField(201, "id", schemapb.DataType_Int64, false), // legacy sub-field
		parityScalarSubField(202, "name", schemapb.DataType_VarChar, true),
		parityScalarSubField(203, "i8", schemapb.DataType_Int8, true),
		parityScalarSubField(204, "f", schemapb.DataType_Float, true),
		parityScalarSubField(205, "flag", schemapb.DataType_Bool, true),
		parityNestedSubField(206, "tags", schemapb.DataType_Int64, true, true),     // nullable inner + leaf
		parityNestedSubField(207, "words", schemapb.DataType_VarChar, false, true), // non-null inner, nullable leaf
		parityNestedSubField(208, "codes", schemapb.DataType_Int32, true, false),   // nullable inner, non-null leaf
		parityVectorSubField(209, "emb", true),
		parityVectorSubField(210, "emb2", false), // legacy vector sub-field
	}
}

func paritySchema(subFields []*schemapb.FieldSchema) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "layout_parity",
		Fields: []*schemapb.FieldSchema{
			{FieldID: parityPKID, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: parityVecID, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(parityDim)}},
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID:  parityStructID,
			Name:     "s",
			Nullable: true,
			Fields:   subFields,
		}},
	}
}

func parityRows() []parityRow {
	v := func(values ...float32) []float32 { return values }
	// l() must be an empty array, not a nil slice (which encodes as null)
	l := func(values ...any) []any { return append([]any{}, values...) }
	return []parityRow{
		{pk: 1, vec: v(1, 0, 0, 0), s: []parityElem{
			{
				"id": int64(10), "name": "a", "i8": int64(1), "f": float32(0.5), "flag": true,
				"tags": l(int64(1), int64(2)), "words": l("x", "y"), "codes": l(int64(7)),
				"emb": v(1, 0, 0, 0), "emb2": v(0, 1, 0, 0),
			},
			{
				"id": int64(11), "name": "bb", "i8": int64(-2), "f": float32(1.25), "flag": false,
				"tags": l(int64(3)), "words": l(), "codes": l(int64(8), int64(9)),
				"emb": v(0, 1, 0, 0), "emb2": v(0, 0, 1, 0),
			},
		}},
		{pk: 2, vec: v(0, 1, 0, 0), s: []parityElem{
			{
				"id": int64(20), "name": nil, "i8": nil, "f": float32(2.5), "flag": nil,
				"tags": l(int64(1), nil), "words": l(nil, "w"), "codes": nil,
				"emb": nil, "emb2": v(1, 1, 0, 0),
			},
			{
				"id": int64(21), "name": "c", "i8": int64(127), "f": nil, "flag": true,
				"tags": nil, "words": l("v"), "codes": l(int64(1), int64(2)),
				"emb": v(0.5, 0.5, 0, 0), "emb2": v(0, 0, 0, 1),
			},
			{
				"id": int64(22), "name": nil, "i8": int64(-128), "f": float32(-0.75), "flag": false,
				"tags": l(), "words": l(nil), "codes": l(),
				"emb": nil, "emb2": v(1, 0, 1, 0),
			},
		}},
		{pk: 3, vec: v(0, 0, 1, 0), s: nil},            // null struct row
		{pk: 4, vec: v(0, 0, 0, 1), s: []parityElem{}}, // empty struct list
		{pk: 5, vec: v(1, 1, 0, 0), s: []parityElem{ // every element-nullable position null
			{
				"id": int64(50), "name": nil, "i8": nil, "f": nil, "flag": nil,
				"tags": nil, "words": l(nil), "codes": nil,
				"emb": nil, "emb2": v(0.25, 0, 0, 0),
			},
		}},
		{pk: 6, vec: v(0, 1, 1, 0), s: []parityElem{
			{
				"id": int64(60), "name": "dddd", "i8": int64(5), "f": float32(3), "flag": true,
				"tags": l(nil, nil), "words": l(), "codes": l(int64(0)),
				"emb": v(0, 0, 0, 1), "emb2": v(0, 0, 0, 0.5),
			},
			{
				"id": int64(61), "name": "e", "i8": int64(0), "f": float32(0), "flag": false,
				"tags": l(int64(9), int64(8), int64(7), int64(6)), "words": l("a", "b", "c", "d"),
				"codes": l(int64(1), int64(2), int64(3), int64(4)), "emb": v(0.125, 0, 0, 0), "emb2": v(0, 0.125, 0, 0),
			},
		}},
		{pk: 7, vec: v(0, 0, 1, 1), s: nil}, // trailing null struct row
	}
}

// parityAllDroppedReadSubFields are the element-nullable base sub-fields plus
// the added "extra"; a file that carries none of them can still be imported.
func parityAllDroppedReadSubFields() []*schemapb.FieldSchema {
	return []*schemapb.FieldSchema{
		parityScalarSubField(202, "name", schemapb.DataType_VarChar, true),
		parityScalarSubField(203, "i8", schemapb.DataType_Int8, true),
		parityScalarSubField(204, "f", schemapb.DataType_Float, true),
		parityScalarSubField(205, "flag", schemapb.DataType_Bool, true),
		parityNestedSubField(206, "tags", schemapb.DataType_Int64, true, true),
		parityNestedSubField(208, "codes", schemapb.DataType_Int32, true, false),
		parityVectorSubField(209, "emb", true),
		parityScalarSubField(parityExtraID, "extra", schemapb.DataType_Int64, true),
	}
}

// parityAllDroppedRows keeps the row shapes of parityRows (null rows, empty
// lists, 1-3 elements) with elements that carry only dropped sub-fields.
func parityAllDroppedRows() []parityRow {
	rows := parityRows()
	for i := range rows {
		if rows[i].s == nil {
			continue
		}
		elems := make([]parityElem, len(rows[i].s))
		for j := range elems {
			elems[j] = parityElem{"dropped": int64(1000 + j), "dropped_emb": []float32{float32(j), 0, 0, 1}}
		}
		rows[i].s = elems
	}
	return rows
}

func parityRawName(t *testing.T, field *schemapb.FieldSchema) string {
	name, err := typeutil.ExtractStructFieldName(field.GetName())
	require.NoError(t, err)
	return name
}

// ---------------------------------------------------------------------------
// Insert path: compact gRPC FieldData (the client layout) -> Validate -> storage
// ---------------------------------------------------------------------------

// parityCompactLeaf builds one compact list of scalars: only the non-null
// values are in the payload; valid_data has one entry per logical value iff
// the level is nullable.
func parityCompactLeaf(t *testing.T, dataType schemapb.DataType, values []any, nullable bool) *schemapb.ScalarField {
	var validData []bool
	if nullable {
		validData = make([]bool, len(values))
	}
	var longs []int64
	var ints []int32
	var floats []float32
	var doubles []float64
	var bools []bool
	var strs []string
	for i, value := range values {
		if value == nil {
			require.True(t, nullable, "test data puts a null at a non-nullable position")
			continue
		}
		if nullable {
			validData[i] = true
		}
		switch dataType {
		case schemapb.DataType_Int64:
			longs = append(longs, value.(int64))
		case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
			ints = append(ints, int32(value.(int64)))
		case schemapb.DataType_Float:
			floats = append(floats, value.(float32))
		case schemapb.DataType_Double:
			doubles = append(doubles, value.(float64))
		case schemapb.DataType_Bool:
			bools = append(bools, value.(bool))
		case schemapb.DataType_VarChar, schemapb.DataType_String:
			strs = append(strs, value.(string))
		default:
			require.FailNow(t, "unsupported leaf type", dataType.String())
		}
	}
	row := &schemapb.ScalarField{}
	switch dataType {
	case schemapb.DataType_Int64:
		row.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: longs}}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		row.Data = &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: ints}}
	case schemapb.DataType_Float:
		row.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: floats}}
	case schemapb.DataType_Double:
		row.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: doubles}}
	case schemapb.DataType_Bool:
		row.Data = &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: bools}}
	default:
		row.Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: strs}}
	}
	if nullable {
		typeutil.SetArrayElementValidData(row, validData)
	}
	return row
}

// parityCompactSubFieldRow builds one compact client row of a sub-field from
// the logical element values (one per struct element).
func parityCompactSubFieldRow(t *testing.T, field *schemapb.FieldSchema, values []any) any {
	switch {
	case field.GetDataType() == schemapb.DataType_ArrayOfVector:
		var payload []float32
		var validData []bool
		if field.GetElementNullable() {
			validData = make([]bool, len(values))
		}
		for i, value := range values {
			if value == nil {
				require.True(t, field.GetElementNullable(), "test data puts a null vector into %s", field.GetName())
				continue
			}
			payload = append(payload, value.([]float32)...)
			if validData != nil {
				validData[i] = true
			}
		}
		row := &schemapb.VectorField{
			Dim:  parityDim,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: payload}},
		}
		if field.GetElementNullable() {
			typeutil.SetVectorArrayElementValidData(row, validData)
		}
		return row
	case typeutil.IsNestedArrayTypeSchema(field.GetTypeSchema()):
		leaf, leafNullable, _, err := typeutil.GetFieldArrayLeaf(field)
		require.NoError(t, err)
		children := make([]*schemapb.ScalarField, 0, len(values))
		var validData []bool
		if field.GetElementNullable() {
			validData = make([]bool, len(values))
		}
		for i, value := range values {
			if value == nil {
				require.True(t, field.GetElementNullable(), "test data puts a null inner array into %s", field.GetName())
				continue
			}
			children = append(children, parityCompactLeaf(t, leaf, value.([]any), leafNullable))
			if validData != nil {
				validData[i] = true
			}
		}
		row := &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{ElementType: leaf, Data: children},
		}}
		if field.GetElementNullable() {
			typeutil.SetArrayElementValidData(row, validData)
		}
		return row
	default:
		return parityCompactLeaf(t, field.GetElementType(), values, field.GetElementNullable())
	}
}

// parityInsertData runs the logical rows through the insert path. Sub-fields
// the rows do not mention (an added sub-field) are sent as all-null elements,
// which is what the proxy's omitted-sub-field fill produces before Validate.
func parityInsertData(t *testing.T, schema *schemapb.CollectionSchema, rows []parityRow) *storage.InsertData {
	numRows := len(rows)
	pks := make([]int64, 0, numRows)
	vecs := make([]float32, 0, numRows*parityDim)
	rowValid := make([]bool, numRows)
	for i, row := range rows {
		pks = append(pks, row.pk)
		vecs = append(vecs, row.vec...)
		rowValid[i] = row.s != nil
	}
	fieldsData := []*schemapb.FieldData{
		{
			Type: schemapb.DataType_Int64, FieldName: "pk", FieldId: parityPKID,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
			}},
		},
		{
			Type: schemapb.DataType_FloatVector, FieldName: "vec", FieldId: parityVecID,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  parityDim,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: vecs}},
			}},
		},
	}

	// Flattened sub-field columns, the shape checkAndFlattenStructFieldData
	// hands to Validate: one FieldData per sub-field named "s[sub]", compact
	// rows for the valid struct rows only, row validity on every column of a
	// nullable struct.
	for _, field := range schema.GetStructArrayFields()[0].GetFields() {
		raw := parityRawName(t, field)
		var scalarRows []*schemapb.ScalarField
		var vectorRows []*schemapb.VectorField
		for _, row := range rows {
			if row.s == nil {
				continue
			}
			values := make([]any, len(row.s))
			for j, elem := range row.s {
				values[j] = elem[raw]
			}
			switch compact := parityCompactSubFieldRow(t, field, values).(type) {
			case *schemapb.ScalarField:
				scalarRows = append(scalarRows, compact)
			case *schemapb.VectorField:
				vectorRows = append(vectorRows, compact)
			}
		}
		var fieldData *schemapb.FieldData
		if field.GetDataType() == schemapb.DataType_ArrayOfVector {
			fieldData = &schemapb.FieldData{
				Type: schemapb.DataType_ArrayOfVector, FieldName: field.GetName(), FieldId: field.GetFieldID(),
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
					Dim: parityDim,
					Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
						Dim: parityDim, ElementType: field.GetElementType(), Data: vectorRows,
					}},
				}},
			}
		} else {
			fieldData = &schemapb.FieldData{
				Type: schemapb.DataType_Array, FieldName: field.GetName(), FieldId: field.GetFieldID(),
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
						ElementType: field.GetElementType(), Data: scalarRows,
					}},
				}},
			}
		}
		if schema.GetStructArrayFields()[0].GetNullable() {
			typeutil.SetFieldDataValidData(fieldData, append([]bool(nil), rowValid...))
		}
		fieldsData = append(fieldsData, fieldData)
	}

	msg := &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{
		FieldsData: fieldsData,
		NumRows:    uint64(numRows),
		Version:    msgpb.InsertDataVersion_ColumnBased,
	}}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	// same options as the proxy insert task
	err = fieldvalidator.NewValidateUtil(
		fieldvalidator.WithNANCheck(),
		fieldvalidator.WithOverflowCheck(),
		fieldvalidator.WithMaxLenCheck(),
		fieldvalidator.WithMaxCapCheck(),
	).Validate(msg.GetFieldsData(), helper, msg.NRows())
	require.NoError(t, err)

	data, err := storage.ColumnBasedInsertMsgToInsertData(msg, schema)
	require.NoError(t, err)
	return data
}

// ---------------------------------------------------------------------------
// Import files, written from the same logical rows in the user-facing shapes
// ---------------------------------------------------------------------------

// parityStructValue renders one struct row for JSON / CSV: the list of element
// objects. Keys come from the writer schema; a null value is written as null,
// or the key is left out when omitNull says so for that element.
func parityStructValue(t *testing.T, writeSubFields []*schemapb.FieldSchema, elems []parityElem, omitNull func(j int) bool) any {
	if elems == nil {
		return nil
	}
	out := make([]any, 0, len(elems))
	for j, elem := range elems {
		object := make(map[string]any, len(writeSubFields))
		for _, field := range writeSubFields {
			raw := parityRawName(t, field)
			value, ok := elem[raw]
			if !ok || (value == nil && omitNull(j)) {
				continue
			}
			object[raw] = value
		}
		out = append(out, object)
	}
	return out
}

func parityWriteJSON(t *testing.T, path string, writeSchema *schemapb.CollectionSchema, rows []parityRow) {
	subFields := writeSchema.GetStructArrayFields()[0].GetFields()
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"pk":  row.pk,
			"vec": row.vec,
			// odd elements leave null element-nullable keys out
			"s": parityStructValue(t, subFields, row.s, func(j int) bool { return j%2 == 1 }),
		})
	}
	content, err := json.Marshal(out)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, content, 0o600))
}

func parityWriteCSV(t *testing.T, path string, writeSchema *schemapb.CollectionSchema, rows []parityRow) {
	subFields := writeSchema.GetStructArrayFields()[0].GetFields()
	var buf bytes.Buffer
	w := stdcsv.NewWriter(&buf)
	require.NoError(t, w.Write([]string{"pk", "vec", "s"}))
	nullRows := 0
	for _, row := range rows {
		vec, err := json.Marshal(row.vec)
		require.NoError(t, err)
		var cell string
		if row.s == nil {
			// a null struct row as the null key and as JSON null
			if nullRows%2 == 0 {
				cell = parityNullKey
			} else {
				cell = "null"
			}
			nullRows++
		} else {
			// even elements leave null element-nullable keys out
			content, err := json.Marshal(parityStructValue(t, subFields, row.s, func(j int) bool { return j%2 == 0 }))
			require.NoError(t, err)
			cell = string(content)
		}
		require.NoError(t, w.Write([]string{strconv.FormatInt(row.pk, 10), string(vec), cell}))
	}
	w.Flush()
	require.NoError(t, w.Error())
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
}

// parityAppendArrow appends one logical value to an Arrow builder whose type
// follows the file schema.
func parityAppendArrow(t *testing.T, b array.Builder, value any) {
	if value == nil {
		b.AppendNull()
		return
	}
	switch builder := b.(type) {
	case *array.Int64Builder:
		builder.Append(value.(int64))
	case *array.Int32Builder:
		builder.Append(int32(value.(int64)))
	case *array.Int8Builder:
		builder.Append(int8(value.(int64)))
	case *array.Float32Builder:
		builder.Append(value.(float32))
	case *array.BooleanBuilder:
		builder.Append(value.(bool))
	case *array.StringBuilder:
		builder.Append(value.(string))
	case *array.ListBuilder:
		builder.Append(true)
		switch items := value.(type) {
		case []float32:
			for _, item := range items {
				parityAppendArrow(t, builder.ValueBuilder(), item)
			}
		case []any:
			for _, item := range items {
				parityAppendArrow(t, builder.ValueBuilder(), item)
			}
		case []parityElem:
			for _, item := range items {
				parityAppendArrow(t, builder.ValueBuilder(), item)
			}
		default:
			require.FailNow(t, "unexpected list value", "%T", value)
		}
	case *array.StructBuilder:
		elem := value.(parityElem)
		builder.Append(true)
		structType := builder.Type().(*arrow.StructType)
		for i := 0; i < builder.NumField(); i++ {
			// a key missing from the element (an added sub-field) is a null child
			parityAppendArrow(t, builder.FieldBuilder(i), elem[structType.Field(i).Name])
		}
	default:
		require.FailNow(t, "unexpected arrow builder", "%T", b)
	}
}

func parityWriteParquet(t *testing.T, path string, writeSchema *schemapb.CollectionSchema, rows []parityRow) {
	ut, err := parquetreader.ConvertToArrowSchemaForUT(writeSchema, false)
	require.NoError(t, err)
	// the struct column carries null struct rows, so it must be nullable
	fields := make([]arrow.Field, 0, ut.NumFields())
	for _, field := range ut.Fields() {
		if field.Name == "s" {
			field.Nullable = true
		}
		fields = append(fields, field)
	}
	arrowSchema := arrow.NewSchema(fields, nil)

	rb := array.NewRecordBuilder(memory.NewGoAllocator(), arrowSchema)
	defer rb.Release()
	for _, row := range rows {
		values := map[string]any{"pk": row.pk, "vec": row.vec}
		if row.s != nil {
			values["s"] = row.s
		}
		for i, field := range arrowSchema.Fields() {
			parityAppendArrow(t, rb.Field(i), values[field.Name])
		}
	}
	record := rb.NewRecord()
	defer record.Release()

	var buf bytes.Buffer
	fw, err := pqarrow.NewFileWriter(arrowSchema, &buf,
		parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(len(rows)))), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, fw.Write(record))
	require.NoError(t, fw.Close())
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))
}

type parityReader interface {
	Read() (*storage.InsertData, error)
	Close()
}

// parityReadAll drains a reader and applies the datanode post-processing
// preimport and import run on every batch.
func parityReadAll(t *testing.T, schema *schemapb.CollectionSchema, reader parityReader) *storage.InsertData {
	defer reader.Close()
	var all *storage.InsertData
	for {
		data, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if all == nil {
			all = data
			continue
		}
		require.NoError(t, storage.MergeInsertData(all, data))
	}
	require.NotNil(t, all)
	require.NoError(t, CheckRowsEqual(schema, all))
	require.NoError(t, NormalizeStructArrayFields(schema, all))
	return all
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

func parityRequireSameInsertData(t *testing.T, schema *schemapb.CollectionSchema, want, got *storage.InsertData) {
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		w, ok := want.Data[field.GetFieldID()]
		require.True(t, ok, "insert data lacks field %s", field.GetName())
		g, ok := got.Data[field.GetFieldID()]
		require.True(t, ok, "import data lacks field %s", field.GetName())
		require.Equal(t, w.RowNum(), g.RowNum(), "row count of field %s", field.GetName())
		require.Equal(t, w.GetNullable(), g.GetNullable(), "nullable of field %s", field.GetName())
		if w.GetNullable() {
			require.Equal(t, w.GetValidData(), g.GetValidData(), "row validity of field %s", field.GetName())
		}
		switch wd := w.(type) {
		case *storage.ArrayFieldData:
			gd, ok := g.(*storage.ArrayFieldData)
			require.True(t, ok, "field %s: import column is %T", field.GetName(), g)
			require.Equal(t, wd.ElementType, gd.ElementType, "element type of field %s", field.GetName())
			require.Equal(t, wd.ElementNullable, gd.ElementNullable, "element_nullable of field %s", field.GetName())
		case *storage.VectorArrayFieldData:
			gd, ok := g.(*storage.VectorArrayFieldData)
			require.True(t, ok, "field %s: import column is %T", field.GetName(), g)
			require.Equal(t, wd.Dim, gd.Dim, "dim of field %s", field.GetName())
			require.Equal(t, wd.ElementType, gd.ElementType, "element type of field %s", field.GetName())
			require.Equal(t, wd.ElementNullable, gd.ElementNullable, "element_nullable of field %s", field.GetName())
		}
		for i := 0; i < w.RowNum(); i++ {
			wantRow, gotRow := w.GetRow(i), g.GetRow(i)
			switch wr := wantRow.(type) {
			case nil:
				require.Nil(t, gotRow, "field %s row %d should be null", field.GetName(), i)
			case *schemapb.ScalarField:
				gr, ok := gotRow.(*schemapb.ScalarField)
				require.True(t, ok, "field %s row %d: import row is %T", field.GetName(), i, gotRow)
				require.True(t, proto.Equal(wr, gr), "field %s row %d\ninsert: %v\nimport: %v", field.GetName(), i, wr, gr)
			case *schemapb.VectorField:
				gr, ok := gotRow.(*schemapb.VectorField)
				require.True(t, ok, "field %s row %d: import row is %T", field.GetName(), i, gotRow)
				require.True(t, proto.Equal(wr, gr), "field %s row %d\ninsert: %v\nimport: %v", field.GetName(), i, wr, gr)
			default:
				require.Equal(t, wantRow, gotRow, "field %s row %d", field.GetName(), i)
			}
		}
	}
}

func parityBuildRecord(t *testing.T, schema *schemapb.CollectionSchema, data *storage.InsertData) arrow.Record {
	arrowSchema, err := storage.ConvertToArrowSchema(schema, false)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	require.NoError(t, storage.BuildRecord(builder, data, schema))
	return builder.NewRecord()
}

func parityRequireSameRecord(t *testing.T, schema *schemapb.CollectionSchema, want, got *storage.InsertData) {
	wantRecord := parityBuildRecord(t, schema, want)
	defer wantRecord.Release()
	gotRecord := parityBuildRecord(t, schema, got)
	defer gotRecord.Release()
	require.Equal(t, wantRecord.NumRows(), gotRecord.NumRows())
	require.Equal(t, wantRecord.NumCols(), gotRecord.NumCols())
	for i := 0; i < int(wantRecord.NumCols()); i++ {
		require.True(t, array.Equal(wantRecord.Column(i), gotRecord.Column(i)),
			"arrow column %s differs\ninsert: %v\nimport: %v", wantRecord.ColumnName(i), wantRecord.Column(i), gotRecord.Column(i))
	}
	require.True(t, array.RecordEqual(wantRecord, gotRecord))
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

func TestImportLayoutParityWithInsert(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := storage.NewLocalChunkManager()
	const bufferSize = 64 * 1024 * 1024

	baseSchema := paritySchema(parityBaseSubFields())

	// Sub-field evolution: the files were written before sub-field "extra" was
	// added and still carry the dropped sub-field "dropped". Import fills
	// "extra" with null elements and ignores "dropped".
	writeEvolved := paritySchema(append(parityBaseSubFields(),
		parityScalarSubField(parityDroppedID, "dropped", schemapb.DataType_Int64, true)))
	readEvolved := paritySchema(append(parityBaseSubFields(),
		parityScalarSubField(parityExtraID, "extra", schemapb.DataType_Int64, true)))
	evolvedRows := parityRows()
	for _, row := range evolvedRows {
		for j, elem := range row.s {
			elem["dropped"] = int64(1000 + j)
		}
	}

	// Every sub-field of the files was dropped: the struct column carries none
	// of the current sub-fields, all of which are element-nullable. Import
	// keeps the row shape of the column and fills every element with null.
	readAllNull := paritySchema(parityAllDroppedReadSubFields())
	writeDropped := paritySchema([]*schemapb.FieldSchema{
		parityScalarSubField(parityDroppedID, "dropped", schemapb.DataType_Int64, true),
		parityVectorSubField(parityDroppedVecID, "dropped_emb", true),
	})
	writeDroppedVec := paritySchema([]*schemapb.FieldSchema{
		parityVectorSubField(parityDroppedVecID, "dropped_emb", true),
	})

	cases := []struct {
		name        string
		writeSchema *schemapb.CollectionSchema
		readSchema  *schemapb.CollectionSchema
		rows        []parityRow
	}{
		{name: "same_schema", writeSchema: baseSchema, readSchema: baseSchema, rows: parityRows()},
		{name: "added_and_dropped_sub_fields", writeSchema: writeEvolved, readSchema: readEvolved, rows: evolvedRows},
		{name: "all_sub_fields_dropped", writeSchema: writeDropped, readSchema: readAllNull, rows: parityAllDroppedRows()},
		{name: "all_sub_fields_dropped_vector_only", writeSchema: writeDroppedVec, readSchema: readAllNull, rows: parityAllDroppedRows()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := parityInsertData(t, tc.readSchema, tc.rows)
			dir := t.TempDir()

			formats := []struct {
				name string
				open func(t *testing.T) parityReader
			}{
				{name: "json", open: func(t *testing.T) parityReader {
					path := filepath.Join(dir, "rows.json")
					parityWriteJSON(t, path, tc.writeSchema, tc.rows)
					reader, err := jsonreader.NewReader(ctx, cm, tc.readSchema, path, bufferSize)
					require.NoError(t, err)
					return reader
				}},
				{name: "csv", open: func(t *testing.T) parityReader {
					path := filepath.Join(dir, "rows.csv")
					parityWriteCSV(t, path, tc.writeSchema, tc.rows)
					reader, err := csvreader.NewReader(ctx, cm, tc.readSchema, path, bufferSize, ',', parityNullKey)
					require.NoError(t, err)
					return reader
				}},
				{name: "parquet", open: func(t *testing.T) parityReader {
					path := filepath.Join(dir, "rows.parquet")
					parityWriteParquet(t, path, tc.writeSchema, tc.rows)
					reader, err := parquetreader.NewReader(ctx, cm, tc.readSchema, path, bufferSize)
					require.NoError(t, err)
					return reader
				}},
			}
			for _, format := range formats {
				t.Run(format.name, func(t *testing.T) {
					got := parityReadAll(t, tc.readSchema, format.open(t))
					parityRequireSameInsertData(t, tc.readSchema, want, got)
					parityRequireSameRecord(t, tc.readSchema, want, got)
				})
			}
		})
	}
}

// parityNonNullableStruct returns schema with its struct and sub-fields
// non-nullable.
func parityNonNullableStruct(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	schema = proto.Clone(schema).(*schemapb.CollectionSchema)
	structField := schema.GetStructArrayFields()[0]
	structField.Nullable = false
	for _, field := range structField.GetFields() {
		field.Nullable = false
		if field.GetTypeSchema() != nil {
			field.GetTypeSchema().Nullable = false
		}
	}
	return schema
}

// TestCreateFieldReaders_AllStructSubFieldsDropped reads a Parquet struct
// column none of whose children is a current sub-field through
// CreateFieldReaders: every element-nullable sub-field gets a reader that
// yields the column shape with null elements, and NormalizeStructArrayFields
// leaves the result unchanged.
func TestCreateFieldReaders_AllStructSubFieldsDropped(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	writeSchema := paritySchema([]*schemapb.FieldSchema{
		parityScalarSubField(parityDroppedID, "dropped", schemapb.DataType_Int64, true),
	})

	read := func(t *testing.T, schema *schemapb.CollectionSchema, rows []parityRow) (*storage.InsertData, error) {
		path := filepath.Join(t.TempDir(), "rows.parquet")
		parityWriteParquet(t, path, writeSchema, rows)
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		pqReader, err := file.NewParquetReader(bytes.NewReader(content))
		require.NoError(t, err)
		defer pqReader.Close()
		fileReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 64}, memory.DefaultAllocator)
		require.NoError(t, err)

		crs, err := parquetreader.CreateFieldReaders(ctx, fileReader, schema)
		if err != nil {
			return nil, err
		}
		for _, field := range schema.GetStructArrayFields()[0].GetFields() {
			require.Contains(t, crs, field.GetFieldID(), "no reader for %s", field.GetName())
		}
		data, err := storage.NewInsertData(schema)
		require.NoError(t, err)
		for fieldID, cr := range crs {
			rowsData, validData, err := cr.Next(64)
			if err != nil {
				return nil, err
			}
			require.NoError(t, data.Data[fieldID].AppendRows(rowsData, validData))
		}
		return data, nil
	}

	requireNormalizeNoop := func(t *testing.T, schema *schemapb.CollectionSchema, data *storage.InsertData, rows []parityRow) {
		before := make(map[int64]storage.FieldData)
		for _, field := range schema.GetStructArrayFields()[0].GetFields() {
			before[field.GetFieldID()] = data.Data[field.GetFieldID()]
		}
		require.NoError(t, CheckRowsEqual(schema, data))
		require.NoError(t, NormalizeStructArrayFields(schema, data))
		for fieldID, fieldData := range before {
			require.Same(t, fieldData, data.Data[fieldID], "field %d was refilled", fieldID)
		}
		// the columns equal what insert stores for the same logical rows
		want := parityInsertData(t, schema, rows)
		for _, field := range schema.GetStructArrayFields()[0].GetFields() {
			w, g := want.Data[field.GetFieldID()], data.Data[field.GetFieldID()]
			require.Equal(t, w.RowNum(), g.RowNum())
			require.Equal(t, w.GetValidData(), g.GetValidData(), "row validity of %s", field.GetName())
			for i := 0; i < w.RowNum(); i++ {
				wantRow, gotRow := w.GetRow(i), g.GetRow(i)
				if wantRow == nil {
					require.Nil(t, gotRow, "field %s row %d", field.GetName(), i)
					continue
				}
				require.True(t, proto.Equal(wantRow.(proto.Message), gotRow.(proto.Message)),
					"field %s row %d\ninsert: %v\nimport: %v", field.GetName(), i, wantRow, gotRow)
			}
		}
	}

	t.Run("nullable struct", func(t *testing.T) {
		schema := paritySchema(parityAllDroppedReadSubFields())
		rows := parityAllDroppedRows()
		data, err := read(t, schema, rows)
		require.NoError(t, err)
		requireNormalizeNoop(t, schema, data, rows)
	})

	t.Run("non-nullable struct", func(t *testing.T) {
		schema := parityNonNullableStruct(paritySchema(parityAllDroppedReadSubFields()))
		rows := make([]parityRow, 0)
		for _, row := range parityAllDroppedRows() {
			if row.s != nil {
				rows = append(rows, row)
			}
		}
		data, err := read(t, schema, rows)
		require.NoError(t, err)
		requireNormalizeNoop(t, schema, data, rows)

		_, err = read(t, schema, parityAllDroppedRows())
		require.ErrorIs(t, err, merr.ErrImportFailed, "a null struct row of a non-nullable struct")
	})

	t.Run("missing non-element-nullable sub-field", func(t *testing.T) {
		schema := paritySchema(append(parityAllDroppedReadSubFields(),
			parityScalarSubField(201, "id", schemapb.DataType_Int64, false)))
		_, err := read(t, schema, parityAllDroppedRows())
		require.ErrorIs(t, err, merr.ErrImportFailed)
	})
}
