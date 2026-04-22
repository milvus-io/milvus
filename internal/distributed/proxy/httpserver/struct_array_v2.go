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

package httpserver

import (
	"encoding/base64"
	"fmt"

	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// structArrayRow holds per-row struct array data, keyed by short sub-field name
// (i.e. the user-facing name without the `structName[...]` wrapper).
// Each map entry is the per-row payload for a sub-field:
//   - For an Array sub-field (scalar elements), the value is a *schemapb.ScalarField
//     whose ArrayData contains one entry per struct element in this row.
//   - For an ArrayOfVector sub-field, the value is a *schemapb.VectorField whose
//     data holds one vector per struct element in this row.
type structArrayRow map[string]any

// subShortName returns the user-visible sub-field name. Milvus stores struct
// sub-fields as `structName[subName]` after collection creation, but RESTful
// users always refer to them by their original short name.
func subShortName(sub *schemapb.FieldSchema) string {
	name, err := typeutil.ExtractStructFieldName(sub.GetName())
	if err != nil || name == "" {
		return sub.GetName()
	}
	return name
}

// parseStructArrayRow parses the raw JSON value of a struct array field for one row
// (expected to be `[{sub1: v, sub2: v}, ...]`) and returns a structArrayRow covering
// all sub-fields declared in the schema.
func parseStructArrayRow(rawJSON string, structSchema *schemapb.StructArrayFieldSchema) (structArrayRow, error) {
	if rawJSON == "" {
		return nil, merr.WrapErrParameterInvalidMsg("missing struct array field: %s", structSchema.GetName())
	}
	parsed := gjson.Parse(rawJSON)
	if !parsed.IsArray() {
		return nil, merr.WrapErrParameterInvalidMsg(
			"struct array field %s expects a JSON array of objects", structSchema.GetName())
	}
	elems := parsed.Array()
	// Collect sub-field values keyed by short sub-field name, preserving element order.
	collected := make(map[string][]gjson.Result, len(structSchema.GetFields()))
	expected := make(map[string]struct{}, len(structSchema.GetFields()))
	for _, sub := range structSchema.GetFields() {
		key := subShortName(sub)
		collected[key] = make([]gjson.Result, 0, len(elems))
		expected[key] = struct{}{}
	}
	for idx, elem := range elems {
		if elem.Type != gjson.JSON || !elem.IsObject() {
			return nil, merr.WrapErrParameterInvalidMsg(
				"struct array field %s element #%d must be a JSON object", structSchema.GetName(), idx)
		}
		seen := make(map[string]struct{}, len(structSchema.GetFields()))
		elem.ForEach(func(key, value gjson.Result) bool {
			name := key.String()
			if _, ok := expected[name]; !ok {
				return true // ignore unknown keys to allow forward compatibility
			}
			collected[name] = append(collected[name], value)
			seen[name] = struct{}{}
			return true
		})
		if len(seen) != len(expected) {
			for name := range expected {
				if _, ok := seen[name]; !ok {
					return nil, merr.WrapErrParameterInvalidMsg(
						"struct array field %s element #%d missing sub-field %s",
						structSchema.GetName(), idx, name)
				}
			}
		}
	}

	row := structArrayRow{}
	for _, sub := range structSchema.GetFields() {
		key := subShortName(sub)
		vals := collected[key]
		switch sub.GetDataType() {
		case schemapb.DataType_Array:
			scalar, err := buildStructSubArrayScalar(sub, vals)
			if err != nil {
				return nil, err
			}
			row[key] = scalar
		case schemapb.DataType_ArrayOfVector:
			vec, err := buildStructSubVectorArray(sub, vals)
			if err != nil {
				return nil, err
			}
			row[key] = vec
		default:
			return nil, merr.WrapErrParameterInvalidMsg(
				"sub-field %s of struct %s has unsupported data type %s",
				key, structSchema.GetName(), sub.GetDataType())
		}
	}
	return row, nil
}

// buildStructSubArrayScalar builds a ScalarField (one row) for an Array sub-field
// by unmarshalling each gjson element according to the declared ElementType.
func buildStructSubArrayScalar(sub *schemapb.FieldSchema, vals []gjson.Result) (*schemapb.ScalarField, error) {
	switch sub.GetElementType() {
	case schemapb.DataType_Bool:
		arr := make([]bool, 0, len(vals))
		for _, v := range vals {
			if v.Type != gjson.True && v.Type != gjson.False {
				return nil, wrapStructSubParseError(sub, v, "expect bool")
			}
			arr = append(arr, v.Bool())
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: arr}},
		}, nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		arr := make([]int32, 0, len(vals))
		for _, v := range vals {
			if v.Type != gjson.Number {
				return nil, wrapStructSubParseError(sub, v, "expect integer")
			}
			arr = append(arr, int32(v.Int()))
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: arr}},
		}, nil
	case schemapb.DataType_Int64:
		arr := make([]int64, 0, len(vals))
		for _, v := range vals {
			if v.Type != gjson.Number {
				return nil, wrapStructSubParseError(sub, v, "expect integer")
			}
			arr = append(arr, v.Int())
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: arr}},
		}, nil
	case schemapb.DataType_Float:
		arr := make([]float32, 0, len(vals))
		for _, v := range vals {
			if v.Type != gjson.Number {
				return nil, wrapStructSubParseError(sub, v, "expect float")
			}
			arr = append(arr, float32(v.Float()))
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: arr}},
		}, nil
	case schemapb.DataType_Double:
		arr := make([]float64, 0, len(vals))
		for _, v := range vals {
			if v.Type != gjson.Number {
				return nil, wrapStructSubParseError(sub, v, "expect double")
			}
			arr = append(arr, v.Float())
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: arr}},
		}, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		arr := make([]string, 0, len(vals))
		for _, v := range vals {
			if v.Type != gjson.String {
				return nil, wrapStructSubParseError(sub, v, "expect string")
			}
			arr = append(arr, v.String())
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: arr}},
		}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg(
			"sub-field %s has unsupported array element type %s",
			sub.GetName(), sub.GetElementType())
	}
}

// buildStructSubVectorArray builds a VectorField (one row) for an ArrayOfVector sub-field.
// For FloatVector element type, values are expected as JSON arrays of numbers.
// For byte-backed vector types (Binary/Float16/BFloat16/Int8), values are either
// JSON arrays of float32 or base64 strings (same convention as top-level vectors).
func buildStructSubVectorArray(sub *schemapb.FieldSchema, vals []gjson.Result) (*schemapb.VectorField, error) {
	dim, err := getDim(sub)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg(
			"sub-field %s: %s", sub.GetName(), err.Error())
	}
	out := &schemapb.VectorField{Dim: dim}
	switch sub.GetElementType() {
	case schemapb.DataType_FloatVector:
		packed := &schemapb.FloatArray{}
		for _, v := range vals {
			if !v.IsArray() {
				return nil, wrapStructSubParseError(sub, v, "expect float vector array")
			}
			var row []float32
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, wrapStructSubParseError(sub, v, err.Error())
			}
			if int64(len(row)) != dim {
				return nil, merr.WrapErrParameterInvalidMsg(
					"sub-field %s vector dim mismatch: expect %d, got %d",
					sub.GetName(), dim, len(row))
			}
			packed.Data = append(packed.Data, row...)
		}
		out.Data = &schemapb.VectorField_VectorArray{
			VectorArray: &schemapb.VectorArray{
				ElementType: schemapb.DataType_FloatVector,
				Dim:         dim,
				Data: []*schemapb.VectorField{
					{
						Dim:  dim,
						Data: &schemapb.VectorField_FloatVector{FloatVector: packed},
					},
				},
			},
		}
		return out, nil
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		isFloat16 := sub.GetElementType() == schemapb.DataType_Float16Vector
		bytesPerVec := dim * 2
		buf := make([]byte, 0, int(bytesPerVec)*len(vals))
		for _, v := range vals {
			b, err := decodeByteVectorElement(v, dim, bytesPerVec, isFloat16)
			if err != nil {
				return nil, wrapStructSubParseError(sub, v, err.Error())
			}
			buf = append(buf, b...)
		}
		var packed *schemapb.VectorField
		if isFloat16 {
			packed = &schemapb.VectorField{
				Dim:  dim,
				Data: &schemapb.VectorField_Float16Vector{Float16Vector: buf},
			}
		} else {
			packed = &schemapb.VectorField{
				Dim:  dim,
				Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: buf},
			}
		}
		out.Data = &schemapb.VectorField_VectorArray{
			VectorArray: &schemapb.VectorArray{
				ElementType: sub.GetElementType(),
				Dim:         dim,
				Data:        []*schemapb.VectorField{packed},
			},
		}
		return out, nil
	case schemapb.DataType_BinaryVector:
		bytesPerVec := dim / 8
		buf := make([]byte, 0, int(bytesPerVec)*len(vals))
		for _, v := range vals {
			if v.Type != gjson.String {
				return nil, wrapStructSubParseError(sub, v, "binary vector must be base64-encoded string")
			}
			var row []byte
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, wrapStructSubParseError(sub, v, err.Error())
			}
			if int64(len(row)) != bytesPerVec {
				return nil, merr.WrapErrParameterInvalidMsg(
					"sub-field %s binary vector byte-length mismatch: expect %d, got %d",
					sub.GetName(), bytesPerVec, len(row))
			}
			buf = append(buf, row...)
		}
		out.Data = &schemapb.VectorField_VectorArray{
			VectorArray: &schemapb.VectorArray{
				ElementType: schemapb.DataType_BinaryVector,
				Dim:         dim,
				Data: []*schemapb.VectorField{
					{
						Dim:  dim,
						Data: &schemapb.VectorField_BinaryVector{BinaryVector: buf},
					},
				},
			},
		}
		return out, nil
	case schemapb.DataType_Int8Vector:
		buf := make([]byte, 0, int(dim)*len(vals))
		for _, v := range vals {
			if !v.IsArray() {
				return nil, wrapStructSubParseError(sub, v, "expect int8 vector array")
			}
			var row []int8
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, wrapStructSubParseError(sub, v, err.Error())
			}
			if int64(len(row)) != dim {
				return nil, merr.WrapErrParameterInvalidMsg(
					"sub-field %s int8 vector dim mismatch: expect %d, got %d",
					sub.GetName(), dim, len(row))
			}
			for _, x := range row {
				buf = append(buf, byte(x))
			}
		}
		out.Data = &schemapb.VectorField_VectorArray{
			VectorArray: &schemapb.VectorArray{
				ElementType: schemapb.DataType_Int8Vector,
				Dim:         dim,
				Data: []*schemapb.VectorField{
					{
						Dim:  dim,
						Data: &schemapb.VectorField_Int8Vector{Int8Vector: buf},
					},
				},
			},
		}
		return out, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg(
			"sub-field %s has unsupported vector element type %s",
			sub.GetName(), sub.GetElementType())
	}
}

func decodeByteVectorElement(v gjson.Result, dim, bytesPerVec int64, isFloat16 bool) ([]byte, error) {
	if v.IsArray() {
		var row []float32
		if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
			return nil, err
		}
		if int64(len(row)) != dim {
			return nil, fmt.Errorf("vector dim mismatch: expect %d, got %d", dim, len(row))
		}
		if isFloat16 {
			return typeutil.Float32ArrayToFloat16Bytes(row), nil
		}
		return typeutil.Float32ArrayToBFloat16Bytes(row), nil
	}
	if v.Type != gjson.String {
		return nil, fmt.Errorf("expect float vector array or base64 string")
	}
	var row []byte
	if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
		return nil, err
	}
	if int64(len(row)) != bytesPerVec {
		return nil, fmt.Errorf("byte length mismatch: expect %d, got %d", bytesPerVec, len(row))
	}
	return row, nil
}

func wrapStructSubParseError(sub *schemapb.FieldSchema, v gjson.Result, msg string) error {
	return merr.WrapErrParameterInvalidMsg(
		"sub-field %s parse error: %s (value=%s)", sub.GetName(), msg, v.Raw)
}

// isEmbeddingListData reports whether the `data` JSON of a search body is shaped
// as an embedding list (one list of vectors per nq) rather than an element-level
// query (one vector per nq). The caller has already resolved annsField to a
// struct ArrayOfVector sub-field, so the choice is strictly between:
//   - element-level: `data[0]` is a primitive vector (a number array for Float /
//     Float16 / BFloat16 / Int8, or a base64 string for BinaryVector / byte-backed
//     vectors) — one vector per nq query.
//   - embedding list: `data[0]` is itself an array of vectors — one list per nq.
//
// Detection walks two levels deep without committing to an element type:
//
//	data[0] is a string   → element-level (binary/byte-backed base64 per query)
//	data[0] is an array   → look at data[0][0]:
//	    primitive        → element-level
//	    array or string  → embedding list
func isEmbeddingListData(body string) bool {
	raw := gjson.Get(body, HTTPRequestData)
	if !raw.IsArray() {
		return false
	}
	arr := raw.Array()
	if len(arr) == 0 {
		return false
	}
	first := arr[0]
	if first.Type == gjson.String {
		return false
	}
	if !first.IsArray() {
		return false
	}
	inner := first.Array()
	if len(inner) == 0 {
		return false
	}
	firstInner := inner[0]
	return firstInner.IsArray() || firstInner.Type == gjson.String
}

// convertEmbListQueries2Placeholder parses a search body whose "data" is a JSON
// array of embedding lists. Each top-level element represents one nq query and
// is itself a JSON array of vectors of the given element type. The resulting
// PlaceholderValue carries one flat byte buffer per nq (all vectors of that
// query concatenated), tagged with the matching EmbList* placeholder type.
//
// Request shape examples:
//   - FloatVector element: [ [[0.1,0.2,0.3,0.4], [0.5,0.6,0.7,0.8]],
//     [[0.9,1.0,1.1,1.2]] ]
//   - BinaryVector element: [ ["base64_1", "base64_2"], ["base64_3"] ]
func convertEmbListQueries2Placeholder(body string, elemType schemapb.DataType, dim int64) (*commonpb.PlaceholderValue, error) {
	raw := gjson.Get(body, HTTPRequestData)
	if !raw.IsArray() {
		return nil, merr.WrapErrParameterInvalidMsg("search data must be an array of embedding lists")
	}
	queries := raw.Array()
	if len(queries) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("search data is empty")
	}

	placeholderType, err := embListPlaceholderType(elemType)
	if err != nil {
		return nil, err
	}

	values := make([][]byte, 0, len(queries))
	for qIdx, q := range queries {
		if !q.IsArray() {
			return nil, merr.WrapErrParameterInvalidMsg(
				"search data[%d] must be an array of vectors", qIdx)
		}
		vecs := q.Array()
		if len(vecs) == 0 {
			return nil, merr.WrapErrParameterInvalidMsg(
				"search data[%d] embedding list is empty", qIdx)
		}
		buf, err := encodeEmbListQuery(vecs, elemType, dim, qIdx)
		if err != nil {
			return nil, err
		}
		values = append(values, buf)
	}
	return &commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   placeholderType,
		Values: values,
	}, nil
}

func embListPlaceholderType(elemType schemapb.DataType) (commonpb.PlaceholderType, error) {
	switch elemType {
	case schemapb.DataType_FloatVector:
		return commonpb.PlaceholderType_EmbListFloatVector, nil
	case schemapb.DataType_Float16Vector:
		return commonpb.PlaceholderType_EmbListFloat16Vector, nil
	case schemapb.DataType_BFloat16Vector:
		return commonpb.PlaceholderType_EmbListBFloat16Vector, nil
	case schemapb.DataType_BinaryVector:
		return commonpb.PlaceholderType_EmbListBinaryVector, nil
	case schemapb.DataType_Int8Vector:
		return commonpb.PlaceholderType_EmbListInt8Vector, nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg(
			"unsupported embedding list element type %s", elemType)
	}
}

// encodeEmbListQuery flattens one query's list of vectors into a single byte
// buffer (all vectors concatenated). The wire format used by proxy/query nodes
// for EmbList* placeholders is `dim * N * bytes_per_elem` bytes, no explicit
// separators — each query's total byte count divided by the per-vector size
// determines N.
func encodeEmbListQuery(vecs []gjson.Result, elemType schemapb.DataType, dim int64, qIdx int) ([]byte, error) {
	switch elemType {
	case schemapb.DataType_FloatVector:
		buf := make([]byte, 0, int(dim*4)*len(vecs))
		for vIdx, v := range vecs {
			if !v.IsArray() {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] must be a float vector array", qIdx, vIdx)
			}
			var row []float32
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] parse fail: %s", qIdx, vIdx, err.Error())
			}
			if int64(len(row)) != dim {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] dim mismatch: expect %d got %d", qIdx, vIdx, dim, len(row))
			}
			buf = append(buf, typeutil.Float32ArrayToBytes(row)...)
		}
		return buf, nil
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		isFloat16 := elemType == schemapb.DataType_Float16Vector
		bytesPerVec := dim * 2
		buf := make([]byte, 0, int(bytesPerVec)*len(vecs))
		for vIdx, v := range vecs {
			b, err := decodeByteVectorElement(v, dim, bytesPerVec, isFloat16)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d]: %s", qIdx, vIdx, err.Error())
			}
			buf = append(buf, b...)
		}
		return buf, nil
	case schemapb.DataType_BinaryVector:
		bytesPerVec := dim / 8
		buf := make([]byte, 0, int(bytesPerVec)*len(vecs))
		for vIdx, v := range vecs {
			if v.Type != gjson.String {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] binary vector must be base64-encoded string", qIdx, vIdx)
			}
			var row []byte
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] parse fail: %s", qIdx, vIdx, err.Error())
			}
			if int64(len(row)) != bytesPerVec {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] byte-length mismatch: expect %d got %d", qIdx, vIdx, bytesPerVec, len(row))
			}
			buf = append(buf, row...)
		}
		return buf, nil
	case schemapb.DataType_Int8Vector:
		buf := make([]byte, 0, int(dim)*len(vecs))
		for vIdx, v := range vecs {
			if !v.IsArray() {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] must be an int8 vector array", qIdx, vIdx)
			}
			var row []int8
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] parse fail: %s", qIdx, vIdx, err.Error())
			}
			if int64(len(row)) != dim {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] dim mismatch: expect %d got %d", qIdx, vIdx, dim, len(row))
			}
			buf = append(buf, typeutil.Int8ArrayToBytes(row)...)
		}
		return buf, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg(
			"unsupported embedding list element type %s", elemType)
	}
}

// buildStructArrayFieldData aggregates per-row struct array payloads into a single
// schemapb.FieldData of type DataType_ArrayOfStruct.
func buildStructArrayFieldData(structSchema *schemapb.StructArrayFieldSchema, perRow []structArrayRow) (*schemapb.FieldData, error) {
	if len(perRow) == 0 {
		return nil, fmt.Errorf("struct array field %s has no rows", structSchema.GetName())
	}
	subs := structSchema.GetFields()
	subFieldData := make([]*schemapb.FieldData, 0, len(subs))
	for _, sub := range subs {
		short := subShortName(sub)
		switch sub.GetDataType() {
		case schemapb.DataType_Array:
			arrayArray := &schemapb.ArrayArray{
				Data:        make([]*schemapb.ScalarField, 0, len(perRow)),
				ElementType: sub.GetElementType(),
			}
			for rowIdx, row := range perRow {
				val, ok := row[short]
				if !ok {
					return nil, fmt.Errorf("struct %s row %d missing sub-field %s",
						structSchema.GetName(), rowIdx, short)
				}
				scalar, ok := val.(*schemapb.ScalarField)
				if !ok {
					return nil, fmt.Errorf("struct %s sub-field %s row %d: unexpected payload type %T",
						structSchema.GetName(), short, rowIdx, val)
				}
				arrayArray.Data = append(arrayArray.Data, scalar)
			}
			subFieldData = append(subFieldData, &schemapb.FieldData{
				Type:      schemapb.DataType_Array,
				FieldName: short,
				FieldId:   sub.GetFieldID(),
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{ArrayData: arrayArray},
					},
				},
			})
		case schemapb.DataType_ArrayOfVector:
			dim, err := getDim(sub)
			if err != nil {
				return nil, err
			}
			vecArray := &schemapb.VectorArray{
				ElementType: sub.GetElementType(),
				Dim:         dim,
				Data:        make([]*schemapb.VectorField, 0, len(perRow)),
			}
			for rowIdx, row := range perRow {
				val, ok := row[short]
				if !ok {
					return nil, fmt.Errorf("struct %s row %d missing sub-field %s",
						structSchema.GetName(), rowIdx, short)
				}
				vf, ok := val.(*schemapb.VectorField)
				if !ok {
					return nil, fmt.Errorf("struct %s sub-field %s row %d: unexpected payload type %T",
						structSchema.GetName(), short, rowIdx, val)
				}
				// vf.Data is a VectorField_VectorArray wrapping a single per-row VectorField.
				if va := vf.GetVectorArray(); va != nil && len(va.Data) == 1 {
					vecArray.Data = append(vecArray.Data, va.Data[0])
				} else {
					return nil, fmt.Errorf("struct %s sub-field %s row %d: malformed vector payload",
						structSchema.GetName(), short, rowIdx)
				}
			}
			subFieldData = append(subFieldData, &schemapb.FieldData{
				Type:      schemapb.DataType_ArrayOfVector,
				FieldName: short,
				FieldId:   sub.GetFieldID(),
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: dim,
						Data: &schemapb.VectorField_VectorArray{
							VectorArray: vecArray,
						},
					},
				},
			})
		default:
			return nil, fmt.Errorf("unsupported struct sub-field data type: %s", sub.GetDataType())
		}
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfStruct,
		FieldName: structSchema.GetName(),
		FieldId:   structSchema.GetFieldID(),
		Field: &schemapb.FieldData_StructArrays{
			StructArrays: &schemapb.StructArrayField{Fields: subFieldData},
		},
	}, nil
}

// extractStructArrayRow returns a JSON-friendly representation (list of struct
// elements) for a single row of a struct array FieldData.
// Each struct element is a map keyed by sub-field name. Scalar sub-fields map to
// their native scalar element; vector sub-fields map to the encoded per-element
// vector (float slice / byte slice / base64 string depending on element type).
//
// `schema` is the authoritative source for vector sub-field dimensions: the
// proto wire format carries `dim` redundantly in both VectorField.Dim and
// VectorArray.Dim, and we've seen paths (e.g. the query/search reconstruct
// path) that leave them at zero. Reading dim from schema removes that ambiguity.
func extractStructArrayRow(fd *schemapb.FieldData, rowIdx int, schema *schemapb.CollectionSchema) ([]map[string]interface{}, error) {
	subs := fd.GetStructArrays().GetFields()
	if len(subs) == 0 {
		return []map[string]interface{}{}, nil
	}

	// Build a lookup of sub-field short-name -> dim (for ArrayOfVector subs).
	subDims := map[string]int64{}
	for _, sf := range schema.GetStructArrayFields() {
		if sf.GetName() != fd.GetFieldName() {
			continue
		}
		for _, sub := range sf.GetFields() {
			if sub.GetDataType() != schemapb.DataType_ArrayOfVector {
				continue
			}
			dim, err := getDim(sub)
			if err != nil {
				return nil, fmt.Errorf("schema sub-field %s has no dim: %w", sub.GetName(), err)
			}
			short, _ := typeutil.ExtractStructFieldName(sub.GetName())
			if short == "" {
				short = sub.GetName()
			}
			subDims[short] = dim
		}
		break
	}

	// Determine element count for this row: all sub-fields share the same count.
	elemCount, err := structSubElemCount(subs[0], rowIdx, subDims)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, elemCount)
	for i := 0; i < elemCount; i++ {
		out[i] = make(map[string]interface{}, len(subs))
	}
	for _, sub := range subs {
		short, err := typeutil.ExtractStructFieldName(sub.GetFieldName())
		if err != nil || short == "" {
			short = sub.GetFieldName()
		}
		switch sub.GetType() {
		case schemapb.DataType_Array:
			rowData := sub.GetScalars().GetArrayData().GetData()
			if rowIdx >= len(rowData) {
				return nil, fmt.Errorf("struct sub-field %s missing row %d", short, rowIdx)
			}
			values := scalarArrayToInterfaces(rowData[rowIdx])
			if len(values) != elemCount {
				return nil, fmt.Errorf("struct sub-field %s element count mismatch: expect %d got %d",
					short, elemCount, len(values))
			}
			for i, v := range values {
				out[i][short] = v
			}
		case schemapb.DataType_ArrayOfVector:
			va := sub.GetVectors().GetVectorArray()
			if va == nil {
				return nil, fmt.Errorf("struct sub-field %s has no vector array", short)
			}
			if rowIdx >= len(va.GetData()) {
				return nil, fmt.Errorf("struct sub-field %s missing row %d", short, rowIdx)
			}
			dim, ok := subDims[short]
			if !ok || dim <= 0 {
				return nil, fmt.Errorf("schema missing dim for struct sub-field %s", short)
			}
			values, err := vectorFieldToInterfaces(va.GetData()[rowIdx], va.GetElementType(), dim)
			if err != nil {
				return nil, err
			}
			if len(values) != elemCount {
				return nil, fmt.Errorf("struct sub-field %s vector element count mismatch: expect %d got %d",
					short, elemCount, len(values))
			}
			for i, v := range values {
				out[i][short] = v
			}
		default:
			return nil, fmt.Errorf("unsupported struct sub-field type %s", sub.GetType())
		}
	}
	return out, nil
}

// structSubElemCount returns the number of struct elements in a single row of
// the first sub-field, used as the canonical row length.
func structSubElemCount(sub *schemapb.FieldData, rowIdx int, subDims map[string]int64) (int, error) {
	switch sub.GetType() {
	case schemapb.DataType_Array:
		rowData := sub.GetScalars().GetArrayData().GetData()
		if rowIdx >= len(rowData) {
			return 0, fmt.Errorf("struct sub-field %s row %d out of range", sub.GetFieldName(), rowIdx)
		}
		return len(scalarArrayToInterfaces(rowData[rowIdx])), nil
	case schemapb.DataType_ArrayOfVector:
		va := sub.GetVectors().GetVectorArray()
		if va == nil || rowIdx >= len(va.GetData()) {
			return 0, fmt.Errorf("struct sub-field %s row %d out of range", sub.GetFieldName(), rowIdx)
		}
		short, _ := typeutil.ExtractStructFieldName(sub.GetFieldName())
		if short == "" {
			short = sub.GetFieldName()
		}
		dim, ok := subDims[short]
		if !ok || dim <= 0 {
			return 0, fmt.Errorf("schema missing dim for struct sub-field %s", short)
		}
		return vectorFieldElemCount(va.GetData()[rowIdx], va.GetElementType(), dim)
	default:
		return 0, fmt.Errorf("unsupported struct sub-field type %s", sub.GetType())
	}
}

func scalarArrayToInterfaces(sf *schemapb.ScalarField) []interface{} {
	switch sf.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		src := sf.GetBoolData().GetData()
		out := make([]interface{}, len(src))
		for i, v := range src {
			out[i] = v
		}
		return out
	case *schemapb.ScalarField_IntData:
		src := sf.GetIntData().GetData()
		out := make([]interface{}, len(src))
		for i, v := range src {
			out[i] = v
		}
		return out
	case *schemapb.ScalarField_LongData:
		src := sf.GetLongData().GetData()
		out := make([]interface{}, len(src))
		for i, v := range src {
			out[i] = v
		}
		return out
	case *schemapb.ScalarField_FloatData:
		src := sf.GetFloatData().GetData()
		out := make([]interface{}, len(src))
		for i, v := range src {
			out[i] = v
		}
		return out
	case *schemapb.ScalarField_DoubleData:
		src := sf.GetDoubleData().GetData()
		out := make([]interface{}, len(src))
		for i, v := range src {
			out[i] = v
		}
		return out
	case *schemapb.ScalarField_StringData:
		src := sf.GetStringData().GetData()
		out := make([]interface{}, len(src))
		for i, v := range src {
			out[i] = v
		}
		return out
	default:
		return nil
	}
}

func vectorFieldElemCount(vf *schemapb.VectorField, elemType schemapb.DataType, dim int64) (int, error) {
	if dim <= 0 {
		return 0, fmt.Errorf("invalid dim %d", dim)
	}
	switch elemType {
	case schemapb.DataType_FloatVector:
		return len(vf.GetFloatVector().GetData()) / int(dim), nil
	case schemapb.DataType_Float16Vector:
		return len(vf.GetFloat16Vector()) / int(dim*2), nil
	case schemapb.DataType_BFloat16Vector:
		return len(vf.GetBfloat16Vector()) / int(dim*2), nil
	case schemapb.DataType_BinaryVector:
		return len(vf.GetBinaryVector()) / int(dim/8), nil
	case schemapb.DataType_Int8Vector:
		return len(vf.GetInt8Vector()) / int(dim), nil
	default:
		return 0, fmt.Errorf("unsupported vector element type %s", elemType)
	}
}

func vectorFieldToInterfaces(vf *schemapb.VectorField, elemType schemapb.DataType, dim int64) ([]interface{}, error) {
	if dim <= 0 {
		return nil, fmt.Errorf("invalid dim %d", dim)
	}
	switch elemType {
	case schemapb.DataType_FloatVector:
		buf := vf.GetFloatVector().GetData()
		count := len(buf) / int(dim)
		out := make([]interface{}, count)
		for i := 0; i < count; i++ {
			out[i] = buf[i*int(dim) : (i+1)*int(dim)]
		}
		return out, nil
	case schemapb.DataType_Float16Vector:
		buf := vf.GetFloat16Vector()
		step := int(dim * 2)
		count := len(buf) / step
		out := make([]interface{}, count)
		for i := 0; i < count; i++ {
			out[i] = base64.StdEncoding.EncodeToString(buf[i*step : (i+1)*step])
		}
		return out, nil
	case schemapb.DataType_BFloat16Vector:
		buf := vf.GetBfloat16Vector()
		step := int(dim * 2)
		count := len(buf) / step
		out := make([]interface{}, count)
		for i := 0; i < count; i++ {
			out[i] = base64.StdEncoding.EncodeToString(buf[i*step : (i+1)*step])
		}
		return out, nil
	case schemapb.DataType_BinaryVector:
		buf := vf.GetBinaryVector()
		step := int(dim / 8)
		count := len(buf) / step
		out := make([]interface{}, count)
		for i := 0; i < count; i++ {
			out[i] = base64.StdEncoding.EncodeToString(buf[i*step : (i+1)*step])
		}
		return out, nil
	case schemapb.DataType_Int8Vector:
		buf := vf.GetInt8Vector()
		step := int(dim)
		count := len(buf) / step
		out := make([]interface{}, count)
		for i := 0; i < count; i++ {
			seg := buf[i*step : (i+1)*step]
			row := make([]int8, step)
			for j, b := range seg {
				row[j] = int8(b)
			}
			out[i] = row
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported vector element type %s", elemType)
	}
}
