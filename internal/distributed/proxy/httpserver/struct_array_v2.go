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
				FieldName: sub.GetName(),
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
				FieldName: sub.GetName(),
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
func extractStructArrayRow(fd *schemapb.FieldData, rowIdx int) ([]map[string]interface{}, error) {
	subs := fd.GetStructArrays().GetFields()
	if len(subs) == 0 {
		return []map[string]interface{}{}, nil
	}
	// Determine element count for this row: all sub-fields share the same count.
	elemCount, err := structSubElemCount(subs[0], rowIdx)
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
			values, err := vectorFieldToInterfaces(va.GetData()[rowIdx], va.GetElementType(), va.GetDim())
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

func structSubElemCount(sub *schemapb.FieldData, rowIdx int) (int, error) {
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
		return vectorFieldElemCount(va.GetData()[rowIdx], va.GetElementType(), va.GetDim())
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
