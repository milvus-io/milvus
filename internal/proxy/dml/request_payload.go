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

package dml

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RequestPayload measures client field values before functions, conversions or
// read-before-write processing change them. Primary keys are part of FieldBytes.
type RequestPayload struct {
	FieldBytes      int64
	PrimaryKeyBytes int64
}

func (p *RequestPayload) measure(fields []*schemapb.FieldData, schema *typeutil.SchemaHelper) {
	if p == nil {
		return
	}
	*p = RequestPayload{}
	for _, field := range fields {
		if nested := field.GetStructArrays(); nested != nil {
			parent := schema.GetStructArrayFieldFromName(field.GetFieldName())
			for _, child := range nested.GetFields() {
				p.FieldBytes += fieldValueBytes(child, findStructChildSchema(parent, child.GetFieldName()))
			}
			continue
		}
		def, _ := schema.GetFieldFromName(field.GetFieldName())
		if def.GetIsFunctionOutput() {
			continue
		}
		size := fieldValueBytes(field, def)
		p.FieldBytes += size
		if def.GetIsPrimaryKey() {
			p.PrimaryKeyBytes += size
		}
	}
}

func fieldValueBytes(field *schemapb.FieldData, def *schemapb.FieldSchema) int64 {
	switch data := field.GetField().(type) {
	case *schemapb.FieldData_Scalars:
		kind := def.GetDataType()
		if kind == schemapb.DataType_None {
			kind = field.GetType()
		}
		return scalarValueBytes(data.Scalars, kind, def.GetElementType())
	case *schemapb.FieldData_Vectors:
		return vectorValueBytes(data.Vectors)
	default:
		return 0
	}
}

func contentBytes[T ~string | ~[]byte](values []T) int64 {
	var size int64
	for _, value := range values {
		size += int64(len(value))
	}
	return size
}

func scalarValueBytes(field *schemapb.ScalarField, kind, elementType schemapb.DataType) int64 {
	switch v := field.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		return int64(len(v.BoolData.GetData()))
	case *schemapb.ScalarField_IntData:
		n := int64(len(v.IntData.GetData()))
		switch kind {
		case schemapb.DataType_Int8:
			return n
		case schemapb.DataType_Int16:
			return 2 * n
		default:
			return 4 * n
		}
	case *schemapb.ScalarField_LongData:
		return 8 * int64(len(v.LongData.GetData()))
	case *schemapb.ScalarField_FloatData:
		return 4 * int64(len(v.FloatData.GetData()))
	case *schemapb.ScalarField_DoubleData:
		return 8 * int64(len(v.DoubleData.GetData()))
	case *schemapb.ScalarField_StringData:
		return contentBytes(v.StringData.GetData())
	case *schemapb.ScalarField_BytesData:
		return contentBytes(v.BytesData.GetData())
	case *schemapb.ScalarField_JsonData:
		return contentBytes(v.JsonData.GetData())
	case *schemapb.ScalarField_GeometryData:
		return contentBytes(v.GeometryData.GetData())
	case *schemapb.ScalarField_GeometryWktData:
		return contentBytes(v.GeometryWktData.GetData())
	case *schemapb.ScalarField_MolData:
		return contentBytes(v.MolData.GetData())
	case *schemapb.ScalarField_MolSmilesData:
		return contentBytes(v.MolSmilesData.GetData())
	case *schemapb.ScalarField_TimestamptzData:
		return 8 * int64(len(v.TimestamptzData.GetData()))
	case *schemapb.ScalarField_DateData:
		return 4 * int64(len(v.DateData.GetData()))
	case *schemapb.ScalarField_TimeData:
		return 8 * int64(len(v.TimeData.GetData()))
	case *schemapb.ScalarField_ArrayData:
		if elementType == schemapb.DataType_None {
			elementType = v.ArrayData.GetElementType()
		}
		var size int64
		for _, row := range v.ArrayData.GetData() {
			size += scalarValueBytes(row, elementType, schemapb.DataType_None)
		}
		return size
	default:
		return 0
	}
}

func vectorValueBytes(field *schemapb.VectorField) int64 {
	// An FP32 placeholder sent to a half-precision column is still an FP32
	// client payload. Inspect the supplied representation before conversion.
	switch v := field.GetData().(type) {
	case *schemapb.VectorField_FloatVector:
		return 4 * int64(len(v.FloatVector.GetData()))
	case *schemapb.VectorField_Float16Vector:
		return int64(len(v.Float16Vector))
	case *schemapb.VectorField_Bfloat16Vector:
		return int64(len(v.Bfloat16Vector))
	case *schemapb.VectorField_BinaryVector:
		return int64(len(v.BinaryVector))
	case *schemapb.VectorField_Int8Vector:
		return int64(len(v.Int8Vector))
	case *schemapb.VectorField_SparseFloatVector:
		return contentBytes(v.SparseFloatVector.GetContents())
	case *schemapb.VectorField_VectorArray:
		var size int64
		for _, row := range v.VectorArray.GetData() {
			size += vectorValueBytes(row)
		}
		return size
	default:
		return 0
	}
}

func primaryKeyBytes(ids *schemapb.IDs) int64 {
	return 8*int64(len(ids.GetIntId().GetData())) + contentBytes(ids.GetStrId().GetData())
}
