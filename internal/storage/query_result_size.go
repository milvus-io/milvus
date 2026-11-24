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

package storage

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// LogicalQueryResultBytes counts the values present in the final result.
// It uses the logical scalar widths, including for packed protobuf integers.
func LogicalQueryResultBytes(fields []*schemapb.FieldData, schema *schemapb.CollectionSchema) (uint64, error) {
	fieldSchemas := make(map[int64]*schemapb.FieldSchema)
	if schema != nil {
		for _, field := range typeutil.GetAllFieldSchemas(schema) {
			fieldSchemas[field.GetFieldID()] = field
		}
	}
	var total uint64
	if err := addResultFields(&total, fields, fieldSchemas); err != nil {
		return 0, err
	}
	return total, nil
}

func addResultFields(total *uint64, fields []*schemapb.FieldData, schemas map[int64]*schemapb.FieldSchema) error {
	for _, f := range fields {
		valid := typeutil.GetFieldDataValidData(f)
		var known bool
		switch data := f.GetField().(type) {
		case nil:
			known = addResultValues(total, 0, valid, 0, nil)
		case *schemapb.FieldData_Scalars:
			known = addResultScalar(total, f.GetType(), data.Scalars, valid, schemas[f.GetFieldId()])
		case *schemapb.FieldData_Vectors:
			known = addResultVector(total, data.Vectors, valid)
		case *schemapb.FieldData_StructArrays:
			if err := addResultFields(total, data.StructArrays.GetFields(), schemas); err != nil {
				return err
			}
			known = true
		}
		if !known {
			return merr.WrapErrServiceInternalMsg("missing logical size for query result field %q (id %d, type %s)", f.GetFieldName(), f.GetFieldId(), f.GetType())
		}
	}
	return nil
}

func addResultScalar(total *uint64, kind schemapb.DataType, data *schemapb.ScalarField, valid []bool, schema *schemapb.FieldSchema) bool {
	switch values := data.GetData().(type) {
	case nil:
		return addResultValues(total, 0, valid, 0, nil)
	case *schemapb.ScalarField_BoolData:
		return addResultValues(total, len(values.BoolData.GetData()), valid, 1, nil)
	case *schemapb.ScalarField_IntData:
		width := logicalScalarType(kind, false, nil).width
		return width != 0 && addResultValues(total, len(values.IntData.GetData()), valid, width, nil)
	case *schemapb.ScalarField_LongData:
		return addResultValues(total, len(values.LongData.GetData()), valid, 8, nil)
	case *schemapb.ScalarField_FloatData:
		return addResultValues(total, len(values.FloatData.GetData()), valid, 4, nil)
	case *schemapb.ScalarField_DoubleData:
		return addResultValues(total, len(values.DoubleData.GetData()), valid, 8, nil)
	case *schemapb.ScalarField_TimestamptzData:
		return addResultValues(total, len(values.TimestamptzData.GetData()), valid, 8, nil)
	case *schemapb.ScalarField_DateData:
		return addResultValues(total, len(values.DateData.GetData()), valid, 4, nil)
	case *schemapb.ScalarField_TimeData:
		return addResultValues(total, len(values.TimeData.GetData()), valid, 8, nil)
	case *schemapb.ScalarField_StringData:
		if kind == schemapb.DataType_Timestamptz {
			return addResultValues(total, len(values.StringData.GetData()), valid, 8, nil)
		}
		return addResultByteValues(total, values.StringData.GetData(), valid)
	case *schemapb.ScalarField_BytesData:
		return addResultByteValues(total, values.BytesData.GetData(), valid)
	case *schemapb.ScalarField_JsonData:
		return addResultByteValues(total, values.JsonData.GetData(), valid)
	case *schemapb.ScalarField_GeometryData:
		return addResultByteValues(total, values.GeometryData.GetData(), valid)
	case *schemapb.ScalarField_GeometryWktData:
		return addResultByteValues(total, values.GeometryWktData.GetData(), valid)
	case *schemapb.ScalarField_MolData:
		return addResultByteValues(total, values.MolData.GetData(), valid)
	case *schemapb.ScalarField_MolSmilesData:
		return addResultByteValues(total, values.MolSmilesData.GetData(), valid)
	case *schemapb.ScalarField_ArrayData:
		rows := values.ArrayData.GetData()
		element := logicalScalarType(values.ArrayData.GetElementType(), false, nil)
		if schema.GetDataType() == schemapb.DataType_Array {
			if arrayType := logicalTypeForField(schema, false); arrayType.element != nil {
				element = *arrayType.element
			}
		}
		return addResultValues(total, len(rows), valid, 0, func(i int) (uint64, bool) {
			var size datapb.FieldLogicalSize
			ok := element.addProtoElements(&size, rows[i])
			return size.PayloadBytes, ok
		})
	}
	return false
}

func addResultVector(total *uint64, vector *schemapb.VectorField, valid []bool) bool {
	dim := uint64(max(vector.GetDim(), 0))
	switch values := vector.GetData().(type) {
	case nil:
		return addResultValues(total, 0, valid, 0, nil)
	case *schemapb.VectorField_FloatVector:
		return addResultDenseVector(total, len(values.FloatVector.GetData()), dim, 4, valid)
	case *schemapb.VectorField_BinaryVector:
		return addResultDenseVector(total, len(values.BinaryVector), (dim+7)/8, 1, valid)
	case *schemapb.VectorField_Float16Vector:
		return addResultDenseVector(total, len(values.Float16Vector), dim*2, 1, valid)
	case *schemapb.VectorField_Bfloat16Vector:
		return addResultDenseVector(total, len(values.Bfloat16Vector), dim*2, 1, valid)
	case *schemapb.VectorField_Int8Vector:
		return addResultDenseVector(total, len(values.Int8Vector), dim, 1, valid)
	case *schemapb.VectorField_SparseFloatVector:
		return addResultByteValues(total, values.SparseFloatVector.GetContents(), valid)
	case *schemapb.VectorField_VectorArray:
		rows := values.VectorArray.GetData()
		return addResultValues(total, len(rows), valid, 0, func(i int) (uint64, bool) {
			var bytes uint64
			row := rows[i]
			ok := addResultVector(&bytes, row, typeutil.GetVectorArrayElementValidData(row))
			return bytes, ok
		})
	}
	return false
}

func addResultDenseVector(total *uint64, values int, stride, width uint64, valid []bool) bool {
	if values == 0 {
		return addResultValues(total, 0, valid, 0, nil)
	}
	return stride != 0 && uint64(values)%stride == 0 &&
		addResultValues(total, int(uint64(values)/stride), valid, stride*width, nil)
}

func addResultByteValues[T ~string | ~[]byte](total *uint64, rows []T, valid []bool) bool {
	return addResultValues(total, len(rows), valid, 0, func(i int) (uint64, bool) {
		return uint64(len(rows[i])), true
	})
}

// Result producers use both dense arrays with NULL placeholders and compact
// arrays holding only non-NULL values. Neither representation charges a NULL.
func addResultValues(total *uint64, rows int, valid []bool, width uint64, sizeAt func(int) (uint64, bool)) bool {
	actual := len(valid)
	if actual > 0 {
		for _, present := range valid {
			if !present {
				actual--
			}
		}
		if rows != len(valid) && rows != actual {
			return false
		}
	} else {
		actual = rows
	}
	if sizeAt == nil {
		return addLogicalProduct(total, uint64(actual), width)
	}
	for i := 0; i < rows; i++ {
		if len(valid) == rows && !valid[i] {
			continue
		}
		size, ok := sizeAt(i)
		if !ok || !addLogicalCount(total, size) {
			return false
		}
	}
	return true
}
