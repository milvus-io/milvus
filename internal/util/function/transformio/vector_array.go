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

package transformio

import (
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func vectorArrayToArrow(field *schemapb.FieldData, rows int64, alloc memory.Allocator) (arrow.Array, error) {
	data := field.GetVectors().GetVectorArray()
	if data == nil {
		return nil, merr.WrapErrParameterInvalidMsg("vector array payload is missing")
	}
	dim := data.GetDim()
	if dim <= 0 {
		dim = field.GetVectors().GetDim()
	}
	childField, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "item", DataType: data.GetElementType(), Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: strconv.FormatInt(dim, 10)}}})
	if err != nil {
		return nil, err
	}
	indexes, err := logicalIndexes(typeutil.GetFieldDataValidData(field), rows, len(data.GetData()))
	if err != nil {
		return nil, err
	}
	builder := array.NewListBuilderWithField(alloc, childField)
	defer builder.Release()
	for _, index := range indexes {
		builder.Append(index >= 0)
		if index < 0 {
			continue
		}
		row := data.GetData()[index]
		if row == nil || row.GetDim() != dim {
			return nil, merr.WrapErrParameterInvalidMsg("vector array row dimension differs from field dimension")
		}
		count, err := vectorValueCount(row, data.GetElementType())
		if err != nil {
			return nil, err
		}
		if valid := row.GetValidData(); len(valid) > 0 {
			count = len(valid)
		}
		values, err := vectorToArrow(&schemapb.FieldData{Type: data.GetElementType(), Field: &schemapb.FieldData_Vectors{Vectors: row}}, int64(count), alloc)
		if err != nil {
			return nil, err
		}
		for element := 0; element < values.Len(); element++ {
			if err := appendArrowValue(builder.ValueBuilder(), values, element); err != nil {
				values.Release()
				return nil, err
			}
		}
		values.Release()
	}
	return builder.NewArray(), nil
}

func vectorValueCount(vector *schemapb.VectorField, dataType schemapb.DataType) (int, error) {
	if vector.GetDim() <= 0 {
		return 0, merr.WrapErrParameterInvalidMsg("vector dimension must be positive")
	}
	dim := int(vector.GetDim())
	length, width := 0, dim
	switch dataType {
	case schemapb.DataType_FloatVector:
		length = len(vector.GetFloatVector().GetData())
	case schemapb.DataType_BinaryVector:
		if dim%8 != 0 {
			return 0, merr.WrapErrParameterInvalidMsg("binary vector dimension must be byte aligned")
		}
		width = dim / 8
		length = len(vector.GetBinaryVector())
	case schemapb.DataType_Float16Vector:
		width = dim * 2
		length = len(vector.GetFloat16Vector())
	case schemapb.DataType_BFloat16Vector:
		width = dim * 2
		length = len(vector.GetBfloat16Vector())
	case schemapb.DataType_Int8Vector:
		length = len(vector.GetInt8Vector())
	case schemapb.DataType_SparseFloatVector:
		return len(vector.GetSparseFloatVector().GetContents()), nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg("unsupported vector array element type %s", dataType)
	}
	if width <= 0 || length%width != 0 {
		return 0, merr.WrapErrParameterInvalidMsg("vector array payload is not dimension aligned")
	}
	return length / width, nil
}

func arrowToVectorArray(values arrow.Array, schema *schemapb.FieldSchema) (*schemapb.VectorField, error) {
	dim, err := typeutil.GetDim(schema)
	if err != nil {
		return nil, merr.Wrap(err, "transform vector array output dimension")
	}
	rows := make([]*schemapb.VectorField, values.Len())
	childSchema := &schemapb.FieldSchema{Name: "item", DataType: schema.GetElementType(), Nullable: true, TypeParams: schema.GetTypeParams()}
	for row := range rows {
		start, end, children, ok := listValues(values, row)
		if !ok {
			return nil, typeMismatch(schemapb.DataType_ArrayOfVector, values)
		}
		if values.IsNull(row) {
			rows[row] = &schemapb.VectorField{Dim: dim}
			continue
		}
		sliced := array.NewSlice(children, start, end)
		vector, err := arrowToVector(sliced, childSchema)
		if err != nil {
			sliced.Release()
			return nil, err
		}
		if sliced.NullN() > 0 || schema.GetElementNullable() {
			if sliced.NullN() > 0 && !schema.GetElementNullable() {
				sliced.Release()
				return nil, merr.WrapErrFunctionFailedMsg("vector array output has undeclared null elements")
			}
			vector.ValidData = make([]bool, sliced.Len())
			for index := range vector.ValidData {
				vector.ValidData[index] = sliced.IsValid(index)
			}
		}
		sliced.Release()
		rows[row] = vector
	}
	return &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: dim, Data: rows, ElementType: schema.GetElementType()}}}, nil
}
