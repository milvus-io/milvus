// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package arrowconv

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ArrowChunkedTypedColumnToFieldData infers scalar/List/Struct types from Arrow.
// Null values are rejected. Supports sliced arrays, empty columns and multiple
// chunks. Exported strings share Arrow value buffers, which must be immutable,
// Go-managed and not recycled on Release.
func ArrowChunkedTypedColumnToFieldData(name string, column *arrow.Chunked) (*schemapb.FieldData, error) {
	if column == nil {
		return nil, merr.WrapErrServiceInternalMsg("missing typed Arrow column %s", name)
	}
	if isStructList(column.DataType()) {
		return ArrowChunkedStructColumnToFieldData(name, column, nil)
	}
	dataType, err := inferScalarType(column.DataType())
	if err != nil {
		return nil, err
	}
	var output *schemapb.FieldData
	for _, chunk := range column.Chunks() {
		data, err := exportScalarRange(chunk, 0, int64(chunk.Len()))
		if err != nil {
			return nil, err
		}
		field := &schemapb.FieldData{FieldName: name, Type: dataType, Field: &schemapb.FieldData_Scalars{Scalars: data}}
		if output == nil {
			output = field
		} else if err := typeutil.MergeFieldData([]*schemapb.FieldData{output}, []*schemapb.FieldData{field}); err != nil {
			return nil, err
		}
	}
	if output == nil {
		builder := array.NewBuilder(memory.DefaultAllocator, column.DataType())
		empty := builder.NewArray()
		builder.Release()
		defer empty.Release()
		data, err := exportScalarRange(empty, 0, 0)
		if err != nil {
			return nil, err
		}
		output = &schemapb.FieldData{FieldName: name, Type: dataType, Field: &schemapb.FieldData_Scalars{Scalars: data}}
	}
	return output, nil
}

func inferScalarType(dtype arrow.DataType) (schemapb.DataType, error) {
	switch dtype.ID() {
	case arrow.STRING:
		return schemapb.DataType_VarChar, nil
	case arrow.INT64:
		return schemapb.DataType_Int64, nil
	case arrow.BOOL:
		return schemapb.DataType_Bool, nil
	case arrow.FLOAT64:
		return schemapb.DataType_Double, nil
	case arrow.LIST:
		if _, err := inferScalarType(dtype.(*arrow.ListType).Elem()); err != nil {
			return 0, err
		}
		return schemapb.DataType_Array, nil
	default:
		return 0, merr.WrapErrServiceInternalMsg("unsupported typed Arrow type %s", dtype)
	}
}

func scalarArray(element schemapb.DataType, values []*schemapb.ScalarField) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: element, Data: values}}}
}

type scalarInt64Cell struct {
	field  schemapb.ScalarField
	value  schemapb.ScalarField_LongData
	values schemapb.LongArray
}

func (c *scalarInt64Cell) init(values []int64) *schemapb.ScalarField {
	c.values.Data = values
	c.value.LongData = &c.values
	c.field.Data = &c.value
	return &c.field
}

func exportInt64Range(values arrow.Array, begin, end int64) ([]int64, error) {
	if begin < 0 || end < begin || end > int64(values.Len()) {
		return nil, merr.WrapErrServiceInternalMsg("typed Arrow range out of bounds")
	}
	result := make([]int64, end-begin)
	for i := begin; i < end; i++ {
		if values.IsNull(int(i)) {
			return nil, merr.WrapErrServiceInternalMsg("null typed Arrow value")
		}
		result[i-begin] = values.(*array.Int64).Value(int(i))
	}
	return result, nil
}

func exportScalarRange(values arrow.Array, begin, end int64) (*schemapb.ScalarField, error) {
	if begin < 0 || end < begin || end > int64(values.Len()) {
		return nil, merr.WrapErrServiceInternalMsg("typed Arrow range out of bounds")
	}
	for i := begin; i < end; i++ {
		if values.IsNull(int(i)) {
			return nil, merr.WrapErrServiceInternalMsg("null typed Arrow value")
		}
	}
	switch v := values.(type) {
	case *array.String:
		data := make([]string, end-begin)
		for i := begin; i < end; i++ {
			data[i-begin] = v.Value(int(i))
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data}}}, nil
	case *array.Int64:
		data, err := exportInt64Range(values, begin, end)
		if err != nil {
			return nil, err
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}}}, nil
	case *array.Boolean:
		data := make([]bool, end-begin)
		for i := begin; i < end; i++ {
			data[i-begin] = v.Value(int(i))
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: data}}}, nil
	case *array.Float64:
		data := make([]float64, end-begin)
		for i := begin; i < end; i++ {
			data[i-begin] = v.Value(int(i))
		}
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: data}}}, nil
	case *array.List:
		element, err := inferScalarType(v.ListValues().DataType())
		if err != nil {
			return nil, err
		}
		data := make([]*schemapb.ScalarField, end-begin)
		// Batch protobuf wrappers for nested integer arrays; their payloads remain
		// independently owned, avoiding per-value wrapper allocations.
		var integers []scalarInt64Cell
		if element == schemapb.DataType_Int64 {
			integers = make([]scalarInt64Cell, len(data))
		}
		for i := begin; i < end; i++ {
			first, last := v.ValueOffsets(int(i))
			if integers != nil {
				payload, err := exportInt64Range(v.ListValues(), first, last)
				if err != nil {
					return nil, err
				}
				data[i-begin] = integers[i-begin].init(payload)
			} else {
				data[i-begin], err = exportScalarRange(v.ListValues(), first, last)
				if err != nil {
					return nil, err
				}
			}
		}
		return scalarArray(element, data), nil
	default:
		return nil, merr.WrapErrServiceInternalMsg("unsupported typed Arrow values %s", values.DataType())
	}
}
