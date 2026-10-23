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
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// exportComputedArrayChild exports one complete StructArray child. Its List
// layout has already been checked by the parent converter. Chunk rows are
// appended directly; only the final result needs a FieldData wrapper.
func exportComputedArrayChild(name string, column *arrow.Chunked) (*schemapb.FieldData, error) {
	if err := validateTypedScalarType(column.DataType()); err != nil {
		return nil, err
	}
	element, err := inferScalarType(column.DataType().(*arrow.ListType).Elem())
	if err != nil {
		return nil, err
	}
	chunks := column.Chunks()
	if len(chunks) == 0 {
		builder := array.NewBuilder(memory.DefaultAllocator, column.DataType())
		empty := builder.NewArray()
		builder.Release()
		defer empty.Release()
		chunks = []arrow.Array{empty}
	}
	rows := make([]*schemapb.ScalarField, 0, column.Len())
	for _, chunk := range chunks {
		data, err := exportListChunk(chunk.(*array.List))
		if err != nil {
			return nil, err
		}
		rows = append(rows, data.GetArrayData().Data...)
	}
	return &schemapb.FieldData{
		FieldName: name,
		Type:      schemapb.DataType_Array,
		Field:     &schemapb.FieldData_Scalars{Scalars: scalarArray(element, rows)},
	}, nil
}

// validateTypedScalarType checks the leaf once, including for empty columns
// whose payload traversal cannot reach it. Lists have a single element type.
func validateTypedScalarType(dtype arrow.DataType) error {
	for depth := 0; dtype.ID() == arrow.LIST; depth++ {
		if depth == 2 {
			return merr.WrapErrServiceInternalMsg("computed StructArray children support at most two List levels")
		}
		dtype = dtype.(*arrow.ListType).Elem()
	}
	_, err := inferScalarType(dtype)
	return err
}

// inferScalarType maps only the current level; nested values are converted
// separately when exporting each complete List column.
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

// exportListChunk exports the entire logical array, including sliced chunks.
func exportListChunk(lists *array.List) (*schemapb.ScalarField, error) {
	if _, nested := lists.ListValues().(*array.List); nested {
		return exportNestedList(lists)
	}
	return exportScalarList(lists)
}

// exportScalarList exports every row of a List<scalar> column. Offsets describe
// row boundaries, not an optional selection. Only the logical child values of
// this column are read, so sliced chunks do not copy unused backing values.
func exportScalarList(lists *array.List) (*schemapb.ScalarField, error) {
	if lists.NullN() != 0 {
		return nil, merr.WrapErrServiceInternalMsg("null typed Arrow value")
	}
	element, err := inferScalarType(lists.ListValues().DataType())
	if err != nil {
		return nil, err
	}
	if element == schemapb.DataType_Array {
		return nil, merr.WrapErrServiceInternalMsg("expected scalar List elements")
	}
	var first, last int64
	if lists.Len() > 0 {
		first, _ = lists.ValueOffsets(0)
		_, last = lists.ValueOffsets(lists.Len() - 1)
	}
	values := array.NewSlice(lists.ListValues(), first, last)
	defer values.Release()
	if values.NullN() != 0 {
		return nil, merr.WrapErrServiceInternalMsg("null typed Arrow value")
	}
	rows := make([]*schemapb.ScalarField, lists.Len())
	var integers []scalarInt64Cell
	if element == schemapb.DataType_Int64 {
		integers = make([]scalarInt64Cell, len(rows))
	}
	for row := range rows {
		begin, end := lists.ValueOffsets(row)
		begin, end = begin-first, end-first
		switch v := values.(type) {
		case *array.String:
			data := make([]string, end-begin)
			for i := range data {
				data[i] = strings.Clone(v.Value(int(begin) + i))
			}
			rows[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data}}}
		case *array.Int64:
			rows[row] = integers[row].init(append([]int64{}, v.Int64Values()[begin:end]...))
		case *array.Boolean:
			data := make([]bool, end-begin)
			for i := range data {
				data[i] = v.Value(int(begin) + i)
			}
			rows[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: data}}}
		case *array.Float64:
			rows[row] = &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: append([]float64{}, v.Float64Values()[begin:end]...)}}}
		}
	}
	return scalarArray(element, rows), nil
}

// exportNestedList exports List<List<scalar>>: convert the matching-line arrays
// once, then group them into documents using the outer offsets. No recursive
// range export or per-document Arrow slices are needed.
func exportNestedList(documents *array.List) (*schemapb.ScalarField, error) {
	if documents.NullN() != 0 {
		return nil, merr.WrapErrServiceInternalMsg("null typed Arrow value")
	}
	var first, last int64
	if documents.Len() > 0 {
		first, _ = documents.ValueOffsets(0)
		_, last = documents.ValueOffsets(documents.Len() - 1)
	}
	values := array.NewSlice(documents.ListValues(), first, last)
	defer values.Release()
	converted, err := exportScalarList(values.(*array.List))
	if err != nil {
		return nil, err
	}
	elements := converted.GetArrayData()
	rows := make([]*schemapb.ScalarField, documents.Len())
	for row := range rows {
		begin, end := documents.ValueOffsets(row)
		begin, end = begin-first, end-first
		rows[row] = scalarArray(elements.ElementType, elements.Data[begin:end:end])
	}
	return scalarArray(schemapb.DataType_Array, rows), nil
}
