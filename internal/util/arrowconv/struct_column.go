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

// ArrowChunkedStructColumnToFieldData converts Struct<List<...>> columns to
// Milvus StructArray fields. Each child already holds a document's array; no
// row/struct transposition is performed. A declaration preserves logical types,
// field IDs and parent nullability; computed columns infer non-null child types.
func ArrowChunkedStructColumnToFieldData(name string, column *arrow.Chunked, schema *schemapb.StructArrayFieldSchema) (*schemapb.FieldData, error) {
	order, err := structArrayChildOrder(name, column, schema)
	if err != nil {
		return nil, err
	}
	valid, err := structArrayValidity(name, column, schema.GetNullable())
	if err != nil {
		return nil, err
	}

	children := make([]*schemapb.FieldData, len(order))
	for i, index := range order {
		var child *schemapb.FieldData
		if schema == nil {
			child, err = inferredStructArrayChild(column, index)
			if err == nil {
				child.FieldId = int64(i + 1)
			}
		} else {
			child, err = declaredStructArrayChild(column, index, schema.Fields[i])
		}
		if err != nil {
			field := column.DataType().(*arrow.StructType).Field(index)
			return nil, merr.Wrapf(err, "export struct child %s", field.Name)
		}
		typeutil.SetFieldDataValidData(child, valid)
		children[i] = child
	}
	return &schemapb.FieldData{
		FieldName: name,
		FieldId:   schema.GetFieldID(),
		Type:      schemapb.DataType_ArrayOfStruct,
		Field:     &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: children}},
	}, nil
}

// structArrayChildOrder validates the Struct<List<...>> layout and resolves
// Arrow column indices in schema order (or physical order for computed fields).
func structArrayChildOrder(name string, column *arrow.Chunked, schema *schemapb.StructArrayFieldSchema) ([]int, error) {
	if column == nil || column.DataType().ID() != arrow.STRUCT {
		return nil, merr.WrapErrServiceInternalMsg("struct column %s must be Struct with array children", name)
	}
	st := column.DataType().(*arrow.StructType)
	if st.NumFields() == 0 {
		return nil, merr.WrapErrServiceInternalMsg("struct column %s has no children", name)
	}
	if schema != nil && (schema.Name != name || len(schema.Fields) != st.NumFields()) {
		return nil, merr.WrapErrServiceInternalMsg("struct column %s does not match its schema", name)
	}
	order := make([]int, st.NumFields())
	indices := make(map[string]int, st.NumFields())
	for i, field := range st.Fields() {
		if _, exists := indices[field.Name]; exists || field.Name == "" || field.Type.ID() != arrow.LIST {
			return nil, merr.WrapErrServiceInternalMsg("invalid or duplicate array child %q in struct column %s", field.Name, name)
		}
		indices[field.Name] = i
		order[i] = i
	}
	for i, field := range schema.GetFields() {
		index, found := indices[field.GetName()]
		if field == nil || !found {
			return nil, merr.WrapErrServiceInternalMsg("missing or duplicate struct child %s", field.GetName())
		}
		order[i] = index
		delete(indices, field.GetName()) // A declaration may consume each child only once.
	}
	return order, nil
}

// structArrayValidity checks document nullability and child alignment once for
// all exported fields. Null parents mask their child payloads.
func structArrayValidity(name string, column *arrow.Chunked, nullable bool) ([]bool, error) {
	st := column.DataType().(*arrow.StructType)
	var valid []bool
	if nullable {
		valid = make([]bool, 0, column.Len())
	}
	for _, chunk := range column.Chunks() {
		rows := chunk.(*array.Struct)
		first := rows.Field(0).(*array.List)
		for row := 0; row < rows.Len(); row++ {
			present := rows.IsValid(row)
			if !present && !nullable {
				return nil, merr.WrapErrServiceInternalMsg("null non-nullable struct column %s", name)
			}
			if nullable {
				valid = append(valid, present)
			}
			// Parent validity masks every child's payload.
			if !present {
				continue
			}
			begin, end := first.ValueOffsets(row)
			for i := 0; i < rows.NumField(); i++ {
				child := rows.Field(i).(*array.List)
				childBegin, childEnd := child.ValueOffsets(row)
				if child.IsNull(row) || end-begin != childEnd-childBegin {
					return nil, merr.WrapErrServiceInternalMsg("null or unaligned array child %s at document %d", st.Field(i).Name, row)
				}
			}
		}
	}
	return valid, nil
}

func inferredStructArrayChild(column *arrow.Chunked, index int) (*schemapb.FieldData, error) {
	field := column.DataType().(*arrow.StructType).Field(index)
	chunks := make([]arrow.Array, len(column.Chunks()))
	for i, chunk := range column.Chunks() {
		chunks[i] = chunk.(*array.Struct).Field(index)
	}
	values := arrow.NewChunked(field.Type, chunks)
	defer values.Release()
	return exportComputedArrayChild(field.Name, values)
}

// declaredStructArrayChild retains collection element types (e.g. Text or
// Int16) which cannot be inferred from the Arrow physical layout alone.
func declaredStructArrayChild(column *arrow.Chunked, index int, schema *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	if schema.DataType != schemapb.DataType_Array && schema.DataType != schemapb.DataType_ArrayOfVector {
		return nil, merr.WrapErrServiceInternalMsg("struct child %s must declare an array type", schema.Name)
	}
	dtype := column.DataType().(*arrow.StructType).Field(index).Type.(*arrow.ListType)
	builder := array.NewBuilder(memory.DefaultAllocator, dtype.Elem())
	empty := builder.NewArray()
	builder.Release()
	defer empty.Release()
	// Validate the declared element layout even for a column with no chunks.
	value, err := declaredStructRange(empty, 0, 0, schema)
	if err != nil {
		return nil, err
	}
	output := &schemapb.FieldData{FieldName: schema.Name, FieldId: schema.FieldID, Type: schema.DataType}
	if schema.DataType == schemapb.DataType_Array {
		if value.GetScalars() == nil {
			return nil, merr.WrapErrServiceInternalMsg("non-scalar struct array child %s", schema.Name)
		}
		output.Field = &schemapb.FieldData_Scalars{Scalars: scalarArray(schema.ElementType, nil)}
	} else {
		if value.GetVectors() == nil {
			return nil, merr.WrapErrServiceInternalMsg("non-vector struct vector child %s", schema.Name)
		}
		dim := value.GetVectors().Dim
		output.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: dim, ElementType: schema.ElementType}}}}
	}
	for _, chunk := range column.Chunks() {
		rows := chunk.(*array.Struct)
		lists := rows.Field(index).(*array.List)
		for row := 0; row < rows.Len(); row++ {
			var value *schemapb.FieldData
			var err error
			if rows.IsNull(row) {
				value, err = declaredStructRange(empty, 0, 0, schema)
			} else {
				begin, end := lists.ValueOffsets(row)
				value, err = declaredStructRange(lists.ListValues(), begin, end, schema)
			}
			if err != nil {
				return nil, err
			}
			if schema.DataType == schemapb.DataType_Array {
				data := output.GetScalars().GetArrayData()
				data.Data = append(data.Data, value.GetScalars())
			} else {
				data := output.GetVectors().GetVectorArray()
				data.Data = append(data.Data, value.GetVectors())
			}
		}
	}
	return output, nil
}

func declaredStructRange(values arrow.Array, begin, end int64, schema *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	selected := array.NewSlice(values, begin, end)
	defer selected.Release()
	// Nullable on a struct child describes the document array, not its elements.
	if selected.NullN() != 0 {
		return nil, merr.WrapErrServiceInternalMsg("null struct child element %s", schema.Name)
	}
	if schema.TypeSchema != nil || schema.ElementType == schemapb.DataType_Array {
		root := schema.TypeSchema
		if schema.DataType != schemapb.DataType_Array || schema.ElementType != schemapb.DataType_Array ||
			root == nil || root.Nullable || root.GetArrayElement().GetArrayElement() == nil {
			return nil, merr.WrapErrServiceInternalMsg("invalid nested array definition for %s", schema.Name)
		}
		data, err := exportDeclaredArrayValues(selected, root.GetArrayElement(), schema.Name)
		if err != nil {
			return nil, err
		}
		return &schemapb.FieldData{Type: schemapb.DataType_Array, Field: &schemapb.FieldData_Scalars{Scalars: data}}, nil
	}
	element := &schemapb.FieldSchema{Name: schema.Name, FieldID: schema.FieldID, DataType: schema.ElementType, TypeParams: schema.TypeParams, IndexParams: schema.IndexParams}
	return ArrowColumnToFieldData(selected, element)
}

// exportDeclaredArrayValues follows TypeSchema instead of inferring away the
// declared leaf type (for example Int16 stored as Arrow Int32). The current
// collection contract disallows null nested array elements at every level.
func exportDeclaredArrayValues(values arrow.Array, schema *schemapb.TypeSchema, name string) (*schemapb.ScalarField, error) {
	if schema == nil || schema.Nullable || values.NullN() != 0 {
		return nil, merr.WrapErrServiceInternalMsg("invalid or null nested array values for %s", name)
	}
	switch node := schema.Kind.(type) {
	case *schemapb.TypeSchema_LeafType:
		switch node.LeafType {
		case schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
			schemapb.DataType_Int64, schemapb.DataType_Float, schemapb.DataType_Double, schemapb.DataType_VarChar:
		default:
			return nil, merr.WrapErrServiceInternalMsg("unsupported nested array leaf type %s", node.LeafType)
		}
		field, err := ArrowColumnToFieldData(values, &schemapb.FieldSchema{Name: name, DataType: node.LeafType, TypeParams: schema.TypeParams})
		if err != nil {
			return nil, err
		}
		return field.GetScalars(), nil
	case *schemapb.TypeSchema_ArrayElement:
		lists, ok := values.(*array.List)
		if !ok || node.ArrayElement == nil {
			return nil, merr.WrapErrServiceInternalMsg("nested array %s does not match Arrow layout %s", name, values.DataType())
		}
		element := node.ArrayElement.GetLeafType()
		if node.ArrayElement.GetArrayElement() != nil {
			element = schemapb.DataType_Array
		}
		data := make([]*schemapb.ScalarField, lists.Len())
		// Even an empty document/column must validate the full declared leaf layout.
		if lists.Len() == 0 {
			empty := array.NewSlice(lists.ListValues(), 0, 0)
			_, err := exportDeclaredArrayValues(empty, node.ArrayElement, name)
			empty.Release()
			if err != nil {
				return nil, err
			}
		}
		for i := range data {
			begin, end := lists.ValueOffsets(i)
			selected := array.NewSlice(lists.ListValues(), begin, end)
			value, err := exportDeclaredArrayValues(selected, node.ArrayElement, name)
			selected.Release()
			if err != nil {
				return nil, err
			}
			data[i] = value
		}
		return scalarArray(element, data), nil
	default:
		return nil, merr.WrapErrServiceInternalMsg("missing nested array type for %s", name)
	}
}
