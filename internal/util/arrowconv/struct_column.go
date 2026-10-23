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

func isStructList(dtype arrow.DataType) bool {
	list, ok := dtype.(*arrow.ListType)
	return ok && list.Elem().ID() == arrow.STRUCT
}

type structColumnChild struct {
	index  int
	schema *schemapb.FieldSchema
	output *schemapb.FieldData
}

// ArrowChunkedStructColumnToFieldData converts List<Struct> chunks, using the
// same row traversal for inferred and declared structs. A nil schema infers
// child types and rejects nulls; a declaration supplies IDs, logical element
// types and document nullability. Inferred strings share Arrow value buffers,
// which must be immutable, Go-managed and not recycled on Release.
// Collection child schemas describe arrays per document, while the Arrow
// struct children describe individual elements inside each document.
func ArrowChunkedStructColumnToFieldData(name string, column *arrow.Chunked, schema *schemapb.StructArrayFieldSchema) (*schemapb.FieldData, error) {
	if column == nil || !isStructList(column.DataType()) {
		return nil, merr.WrapErrServiceInternalMsg("struct column %s must be List<Struct>", name)
	}
	st := column.DataType().(*arrow.ListType).Elem().(*arrow.StructType)
	if st.NumFields() == 0 {
		return nil, merr.WrapErrServiceInternalMsg("struct column %s has no children", name)
	}
	indices := make(map[string]int, st.NumFields())
	for i, field := range st.Fields() {
		if _, exists := indices[field.Name]; exists {
			return nil, merr.WrapErrServiceInternalMsg("duplicate struct child %s", field.Name)
		}
		indices[field.Name] = i
	}
	children := make([]structColumnChild, st.NumFields())
	if schema != nil && (schema.Name != name || len(schema.Fields) != len(children)) {
		return nil, merr.WrapErrServiceInternalMsg("struct column %s does not match its schema", name)
	}
	used := make(map[int]bool, len(children))
	for i := range children {
		index := i
		var declared *schemapb.FieldSchema
		if schema != nil {
			declared = schema.Fields[i]
			var found bool
			index, found = indices[declared.GetName()]
			if declared == nil || !found || used[index] {
				return nil, merr.WrapErrServiceInternalMsg("missing or duplicate struct child %s", declared.GetName())
			}
			used[index] = true
			if declared.DataType != schemapb.DataType_Array && declared.DataType != schemapb.DataType_ArrayOfVector {
				return nil, merr.WrapErrServiceInternalMsg("struct child %s must declare an array type", declared.Name)
			}
		}
		field := st.Field(index)
		output := &schemapb.FieldData{FieldId: int64(i + 1), FieldName: field.Name, Type: schemapb.DataType_Array}
		if declared == nil {
			element, err := inferScalarType(field.Type)
			if err != nil {
				return nil, err
			}
			output.Field = &schemapb.FieldData_Scalars{Scalars: scalarArray(element, nil)}
		} else {
			output.FieldId, output.Type = declared.FieldID, declared.DataType
			if declared.DataType == schemapb.DataType_ArrayOfVector {
				if err := validateStructVectorType(field.Type, declared); err != nil {
					return nil, err
				}
			}
			// Validate even zero-row columns against their declared physical layout.
			b := array.NewBuilder(memory.DefaultAllocator, field.Type)
			empty := b.NewArray()
			b.Release()
			value, err := declaredStructRange(empty, 0, 0, declared)
			empty.Release()
			if err != nil {
				return nil, err
			}
			if declared.DataType == schemapb.DataType_Array {
				if value.GetScalars() == nil {
					return nil, merr.WrapErrServiceInternalMsg("non-scalar struct array child %s", declared.Name)
				}
				output.Field = &schemapb.FieldData_Scalars{Scalars: scalarArray(declared.ElementType, nil)}
			} else {
				if value.GetVectors() == nil {
					return nil, merr.WrapErrServiceInternalMsg("non-vector struct vector child %s", declared.Name)
				}
				dim := value.GetVectors().Dim
				output.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: dim, ElementType: declared.ElementType}}}}
			}
		}
		children[i] = structColumnChild{index: index, schema: declared, output: output}
	}
	output := &schemapb.FieldData{FieldName: name, Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{}}}
	if schema != nil {
		output.FieldId = schema.FieldID
	}
	for _, child := range children {
		output.GetStructArrays().Fields = append(output.GetStructArrays().Fields, child.output)
	}
	var valid []bool
	if schema != nil && schema.Nullable {
		valid = make([]bool, 0, column.Len())
	}
	for _, chunk := range column.Chunks() {
		rows := chunk.(*array.List)
		fields := rows.ListValues().(*array.Struct)
		for row := 0; row < rows.Len(); row++ {
			present := rows.IsValid(row)
			if !present && (schema == nil || !schema.Nullable) {
				return nil, merr.WrapErrServiceInternalMsg("null non-nullable struct column %s", name)
			}
			if valid != nil {
				valid = append(valid, present)
			}
			begin, end := rows.ValueOffsets(row)
			if !present {
				end = begin
			}
			for j := begin; j < end; j++ {
				if fields.IsNull(int(j)) {
					return nil, merr.WrapErrServiceInternalMsg("null struct element in %s", name)
				}
			}
			for _, child := range children {
				values := fields.Field(child.index)
				if child.schema == nil {
					value, err := exportScalarRange(values, begin, end)
					if err != nil {
						return nil, err
					}
					arrayData := child.output.GetScalars().GetArrayData()
					arrayData.Data = append(arrayData.Data, value)
				} else {
					value, err := declaredStructRange(values, begin, end, child.schema)
					if err != nil {
						return nil, err
					}
					if child.schema.DataType == schemapb.DataType_Array {
						data := child.output.GetScalars().GetArrayData()
						data.Data = append(data.Data, value.GetScalars())
					} else {
						data := child.output.GetVectors().GetVectorArray()
						data.Data = append(data.Data, value.GetVectors())
					}
				}
			}
		}
	}
	// Struct document validity is represented by the same bitmap on each child.
	for _, child := range children {
		typeutil.SetFieldDataValidData(child.output, valid)
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
	element := &schemapb.FieldSchema{Name: schema.Name, FieldID: schema.FieldID, DataType: schema.ElementType, TypeParams: schema.TypeParams}
	return ArrowColumnToFieldData(selected, element)
}

// A FixedSizeBinary Arrow type does not identify its vector element type or
// dimension. Validate both before the native converter copies its bytes.
func validateStructVectorType(dtype arrow.DataType, schema *schemapb.FieldSchema) error {
	fixed, ok := dtype.(*arrow.FixedSizeBinaryType)
	dim, err := typeutil.GetDim(schema)
	if !ok || err != nil || dim <= 0 || fixed.ByteWidth <= 0 {
		return merr.WrapErrServiceInternalMsg("invalid struct vector layout or dimension for %s", schema.Name)
	}
	width := int64(fixed.ByteWidth)
	valid := false
	switch schema.ElementType {
	case schemapb.DataType_FloatVector:
		valid = width%4 == 0 && dim == width/4
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		valid = width%2 == 0 && dim == width/2
	case schemapb.DataType_BinaryVector:
		valid = dim%8 == 0 && dim/8 == width
	case schemapb.DataType_Int8Vector:
		valid = dim == width
	}
	if !valid {
		return merr.WrapErrServiceInternalMsg("struct vector %s type/dimension does not match Arrow byte width", schema.Name)
	}
	return nil
}
