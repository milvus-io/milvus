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
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The tests below describe struct values with a small model and build both
// layouts from it: the read layout Query returns (dense at every nullable
// scalar level) and the compact layout an insert request uses.

const (
	ccStructName  = "clips"
	ccStructID    = 200
	ccVectorDim   = 2
	ccMaxCapacity = "8"
)

type ccKind int

const (
	ccScalar ccKind = iota
	ccNested
	ccVector
)

type ccChild struct {
	name string
	id   int64
	kind ccKind
	// Scalar element type, or the leaf type of a nested Array.
	elementType schemapb.DataType
	// element_nullable of a scalar or vector Array, or the inner Array
	// nullability of a nested Array.
	elementNullable bool
	// Leaf nullability of a nested Array.
	leafNullable bool
	// An added sub-field holds only null elements on rows written before it
	// was added.
	added bool
}

// elementsOf returns the element validity of the sub-field on row.
func (c ccChild) elementsOf(row ccRow) []bool {
	if c.added {
		return make([]bool, len(row.elements))
	}
	return row.elements
}

// ccRow is one entity row of the struct field. Every sub-field has
// len(elements) struct elements; elements gives their validity for the
// sub-fields whose element level is nullable, and leaves gives the leaf
// validity of each element's inner Array for nested sub-fields with a
// nullable leaf. Bits are ignored by levels that are not nullable.
type ccRow struct {
	null     bool
	elements []bool
	leaves   [][]bool
}

// ccRows covers a row with null and valid elements (the first valid element
// holds a zero value and a valid empty inner Array follows a null one), a row
// whose elements are all null, an empty struct, a null struct or a row of
// null leaves, and a row without null slots.
func ccRows(rowNullable bool) []ccRow {
	rows := []ccRow{
		{elements: []bool{true, false, true}, leaves: [][]bool{{true, false}, {true}, {}}},
		{elements: []bool{false, false}, leaves: [][]bool{{true}, {false}}},
		{elements: []bool{}, leaves: [][]bool{}},
		{elements: []bool{true}, leaves: [][]bool{{false, false}}},
		{elements: []bool{true, true}, leaves: [][]bool{{true, true}, {true}}},
	}
	if rowNullable {
		rows[3] = ccRow{null: true}
	}
	return rows
}

func ccScalarChildren() []ccChild {
	return []ccChild{
		{name: "b", id: 201, kind: ccScalar, elementType: schemapb.DataType_Bool, elementNullable: true},
		{name: "i8", id: 202, kind: ccScalar, elementType: schemapb.DataType_Int8, elementNullable: true},
		{name: "i16", id: 203, kind: ccScalar, elementType: schemapb.DataType_Int16, elementNullable: true},
		{name: "i32", id: 204, kind: ccScalar, elementType: schemapb.DataType_Int32, elementNullable: true},
		{name: "score", id: 205, kind: ccScalar, elementType: schemapb.DataType_Int64, elementNullable: true},
		{name: "f", id: 206, kind: ccScalar, elementType: schemapb.DataType_Float, elementNullable: true},
		{name: "d", id: 207, kind: ccScalar, elementType: schemapb.DataType_Double, elementNullable: true},
		{name: "s", id: 208, kind: ccScalar, elementType: schemapb.DataType_VarChar, elementNullable: true},
		{name: "plain", id: 209, kind: ccScalar, elementType: schemapb.DataType_Int64},
	}
}

func ccNestedChildren() []ccChild {
	return []ccChild{
		{name: "tags", id: 211, kind: ccNested, elementType: schemapb.DataType_VarChar, elementNullable: true, leafNullable: true},
		{name: "tags_inner", id: 212, kind: ccNested, elementType: schemapb.DataType_VarChar, elementNullable: true},
		{name: "tags_leaf", id: 213, kind: ccNested, elementType: schemapb.DataType_VarChar, leafNullable: true},
		{name: "tags_plain", id: 214, kind: ccNested, elementType: schemapb.DataType_VarChar},
		{name: "ints", id: 215, kind: ccNested, elementType: schemapb.DataType_Int64, elementNullable: true, leafNullable: true},
	}
}

func ccVectorChildren() []ccChild {
	return []ccChild{
		{name: "vec", id: 221, kind: ccVector, elementType: schemapb.DataType_FloatVector, elementNullable: true},
		{name: "plain_vec", id: 222, kind: ccVector, elementType: schemapb.DataType_FloatVector},
	}
}

func ccAllChildren() []ccChild {
	children := append(ccScalarChildren(), ccNestedChildren()...)
	return append(children, ccVectorChildren()...)
}

func ccKV(key, value string) *commonpb.KeyValuePair {
	return &commonpb.KeyValuePair{Key: key, Value: value}
}

func ccChildSchema(child ccChild, rowNullable bool) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{
		FieldID:         child.id,
		Name:            typeutil.ConcatStructFieldName(ccStructName, child.name),
		Nullable:        rowNullable,
		ElementNullable: child.elementNullable,
		TypeParams:      []*commonpb.KeyValuePair{ccKV(common.MaxCapacityKey, ccMaxCapacity)},
	}
	switch child.kind {
	case ccScalar:
		field.DataType = schemapb.DataType_Array
		field.ElementType = child.elementType
		if child.elementType == schemapb.DataType_VarChar {
			field.TypeParams = append(field.TypeParams, ccKV(common.MaxLengthKey, "64"))
		}
	case ccNested:
		leaf := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: child.elementType}, Nullable: child.leafNullable}
		if child.elementType == schemapb.DataType_VarChar {
			leaf.TypeParams = []*commonpb.KeyValuePair{ccKV(common.MaxLengthKey, "64")}
		}
		field.DataType = schemapb.DataType_Array
		field.ElementType = schemapb.DataType_Array
		field.TypeSchema = &schemapb.TypeSchema{
			Nullable:   rowNullable,
			TypeParams: []*commonpb.KeyValuePair{ccKV(common.MaxCapacityKey, ccMaxCapacity)},
			Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
				Nullable:   child.elementNullable,
				TypeParams: []*commonpb.KeyValuePair{ccKV(common.MaxCapacityKey, ccMaxCapacity)},
				Kind:       &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf},
			}},
		}
	case ccVector:
		field.DataType = schemapb.DataType_ArrayOfVector
		field.ElementType = child.elementType
		field.TypeParams = append(field.TypeParams, ccKV(common.DimKey, fmt.Sprint(ccVectorDim)))
	}
	return field
}

func ccStructSchema(children []ccChild, rowNullable bool) *schemapb.StructArrayFieldSchema {
	fields := make([]*schemapb.FieldSchema, 0, len(children))
	for _, child := range children {
		fields = append(fields, ccChildSchema(child, rowNullable))
	}
	return &schemapb.StructArrayFieldSchema{FieldID: ccStructID, Name: ccStructName, Nullable: rowNullable, Fields: fields}
}

func ccCollectionSchema(children []ccChild, rowNullable bool) *schemaInfo {
	return mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "struct_compact_partial_update",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "marker", DataType: schemapb.DataType_Int64},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{ccStructSchema(children, rowNullable)},
	})
}

// ccLeaf builds one scalar Array value of the given type. Slot 0 holds a
// real zero value, so a placeholder cannot be told from a valid value by its
// content. dense keeps a zero placeholder for every invalid slot.
func ccLeaf(elementType schemapb.DataType, seed int, validData []bool, nullable, dense bool) *schemapb.ScalarField {
	var (
		bools   []bool
		ints    []int32
		longs   []int64
		floats  []float32
		doubles []float64
		strings []string
	)
	for slot, bit := range validData {
		valid := !nullable || bit
		if !valid && !dense {
			continue
		}
		value := seed*10 + slot
		if slot == 0 || !valid {
			value = 0
		}
		bools = append(bools, value%2 == 1)
		ints = append(ints, int32(value))
		longs = append(longs, int64(value))
		floats = append(floats, float32(value)+0.5*float32(min(value, 1)))
		doubles = append(doubles, float64(value)+0.25*float64(min(value, 1)))
		if value == 0 {
			strings = append(strings, "")
		} else {
			strings = append(strings, fmt.Sprintf("v%d", value))
		}
	}
	row := &schemapb.ScalarField{}
	switch elementType {
	case schemapb.DataType_Bool:
		row.Data = &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: bools}}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		row.Data = &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: ints}}
	case schemapb.DataType_Int64:
		row.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: longs}}
	case schemapb.DataType_Float:
		row.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: floats}}
	case schemapb.DataType_Double:
		row.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: doubles}}
	case schemapb.DataType_VarChar:
		row.Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: strings}}
	default:
		panic(fmt.Sprintf("unsupported element type %s", elementType))
	}
	if nullable {
		typeutil.SetArrayElementValidData(row, append([]bool{}, validData...))
	}
	return row
}

func ccScalarRow(child ccChild, rowIndex int, row ccRow, dense bool) *schemapb.ScalarField {
	elements := child.elementsOf(row)
	if child.kind == ccScalar {
		return ccLeaf(child.elementType, rowIndex+1, elements, child.elementNullable, dense)
	}
	inner := make([]*schemapb.ScalarField, 0, len(elements))
	for element, valid := range elements {
		if child.elementNullable && !valid {
			if dense {
				// Query output for a null inner Array: an empty value of the leaf type.
				inner = append(inner, ccLeaf(child.elementType, 0, nil, false, true))
			}
			continue
		}
		leafValid := row.leaves[element]
		if !child.leafNullable {
			leafValid = make([]bool, len(row.leaves[element]))
		}
		inner = append(inner, ccLeaf(child.elementType, (rowIndex+1)*10+element, leafValid, child.leafNullable, dense))
	}
	value := &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
		ArrayData: &schemapb.ArrayArray{ElementType: child.elementType, Data: inner},
	}}
	if child.elementNullable {
		typeutil.SetArrayElementValidData(value, append([]bool{}, elements...))
	}
	return value
}

// ccVectorRow is the same in both layouts: vector elements are compact.
func ccVectorRow(child ccChild, rowIndex int, row ccRow) *schemapb.VectorField {
	elements := child.elementsOf(row)
	values := make([]float32, 0, len(elements)*ccVectorDim)
	for element, valid := range elements {
		if child.elementNullable && !valid {
			continue
		}
		values = append(values, float32(rowIndex+1), float32(element))
	}
	value := &schemapb.VectorField{Dim: ccVectorDim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: values}}}
	if child.elementNullable {
		typeutil.SetVectorArrayElementValidData(value, append([]bool{}, elements...))
	}
	return value
}

// ccStructField builds the struct field for the rows at order. denseRows
// keeps a placeholder for every null struct row, as Query does; otherwise null
// rows are omitted, as in an insert request. denseElements selects the read
// layout of scalar and nested cells.
func ccStructField(children []ccChild, rows []ccRow, order []int, rowNullable, denseRows, denseElements bool) *schemapb.FieldData {
	fields := make([]*schemapb.FieldData, 0, len(children))
	for _, child := range children {
		rowValidity := make([]bool, 0, len(order))
		scalarRows := make([]*schemapb.ScalarField, 0, len(order))
		vectorRows := make([]*schemapb.VectorField, 0, len(order))
		for _, rowIndex := range order {
			row := rows[rowIndex]
			rowValidity = append(rowValidity, !row.null)
			if row.null {
				if !denseRows {
					continue
				}
				if child.kind == ccVector {
					placeholder, err := typeutil.NewEmptyArrayOfVectorRow(ccVectorDim, child.elementType)
					if err != nil {
						panic(err)
					}
					vectorRows = append(vectorRows, placeholder)
				} else {
					scalarRows = append(scalarRows, &schemapb.ScalarField{})
				}
				continue
			}
			if child.kind == ccVector {
				vectorRows = append(vectorRows, ccVectorRow(child, rowIndex, row))
			} else {
				scalarRows = append(scalarRows, ccScalarRow(child, rowIndex, row, denseElements))
			}
		}
		field := &schemapb.FieldData{FieldName: child.name, FieldId: child.id}
		if child.kind == ccVector {
			field.Type = schemapb.DataType_ArrayOfVector
			field.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim: ccVectorDim,
				Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
					Dim: ccVectorDim, ElementType: child.elementType, Data: vectorRows,
				}},
			}}
		} else {
			elementType := child.elementType
			if child.kind == ccNested {
				elementType = schemapb.DataType_Array
			}
			field.Type = schemapb.DataType_Array
			field.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: elementType, Data: scalarRows}},
			}}
		}
		if rowNullable {
			typeutil.SetFieldDataValidData(field, rowValidity)
		}
		fields = append(fields, field)
	}
	return &schemapb.FieldData{
		FieldName: ccStructName, FieldId: ccStructID, Type: schemapb.DataType_ArrayOfStruct,
		Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: fields}},
	}
}

func ccLongField(name string, id int64, values []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: name, FieldId: id, Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
		}},
	}
}

func ccIndexOrder(n int) []int {
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	return order
}

func TestCompactRetrievedStructField(t *testing.T) {
	for _, rowNullable := range []bool{false, true} {
		t.Run(fmt.Sprintf("rowNullable=%t", rowNullable), func(t *testing.T) {
			children := ccAllChildren()
			rows := ccRows(rowNullable)
			order := ccIndexOrder(len(rows))
			structSchema := ccStructSchema(children, rowNullable)
			retrieved := ccStructField(children, rows, order, rowNullable, true, true)
			snapshot := proto.Clone(retrieved)

			compacted, err := compactRetrievedStructField(structSchema, retrieved)
			require.NoError(t, err)
			// Rows stay in place; only scalar and nested cells become compact.
			want := ccStructField(children, rows, order, rowNullable, true, false)
			require.True(t, proto.Equal(want, compacted), "got %v\nwant %v", compacted, want)
			require.True(t, proto.Equal(snapshot, retrieved), "the Query response must not change")

			// Only the converted sub-fields are private copies; the others,
			// vectors included, are the Query response's own FieldData.
			require.NotSame(t, retrieved, compacted)
			require.NotSame(t, retrieved.GetStructArrays(), compacted.GetStructArrays())
			for i, child := range compacted.GetStructArrays().GetFields() {
				source := retrieved.GetStructArrays().GetFields()[i]
				if !structChildNeedsCompaction(structSchema.GetFields()[i]) {
					require.Same(t, source, child, "sub-field %q", child.GetFieldName())
					continue
				}
				require.NotSame(t, source, child, "sub-field %q", child.GetFieldName())
				for row, value := range child.GetScalars().GetArrayData().GetData() {
					require.NotSame(t, source.GetScalars().GetArrayData().GetData()[row], value)
				}
			}

			// Vector payloads are already compact and must be copied as is.
			for i, child := range children {
				if child.kind == ccVector {
					require.True(t, proto.Equal(retrieved.GetStructArrays().GetFields()[i], compacted.GetStructArrays().GetFields()[i]))
				}
			}

			// A retry converts the same unchanged Query response again.
			retried, err := compactRetrievedStructField(structSchema, retrieved)
			require.NoError(t, err)
			require.True(t, proto.Equal(want, retried))
			require.True(t, proto.Equal(snapshot, retrieved), "the Query response must not change")

			// A value that is already compact is kept.
			again, err := compactRetrievedStructField(structSchema, compacted)
			require.NoError(t, err)
			require.True(t, proto.Equal(want, again))
		})
	}

	t.Run("no sub-field needs conversion", func(t *testing.T) {
		for _, rowNullable := range []bool{false, true} {
			children := []ccChild{ccScalarChildren()[8], ccVectorChildren()[0], ccVectorChildren()[1]}
			rows := ccRows(rowNullable)
			retrieved := ccStructField(children, rows, ccIndexOrder(len(rows)), rowNullable, true, true)
			snapshot := proto.Clone(retrieved)
			compacted, err := compactRetrievedStructField(ccStructSchema(children, rowNullable), retrieved)
			require.NoError(t, err)
			require.Same(t, retrieved, compacted)
			require.True(t, proto.Equal(snapshot, retrieved))
		}
	})

	t.Run("unknown sub-field without conversion", func(t *testing.T) {
		children := []ccChild{ccScalarChildren()[8]}
		retrieved := ccStructField(children, ccRows(false), []int{0}, false, true, true)
		retrieved.GetStructArrays().GetFields()[0].FieldName = "missing"
		_, err := compactRetrievedStructField(ccStructSchema(children, false), retrieved)
		require.Error(t, err)
		require.True(t, errors.Is(err, merr.ErrServiceInternal), err.Error())
	})

	t.Run("malformed payload", func(t *testing.T) {
		children := []ccChild{ccScalarChildren()[4]}
		retrieved := ccStructField(children, ccRows(false), []int{0}, false, true, true)
		row := retrieved.GetStructArrays().GetFields()[0].GetScalars().GetArrayData().GetData()[0]
		row.GetLongData().Data = row.GetLongData().GetData()[:1]
		_, err := compactRetrievedStructField(ccStructSchema(children, false), retrieved)
		require.Error(t, err)
		require.True(t, errors.Is(err, merr.ErrServiceInternal), err.Error())
	})

	t.Run("malformed nested payload", func(t *testing.T) {
		children := []ccChild{ccNestedChildren()[0]}
		retrieved := ccStructField(children, ccRows(false), []int{0}, false, true, true)
		row := retrieved.GetStructArrays().GetFields()[0].GetScalars().GetArrayData().GetData()[0]
		row.GetArrayData().Data = row.GetArrayData().GetData()[:1]
		_, err := compactRetrievedStructField(ccStructSchema(children, false), retrieved)
		require.Error(t, err)
		require.True(t, errors.Is(err, merr.ErrServiceInternal), err.Error())
	})

	t.Run("unknown sub-field", func(t *testing.T) {
		children := []ccChild{ccScalarChildren()[4]}
		retrieved := ccStructField(children, ccRows(false), []int{0}, false, true, true)
		retrieved.GetStructArrays().GetFields()[0].FieldName = "missing"
		_, err := compactRetrievedStructField(ccStructSchema(children, false), retrieved)
		require.Error(t, err)
		require.True(t, errors.Is(err, merr.ErrServiceInternal), err.Error())
	})
}

type ccPartialUpdate struct {
	t        *testing.T
	children []ccChild
	rows     []ccRow
	task     *UpsertTask
	reads    int
	// Every Query response and a snapshot taken before it was returned.
	responses []*milvuspb.QueryResults
	snapshots []*milvuspb.QueryResults
	original  []*schemapb.FieldData
	// expect, when set, returns the written value of a sub-field on an
	// existing row; nil keeps the old value.
	expect func(child ccChild, rowIndex int, row ccRow) proto.Message
}

// The rows of the collection are rows[i] at primary key i+1. Query returns
// them in this order, which differs from every request order below.
var ccQueryOrder = []int{2, 0, 4, 1, 3}

func newCCPartialUpdate(t *testing.T, children []ccChild, rowNullable bool, pks []int64, extra ...*schemapb.FieldData) *ccPartialUpdate {
	t.Helper()
	schema := ccCollectionSchema(children, rowNullable)
	markers := make([]int64, len(pks))
	for i, pk := range pks {
		markers[i] = pk * 100
	}
	fields := append([]*schemapb.FieldData{ccLongField("id", 100, pks), ccLongField("marker", 101, markers)}, extra...)
	numRows := uint64(len(pks))
	task := &UpsertTask{
		baseTask: baseTask{MetaCache: newTestCache()},
		ctx:      context.Background(), schema: schema, idAllocator: &allocator.IDAllocator{}, collectionID: 100,
		req: &milvuspb.UpsertRequest{
			Base: commonpbutil.NewMsgBase(), FieldsData: fields, NumRows: uint32(numRows),
			PartialUpdate: true, CollectionName: schema.GetName(),
		},
		upsertMsg: &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{
				Base:           commonpbutil.NewMsgBase(),
				CollectionName: schema.GetName(), CollectionID: 100, PartitionName: "_default",
				FieldsData: fields, NumRows: numRows, Version: msgpb.InsertDataVersion_ColumnBased,
			}},
			DeleteMsg: &msgstream.DeleteMsg{DeleteRequest: &msgpb.DeleteRequest{
				Base: commonpbutil.NewMsgBase(), CollectionName: schema.GetName(), CollectionID: 100, PartitionName: "_default",
			}},
		},
		node: &mockUpsertNode{}, result: &milvuspb.MutationResult{},
		partialUpdateOriginalFields: cloneFieldDataList(fields),
	}
	task.SetTs(12345)
	setPartialUpdateCASTestChannels(task, partialUpdateCASTestVChannels)
	p := &ccPartialUpdate{
		t: t, children: children, rows: ccRows(rowNullable), task: task,
		original: cloneFieldDataList(fields),
	}

	fakeWAL := newPartialUpdateCASTestWAL(t, 9)
	oldWAL := streaming.WAL()
	streaming.SetWALForTest(fakeWAL)
	t.Cleanup(func() { streaming.SetWALForTest(oldWAL) })
	partition := mockey.Mock((*metacache.MetaCache).GetPartitionID).Return(UniqueID(100), nil).Build()
	t.Cleanup(func() { partition.UnPatch() })
	collection := mockey.Mock((*metacache.MetaCache).GetCollectionID).Return(UniqueID(100), nil).Build()
	t.Cleanup(func() { collection.UnPatch() })
	nextID := int64(1)
	alloc := mockey.Mock((*allocator.IDAllocator).Alloc).To(func(_ *allocator.IDAllocator, count uint32) (int64, int64, error) {
		begin := nextID
		nextID += int64(count)
		return begin, nextID, nil
	}).Build()
	t.Cleanup(func() { alloc.UnPatch() })
	read := mockey.Mock(retrieveByPKs).To(func(_ context.Context, task *UpsertTask, ids *schemapb.IDs, outputFields []string) (*milvuspb.QueryResults, segcore.StorageCost, error) {
		require.Equal(t, []string{"*"}, outputFields)
		p.reads++
		requested := typeutil.NewSet(ids.GetIntId().GetData()...)
		var pkValues, markers []int64
		var rowOrder []int
		for _, row := range ccQueryOrder {
			pk := int64(row + 1)
			if requested.Contain(pk) {
				rowOrder = append(rowOrder, row)
				pkValues = append(pkValues, pk)
				markers = append(markers, pk)
			}
		}
		require.NotEmpty(t, pkValues)
		resp := &milvuspb.QueryResults{Status: merr.Success(), FieldsData: []*schemapb.FieldData{
			ccLongField("id", 100, pkValues),
			ccLongField("marker", 101, markers),
			ccStructField(p.children, p.rows, rowOrder, rowNullable, true, true),
		}}
		for _, proof := range task.partialUpdateCASGroups {
			proof.ReadTs = uint64(1000 + p.reads)
		}
		p.responses = append(p.responses, resp)
		p.snapshots = append(p.snapshots, proto.Clone(resp).(*milvuspb.QueryResults))
		return resp, segcore.StorageCost{}, nil
	}).Build()
	t.Cleanup(func() { read.UnPatch() })
	return p
}

// run executes the read-merge-validate preparation of a partial upsert, as
// PreExecute does.
func (p *ccPartialUpdate) run() error {
	if err := p.task.prepareUpsert(context.Background()); err != nil {
		return err
	}
	return p.task.insertPreExecute(context.Background())
}

// retry executes the preparation of a CAS retry attempt.
func (p *ccPartialUpdate) retry() error {
	return p.task.preparePartialUpdateRetryAttempt(context.Background())
}

// checkWritten checks the struct sub-fields written for the request rows, in
// request order. Normalization turns scalar cells back into the read layout,
// so the written value must equal what Query returned for that row.
func (p *ccPartialUpdate) checkWritten(pks []int64, missing map[int64]bool) {
	t := p.t
	written := make(map[int64]*schemapb.FieldData)
	for _, field := range p.task.upsertMsg.InsertMsg.GetFieldsData() {
		written[field.GetFieldId()] = field
	}
	require.Equal(t, int(p.task.upsertMsg.InsertMsg.NRows()), len(pks))
	pkField := written[100]
	require.NotNil(t, pkField)
	// Merge places existing rows first, in request order, then missing rows.
	order := make([]int64, 0, len(pks))
	for _, pk := range pks {
		if !missing[pk] {
			order = append(order, pk)
		}
	}
	for _, pk := range pks {
		if missing[pk] {
			order = append(order, pk)
		}
	}
	require.Equal(t, order, pkField.GetScalars().GetLongData().GetData())
	markers := make([]int64, len(order))
	for i, pk := range order {
		markers[i] = pk * 100
	}
	require.Equal(t, markers, written[101].GetScalars().GetLongData().GetData())

	for _, child := range p.children {
		field := written[child.id]
		require.NotNil(t, field, child.name)
		rowValidity := typeutil.GetFieldDataValidData(field)
		for i, pk := range order {
			if missing[pk] {
				require.False(t, rowValidity[i], "%s row %d", child.name, i)
				continue
			}
			rowIndex := int(pk - 1)
			row := p.rows[rowIndex]
			if len(rowValidity) > 0 {
				require.Equal(t, !row.null, rowValidity[i], "%s row %d", child.name, i)
			} else {
				require.False(t, row.null)
			}
			if row.null {
				continue
			}
			var want proto.Message
			if p.expect != nil {
				want = p.expect(child, rowIndex, row)
			}
			if child.kind == ccVector {
				got := field.GetVectors().GetVectorArray().GetData()[i]
				if want == nil {
					want = ccVectorRow(child, rowIndex, row)
				}
				require.True(t, proto.Equal(want, got), "%s row %d: got %v, want %v", child.name, i, got, want)
				continue
			}
			got := field.GetScalars().GetArrayData().GetData()[i]
			if want == nil {
				want = ccScalarRow(child, rowIndex, row, true)
			}
			require.True(t, proto.Equal(want, got), "%s row %d: got %v, want %v", child.name, i, got, want)
		}
	}
}

func (p *ccPartialUpdate) checkInputsUnchanged() {
	t := p.t
	for i, resp := range p.responses {
		require.True(t, proto.Equal(p.snapshots[i], resp), "Query response %d was modified", i)
	}
	require.Len(t, p.task.partialUpdateOriginalFields, len(p.original))
	for i, field := range p.original {
		require.True(t, proto.Equal(field, p.task.partialUpdateOriginalFields[i]), "original request field %d was modified", i)
	}
}

func TestPartialUpsertKeepsOmittedElementNullableStruct(t *testing.T) {
	variants := []struct {
		name     string
		children []ccChild
	}{
		{"scalar", ccScalarChildren()},
		{"nested", ccNestedChildren()},
		{"vector", ccVectorChildren()},
		{"mixed", ccAllChildren()},
	}
	pks := []int64{2, 5, 1, 4, 3}
	for _, variant := range variants {
		for _, rowNullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rowNullable=%t", variant.name, rowNullable), func(t *testing.T) {
				p := newCCPartialUpdate(t, variant.children, rowNullable, pks)
				require.NoError(t, p.run())
				require.Equal(t, 1, p.reads)
				p.checkWritten(pks, nil)
				p.checkInputsUnchanged()

				// A CAS retry reads again and must convert the new response too.
				require.NoError(t, p.retry())
				require.Equal(t, 2, p.reads)
				p.checkWritten(pks, nil)
				p.checkInputsUnchanged()
			})
		}
	}

	t.Run("mixed batch of existing null struct, element nulls and missing primary keys", func(t *testing.T) {
		// Primary key 4 has a null struct, which Query returns as placeholder
		// rows; 1 and 2 hold null elements and null leaves; 98 and 99 do not
		// exist and take a null struct built in the full format of the merge.
		// The merged column mixes all three before the null rows are dropped.
		missing := map[int64]bool{98: true, 99: true}
		for _, batch := range [][]int64{
			{2, 99, 4, 5, 1, 3},
			{98, 4, 1, 99},
			{4, 99},
		} {
			for _, variant := range []struct {
				name     string
				children []ccChild
			}{
				{"scalar", ccScalarChildren()},
				{"nested", ccNestedChildren()},
				{"vector", ccVectorChildren()},
				{"mixed", ccAllChildren()},
			} {
				t.Run(fmt.Sprintf("%v/%s", batch, variant.name), func(t *testing.T) {
					p := newCCPartialUpdate(t, variant.children, true, batch)
					require.NoError(t, p.run())
					p.checkWritten(batch, missing)
					p.checkInputsUnchanged()

					require.NoError(t, p.retry())
					p.checkWritten(batch, missing)
					p.checkInputsUnchanged()
				})
			}
		}
	})

	t.Run("one existing primary key", func(t *testing.T) {
		p := newCCPartialUpdate(t, ccAllChildren(), true, []int64{1})
		require.NoError(t, p.run())
		p.checkWritten([]int64{1}, nil)
		p.checkInputsUnchanged()
	})
}

func TestPartialUpsertReplacesElementNullableStruct(t *testing.T) {
	pks := []int64{2, 5, 1, 4, 3}
	requestOrder := make([]int, len(pks))
	for i, pk := range pks {
		requestOrder[i] = int(pk - 1)
	}
	for _, rowNullable := range []bool{false, true} {
		t.Run(fmt.Sprintf("compact input/rowNullable=%t", rowNullable), func(t *testing.T) {
			children := ccAllChildren()
			replacement := ccStructField(children, ccRows(rowNullable), requestOrder, rowNullable, false, false)
			p := newCCPartialUpdate(t, children, rowNullable, pks, replacement)
			require.NoError(t, p.run())
			p.checkWritten(pks, nil)
			p.checkInputsUnchanged()
		})

		for _, children := range [][]ccChild{{ccScalarChildren()[4]}, {ccNestedChildren()[0]}, {ccNestedChildren()[2]}} {
			t.Run(fmt.Sprintf("dense input %s/rowNullable=%t", children[0].name, rowNullable), func(t *testing.T) {
				// A client must send compact cells; the read layout is rejected.
				replacement := ccStructField(children, ccRows(rowNullable), requestOrder, rowNullable, false, true)
				p := newCCPartialUpdate(t, children, rowNullable, pks, replacement)
				err := p.run()
				require.Error(t, err)
				require.Contains(t, err.Error(), "compact payload")
				require.True(t, errors.Is(err, merr.ErrParameterInvalid), err.Error())
			})
		}
	}
}

func TestPartialUpsertKeepsOmittedStructWithAddedSubFields(t *testing.T) {
	// Rows written before the sub-fields were added read back N null elements
	// for them, in read layout like any other element-nullable sub-field.
	added := []ccChild{
		{name: "added_s", id: 231, kind: ccScalar, elementType: schemapb.DataType_VarChar, elementNullable: true, added: true},
		{name: "added_n", id: 232, kind: ccNested, elementType: schemapb.DataType_Int64, elementNullable: true, leafNullable: true, added: true},
		{name: "added_v", id: 233, kind: ccVector, elementType: schemapb.DataType_FloatVector, elementNullable: true, added: true},
	}
	pks := []int64{2, 5, 1, 4, 3}
	for _, rowNullable := range []bool{false, true} {
		t.Run(fmt.Sprintf("rowNullable=%t", rowNullable), func(t *testing.T) {
			children := append([]ccChild{ccScalarChildren()[4], ccScalarChildren()[8], ccNestedChildren()[0], ccVectorChildren()[1]}, added...)
			p := newCCPartialUpdate(t, children, rowNullable, pks)
			require.NoError(t, p.run())
			p.checkWritten(pks, nil)
			p.checkInputsUnchanged()
			require.NoError(t, p.retry())
			p.checkWritten(pks, nil)
			p.checkInputsUnchanged()
		})
	}
}

// newCCPathReplace prepares a PATH_REPLACE of the struct field at path.
func newCCPathReplace(t *testing.T, children []ccChild, rowNullable bool, pks []int64, path string, operand *schemapb.FieldData) *ccPartialUpdate {
	t.Helper()
	p := newCCPartialUpdate(t, children, rowNullable, pks, operand)
	p.task.req.FieldOps = []*schemapb.FieldPartialUpdateOp{{
		FieldName: ccStructName, Op: schemapb.FieldPartialUpdateOp_PATH_REPLACE, Path: path,
	}}
	plans, _, err := resolveFieldPartialUpdateOps(p.task.req, p.task.schema.CollectionSchema)
	require.NoError(t, err)
	p.task.fieldPartialUpdatePlans = plans
	p.task.partialUpdateOriginalFields = cloneFieldDataList(p.task.req.GetFieldsData())
	p.original = cloneFieldDataList(p.task.req.GetFieldsData())
	return p
}

// ccOperand builds a PATH_REPLACE operand with one struct element per row.
func ccOperand(rows int, children ...*schemapb.FieldData) *schemapb.FieldData {
	for _, child := range children {
		if child.GetType() == schemapb.DataType_ArrayOfVector {
			data := child.GetVectors().GetVectorArray()
			for len(data.Data) < rows {
				data.Data = append(data.Data, proto.Clone(data.Data[0]).(*schemapb.VectorField))
			}
			continue
		}
		data := child.GetScalars().GetArrayData()
		for len(data.Data) < rows {
			data.Data = append(data.Data, proto.Clone(data.Data[0]).(*schemapb.ScalarField))
		}
	}
	return &schemapb.FieldData{
		FieldName: ccStructName, FieldId: ccStructID, Type: schemapb.DataType_ArrayOfStruct,
		Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: children}},
	}
}

func ccOperandScalar(name string, elementType schemapb.DataType, row *schemapb.ScalarField) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: name, Type: schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: elementType, Data: []*schemapb.ScalarField{row}}},
		}},
	}
}

func ccOperandVector(name string, row *schemapb.VectorField) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: name, Type: schemapb.DataType_ArrayOfVector,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: ccVectorDim,
			Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
				Dim: ccVectorDim, ElementType: schemapb.DataType_FloatVector, Data: []*schemapb.VectorField{row},
			}},
		}},
	}
}

func TestPathReplaceKeepsElementNullableStructSiblings(t *testing.T) {
	// Element 1 is null on pk 1 and valid on pk 5; both rows are replaced.
	pks := []int64{5, 1}
	const index = 1
	score := func() *schemapb.ScalarField {
		row := &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{77}}}}
		typeutil.SetArrayElementValidData(row, []bool{true})
		return row
	}
	replacedScore := func(child ccChild, rowIndex int, row ccRow) *schemapb.ScalarField {
		want := ccScalarRow(child, rowIndex, row, true)
		want.GetLongData().Data[index] = 77
		typeutil.GetArrayElementValidData(want)[index] = true
		return want
	}

	for _, rowNullable := range []bool{false, true} {
		t.Run(fmt.Sprintf("one sub-field/rowNullable=%t", rowNullable), func(t *testing.T) {
			// Every other sub-field keeps its old value.
			children := ccAllChildren()
			operand := ccOperand(len(pks), ccOperandScalar("score", schemapb.DataType_Int64, score()))
			p := newCCPathReplace(t, children, rowNullable, pks, "[1][score]", operand)
			p.expect = func(child ccChild, rowIndex int, row ccRow) proto.Message {
				if child.name == "score" {
					return replacedScore(child, rowIndex, row)
				}
				return nil
			}
			require.NoError(t, p.run())
			p.checkWritten(pks, nil)
			p.checkInputsUnchanged()
			require.NoError(t, p.retry())
			p.checkWritten(pks, nil)
			p.checkInputsUnchanged()
		})

		t.Run(fmt.Sprintf("whole element/rowNullable=%t", rowNullable), func(t *testing.T) {
			// The replaced element of the nested sub-field keeps its own null
			// leaf; the other elements and their leaves keep their old values.
			children := []ccChild{ccScalarChildren()[4], ccNestedChildren()[0], ccVectorChildren()[0]}
			leaf := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"x"}}}}
			typeutil.SetArrayElementValidData(leaf, []bool{true, false})
			tags := &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_VarChar, Data: []*schemapb.ScalarField{leaf},
			}}}
			typeutil.SetArrayElementValidData(tags, []bool{true})
			vec := &schemapb.VectorField{Dim: ccVectorDim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{9, 9}}}}
			typeutil.SetVectorArrayElementValidData(vec, []bool{true})
			operand := ccOperand(len(pks),
				ccOperandScalar("score", schemapb.DataType_Int64, score()),
				ccOperandScalar("tags", schemapb.DataType_Array, tags),
				ccOperandVector("vec", vec))
			p := newCCPathReplace(t, children, rowNullable, pks, "[1]", operand)
			p.expect = func(child ccChild, rowIndex int, row ccRow) proto.Message {
				switch child.name {
				case "score":
					return replacedScore(child, rowIndex, row)
				case "tags":
					want := ccScalarRow(child, rowIndex, row, true)
					replaced := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"x", ""}}}}
					typeutil.SetArrayElementValidData(replaced, []bool{true, false})
					want.GetArrayData().Data[index] = replaced
					typeutil.GetArrayElementValidData(want)[index] = true
					return want
				default:
					elements := append([]bool{}, row.elements...)
					elements[index] = true
					values := make([]float32, 0, len(elements)*ccVectorDim)
					for element, valid := range elements {
						switch {
						case element == index:
							values = append(values, 9, 9)
						case valid:
							values = append(values, float32(rowIndex+1), float32(element))
						}
					}
					want := &schemapb.VectorField{Dim: ccVectorDim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: values}}}
					typeutil.SetVectorArrayElementValidData(want, elements)
					return want
				}
			}
			require.NoError(t, p.run())
			p.checkWritten(pks, nil)
			p.checkInputsUnchanged()
		})
	}
}
