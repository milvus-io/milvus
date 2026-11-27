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

package parquet

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The Parquet reader checks actual null values against the schema level by
// level. The Arrow nullable flags of a file never decide on their own whether
// it is accepted: a file whose levels are all nullable=true and a file whose
// flags are stricter than the schema both import when the values are valid.

// flagElem is one struct element. nil is a null at that level; inside a list
// value a nil entry is a null item.
type flagElem struct {
	a     any // Int64, legacy
	b     any // VarChar, element-nullable
	tags  any // Array<Array<Int64>>, element-nullable, nullable leaf
	codes any // Array<Array<Int64>>, neither level nullable
	emb   any // FloatVector, element-nullable
	emb2  any // FloatVector, legacy
}

// flagRow is one struct row. A nil entry in elems is a null struct slot; null
// marks a null row.
type flagRow struct {
	null  bool
	elems []*flagElem
}

// arrowFlags are the Arrow nullable flags written for every level of the
// struct column.
type arrowFlags struct {
	column, item                           bool // the list<struct> column and its struct item
	a, b, tags, tagsLeaf, codes, codesLeaf bool // scalar children and nested leaf items
	emb, embItem, emb2, emb2Item           bool // vector children and their component items
}

func allNullableFlags() arrowFlags {
	return arrowFlags{true, true, true, true, true, true, true, true, true, true, true, true}
}

// schemaFlags mirrors the schema: struct column nullable = struct nullable,
// struct item nullable=false, child nullable = element_nullable, vector
// component nullable=false, Array<T> leaf nullable = TypeSchema leaf nullable.
func schemaFlags() arrowFlags {
	return arrowFlags{
		column: false, item: false,
		a: false, b: true, tags: true, tagsLeaf: true, codes: false, codesLeaf: false,
		emb: true, embItem: false, emb2: false, emb2Item: false,
	}
}

// strictFlags is false at every level, also where the schema allows null.
func strictFlags() arrowFlags {
	return arrowFlags{}
}

func flagSchema() *schemapb.CollectionSchema {
	return enStructSchema(
		enScalarSubField(201, "s[a]", schemapb.DataType_Int64, false),
		enScalarSubField(202, "s[b]", schemapb.DataType_VarChar, true),
		enNestedSubField(203, "s[tags]", schemapb.DataType_Int64, true, true),
		enNestedSubField(204, "s[codes]", schemapb.DataType_Int64, false, false),
		enVectorSubField(205, "s[emb]", schemapb.DataType_FloatVector, true),
		enVectorSubField(206, "s[emb2]", schemapb.DataType_FloatVector, false),
	)
}

func flagStructType(f arrowFlags) *arrow.StructType {
	listOf := func(elem arrow.DataType, nullable bool) arrow.DataType {
		return arrow.ListOfField(arrow.Field{Name: "item", Type: elem, Nullable: nullable})
	}
	return arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: f.a},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: f.b},
		arrow.Field{Name: "tags", Type: listOf(arrow.PrimitiveTypes.Int64, f.tagsLeaf), Nullable: f.tags},
		arrow.Field{Name: "codes", Type: listOf(arrow.PrimitiveTypes.Int64, f.codesLeaf), Nullable: f.codes},
		arrow.Field{Name: "emb", Type: listOf(arrow.PrimitiveTypes.Float32, f.embItem), Nullable: f.emb},
		arrow.Field{Name: "emb2", Type: listOf(arrow.PrimitiveTypes.Float32, f.emb2Item), Nullable: f.emb2},
	)
}

func appendInt64List(b *array.ListBuilder, value any) {
	if value == nil {
		b.AppendNull()
		return
	}
	b.Append(true)
	vb := b.ValueBuilder().(*array.Int64Builder)
	for _, item := range value.([]any) {
		if item == nil {
			vb.AppendNull()
		} else {
			vb.Append(item.(int64))
		}
	}
}

func appendFloat32List(b *array.ListBuilder, value any) {
	if value == nil {
		b.AppendNull()
		return
	}
	b.Append(true)
	vb := b.ValueBuilder().(*array.Float32Builder)
	for _, item := range value.([]any) {
		if item == nil {
			vb.AppendNull()
		} else {
			vb.Append(item.(float32))
		}
	}
}

// writeFlagFile writes rows to a Parquet file with the given flags and returns
// its path.
func writeFlagFile(t *testing.T, f arrowFlags, rows []flagRow) string {
	t.Helper()
	mem := memory.NewGoAllocator()
	structType := flagStructType(f)
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: structType, Nullable: f.item}), Nullable: f.column},
	}, nil)

	pkBuilder := array.NewInt64Builder(mem)
	defer pkBuilder.Release()
	lb := array.NewListBuilderWithField(mem, arrow.Field{Name: "item", Type: structType, Nullable: f.item})
	defer lb.Release()
	sb := lb.ValueBuilder().(*array.StructBuilder)
	for i, row := range rows {
		pkBuilder.Append(int64(i))
		if row.null {
			lb.AppendNull()
			continue
		}
		lb.Append(true)
		for _, elem := range row.elems {
			if elem == nil {
				sb.AppendNull()
				continue
			}
			sb.Append(true)
			if elem.a == nil {
				sb.FieldBuilder(0).AppendNull()
			} else {
				sb.FieldBuilder(0).(*array.Int64Builder).Append(elem.a.(int64))
			}
			if elem.b == nil {
				sb.FieldBuilder(1).AppendNull()
			} else {
				sb.FieldBuilder(1).(*array.StringBuilder).Append(elem.b.(string))
			}
			appendInt64List(sb.FieldBuilder(2).(*array.ListBuilder), elem.tags)
			appendInt64List(sb.FieldBuilder(3).(*array.ListBuilder), elem.codes)
			appendFloat32List(sb.FieldBuilder(4).(*array.ListBuilder), elem.emb)
			appendFloat32List(sb.FieldBuilder(5).(*array.ListBuilder), elem.emb2)
		}
	}
	pkArray := pkBuilder.NewArray()
	defer pkArray.Release()
	structArray := lb.NewArray()
	defer structArray.Release()
	record := array.NewRecord(arrowSchema, []arrow.Array{pkArray, structArray}, int64(len(rows)))
	defer record.Release()

	path := fmt.Sprintf("%s/struct_nullable_flags_%d.parquet", t.TempDir(), rand.Int())
	wf, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	require.NoError(t, err)
	fw, err := pqarrow.NewFileWriter(arrowSchema, wf, parquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	require.NoError(t, err)
	require.NoError(t, fw.Write(record))
	require.NoError(t, fw.Close())
	return path
}

// readFlagFile imports a file through the Parquet reader, returning the first
// error from opening or reading it.
func readFlagFile(t *testing.T, schema *schemapb.CollectionSchema, path string) (*storage.InsertData, error) {
	t.Helper()
	ctx := context.Background()
	factory := storage.NewChunkManagerFactory("local", objectstorage.RootPath(testOutputPath))
	cm, err := factory.NewPersistentStorageChunkManager(ctx)
	require.NoError(t, err)
	reader, err := NewReader(ctx, cm, schema, path, 64*1024*1024)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return reader.Read()
}

// l builds a list value; l() is an empty list, not a null one.
func l(values ...any) []any { return append([]any{}, values...) }

// rowsWithNulls carries nulls only at levels the schema allows.
func rowsWithNulls() []flagRow {
	return []flagRow{
		{elems: []*flagElem{
			{a: int64(1), b: "x", tags: l(int64(1), nil), codes: l(int64(5)), emb: l(float32(0.1), float32(0.2)), emb2: l(float32(1), float32(0))},
			{a: int64(2), b: nil, tags: nil, codes: l(int64(6), int64(7)), emb: nil, emb2: l(float32(0), float32(1))},
		}},
		{elems: []*flagElem{
			{a: int64(3), b: "y", tags: l(), codes: l(), emb: l(float32(0.3), float32(0.4)), emb2: l(float32(1), float32(1))},
		}},
		{elems: []*flagElem{}},
	}
}

// rowsWithoutNulls carries no null at any level.
func rowsWithoutNulls() []flagRow {
	rows := rowsWithNulls()
	rows[0].elems[0].tags = l(int64(1), int64(3))
	rows[0].elems[1].b = "z"
	rows[0].elems[1].tags = l(int64(2))
	rows[0].elems[1].emb = l(float32(0.5), float32(0.6))
	return rows
}

func scalarRows(t *testing.T, data *storage.InsertData, fieldID int64) []*schemapb.ScalarField {
	t.Helper()
	fd, ok := data.Data[fieldID].(*storage.ArrayFieldData)
	require.True(t, ok)
	return fd.Data
}

func vectorRows(t *testing.T, data *storage.InsertData, fieldID int64) []*schemapb.VectorField {
	t.Helper()
	fd, ok := data.Data[fieldID].(*storage.VectorArrayFieldData)
	require.True(t, ok)
	return fd.Data
}

func assertRowsWithNulls(t *testing.T, data *storage.InsertData) {
	t.Helper()
	a := scalarRows(t, data, 201)
	require.Len(t, a, 3)
	assert.Equal(t, []int64{1, 2}, a[0].GetLongData().GetData())
	assert.Equal(t, []int64{3}, a[1].GetLongData().GetData())
	assert.Empty(t, a[2].GetLongData().GetData())
	assert.Nil(t, typeutil.GetArrayElementValidData(a[0]))

	b := scalarRows(t, data, 202)
	assert.Equal(t, []string{"x", ""}, b[0].GetStringData().GetData())
	assert.Equal(t, []bool{true, false}, typeutil.GetArrayElementValidData(b[0]))
	assert.Equal(t, []bool{true}, typeutil.GetArrayElementValidData(b[1]))
	assert.Empty(t, typeutil.GetArrayElementValidData(b[2]))

	tags := scalarRows(t, data, 203)
	assert.Equal(t, []bool{true, false}, typeutil.GetArrayElementValidData(tags[0]))
	inner := tags[0].GetArrayData().GetData()
	require.Len(t, inner, 2)
	assert.Equal(t, []int64{1, 0}, inner[0].GetLongData().GetData())
	assert.Equal(t, []bool{true, false}, typeutil.GetArrayElementValidData(inner[0]))
	assert.Empty(t, inner[1].GetLongData().GetData())
	assert.Nil(t, typeutil.GetArrayElementValidData(inner[1]))
	assert.Equal(t, []bool{true}, typeutil.GetArrayElementValidData(tags[1]))

	codes := scalarRows(t, data, 204)
	assert.Nil(t, typeutil.GetArrayElementValidData(codes[0]))
	codeInner := codes[0].GetArrayData().GetData()
	require.Len(t, codeInner, 2)
	assert.Equal(t, []int64{5}, codeInner[0].GetLongData().GetData())
	assert.Equal(t, []int64{6, 7}, codeInner[1].GetLongData().GetData())
	assert.Nil(t, typeutil.GetArrayElementValidData(codeInner[1]))

	emb := vectorRows(t, data, 205)
	assert.Equal(t, []float32{0.1, 0.2}, emb[0].GetFloatVector().GetData())
	assert.Equal(t, []bool{true, false}, typeutil.GetVectorArrayElementValidData(emb[0]))
	assert.Equal(t, []bool{true}, typeutil.GetVectorArrayElementValidData(emb[1]))

	emb2 := vectorRows(t, data, 206)
	assert.Equal(t, []float32{1, 0, 0, 1}, emb2[0].GetFloatVector().GetData())
	assert.Nil(t, typeutil.GetVectorArrayElementValidData(emb2[0]))
}

func assertRowsWithoutNulls(t *testing.T, data *storage.InsertData) {
	t.Helper()
	b := scalarRows(t, data, 202)
	assert.Equal(t, []string{"x", "z"}, b[0].GetStringData().GetData())
	assert.Equal(t, []bool{true, true}, typeutil.GetArrayElementValidData(b[0]))

	tags := scalarRows(t, data, 203)
	assert.Equal(t, []bool{true, true}, typeutil.GetArrayElementValidData(tags[0]))
	inner := tags[0].GetArrayData().GetData()
	assert.Equal(t, []int64{1, 3}, inner[0].GetLongData().GetData())
	assert.Equal(t, []bool{true, true}, typeutil.GetArrayElementValidData(inner[0]))
	assert.Equal(t, []int64{2}, inner[1].GetLongData().GetData())

	emb := vectorRows(t, data, 205)
	assert.Equal(t, []float32{0.1, 0.2, 0.5, 0.6}, emb[0].GetFloatVector().GetData())
	assert.Equal(t, []bool{true, true}, typeutil.GetVectorArrayElementValidData(emb[0]))
}

func TestStructNullableFlags_Accepted(t *testing.T) {
	schema := flagSchema()
	t.Run("every level nullable, nulls where the schema allows", func(t *testing.T) {
		data, err := readFlagFile(t, schema, writeFlagFile(t, allNullableFlags(), rowsWithNulls()))
		require.NoError(t, err)
		assertRowsWithNulls(t, data)
	})
	t.Run("flags mirror the schema, nulls where the schema allows", func(t *testing.T) {
		data, err := readFlagFile(t, schema, writeFlagFile(t, schemaFlags(), rowsWithNulls()))
		require.NoError(t, err)
		assertRowsWithNulls(t, data)
	})
	t.Run("every level nullable, no nulls", func(t *testing.T) {
		data, err := readFlagFile(t, schema, writeFlagFile(t, allNullableFlags(), rowsWithoutNulls()))
		require.NoError(t, err)
		assertRowsWithoutNulls(t, data)
	})
	t.Run("flags stricter than the schema, no nulls", func(t *testing.T) {
		data, err := readFlagFile(t, schema, writeFlagFile(t, strictFlags(), rowsWithoutNulls()))
		require.NoError(t, err)
		assertRowsWithoutNulls(t, data)
	})
	t.Run("nullable struct, every level non-nullable", func(t *testing.T) {
		nullableSchema := flagSchema()
		nullableSchema.StructArrayFields[0].Nullable = true
		for _, sub := range nullableSchema.StructArrayFields[0].Fields {
			sub.Nullable = true
			if sub.GetTypeSchema() != nil {
				sub.TypeSchema.Nullable = true
			}
		}
		data, err := readFlagFile(t, nullableSchema, writeFlagFile(t, strictFlags(), rowsWithoutNulls()))
		require.NoError(t, err)
		assertRowsWithoutNulls(t, data)
	})
}

func TestStructNullableFlags_NullAtNonNullableLevelRejected(t *testing.T) {
	schema := flagSchema()
	cases := []struct {
		name   string
		mutate func(rows []flagRow) []flagRow
		msg    string
	}{
		{"null legacy scalar element", func(rows []flagRow) []flagRow {
			rows[0].elems[0].a = nil
			return rows
		}, "array element is not allowed to be null value for field 's[a]'"},
		{"null inner array of a non-element-nullable nested sub-field", func(rows []flagRow) []flagRow {
			rows[0].elems[1].codes = nil
			return rows
		}, "array element is not allowed to be null value for field 's[codes]'"},
		{"null leaf of a non-nullable nested leaf", func(rows []flagRow) []flagRow {
			rows[0].elems[1].codes = l(int64(6), nil)
			return rows
		}, "nested array leaf is not allowed to be null value for field 's[codes]'"},
		{"null legacy vector", func(rows []flagRow) []flagRow {
			rows[1].elems[0].emb2 = nil
			return rows
		}, "array element is not allowed to be null value for field 's[emb2]'"},
		{"null component of an element-nullable vector", func(rows []flagRow) []flagRow {
			rows[0].elems[0].emb = l(float32(0.1), nil)
			return rows
		}, "array element is not allowed to be null value for field 's[emb]'"},
		{"null struct element", func(rows []flagRow) []flagRow {
			rows[1].elems = append(rows[1].elems, nil)
			return rows
		}, "element 1 of struct field 's' is null"},
		{"null row of a non-nullable struct", func(rows []flagRow) []flagRow {
			rows[2].null = true
			return rows
		}, "is not nullable but the file contains null value"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeFlagFile(t, allNullableFlags(), tc.mutate(rowsWithNulls()))
			_, err := readFlagFile(t, schema, path)
			assert.ErrorIs(t, err, merr.ErrImportFailed)
			assert.ErrorContains(t, err, tc.msg)
		})
	}
}
