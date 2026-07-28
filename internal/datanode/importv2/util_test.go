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

package importv2

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func Test_AppendSystemFieldsData(t *testing.T) {
	const count = 100

	pkField := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "pk",
		IsPrimaryKey: true,
		AutoID:       true,
	}
	vecField := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "4",
			},
		},
	}
	int64Field := &schemapb.FieldSchema{
		FieldID:  102,
		Name:     "int64",
		DataType: schemapb.DataType_Int64,
	}

	schema := &schemapb.CollectionSchema{}
	task := &ImportTask{
		req: &datapb.ImportRequest{
			Ts:     1000,
			Schema: schema,
		},
		allocator: allocator.NewLocalAllocator(0, count*2),
	}

	pkField.DataType = schemapb.DataType_Int64
	schema.Fields = []*schemapb.FieldSchema{pkField, vecField, int64Field}
	insertData, err := testutil.CreateInsertData(schema, count)
	assert.NoError(t, err)
	assert.Equal(t, 0, insertData.Data[pkField.GetFieldID()].RowNum())
	assert.Nil(t, insertData.Data[common.RowIDField])
	assert.Nil(t, insertData.Data[common.TimeStampField])
	rowNum, _ := GetInsertDataRowCount(insertData, task.GetSchema())
	err = AppendSystemFieldsData(task, insertData, rowNum)
	assert.NoError(t, err)
	assert.Equal(t, count, insertData.Data[pkField.GetFieldID()].RowNum())
	assert.Equal(t, count, insertData.Data[common.RowIDField].RowNum())
	assert.Equal(t, count, insertData.Data[common.TimeStampField].RowNum())

	pkField.DataType = schemapb.DataType_VarChar
	schema.Fields = []*schemapb.FieldSchema{pkField, vecField, int64Field}
	insertData, err = testutil.CreateInsertData(schema, count)
	assert.NoError(t, err)
	assert.Equal(t, 0, insertData.Data[pkField.GetFieldID()].RowNum())
	assert.Nil(t, insertData.Data[common.RowIDField])
	assert.Nil(t, insertData.Data[common.TimeStampField])
	rowNum, _ = GetInsertDataRowCount(insertData, task.GetSchema())
	err = AppendSystemFieldsData(task, insertData, rowNum)
	assert.NoError(t, err)
	assert.Equal(t, count, insertData.Data[pkField.GetFieldID()].RowNum())
	assert.Equal(t, count, insertData.Data[common.RowIDField].RowNum())
	assert.Equal(t, count, insertData.Data[common.TimeStampField].RowNum())
}

func Test_AppendSystemFieldsData_AllowInsertAutoID_KeepUserPK(t *testing.T) {
	const count = 10

	pkField := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
		AutoID:       true,
	}
	vecField := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "4"},
		},
	}

	schema := &schemapb.CollectionSchema{}
	schema.Fields = []*schemapb.FieldSchema{pkField, vecField}
	schema.Properties = []*commonpb.KeyValuePair{{Key: common.AllowInsertAutoIDKey, Value: "true"}}

	task := &ImportTask{
		req:       &datapb.ImportRequest{Ts: 1000, Schema: schema},
		allocator: allocator.NewLocalAllocator(0, count*2),
	}

	insertData, err := testutil.CreateInsertData(schema, count)
	assert.NoError(t, err)

	userPK := make([]int64, count)
	for i := 0; i < count; i++ {
		userPK[i] = 1000 + int64(i)
	}
	insertData.Data[pkField.GetFieldID()] = &storage.Int64FieldData{Data: userPK}

	rowNum, _ := GetInsertDataRowCount(insertData, task.GetSchema())
	err = AppendSystemFieldsData(task, insertData, rowNum)
	assert.NoError(t, err)

	got := insertData.Data[pkField.GetFieldID()].(*storage.Int64FieldData)
	assert.Equal(t, count, got.RowNum())
	for i := 0; i < count; i++ {
		assert.Equal(t, userPK[i], got.Data[i])
	}
}

func Test_UnsetAutoID(t *testing.T) {
	pkField := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
		AutoID:       true,
	}
	vecField := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
	}

	schema := &schemapb.CollectionSchema{}
	schema.Fields = []*schemapb.FieldSchema{pkField, vecField}
	UnsetAutoID(schema)
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() {
			assert.False(t, schema.GetFields()[0].GetAutoID())
		}
	}
}

func Test_PickSegment(t *testing.T) {
	const (
		vchannel    = "ch-0"
		partitionID = 10
	)
	task := &ImportTask{
		req: &datapb.ImportRequest{
			RequestSegments: []*datapb.ImportRequestSegment{
				{
					SegmentID:   100,
					PartitionID: partitionID,
					Vchannel:    vchannel,
				},
				{
					SegmentID:   101,
					PartitionID: partitionID,
					Vchannel:    vchannel,
				},
				{
					SegmentID:   102,
					PartitionID: partitionID,
					Vchannel:    vchannel,
				},
				{
					SegmentID:   103,
					PartitionID: partitionID,
					Vchannel:    vchannel,
				},
			},
		},
	}

	importedSize := map[int64]int{}

	totalSize := 8 * 1024 * 1024 * 1024
	batchSize := 1 * 1024 * 1024

	for totalSize > 0 {
		picked, err := PickSegment(task.req.GetRequestSegments(), vchannel, partitionID)
		assert.NoError(t, err)
		importedSize[picked] += batchSize
		totalSize -= batchSize
	}
	expectSize := 2 * 1024 * 1024 * 1024
	fn := func(actual int) {
		t.Logf("actual=%d, expect*0.8=%f, expect*1.2=%f", actual, float64(expectSize)*0.9, float64(expectSize)*1.1)
		assert.True(t, float64(actual) > float64(expectSize)*0.8)
		assert.True(t, float64(actual) < float64(expectSize)*1.2)
	}
	fn(importedSize[int64(100)])
	fn(importedSize[int64(101)])
	fn(importedSize[int64(102)])
	fn(importedSize[int64(103)])

	// test no candidate segments found
	_, err := PickSegment(task.req.GetRequestSegments(), "ch-2", 20)
	assert.Error(t, err)
}

func Test_CheckRowsEqual(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
				AutoID:       true,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "4",
					},
				},
			},
			{
				FieldID:  102,
				Name:     "flag",
				DataType: schemapb.DataType_Double,
				Nullable: true,
			},
			{
				FieldID:   103,
				Name:      "dynamic",
				DataType:  schemapb.DataType_JSON,
				IsDynamic: true,
			},
			{
				FieldID:          104,
				Name:             "functionOutput",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
			},
		},
	}

	// empty insertData
	insertData := &storage.InsertData{
		Data: make(map[int64]storage.FieldData),
	}
	err := CheckRowsEqual(schema, insertData)
	assert.NoError(t, err)

	insertData, err = storage.NewInsertData(schema)
	assert.NoError(t, err)
	err = CheckRowsEqual(schema, insertData)
	assert.NoError(t, err)

	// row not equal
	insertData, err = testutil.CreateInsertData(schema, 10)
	assert.NoError(t, err)

	newField := &schemapb.FieldSchema{
		FieldID:  200,
		Name:     "new",
		DataType: schemapb.DataType_Bool,
	}
	schema.Fields = append(schema.Fields, newField)
	insertData.Data[newField.GetFieldID()], _ = storage.NewFieldData(newField.GetDataType(), newField, 1)
	err = CheckRowsEqual(schema, insertData)
	assert.Error(t, err)

	// row equal
	insertData, err = testutil.CreateInsertData(schema, 10)
	assert.NoError(t, err)
	err = CheckRowsEqual(schema, insertData)
	assert.NoError(t, err)
}

func Test_CheckStructArrayConsistency(t *testing.T) {
	const (
		structName = "struct_field"
		intSubID   = int64(111)
		strSubID   = int64(112)
		vecSubID   = int64(113)
		dim        = 2
	)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID:  110,
				Name:     structName,
				Nullable: true,
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:     intSubID,
						Name:        typeutil.ConcatStructFieldName(structName, "sub_int"),
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_Int64,
						Nullable:    true,
					},
					{
						FieldID:     strSubID,
						Name:        typeutil.ConcatStructFieldName(structName, "sub_str"),
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_VarChar,
						Nullable:    true,
					},
					{
						FieldID:     vecSubID,
						Name:        typeutil.ConcatStructFieldName(structName, "sub_vec"),
						DataType:    schemapb.DataType_ArrayOfVector,
						ElementType: schemapb.DataType_FloatVector,
						Nullable:    true,
						TypeParams: []*commonpb.KeyValuePair{
							{Key: common.DimKey, Value: fmt.Sprintf("%d", dim)},
						},
					},
				},
			},
		},
	}

	longRow := func(vals ...int64) *schemapb.ScalarField {
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: vals},
			},
		}
	}
	strRow := func(vals ...string) *schemapb.ScalarField {
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{Data: vals},
			},
		}
	}
	vecRow := func(numVectors int) *schemapb.VectorField {
		return &schemapb.VectorField{
			Dim: int64(dim),
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: make([]float32, numVectors*dim)},
			},
		}
	}
	// 3 rows: row 0 has 2 struct elements, row 1 is null, row 2 has 1 element
	buildConsistentData := func() *storage.InsertData {
		return &storage.InsertData{
			Data: map[int64]storage.FieldData{
				common.RowIDField: &storage.Int64FieldData{Data: []int64{1, 2, 3}},
				intSubID: &storage.ArrayFieldData{
					ElementType: schemapb.DataType_Int64,
					Data:        []*schemapb.ScalarField{longRow(1, 2), nil, longRow(3)},
					ValidData:   []bool{true, false, true},
					Nullable:    true,
				},
				strSubID: &storage.ArrayFieldData{
					ElementType: schemapb.DataType_VarChar,
					Data:        []*schemapb.ScalarField{strRow("a", "b"), nil, strRow("c")},
					ValidData:   []bool{true, false, true},
					Nullable:    true,
				},
				vecSubID: &storage.VectorArrayFieldData{
					Dim:         dim,
					ElementType: schemapb.DataType_FloatVector,
					Data:        []*schemapb.VectorField{vecRow(2), vecRow(0), vecRow(1)},
					ValidData:   []bool{true, false, true},
					Nullable:    true,
				},
			},
		}
	}

	t.Run("consistent struct data", func(t *testing.T) {
		err := CheckStructArrayConsistency(schema, buildConsistentData())
		assert.NoError(t, err)
	})

	t.Run("typed empty arrays are valid", func(t *testing.T) {
		insertData := buildConsistentData()
		insertData.Data[intSubID].(*storage.ArrayFieldData).Data[0] = longRow()
		insertData.Data[strSubID].(*storage.ArrayFieldData).Data[0] = strRow()
		insertData.Data[vecSubID].(*storage.VectorArrayFieldData).Data[0] = vecRow(0)
		err := CheckStructArrayConsistency(schema, insertData)
		assert.NoError(t, err)
	})

	t.Run("no struct fields in schema", func(t *testing.T) {
		plainSchema := &schemapb.CollectionSchema{
			Fields: schema.GetFields(),
		}
		err := CheckStructArrayConsistency(plainSchema, buildConsistentData())
		assert.NoError(t, err)
	})

	t.Run("struct columns absent from data", func(t *testing.T) {
		insertData := buildConsistentData()
		delete(insertData.Data, intSubID)
		delete(insertData.Data, strSubID)
		delete(insertData.Data, vecSubID)
		err := CheckStructArrayConsistency(schema, insertData)
		assert.NoError(t, err)
	})

	t.Run("zero-row struct columns are absent", func(t *testing.T) {
		insertData := buildConsistentData()
		intData := insertData.Data[intSubID].(*storage.ArrayFieldData)
		intData.Data, intData.ValidData = nil, nil
		strData := insertData.Data[strSubID].(*storage.ArrayFieldData)
		strData.Data, strData.ValidData = nil, nil
		vecData := insertData.Data[vecSubID].(*storage.VectorArrayFieldData)
		vecData.Data, vecData.ValidData = nil, nil
		err := CheckStructArrayConsistency(schema, insertData)
		assert.NoError(t, err)
	})

	t.Run("diverging scalar element count", func(t *testing.T) {
		insertData := buildConsistentData()
		// row 2: sub_str has 2 elements while sub_int has 1
		insertData.Data[strSubID].(*storage.ArrayFieldData).Data[2] = strRow("c", "d")
		err := CheckStructArrayConsistency(schema, insertData)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.Contains(t, err.Error(), "row 2")
		assert.Contains(t, err.Error(), structName)
		assert.Contains(t, err.Error(), typeutil.ConcatStructFieldName(structName, "sub_int"))
		assert.Contains(t, err.Error(), typeutil.ConcatStructFieldName(structName, "sub_str"))
	})

	t.Run("diverging vector array element count", func(t *testing.T) {
		insertData := buildConsistentData()
		// row 0: sub_vec has 3 vectors while scalar sub-fields have 2 elements
		insertData.Data[vecSubID].(*storage.VectorArrayFieldData).Data[0] = vecRow(3)
		err := CheckStructArrayConsistency(schema, insertData)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.Contains(t, err.Error(), "row 0")
		assert.Contains(t, err.Error(), typeutil.ConcatStructFieldName(structName, "sub_vec"))
	})

	t.Run("diverging valid data", func(t *testing.T) {
		insertData := buildConsistentData()
		// row 1: sub_str claims valid while the siblings claim null
		strData := insertData.Data[strSubID].(*storage.ArrayFieldData)
		strData.Data[1] = strRow()
		strData.ValidData[1] = true
		err := CheckStructArrayConsistency(schema, insertData)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.Contains(t, err.Error(), "row 1")
		assert.Contains(t, err.Error(), "null-ness")
	})

	t.Run("invalid nullable valid data length", func(t *testing.T) {
		tests := []struct {
			name      string
			fieldName string
			mutate    func(*storage.InsertData)
		}{
			{
				name:      "empty scalar mask",
				fieldName: typeutil.ConcatStructFieldName(structName, "sub_int"),
				mutate: func(insertData *storage.InsertData) {
					fieldData := insertData.Data[intSubID].(*storage.ArrayFieldData)
					fieldData.ValidData = nil
				},
			},
			{
				name:      "long vector mask",
				fieldName: typeutil.ConcatStructFieldName(structName, "sub_vec"),
				mutate: func(insertData *storage.InsertData) {
					fieldData := insertData.Data[vecSubID].(*storage.VectorArrayFieldData)
					fieldData.ValidData = append(fieldData.ValidData, true)
				},
			},
			{
				name:      "non-empty mask for zero-row column",
				fieldName: typeutil.ConcatStructFieldName(structName, "sub_str"),
				mutate: func(insertData *storage.InsertData) {
					fieldData := insertData.Data[strSubID].(*storage.ArrayFieldData)
					fieldData.Data = nil
					fieldData.ValidData = []bool{true}
				},
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				insertData := buildConsistentData()
				test.mutate(insertData)
				err := CheckStructArrayConsistency(schema, insertData)
				assert.Error(t, err)
				assert.ErrorIs(t, err, merr.ErrImportSysFailed)
				assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
				assert.Contains(t, err.Error(), "ValidData length")
				assert.Contains(t, err.Error(), test.fieldName)
			})
		}
	})

	t.Run("single sub-field still validates nullable mask", func(t *testing.T) {
		singleFieldSchema := &schemapb.CollectionSchema{
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID:  110,
					Name:     structName,
					Nullable: true,
					Fields:   []*schemapb.FieldSchema{schema.GetStructArrayFields()[0].GetFields()[0]},
				},
			},
		}
		insertData := &storage.InsertData{
			Data: map[int64]storage.FieldData{
				intSubID: &storage.ArrayFieldData{
					ElementType: schemapb.DataType_Int64,
					Data:        []*schemapb.ScalarField{longRow(1)},
					Nullable:    true,
				},
			},
		}
		err := CheckStructArrayConsistency(singleFieldSchema, insertData)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrImportSysFailed)
		assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
		assert.Contains(t, err.Error(), "ValidData length")
	})

	t.Run("invalid scalar array payload", func(t *testing.T) {
		tests := []struct {
			name string
			row  *schemapb.ScalarField
		}{
			{name: "nil payload", row: nil},
			{name: "unset payload", row: &schemapb.ScalarField{}},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				insertData := buildConsistentData()
				insertData.Data[intSubID].(*storage.ArrayFieldData).Data[0] = test.row
				err := CheckStructArrayConsistency(schema, insertData)
				assert.Error(t, err)
				assert.ErrorIs(t, err, merr.ErrImportFailed)
				assert.Equal(t, merr.InputError, merr.GetErrorType(err))
				assert.Contains(t, err.Error(), "row 0")
				assert.Contains(t, err.Error(), typeutil.ConcatStructFieldName(structName, "sub_int"))
				assert.Contains(t, err.Error(), "missing or unsupported scalar payload")
			})
		}
	})

	t.Run("misaligned sub-field row count", func(t *testing.T) {
		insertData := buildConsistentData()
		intData := insertData.Data[intSubID].(*storage.ArrayFieldData)
		intData.Data = intData.Data[:2]
		intData.ValidData = intData.ValidData[:2]
		err := CheckStructArrayConsistency(schema, insertData)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.Contains(t, err.Error(), "misaligned")
	})

	t.Run("partial sub-field set: one present, others absent", func(t *testing.T) {
		// Only sub_int is supplied; sub_str/sub_vec would be backfilled as
		// all-NULL, so the present column's real elements would diverge from
		// the NULL columns. Must be rejected, not silently accepted.
		insertData := buildConsistentData()
		delete(insertData.Data, strSubID)
		delete(insertData.Data, vecSubID)
		err := CheckStructArrayConsistency(schema, insertData)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.Contains(t, err.Error(), "partial sub-field set")
	})

	t.Run("partial sub-field set: one present, one zero-row", func(t *testing.T) {
		insertData := buildConsistentData()
		delete(insertData.Data, vecSubID)
		// sub_str present but empty (zero rows) -> treated as absent -> partial
		insertData.Data[strSubID] = &storage.ArrayFieldData{
			ElementType: schemapb.DataType_VarChar,
			Data:        []*schemapb.ScalarField{},
			ValidData:   []bool{},
			Nullable:    true,
		}
		err := CheckStructArrayConsistency(schema, insertData)
		assert.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.Contains(t, err.Error(), "partial sub-field set")
	})
}

func Test_AppendNullableDefaultFieldsData(t *testing.T) {
	autoIDField := int64(100)
	dynamicFieldID := int64(102)
	functionFieldID := int64(103)
	buildSchemaFn := func() *schemapb.CollectionSchema {
		return &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      autoIDField,
					Name:         "pk",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
					AutoID:       true,
				},
				{
					FieldID:  101,
					Name:     "vec",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "4",
						},
					},
				},
				{
					FieldID:   dynamicFieldID,
					Name:      "dynamic",
					DataType:  schemapb.DataType_JSON,
					IsDynamic: true,
				},
				{
					FieldID:          functionFieldID,
					Name:             "functionOutput",
					DataType:         schemapb.DataType_SparseFloatVector,
					IsFunctionOutput: true,
				},
			},
		}
	}

	const count = 10
	tests := []struct {
		name       string
		fieldID    int64
		dataType   schemapb.DataType
		nullable   bool
		defaultVal *schemapb.ValueField
	}{
		// nullable tests
		{
			name:     "bool is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_Bool,
			nullable: true,
		},
		{
			name:     "int8 is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_Int8,
			nullable: true,
		},
		{
			name:     "int16 is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_Int16,
			nullable: true,
		},
		{
			name:     "int32 is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_Int32,
			nullable: true,
		},
		{
			name:       "int64 is nullable",
			fieldID:    200,
			dataType:   schemapb.DataType_Int64,
			nullable:   true,
			defaultVal: nil,
		},
		{
			name:     "float is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_Float,
			nullable: true,
		},
		{
			name:     "double is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_Double,
			nullable: true,
		},
		{
			name:     "varchar is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_VarChar,
			nullable: true,
		},
		{
			name:     "json is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_JSON,
			nullable: true,
		},
		{
			name:     "array is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_Array,
			nullable: true,
		},

		// default value tests
		{
			name:     "bool is default",
			fieldID:  200,
			dataType: schemapb.DataType_Bool,
			defaultVal: &schemapb.ValueField{
				Data: &schemapb.ValueField_BoolData{
					BoolData: true,
				},
			},
		},
		{
			name:     "int8 is default",
			fieldID:  200,
			dataType: schemapb.DataType_Int8,
			defaultVal: &schemapb.ValueField{
				Data: &schemapb.ValueField_IntData{
					IntData: 99,
				},
			},
		},
		{
			name:     "int16 is default",
			fieldID:  200,
			dataType: schemapb.DataType_Int16,
			defaultVal: &schemapb.ValueField{
				Data: &schemapb.ValueField_IntData{
					IntData: 99,
				},
			},
		},
		{
			name:     "int32 is default",
			fieldID:  200,
			dataType: schemapb.DataType_Int32,
			defaultVal: &schemapb.ValueField{
				Data: &schemapb.ValueField_IntData{
					IntData: 99,
				},
			},
		},
		{
			name:     "int64 is default",
			fieldID:  200,
			dataType: schemapb.DataType_Int64,
			nullable: true,
			defaultVal: &schemapb.ValueField{
				Data: &schemapb.ValueField_LongData{
					LongData: 99,
				},
			},
		},
		{
			name:     "float is default",
			fieldID:  200,
			dataType: schemapb.DataType_Float,
			defaultVal: &schemapb.ValueField{
				Data: &schemapb.ValueField_FloatData{
					FloatData: 99.99,
				},
			},
		},
		{
			name:     "double is default",
			fieldID:  200,
			dataType: schemapb.DataType_Double,
			defaultVal: &schemapb.ValueField{
				Data: &schemapb.ValueField_DoubleData{
					DoubleData: 99.99,
				},
			},
		},
		{
			name:     "varchar is default",
			fieldID:  200,
			dataType: schemapb.DataType_VarChar,
			defaultVal: &schemapb.ValueField{
				Data: &schemapb.ValueField_StringData{
					StringData: "hello world",
				},
			},
		},
		{
			name:     "float vector is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_FloatVector,
			nullable: true,
		},
		{
			name:     "float16 vector is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_Float16Vector,
			nullable: true,
		},
		{
			name:     "bfloat16 vector is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_BFloat16Vector,
			nullable: true,
		},
		{
			name:     "binary vector is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_BinaryVector,
			nullable: true,
		},
		{
			name:     "sparse float vector is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_SparseFloatVector,
			nullable: true,
		},
		{
			name:     "int8 vector is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_Int8Vector,
			nullable: true,
		},
		{
			name:     "array of vector is nullable",
			fieldID:  200,
			dataType: schemapb.DataType_ArrayOfVector,
			nullable: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := buildSchemaFn()
			isVectorType := tt.dataType == schemapb.DataType_FloatVector ||
				tt.dataType == schemapb.DataType_Float16Vector ||
				tt.dataType == schemapb.DataType_BFloat16Vector ||
				tt.dataType == schemapb.DataType_BinaryVector ||
				tt.dataType == schemapb.DataType_SparseFloatVector ||
				tt.dataType == schemapb.DataType_Int8Vector
			fieldSchema := &schemapb.FieldSchema{
				FieldID:      tt.fieldID,
				Name:         fmt.Sprintf("field_%d", tt.fieldID),
				DataType:     tt.dataType,
				Nullable:     tt.nullable,
				DefaultValue: tt.defaultVal,
			}
			if tt.dataType == schemapb.DataType_Array {
				fieldSchema.ElementType = schemapb.DataType_Int64
				fieldSchema.TypeParams = append(fieldSchema.TypeParams, &commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "100"})
			} else if tt.dataType == schemapb.DataType_ArrayOfVector {
				fieldSchema.ElementType = schemapb.DataType_FloatVector
				fieldSchema.TypeParams = append(fieldSchema.TypeParams,
					&commonpb.KeyValuePair{Key: common.DimKey, Value: "8"},
					&commonpb.KeyValuePair{Key: common.MaxCapacityKey, Value: "100"})
			} else if tt.dataType == schemapb.DataType_VarChar {
				fieldSchema.TypeParams = append(fieldSchema.TypeParams, &commonpb.KeyValuePair{Key: common.MaxLengthKey, Value: "100"})
			} else if isVectorType && tt.dataType != schemapb.DataType_SparseFloatVector {
				fieldSchema.TypeParams = append(fieldSchema.TypeParams, &commonpb.KeyValuePair{Key: common.DimKey, Value: "8"})
			}

			// create data without the new field
			insertData, err := testutil.CreateInsertData(schema, count, 100)
			assert.NoError(t, err)

			// add new nullalbe/default field to the schema
			schema.Fields = append(schema.Fields, fieldSchema)

			// prepare a one-row data
			tempSchema := &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{fieldSchema},
			}
			tempData, err := testutil.CreateInsertData(tempSchema, 1, 100)
			assert.NoError(t, err)
			insertData.Data[fieldSchema.GetFieldID()] = tempData.Data[fieldSchema.GetFieldID()]

			// the new field row count is 1, not equal to others
			err = AppendNullableDefaultFieldsData(schema, insertData, count)
			assert.Error(t, err)

			// the new field data is empty, it will be filled by AppendNullableDefaultFieldsData
			insertData.Data[fieldSchema.GetFieldID()], err = storage.NewFieldData(fieldSchema.GetDataType(), fieldSchema, 0)
			assert.NoError(t, err)
			err = AppendNullableDefaultFieldsData(schema, insertData, count)
			assert.NoError(t, err)

			for fieldID, fieldData := range insertData.Data {
				// testutil.CreateInsertData dont create data for autoid, function output fields
				// AppendNullableDefaultFieldsData doesn't fill autoid, dynamic and function output fields
				if fieldID == autoIDField || fieldID == functionFieldID {
					assert.Equal(t, 0, fieldData.RowNum())
				} else {
					assert.Equal(t, count, fieldData.RowNum())
				}
				if fieldID != tt.fieldID {
					continue
				}

				if tt.nullable {
					assert.True(t, fieldData.GetNullable())
				}

				if tt.defaultVal != nil {
					switch tt.dataType {
					case schemapb.DataType_Bool:
						tempFieldData := fieldData.(*storage.BoolFieldData)
						for _, v := range tempFieldData.Data {
							assert.True(t, v)
						}
					case schemapb.DataType_Int8:
						tempFieldData := fieldData.(*storage.Int8FieldData)
						for _, v := range tempFieldData.Data {
							assert.Equal(t, int8(99), v)
						}
					case schemapb.DataType_Int16:
						tempFieldData := fieldData.(*storage.Int16FieldData)
						for _, v := range tempFieldData.Data {
							assert.Equal(t, int16(99), v)
						}
					case schemapb.DataType_Int32:
						tempFieldData := fieldData.(*storage.Int32FieldData)
						for _, v := range tempFieldData.Data {
							assert.Equal(t, int32(99), v)
						}
					case schemapb.DataType_Int64:
						tempFieldData := fieldData.(*storage.Int64FieldData)
						for _, v := range tempFieldData.Data {
							assert.Equal(t, int64(99), v)
						}
					case schemapb.DataType_Float:
						tempFieldData := fieldData.(*storage.FloatFieldData)
						for _, v := range tempFieldData.Data {
							assert.Equal(t, float32(99.99), v)
						}
					case schemapb.DataType_Double:
						tempFieldData := fieldData.(*storage.DoubleFieldData)
						for _, v := range tempFieldData.Data {
							assert.Equal(t, float64(99.99), v)
						}
					case schemapb.DataType_VarChar:
						tempFieldData := fieldData.(*storage.StringFieldData)
						for _, v := range tempFieldData.Data {
							assert.Equal(t, "hello world", v)
						}
					default:
					}
				} else if tt.nullable {
					for i := 0; i < count; i++ {
						assert.Nil(t, fieldData.GetRow(i))
					}
				}
			}
		})
	}
}

func TestUtil_FillDynamicData(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		EnableDynamicField: false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  100,
				Name:     "pk",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  1010,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "16",
					},
				},
			},
		},
	}

	// prepare 10 rows data
	count := 10
	insertData, err := testutil.CreateInsertData(schema, count)
	assert.NoError(t, err)

	// EnableDynamicField is false, do nothing
	err = FillDynamicData(schema, insertData, count)
	assert.NoError(t, err)

	// enable_dynamic_field is true but the dynamic field doesn't exist
	schema.EnableDynamicField = true
	err = FillDynamicData(schema, insertData, count)
	assert.Error(t, err)

	// add a dynamic field
	dynamicFieldID := int64(200)
	dynamicField := &schemapb.FieldSchema{
		FieldID:   dynamicFieldID,
		Name:      "dynamic",
		DataType:  schemapb.DataType_JSON,
		IsDynamic: true,
	}
	schema.Fields = append(schema.Fields, dynamicField)

	// the dynamic field has one row, which is illegal
	insertData.Data[dynamicFieldID], err = storage.NewFieldData(dynamicField.DataType, dynamicField, count)
	assert.NoError(t, err)
	err = insertData.Data[dynamicFieldID].AppendRow([]byte("{}"))
	assert.NoError(t, err)
	err = FillDynamicData(schema, insertData, count)
	assert.Error(t, err)

	// the dynamic field is empty, dynamic data is filled
	insertData.Data[dynamicFieldID], err = storage.NewFieldData(dynamicField.DataType, dynamicField, count)
	assert.NoError(t, err)
	err = FillDynamicData(schema, insertData, count)
	assert.NoError(t, err)
	assert.Equal(t, count, insertData.Data[dynamicFieldID].RowNum())

	// the dynamic field is already filled, do nothing
	err = FillDynamicData(schema, insertData, count)
	assert.NoError(t, err)
	assert.Equal(t, count, insertData.Data[dynamicFieldID].RowNum())
}
