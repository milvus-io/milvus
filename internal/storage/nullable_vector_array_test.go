package storage

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func makeFloatVec(dim int, vals ...float32) *schemapb.VectorField {
	return &schemapb.VectorField{
		Dim: int64(dim),
		Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{Data: vals},
		},
	}
}

// --- VectorArrayFieldData basic operations ---

func TestVectorArrayFieldData_NullableAppendAndGetRow(t *testing.T) {
	fd := &VectorArrayFieldData{
		Dim:         4,
		ElementType: schemapb.DataType_FloatVector,
		Data:        make([]*schemapb.VectorField, 0),
		ValidData:   make([]bool, 0),
		Nullable:    true,
	}

	vec0 := makeFloatVec(4, 1, 2, 3, 4)
	vec2 := makeFloatVec(4, 5, 6, 7, 8)

	// Row 0: valid
	require.NoError(t, fd.AppendRow(vec0))
	// Row 1: null
	require.NoError(t, fd.AppendRow(nil))
	// Row 2: valid
	require.NoError(t, fd.AppendRow(vec2))

	assert.Equal(t, 3, fd.RowNum(), "RowNum should count logical rows including nulls")
	assert.Equal(t, 2, len(fd.Data), "Data should only contain valid rows (compact)")
	assert.Equal(t, []bool{true, false, true}, fd.GetValidData())
	assert.True(t, fd.GetNullable())

	// GetRow
	r0 := fd.GetRow(0)
	assert.Equal(t, vec0, r0)

	r1 := fd.GetRow(1)
	assert.Nil(t, r1, "null row should return nil")

	r2 := fd.GetRow(2)
	assert.Equal(t, vec2, r2)
}

func TestVectorArrayFieldData_NonNullable(t *testing.T) {
	fd := &VectorArrayFieldData{
		Dim:         4,
		ElementType: schemapb.DataType_FloatVector,
		Data:        make([]*schemapb.VectorField, 0),
		Nullable:    false,
	}

	vec := makeFloatVec(4, 1, 2, 3, 4)
	require.NoError(t, fd.AppendRow(vec))

	assert.Equal(t, 1, fd.RowNum())
	assert.False(t, fd.GetNullable())
	assert.Nil(t, fd.GetValidData())
	assert.Equal(t, vec, fd.GetRow(0))
}

func TestVectorArrayFieldData_AppendValidDataRows(t *testing.T) {
	fd := &VectorArrayFieldData{
		Dim:         4,
		ElementType: schemapb.DataType_FloatVector,
		Data: []*schemapb.VectorField{
			makeFloatVec(4, 1, 2, 3, 4),
			makeFloatVec(4, 5, 6, 7, 8),
		},
		Nullable: true,
	}

	err := fd.AppendValidDataRows([]bool{true, false, true})
	require.NoError(t, err)

	assert.Equal(t, []bool{true, false, true}, fd.GetValidData())
	// L2PMapping should be built
	assert.Equal(t, 0, fd.L2PMapping.GetPhysicalOffset(0))
	assert.Equal(t, -1, fd.L2PMapping.GetPhysicalOffset(1))
	assert.Equal(t, 1, fd.L2PMapping.GetPhysicalOffset(2))

	// nil rows is a no-op
	err = fd.AppendValidDataRows(nil)
	assert.NoError(t, err)

	// wrong type
	err = fd.AppendValidDataRows("bad")
	assert.Error(t, err)
}

func TestVectorArrayFieldData_GetMemorySize(t *testing.T) {
	fd := &VectorArrayFieldData{
		Dim:         4,
		ElementType: schemapb.DataType_FloatVector,
		Data: []*schemapb.VectorField{
			makeFloatVec(4, 1, 2, 3, 4),
		},
		ValidData: []bool{true, false},
		Nullable:  true,
	}

	size := fd.GetMemorySize()
	assert.Greater(t, size, 0)
}

func TestVectorArrayFieldData_AllNull(t *testing.T) {
	fd := &VectorArrayFieldData{
		Dim:         4,
		ElementType: schemapb.DataType_FloatVector,
		Data:        make([]*schemapb.VectorField, 0),
		ValidData:   make([]bool, 0),
		Nullable:    true,
	}

	// Append 3 null rows
	for i := 0; i < 3; i++ {
		require.NoError(t, fd.AppendRow(nil))
	}

	assert.Equal(t, 3, fd.RowNum())
	assert.Equal(t, 0, len(fd.Data))
	for i := 0; i < 3; i++ {
		assert.Nil(t, fd.GetRow(i))
	}
}

// --- NewFieldData ---

func TestNewFieldData_NullableArrayOfVector(t *testing.T) {
	schema := &schemapb.FieldSchema{
		FieldID:     100,
		Name:        "vec_arr",
		DataType:    schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_FloatVector,
		Nullable:    true,
		TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
	}
	fd, err := NewFieldData(schemapb.DataType_ArrayOfVector, schema, 10)
	require.NoError(t, err)

	vafd, ok := fd.(*VectorArrayFieldData)
	require.True(t, ok)
	assert.True(t, vafd.Nullable)
	assert.NotNil(t, vafd.ValidData)
	assert.Equal(t, int64(4), vafd.Dim)
}

// --- V2 BuildRecord round-trip for nullable ArrayOfVector ---

func TestBuildRecord_NullableArrayOfVector(t *testing.T) {
	dim := 4
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:     100,
				Name:        "vec_array",
				DataType:    schemapb.DataType_ArrayOfVector,
				ElementType: schemapb.DataType_FloatVector,
				Nullable:    true,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: fmt.Sprintf("%d", dim)},
				},
			},
		},
	}

	vec0 := makeFloatVec(dim, 1, 2, 3, 4)
	vec2 := makeFloatVec(dim, 5, 6, 7, 8)

	insertData := &InsertData{
		Data: map[FieldID]FieldData{
			100: &VectorArrayFieldData{
				Data:        []*schemapb.VectorField{vec0, vec2},
				ElementType: schemapb.DataType_FloatVector,
				Dim:         int64(dim),
				ValidData:   []bool{true, false, true},
				Nullable:    true,
				L2PMapping: func() LogicalToPhysicalMapping {
					var m LogicalToPhysicalMapping
					m.Build([]bool{true, false, true}, 0, 3)
					return m
				}(),
			},
		},
	}

	arrowSchema, err := ConvertToArrowSchema(schema, false)
	require.NoError(t, err)

	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer recordBuilder.Release()

	err = BuildRecord(recordBuilder, insertData, schema)
	require.NoError(t, err)

	record := recordBuilder.NewRecord()
	defer record.Release()

	assert.Equal(t, int64(3), record.NumRows())

	// Verify metadata preserved (elementType + dim)
	field := arrowSchema.Field(0)
	assert.True(t, field.HasMetadata())

	elementTypeStr, ok := field.Metadata.GetValue("elementType")
	assert.True(t, ok)
	assert.Equal(t, fmt.Sprintf("%d", int32(schemapb.DataType_FloatVector)), elementTypeStr)

	dimStr, ok := field.Metadata.GetValue("dim")
	assert.True(t, ok)
	assert.Equal(t, fmt.Sprintf("%d", dim), dimStr)

	// Verify null bitmap: row 0=valid, row 1=null, row 2=valid
	col := record.Column(0)
	assert.True(t, col.IsValid(0))
	assert.True(t, col.IsNull(1))
	assert.True(t, col.IsValid(2))
}

// --- V1 storage ban ---

func TestAddFieldDataToPayload_BanNullableArrayOfVector(t *testing.T) {
	data := &VectorArrayFieldData{
		Dim:         4,
		ElementType: schemapb.DataType_FloatVector,
		Data:        []*schemapb.VectorField{makeFloatVec(4, 1, 2, 3, 4)},
		ValidData:   []bool{true, false},
		Nullable:    true,
	}

	w, err := newInsertEventWriter(schemapb.DataType_ArrayOfVector, WithDim(4), WithElementType(schemapb.DataType_FloatVector))
	require.NoError(t, err)
	defer w.Close()

	err = AddFieldDataToPayload(w, schemapb.DataType_ArrayOfVector, data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nullable ArrayOfVector is not supported in V1 storage format")
}

func TestAddInsertData_BanNullableArrayOfVector(t *testing.T) {
	insertData := &InsertData{Data: make(map[FieldID]FieldData)}

	singleData := []*schemapb.VectorField{makeFloatVec(4, 1, 2, 3, 4)}
	validData := []bool{true, false}

	_, err := AddInsertData(
		schemapb.DataType_ArrayOfVector,
		singleData,
		insertData,
		100,
		2,
		nil,
		4,
		validData,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nullable ArrayOfVector is not supported in V1 storage format")
}

func TestAddInsertData_NonNullableArrayOfVector(t *testing.T) {
	insertData := &InsertData{Data: make(map[FieldID]FieldData)}

	singleData := []*schemapb.VectorField{makeFloatVec(4, 1, 2, 3, 4)}

	n, err := AddInsertData(
		schemapb.DataType_ArrayOfVector,
		singleData,
		insertData,
		100,
		1,
		nil,
		4,
		nil,
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	fd := insertData.Data[100].(*VectorArrayFieldData)
	assert.Equal(t, 1, len(fd.Data))
}

// --- mergeVectorArrayField ---

func TestMergeVectorArrayField(t *testing.T) {
	t.Run("nullable merge", func(t *testing.T) {
		data := &InsertData{Data: make(map[FieldID]FieldData)}

		field1 := &VectorArrayFieldData{
			Dim:         4,
			ElementType: schemapb.DataType_FloatVector,
			Data:        []*schemapb.VectorField{makeFloatVec(4, 1, 2, 3, 4)},
			ValidData:   []bool{true, false},
			Nullable:    true,
		}

		field2 := &VectorArrayFieldData{
			Dim:         4,
			ElementType: schemapb.DataType_FloatVector,
			Data:        []*schemapb.VectorField{makeFloatVec(4, 5, 6, 7, 8)},
			ValidData:   []bool{false, true},
			Nullable:    true,
		}

		MergeFieldData(data, 100, field1)
		MergeFieldData(data, 100, field2)

		merged := data.Data[100].(*VectorArrayFieldData)
		assert.Equal(t, 2, len(merged.Data))
		assert.Equal(t, []bool{true, false, false, true}, merged.ValidData)
		assert.True(t, merged.Nullable)

		// L2PMapping
		assert.Equal(t, 0, merged.L2PMapping.GetPhysicalOffset(0))
		assert.Equal(t, -1, merged.L2PMapping.GetPhysicalOffset(1))
		assert.Equal(t, -1, merged.L2PMapping.GetPhysicalOffset(2))
		assert.Equal(t, 1, merged.L2PMapping.GetPhysicalOffset(3))
	})

	t.Run("non-nullable merge", func(t *testing.T) {
		data := &InsertData{Data: make(map[FieldID]FieldData)}

		field := &VectorArrayFieldData{
			Dim:         4,
			ElementType: schemapb.DataType_FloatVector,
			Data:        []*schemapb.VectorField{makeFloatVec(4, 1, 2, 3, 4)},
			Nullable:    false,
		}

		MergeFieldData(data, 100, field)

		merged := data.Data[100].(*VectorArrayFieldData)
		assert.Equal(t, 1, len(merged.Data))
		assert.Nil(t, merged.ValidData)
	})
}

// --- TransferInsertDataToInsertRecord ---

func TestTransferInsertDataToInsertRecord_NullableArrayOfVector(t *testing.T) {
	vec0 := makeFloatVec(4, 1, 2, 3, 4)
	insertData := &InsertData{
		Data: map[FieldID]FieldData{
			100: &VectorArrayFieldData{
				Dim:         4,
				ElementType: schemapb.DataType_FloatVector,
				Data:        []*schemapb.VectorField{vec0},
				ValidData:   []bool{true, false},
				Nullable:    true,
			},
		},
	}

	record, err := TransferInsertDataToInsertRecord(insertData)
	require.NoError(t, err)

	found := false
	for _, fd := range record.FieldsData {
		if fd.FieldId == 100 {
			found = true
			assert.Equal(t, []bool{true, false}, fd.GetValidData())
			assert.NotNil(t, fd.GetVectors().GetVectorArray())
			assert.Equal(t, 1, len(fd.GetVectors().GetVectorArray().GetData()))
		}
	}
	assert.True(t, found)
}
