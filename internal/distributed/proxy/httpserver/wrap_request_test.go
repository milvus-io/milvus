package httpserver

import (
	"encoding/json"
	"testing"

	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/stretchr/testify/assert"
)

func TestFieldData_AsSchemapb(t *testing.T) {
	t.Run("string_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_String,
			Field: []interface{}{"a", "b", "c"},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("string_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_String,
			Field: []interface{}{1, 2, 3},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})

	t.Run("bool_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Bool,
			Field: []interface{}{true, true, false},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("bool_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Bool,
			Field: []interface{}{1, 2, 3},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})

	t.Run("int8_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int8,
			Field: []interface{}{1, 2, 3},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("int8_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int8,
			Field: []interface{}{"a", "b", "c"},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("int32_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int32,
			Field: []interface{}{1, 2, 3},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("int32_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int32,
			Field: []interface{}{"a", "b", "c"},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("int64_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int64,
			Field: []interface{}{1, 2, 3},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("int64_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Int64,
			Field: []interface{}{"a", "b", "c"},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("float_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Float,
			Field: []interface{}{1.1, 2.1, 3.1},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("float_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Float,
			Field: []interface{}{"a", "b", "c"},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("double_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Double,
			Field: []interface{}{1.1, 2.1, 3.1},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("double_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_Double,
			Field: []interface{}{"a", "b", "c"},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("varchar_not_support", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_VarChar,
			Field: []interface{}{"a", "b", "c"},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})

	// vectors

	t.Run("floatvector_ok", func(t *testing.T) {
		fieldData := FieldData{
			Type: schemapb.DataType_FloatVector,
			Field: []interface{}{
				[]float32{1.1, 2.2, 3.1},
				[]float32{1.1, 2.2, 3.1},
				[]float32{1.1, 2.2, 3.1},
			},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.NoError(t, err)
	})
	t.Run("floatvector_empty_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_FloatVector,
			Field: []interface{}{},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("floatvector_dim=0_error", func(t *testing.T) {
		fieldData := FieldData{
			Type: schemapb.DataType_FloatVector,
			Field: []interface{}{
				[]float32{},
			},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("floatvector_vectorTypeError_error", func(t *testing.T) {
		fieldData := FieldData{
			Type: schemapb.DataType_FloatVector,
			Field: []interface{}{
				[]string{"1"},
			},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
	t.Run("floatvector_error", func(t *testing.T) {
		fieldData := FieldData{
			Type:  schemapb.DataType_FloatVector,
			Field: []interface{}{"a", "b", "c"},
		}
		raw, _ := json.Marshal(fieldData)
		json.Unmarshal(raw, &fieldData)
		_, err := fieldData.AsSchemapb()
		assert.Error(t, err)
	})
}

func Test_vector2Bytes(t *testing.T) {
	ret := vector2Bytes([][]float32{{1.1, 1.2}})
	assert.NotEmpty(t, ret)
}

func Test_binaryVector2Bytes(t *testing.T) {
	ret := binaryVector2Bytes([][]byte{
		[]byte("somebytes"),
	})
	assert.NotEmpty(t, ret)
}

func TestVectorsArray_AsPbVectorArray(t *testing.T) {
	dim := int64(1)
	t.Run("vector_ok", func(t *testing.T) {
		vector := []float32{1, 2}
		v := VectorsArray{
			Dim:     dim,
			Vectors: vector,
		}
		ret := v.AsPbVectorArray()
		da, ok := ret.Array.(*milvuspb.VectorsArray_DataArray)
		assert.True(t, ok)
		assert.Equal(t, dim, da.DataArray.Dim)
		assert.Equal(t, vector, da.DataArray.GetFloatVector().Data)
	})
	t.Run("binary_vector_ok", func(t *testing.T) {
		bv := []byte("somebytes")
		v := VectorsArray{
			// IDs: ,
			Dim:           dim,
			BinaryVectors: bv,
		}
		ret := v.AsPbVectorArray()
		da, ok := ret.Array.(*milvuspb.VectorsArray_DataArray)
		assert.True(t, ok)
		assert.Equal(t, dim, da.DataArray.Dim)
		assert.Equal(t, bv, da.DataArray.GetBinaryVector())
	})
	t.Run("ids_ok", func(t *testing.T) {
		ids := []int64{1, 2, 3}
		cn := "collection"
		paritions := []string{"p1", "p2"}
		field := "field"
		v := VectorsArray{
			IDs: &VectorIDs{
				CollectionName: cn,
				PartitionNames: paritions,
				FieldName:      field,
				IDArray:        ids,
			},
		}
		ret := v.AsPbVectorArray()
		ia, ok := ret.Array.(*milvuspb.VectorsArray_IdArray)
		assert.True(t, ok)
		assert.Equal(t, cn, ia.IdArray.CollectionName)
		assert.Equal(t, paritions, ia.IdArray.PartitionNames)
		assert.Equal(t, field, ia.IdArray.FieldName)
		ints, ok := ia.IdArray.IdArray.IdField.(*schemapb.IDs_IntId)
		assert.True(t, ok)
		assert.Equal(t, ids, ints.IntId.Data)
	})
}
