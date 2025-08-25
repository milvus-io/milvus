package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestPayloadWriter_Failed(t *testing.T) {
	t.Run("wrong input", func(t *testing.T) {
		_, err := NewPayloadWriter(schemapb.DataType_FloatVector)
		require.Error(t, err)
	})
	t.Run("Test Bool", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{}, nil)
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddBoolToPayload([]bool{false}, nil)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{false}, nil)
		require.Error(t, err)
	})

	t.Run("Test Byte", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int8, WithNullable(Params.CommonCfg.MaxBloomFalsePositive.PanicIfEmpty))
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddByteToPayload([]byte{}, nil)
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddByteToPayload([]byte{0}, nil)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddByteToPayload([]byte{0}, nil)
		require.Error(t, err)
	})

	t.Run("Test Int8", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int8)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt8ToPayload([]int8{}, nil)
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddInt8ToPayload([]int8{0}, nil)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt8ToPayload([]int8{0}, nil)
		require.Error(t, err)
	})

	t.Run("Test Int16", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int16)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt16ToPayload([]int16{}, nil)
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddInt16ToPayload([]int16{0}, nil)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt16ToPayload([]int16{0}, nil)
		require.Error(t, err)
	})

	t.Run("Test Int32", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int32)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt32ToPayload([]int32{}, nil)
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddInt32ToPayload([]int32{0}, nil)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt32ToPayload([]int32{0}, nil)
		require.Error(t, err)
	})

	t.Run("Test Int64", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int64, WithNullable(Params.CommonCfg.MaxBloomFalsePositive.PanicIfEmpty))
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt64ToPayload([]int64{}, nil)
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddInt64ToPayload([]int64{0}, nil)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt64ToPayload([]int64{0}, nil)
		require.Error(t, err)
	})

	t.Run("Test Float", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddFloatToPayload([]float32{}, nil)
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddFloatToPayload([]float32{0}, nil)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddFloatToPayload([]float32{0}, nil)
		require.Error(t, err)
	})

	t.Run("Test Double", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Double)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddDoubleToPayload([]float64{}, nil)
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddDoubleToPayload([]float64{0}, nil)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddDoubleToPayload([]float64{0}, nil)
		require.Error(t, err)
	})

	t.Run("Test String", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_String)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddOneStringToPayload("test", false)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddOneStringToPayload("test", false)
		require.Error(t, err)
	})

	t.Run("Test Array", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Array)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddOneArrayToPayload(&schemapb.ScalarField{}, false)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddOneArrayToPayload(&schemapb.ScalarField{}, false)
		require.Error(t, err)
	})

	t.Run("Test Json", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_JSON)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddOneJSONToPayload([]byte{0, 1}, false)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddOneJSONToPayload([]byte{0, 1}, false)
		require.Error(t, err)
	})

	t.Run("Test BinaryVector", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_BinaryVector, WithDim(8))
		require.Nil(t, err)
		require.NotNil(t, w)

		data := make([]byte, 8)
		for i := 0; i < 8; i++ {
			data[i] = 1
		}

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddBinaryVectorToPayload(data, 8)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBinaryVectorToPayload(data, 8)
		require.Error(t, err)
	})

	t.Run("Test FloatVector", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_FloatVector, WithDim(8))
		require.Nil(t, err)
		require.NotNil(t, w)

		data := make([]float32, 8)
		for i := 0; i < 8; i++ {
			data[i] = 1
		}

		err = w.AddFloatToPayload([]float32{}, nil)
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddFloatToPayload(data, nil)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddFloatToPayload(data, nil)
		require.Error(t, err)
	})
}

func TestPayloadWriter_ArrayOfVector(t *testing.T) {
	t.Run("Test ArrayOfFloatVector - Basic", func(t *testing.T) {
		dim := 128
		numRows := 100
		vectorsPerRow := 5

		// Create test data
		vectorArrayData := &VectorArrayFieldData{
			Data:        make([]*schemapb.VectorField, numRows),
			ElementType: schemapb.DataType_FloatVector,
		}

		for i := 0; i < numRows; i++ {
			floatData := make([]float32, vectorsPerRow*dim)
			for j := 0; j < len(floatData); j++ {
				floatData[j] = float32(i*1000 + j) // Predictable values for verification
			}

			vectorArrayData.Data[i] = &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: floatData,
					},
				},
			}
		}

		w, err := NewPayloadWriter(
			schemapb.DataType_ArrayOfVector,
			WithDim(dim),
			WithElementType(schemapb.DataType_FloatVector),
		)
		require.NoError(t, err)
		require.NotNil(t, w)

		err = w.AddVectorArrayFieldDataToPayload(vectorArrayData)
		require.NoError(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		// Verify results
		buffer, err := w.GetPayloadBufferFromWriter()
		require.NoError(t, err)
		require.NotEmpty(t, buffer)

		length, err := w.GetPayloadLengthFromWriter()
		require.NoError(t, err)
		require.Equal(t, numRows, length)
	})

	t.Run("Test ArrayOfFloatVector - Error Cases", func(t *testing.T) {
		// Test missing ElementType
		_, err := NewPayloadWriter(schemapb.DataType_ArrayOfVector, WithDim(128))
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires elementType")

		// Test with correct setup
		w, err := NewPayloadWriter(
			schemapb.DataType_ArrayOfVector,
			WithDim(128),
			WithElementType(schemapb.DataType_FloatVector),
		)
		require.NoError(t, err)
		require.NotNil(t, w)

		// Test adding empty data
		emptyData := &VectorArrayFieldData{
			Data:        []*schemapb.VectorField{},
			ElementType: schemapb.DataType_FloatVector,
		}
		err = w.AddVectorArrayFieldDataToPayload(emptyData)
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty vector array")

		// Test incorrect data type with AddDataToPayloadForUT
		w, err = NewPayloadWriter(
			schemapb.DataType_ArrayOfVector,
			WithDim(128),
			WithElementType(schemapb.DataType_FloatVector),
		)
		require.NoError(t, err)

		wrongData := "not a VectorArrayFieldData"
		err = w.AddDataToPayloadForUT(wrongData, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "incorrect data type")
	})

	t.Run("Test ArrayOfFloatVector - Multiple Batches", func(t *testing.T) {
		// Test adding multiple batches of vector arrays
		dim := 64
		batchSize := 50
		numBatches := 3
		vectorsPerRow := 3

		w, err := NewPayloadWriter(
			schemapb.DataType_ArrayOfVector,
			WithDim(dim),
			WithElementType(schemapb.DataType_FloatVector),
		)
		require.NoError(t, err)

		totalRows := 0
		for batch := 0; batch < numBatches; batch++ {
			batchData := &VectorArrayFieldData{
				Data:        make([]*schemapb.VectorField, batchSize),
				ElementType: schemapb.DataType_FloatVector,
			}

			for i := 0; i < batchSize; i++ {
				floatData := make([]float32, vectorsPerRow*dim)
				for j := 0; j < len(floatData); j++ {
					floatData[j] = float32(batch*10000 + i*100 + j)
				}

				batchData.Data[i] = &schemapb.VectorField{
					Dim: int64(dim),
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: floatData,
						},
					},
				}
			}

			err = w.AddVectorArrayFieldDataToPayload(batchData)
			require.NoError(t, err)
			totalRows += batchSize
		}

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		length, err := w.GetPayloadLengthFromWriter()
		require.NoError(t, err)
		require.Equal(t, totalRows, length)
	})

	t.Run("Test ArrayOfFloatVector - Variable Vectors Per Row", func(t *testing.T) {
		// Test with different number of vectors per row
		dim := 32
		numRows := 20

		w, err := NewPayloadWriter(
			schemapb.DataType_ArrayOfVector,
			WithDim(dim),
			WithElementType(schemapb.DataType_FloatVector),
		)
		require.NoError(t, err)

		vectorArrayData := &VectorArrayFieldData{
			Data:        make([]*schemapb.VectorField, numRows),
			ElementType: schemapb.DataType_FloatVector,
		}

		for i := 0; i < numRows; i++ {
			// Variable number of vectors per row (1 to 10)
			vectorsPerRow := (i % 10) + 1
			floatData := make([]float32, vectorsPerRow*dim)

			for j := 0; j < len(floatData); j++ {
				floatData[j] = float32(i*100 + j)
			}

			vectorArrayData.Data[i] = &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: floatData,
					},
				},
			}
		}

		err = w.AddVectorArrayFieldDataToPayload(vectorArrayData)
		require.NoError(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		length, err := w.GetPayloadLengthFromWriter()
		require.NoError(t, err)
		require.Equal(t, numRows, length)
	})
}

func TestParquetEncoding(t *testing.T) {
	t.Run("test int64 pk", func(t *testing.T) {
		field := &schemapb.FieldSchema{IsPrimaryKey: true, DataType: schemapb.DataType_Int64}

		w, err := NewPayloadWriter(schemapb.DataType_Int64, WithWriterProps(getFieldWriterProps(field)))

		assert.NoError(t, err)
		err = w.AddDataToPayloadForUT([]int64{1, 2, 3}, nil)
		assert.NoError(t, err)

		err = w.FinishPayloadWriter()
		assert.True(t, !w.(*NativePayloadWriter).writerProps.DictionaryEnabled())
		assert.NoError(t, err)
	})

	t.Run("test string pk", func(t *testing.T) {
		field := &schemapb.FieldSchema{IsPrimaryKey: true, DataType: schemapb.DataType_String}

		w, err := NewPayloadWriter(schemapb.DataType_String, WithWriterProps(getFieldWriterProps(field)))

		assert.NoError(t, err)
		err = w.AddOneStringToPayload("1", true)
		assert.NoError(t, err)

		err = w.FinishPayloadWriter()
		assert.True(t, !w.(*NativePayloadWriter).writerProps.DictionaryEnabled())
		assert.NoError(t, err)
	})
}
