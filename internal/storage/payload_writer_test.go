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

func TestParquetEncoding(t *testing.T) {
	t.Run("test int64 pk", func(t *testing.T) {
		field := &schemapb.FieldSchema{IsPrimaryKey: true, DataType: schemapb.DataType_Int64}

		w, err := NewPayloadWriter(schemapb.DataType_Int64, WithWriterProps(getFieldWriterProps(field)))

		assert.NoError(t, err)
		err = w.AddDataToPayload([]int64{1, 2, 3}, nil)
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
