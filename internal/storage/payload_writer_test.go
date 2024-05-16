package storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestPayloadWriter_Failed(t *testing.T) {
	t.Run("Test Bool", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{})
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddBoolToPayload([]bool{false})
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{false})
		require.Error(t, err)
	})

	t.Run("Test Byte", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int8)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddByteToPayload([]byte{})
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddByteToPayload([]byte{0})
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddByteToPayload([]byte{0})
		require.Error(t, err)
	})

	t.Run("Test Int8", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int8)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt8ToPayload([]int8{})
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddInt8ToPayload([]int8{0})
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt8ToPayload([]int8{0})
		require.Error(t, err)
	})

	t.Run("Test Int16", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int16)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt16ToPayload([]int16{})
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddInt16ToPayload([]int16{0})
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt16ToPayload([]int16{0})
		require.Error(t, err)
	})

	t.Run("Test Int32", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int32)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt32ToPayload([]int32{})
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddInt32ToPayload([]int32{0})
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt32ToPayload([]int32{0})
		require.Error(t, err)
	})

	t.Run("Test Int64", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt64ToPayload([]int64{})
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddInt64ToPayload([]int64{0})
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt64ToPayload([]int64{0})
		require.Error(t, err)
	})

	t.Run("Test Float", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddFloatToPayload([]float32{})
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddFloatToPayload([]float32{0})
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddFloatToPayload([]float32{0})
		require.Error(t, err)
	})

	t.Run("Test Double", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Double)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddDoubleToPayload([]float64{})
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddDoubleToPayload([]float64{0})
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddDoubleToPayload([]float64{0})
		require.Error(t, err)
	})

	t.Run("Test String", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_String)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddOneStringToPayload("test")
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddOneStringToPayload("test")
		require.Error(t, err)
	})

	t.Run("Test Array", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Array)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddOneArrayToPayload(&schemapb.ScalarField{})
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddOneArrayToPayload(&schemapb.ScalarField{})
		require.Error(t, err)
	})

	t.Run("Test Json", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_JSON)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddOneJSONToPayload([]byte{0, 1})
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddOneJSONToPayload([]byte{0, 1})
		require.Error(t, err)
	})

	t.Run("Test BinaryVector", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_BinaryVector, 8)
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
		w, err := NewPayloadWriter(schemapb.DataType_FloatVector, 8)
		require.Nil(t, err)
		require.NotNil(t, w)

		data := make([]float32, 8)
		for i := 0; i < 8; i++ {
			data[i] = 1
		}

		err = w.AddFloatToPayload([]float32{})
		require.Error(t, err)

		err = w.FinishPayloadWriter()
		require.NoError(t, err)

		err = w.AddFloatToPayload(data)
		require.Error(t, err)

		w, err = NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddFloatToPayload(data)
		require.Error(t, err)
	})
}
