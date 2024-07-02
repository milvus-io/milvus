package storage

import (
	"testing"

	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/compress"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestPayloadWriter_Failed(t *testing.T) {
	t.Run("wrong input", func(t *testing.T) {
		_, err := NewPayloadWriter(schemapb.DataType_FloatVector)
		require.Error(t, err)

		_, err = NewPayloadWriter(schemapb.DataType_Bool, WithDim(1))
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

func BenchmarkPayloadWriter(b *testing.B) {
	size := 10
	b.Run("Test default encoding ", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			w, err := NewPayloadWriter(schemapb.DataType_Int64)
			assert.NoError(b, err)
			for j := 0; j < size; j++ {
				err = w.AddInt64ToPayload([]int64{int64(j)}, nil)
				assert.NoError(b, err)
			}

			err = w.FinishPayloadWriter()
			assert.NoError(b, err)
		}
		b.ReportAllocs()
	})

	b.Run("Test RLE", func(b *testing.B) {
		b.ResetTimer()
		props := parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Zstd),
			parquet.WithCompressionLevel(3),
			parquet.WithEncoding(parquet.Encodings.RLE),
		)
		for i := 0; i < b.N; i++ {
			w, err := NewPayloadWriter(schemapb.DataType_Int64, WithWriterProps(props))
			assert.NoError(b, err)
			for j := 0; j < size; j++ {
				err = w.AddInt64ToPayload([]int64{int64(j)}, nil)
				assert.NoError(b, err)
			}

			err = w.FinishPayloadWriter()
			assert.NoError(b, err)
		}
		b.ReportAllocs()
	})
}
