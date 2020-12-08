package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func TestPayload_ReaderandWriter(t *testing.T) {

	t.Run("TestBool", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_BOOL)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{false, false, false, false})
		assert.Nil(t, err)
		err = w.AddDataToPayload([]bool{false, false, false, false})
		assert.Nil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		length, err := w.GetPayloadLengthFromWriter()
		assert.Nil(t, err)
		assert.Equal(t, 8, length)
		defer w.ReleasePayloadWriter()

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_BOOL, buffer)
		require.Nil(t, err)
		length, err = r.GetPayloadLengthFromReader()
		assert.Nil(t, err)
		assert.Equal(t, length, 8)
		bools, err := r.GetBoolFromPayload()
		assert.Nil(t, err)
		assert.ElementsMatch(t, []bool{false, false, false, false, false, false, false, false}, bools)
		ibools, _, err := r.GetDataFromPayload()
		bools = ibools.([]bool)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []bool{false, false, false, false, false, false, false, false}, bools)
		defer r.ReleasePayloadReader()

	})

	t.Run("TestInt8", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_INT8)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt8ToPayload([]int8{1, 2, 3})
		assert.Nil(t, err)
		err = w.AddDataToPayload([]int8{4, 5, 6})
		assert.Nil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		length, err := w.GetPayloadLengthFromWriter()
		assert.Nil(t, err)
		assert.Equal(t, 6, length)
		defer w.ReleasePayloadWriter()

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_INT8, buffer)
		require.Nil(t, err)
		length, err = r.GetPayloadLengthFromReader()
		assert.Nil(t, err)
		assert.Equal(t, length, 6)

		int8s, err := r.GetInt8FromPayload()
		assert.Nil(t, err)
		assert.ElementsMatch(t, []int8{1, 2, 3, 4, 5, 6}, int8s)

		iint8s, _, err := r.GetDataFromPayload()
		int8s = iint8s.([]int8)
		assert.Nil(t, err)

		assert.ElementsMatch(t, []int8{1, 2, 3, 4, 5, 6}, int8s)
		defer r.ReleasePayloadReader()
	})

	t.Run("TestInt16", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_INT16)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt16ToPayload([]int16{1, 2, 3})
		assert.Nil(t, err)
		err = w.AddDataToPayload([]int16{1, 2, 3})
		assert.Nil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		length, err := w.GetPayloadLengthFromWriter()
		assert.Nil(t, err)
		assert.Equal(t, 6, length)
		defer w.ReleasePayloadWriter()

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_INT16, buffer)
		require.Nil(t, err)
		length, err = r.GetPayloadLengthFromReader()
		assert.Nil(t, err)
		assert.Equal(t, length, 6)
		int16s, err := r.GetInt16FromPayload()
		assert.Nil(t, err)
		assert.ElementsMatch(t, []int16{1, 2, 3, 1, 2, 3}, int16s)

		iint16s, _, err := r.GetDataFromPayload()
		int16s = iint16s.([]int16)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []int16{1, 2, 3, 1, 2, 3}, int16s)
		defer r.ReleasePayloadReader()
	})

	t.Run("TestInt32", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_INT32)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt32ToPayload([]int32{1, 2, 3})
		assert.Nil(t, err)
		err = w.AddDataToPayload([]int32{1, 2, 3})
		assert.Nil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		length, err := w.GetPayloadLengthFromWriter()
		assert.Nil(t, err)
		assert.Equal(t, 6, length)
		defer w.ReleasePayloadWriter()

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_INT32, buffer)
		require.Nil(t, err)
		length, err = r.GetPayloadLengthFromReader()
		assert.Nil(t, err)
		assert.Equal(t, length, 6)

		int32s, err := r.GetInt32FromPayload()
		assert.Nil(t, err)
		assert.ElementsMatch(t, []int32{1, 2, 3, 1, 2, 3}, int32s)

		iint32s, _, err := r.GetDataFromPayload()
		int32s = iint32s.([]int32)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []int32{1, 2, 3, 1, 2, 3}, int32s)
		defer r.ReleasePayloadReader()
	})

	t.Run("TestInt64", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_INT64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt64ToPayload([]int64{1, 2, 3})
		assert.Nil(t, err)
		err = w.AddDataToPayload([]int64{1, 2, 3})
		assert.Nil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		length, err := w.GetPayloadLengthFromWriter()
		assert.Nil(t, err)
		assert.Equal(t, 6, length)
		defer w.ReleasePayloadWriter()

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_INT64, buffer)
		require.Nil(t, err)
		length, err = r.GetPayloadLengthFromReader()
		assert.Nil(t, err)
		assert.Equal(t, length, 6)

		int64s, err := r.GetInt64FromPayload()
		assert.Nil(t, err)
		assert.ElementsMatch(t, []int64{1, 2, 3, 1, 2, 3}, int64s)

		iint64s, _, err := r.GetDataFromPayload()
		int64s = iint64s.([]int64)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []int64{1, 2, 3, 1, 2, 3}, int64s)
		defer r.ReleasePayloadReader()
	})

	t.Run("TestFloat32", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_FLOAT)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddFloatToPayload([]float32{1.0, 2.0, 3.0})
		assert.Nil(t, err)
		err = w.AddDataToPayload([]float32{1.0, 2.0, 3.0})
		assert.Nil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		length, err := w.GetPayloadLengthFromWriter()
		assert.Nil(t, err)
		assert.Equal(t, 6, length)
		defer w.ReleasePayloadWriter()

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_FLOAT, buffer)
		require.Nil(t, err)
		length, err = r.GetPayloadLengthFromReader()
		assert.Nil(t, err)
		assert.Equal(t, length, 6)

		float32s, err := r.GetFloatFromPayload()
		assert.Nil(t, err)
		assert.ElementsMatch(t, []float32{1.0, 2.0, 3.0, 1.0, 2.0, 3.0}, float32s)

		ifloat32s, _, err := r.GetDataFromPayload()
		float32s = ifloat32s.([]float32)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []float32{1.0, 2.0, 3.0, 1.0, 2.0, 3.0}, float32s)
		defer r.ReleasePayloadReader()
	})

	t.Run("TestDouble", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_DOUBLE)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddDoubleToPayload([]float64{1.0, 2.0, 3.0})
		assert.Nil(t, err)
		err = w.AddDataToPayload([]float64{1.0, 2.0, 3.0})
		assert.Nil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		length, err := w.GetPayloadLengthFromWriter()
		assert.Nil(t, err)
		assert.Equal(t, 6, length)
		defer w.ReleasePayloadWriter()

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_DOUBLE, buffer)
		require.Nil(t, err)
		length, err = r.GetPayloadLengthFromReader()
		assert.Nil(t, err)
		assert.Equal(t, length, 6)

		float64s, err := r.GetDoubleFromPayload()
		assert.Nil(t, err)
		assert.ElementsMatch(t, []float64{1.0, 2.0, 3.0, 1.0, 2.0, 3.0}, float64s)

		ifloat64s, _, err := r.GetDataFromPayload()
		float64s = ifloat64s.([]float64)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []float64{1.0, 2.0, 3.0, 1.0, 2.0, 3.0}, float64s)
		defer r.ReleasePayloadReader()
	})

	t.Run("TestAddOneString", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_STRING)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddOneStringToPayload("hello0")
		assert.Nil(t, err)
		err = w.AddOneStringToPayload("hello1")
		assert.Nil(t, err)
		err = w.AddOneStringToPayload("hello2")
		assert.Nil(t, err)
		err = w.AddDataToPayload("hello3")
		assert.Nil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)
		length, err := w.GetPayloadLengthFromWriter()
		assert.Nil(t, err)
		assert.Equal(t, length, 4)
		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_STRING, buffer)
		assert.Nil(t, err)
		length, err = r.GetPayloadLengthFromReader()
		assert.Nil(t, err)
		assert.Equal(t, length, 4)
		str0, err := r.GetOneStringFromPayload(0)
		assert.Nil(t, err)
		assert.Equal(t, str0, "hello0")
		str1, err := r.GetOneStringFromPayload(1)
		assert.Nil(t, err)
		assert.Equal(t, str1, "hello1")
		str2, err := r.GetOneStringFromPayload(2)
		assert.Nil(t, err)
		assert.Equal(t, str2, "hello2")
		str3, err := r.GetOneStringFromPayload(3)
		assert.Nil(t, err)
		assert.Equal(t, str3, "hello3")

		istr0, _, err := r.GetDataFromPayload(0)
		str0 = istr0.(string)
		assert.Nil(t, err)
		assert.Equal(t, str0, "hello0")

		istr1, _, err := r.GetDataFromPayload(1)
		str1 = istr1.(string)
		assert.Nil(t, err)
		assert.Equal(t, str1, "hello1")

		istr2, _, err := r.GetDataFromPayload(2)
		str2 = istr2.(string)
		assert.Nil(t, err)
		assert.Equal(t, str2, "hello2")

		istr3, _, err := r.GetDataFromPayload(3)
		str3 = istr3.(string)
		assert.Nil(t, err)
		assert.Equal(t, str3, "hello3")

		err = r.ReleasePayloadReader()
		assert.Nil(t, err)
		err = w.ReleasePayloadWriter()
		assert.Nil(t, err)
	})

	t.Run("TestBinaryVector", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_VECTOR_BINARY)
		require.Nil(t, err)
		require.NotNil(t, w)

		in := make([]byte, 16)
		for i := 0; i < 16; i++ {
			in[i] = 1
		}
		in2 := make([]byte, 8)
		for i := 0; i < 8; i++ {
			in2[i] = 1
		}

		err = w.AddBinaryVectorToPayload(in, 8)
		assert.Nil(t, err)
		err = w.AddDataToPayload(in2, 8)
		assert.Nil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		length, err := w.GetPayloadLengthFromWriter()
		assert.Nil(t, err)
		assert.Equal(t, 24, length)
		defer w.ReleasePayloadWriter()

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_VECTOR_BINARY, buffer)
		require.Nil(t, err)
		length, err = r.GetPayloadLengthFromReader()
		assert.Nil(t, err)
		assert.Equal(t, length, 24)

		binVecs, dim, err := r.GetBinaryVectorFromPayload()
		assert.Nil(t, err)
		assert.Equal(t, 8, dim)
		assert.Equal(t, 24, len(binVecs))
		fmt.Println(binVecs)

		ibinVecs, dim, err := r.GetDataFromPayload()
		assert.Nil(t, err)
		binVecs = ibinVecs.([]byte)
		assert.Equal(t, 8, dim)
		assert.Equal(t, 24, len(binVecs))
		defer r.ReleasePayloadReader()
	})

	t.Run("TestFloatVector", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_VECTOR_FLOAT)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddFloatVectorToPayload([]float32{1.0, 2.0}, 1)
		assert.Nil(t, err)
		err = w.AddDataToPayload([]float32{3.0, 4.0}, 1)
		assert.Nil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		length, err := w.GetPayloadLengthFromWriter()
		assert.Nil(t, err)
		assert.Equal(t, 4, length)
		defer w.ReleasePayloadWriter()

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_VECTOR_FLOAT, buffer)
		require.Nil(t, err)
		length, err = r.GetPayloadLengthFromReader()
		assert.Nil(t, err)
		assert.Equal(t, length, 4)

		floatVecs, dim, err := r.GetFloatVectorFromPayload()
		assert.Nil(t, err)
		assert.Equal(t, 1, dim)
		assert.Equal(t, 4, len(floatVecs))
		assert.ElementsMatch(t, []float32{1.0, 2.0, 3.0, 4.0}, floatVecs)

		ifloatVecs, dim, err := r.GetDataFromPayload()
		assert.Nil(t, err)
		floatVecs = ifloatVecs.([]float32)
		assert.Equal(t, 1, dim)
		assert.Equal(t, 4, len(floatVecs))
		assert.ElementsMatch(t, []float32{1.0, 2.0, 3.0, 4.0}, floatVecs)
		defer r.ReleasePayloadReader()
	})
}
