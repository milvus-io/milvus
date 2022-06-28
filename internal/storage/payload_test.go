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

package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

func TestPayload_ReaderAndWriter(t *testing.T) {

	t.Run("TestBool", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
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

		r, err := NewPayloadReader(schemapb.DataType_Bool, buffer)
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
		w, err := NewPayloadWriter(schemapb.DataType_Int8)
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

		r, err := NewPayloadReader(schemapb.DataType_Int8, buffer)
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
		w, err := NewPayloadWriter(schemapb.DataType_Int16)
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

		r, err := NewPayloadReader(schemapb.DataType_Int16, buffer)
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
		w, err := NewPayloadWriter(schemapb.DataType_Int32)
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

		r, err := NewPayloadReader(schemapb.DataType_Int32, buffer)
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
		w, err := NewPayloadWriter(schemapb.DataType_Int64)
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

		r, err := NewPayloadReader(schemapb.DataType_Int64, buffer)
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
		w, err := NewPayloadWriter(schemapb.DataType_Float)
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

		r, err := NewPayloadReader(schemapb.DataType_Float, buffer)
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
		w, err := NewPayloadWriter(schemapb.DataType_Double)
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

		r, err := NewPayloadReader(schemapb.DataType_Double, buffer)
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

	t.Run("TestAddString", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_String)
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

		r, err := NewPayloadReader(schemapb.DataType_String, buffer)
		assert.Nil(t, err)
		length, err = r.GetPayloadLengthFromReader()
		assert.Nil(t, err)
		assert.Equal(t, length, 4)

		str, err := r.GetStringFromPayload()
		assert.Nil(t, err)

		assert.Equal(t, str[0], "hello0")
		assert.Equal(t, str[1], "hello1")
		assert.Equal(t, str[2], "hello2")
		assert.Equal(t, str[3], "hello3")

		istr, _, err := r.GetDataFromPayload()
		strArray := istr.([]string)
		assert.Nil(t, err)
		assert.Equal(t, strArray[0], "hello0")
		assert.Equal(t, strArray[1], "hello1")
		assert.Equal(t, strArray[2], "hello2")
		assert.Equal(t, strArray[3], "hello3")
		r.ReleasePayloadReader()
		w.ReleasePayloadWriter()
	})

	t.Run("TestBinaryVector", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_BinaryVector)
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

		r, err := NewPayloadReader(schemapb.DataType_BinaryVector, buffer)
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
		w, err := NewPayloadWriter(schemapb.DataType_FloatVector)
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

		r, err := NewPayloadReader(schemapb.DataType_FloatVector, buffer)
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

	t.Run("TestAddDataToPayload", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		w.colType = 999
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddDataToPayload([]bool{false, false, false, false})
		assert.NotNil(t, err)

		err = w.AddDataToPayload([]bool{false, false, false, false}, 0)
		assert.NotNil(t, err)

		err = w.AddDataToPayload([]bool{false, false, false, false}, 0, 0)
		assert.NotNil(t, err)

		err = w.AddBoolToPayload([]bool{})
		assert.NotNil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)
		err = w.AddBoolToPayload([]bool{false})
		assert.NotNil(t, err)
	})

	t.Run("TestAddBoolAfterFinish", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		require.Nil(t, err)
		require.NotNil(t, w)

		_, err = w.GetPayloadBufferFromWriter()
		assert.NotNil(t, err)

		err = w.AddBoolToPayload([]bool{})
		assert.NotNil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)
		err = w.AddBoolToPayload([]bool{false})
		assert.NotNil(t, err)
	})

	t.Run("TestAddInt8AfterFinish", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int8)
		require.Nil(t, err)
		require.NotNil(t, w)
		defer w.Close()

		_, err = w.GetPayloadBufferFromWriter()
		assert.NotNil(t, err)

		err = w.AddInt8ToPayload([]int8{})
		assert.NotNil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)
		err = w.AddInt8ToPayload([]int8{0})
		assert.NotNil(t, err)
	})
	t.Run("TestAddInt16AfterFinish", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int16)
		require.Nil(t, err)
		require.NotNil(t, w)
		defer w.Close()

		_, err = w.GetPayloadBufferFromWriter()
		assert.NotNil(t, err)

		err = w.AddInt16ToPayload([]int16{})
		assert.NotNil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)
		err = w.AddInt16ToPayload([]int16{0})
		assert.NotNil(t, err)
	})
	t.Run("TestAddInt32AfterFinish", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int32)
		require.Nil(t, err)
		require.NotNil(t, w)
		defer w.Close()

		_, err = w.GetPayloadBufferFromWriter()
		assert.NotNil(t, err)

		err = w.AddInt32ToPayload([]int32{})
		assert.NotNil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)
		err = w.AddInt32ToPayload([]int32{0})
		assert.NotNil(t, err)
	})
	t.Run("TestAddInt64AfterFinish", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)
		defer w.Close()

		_, err = w.GetPayloadBufferFromWriter()
		assert.NotNil(t, err)

		err = w.AddInt64ToPayload([]int64{})
		assert.NotNil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)
		err = w.AddInt64ToPayload([]int64{0})
		assert.NotNil(t, err)
	})
	t.Run("TestAddFloatAfterFinish", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)
		defer w.Close()

		_, err = w.GetPayloadBufferFromWriter()
		assert.NotNil(t, err)

		err = w.AddFloatToPayload([]float32{})
		assert.NotNil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)
		err = w.AddFloatToPayload([]float32{0.0})
		assert.NotNil(t, err)
	})
	t.Run("TestAddDoubleAfterFinish", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Double)
		require.Nil(t, err)
		require.NotNil(t, w)
		defer w.Close()

		_, err = w.GetPayloadBufferFromWriter()
		assert.NotNil(t, err)

		err = w.AddDoubleToPayload([]float64{})
		assert.NotNil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)
		err = w.AddDoubleToPayload([]float64{0.0})
		assert.NotNil(t, err)
	})
	t.Run("TestAddOneStringAfterFinish", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_String)
		require.Nil(t, err)
		require.NotNil(t, w)
		defer w.Close()

		_, err = w.GetPayloadBufferFromWriter()
		assert.NotNil(t, err)

		err = w.AddOneStringToPayload("")
		assert.NotNil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)
		err = w.AddOneStringToPayload("c")
		assert.NotNil(t, err)
	})
	t.Run("TestAddBinVectorAfterFinish", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_BinaryVector)
		require.Nil(t, err)
		require.NotNil(t, w)
		defer w.Close()

		_, err = w.GetPayloadBufferFromWriter()
		assert.NotNil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		err = w.AddBinaryVectorToPayload([]byte{}, 8)
		assert.NotNil(t, err)
		err = w.AddBinaryVectorToPayload([]byte{1}, 0)
		assert.NotNil(t, err)

		err = w.AddBinaryVectorToPayload([]byte{1, 0, 0, 0, 0, 0, 0, 0}, 8)
		assert.Nil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)
		err = w.AddBinaryVectorToPayload([]byte{1, 0, 0, 0, 0, 0, 0, 0}, 8)
		assert.NotNil(t, err)
	})
	t.Run("TestAddFloatVectorAfterFinish", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_FloatVector)
		require.Nil(t, err)
		require.NotNil(t, w)
		defer w.Close()

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		err = w.AddFloatVectorToPayload([]float32{}, 8)
		assert.NotNil(t, err)
		err = w.AddFloatVectorToPayload([]float32{1.0}, 0)
		assert.NotNil(t, err)

		err = w.AddFloatVectorToPayload([]float32{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}, 8)
		assert.Nil(t, err)
		err = w.FinishPayloadWriter()
		assert.Nil(t, err)
		err = w.AddFloatVectorToPayload([]float32{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}, 8)
		assert.NotNil(t, err)
	})

	t.Run("TestNewReadError", func(t *testing.T) {
		buffer := []byte{0}
		r, err := NewPayloadReader(999, buffer)
		assert.NotNil(t, err)
		assert.Nil(t, r)
	})
	t.Run("TestGetDataError", func(t *testing.T) {
		r := PayloadReader{}
		r.colType = 999

		_, _, err := r.GetDataFromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetBoolError", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int8)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt8ToPayload([]int8{1, 2, 3})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Bool, buffer)
		assert.Nil(t, err)

		_, err = r.GetBoolFromPayload()
		assert.NotNil(t, err)

		r.colType = 999
		_, err = r.GetBoolFromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetBoolError2", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{true, false, true})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Bool, buffer)
		assert.Nil(t, err)

		r.numRows = 99
		_, err = r.GetBoolFromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetInt8Error", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{false, true, true})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Int8, buffer)
		assert.Nil(t, err)

		_, err = r.GetInt8FromPayload()
		assert.NotNil(t, err)

		r.colType = 999
		_, err = r.GetInt8FromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetInt8Error2", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int8)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt8ToPayload([]int8{1, 2, 3})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Int8, buffer)
		assert.Nil(t, err)

		r.numRows = 99
		_, err = r.GetInt8FromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetInt16Error", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{false, true, true})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Int16, buffer)
		assert.Nil(t, err)

		_, err = r.GetInt16FromPayload()
		assert.NotNil(t, err)

		r.colType = 999
		_, err = r.GetInt16FromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetInt16Error2", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int16)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt16ToPayload([]int16{1, 2, 3})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Int16, buffer)
		assert.Nil(t, err)

		r.numRows = 99
		_, err = r.GetInt16FromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetInt32Error", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{false, true, true})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Int32, buffer)
		assert.Nil(t, err)

		_, err = r.GetInt32FromPayload()
		assert.NotNil(t, err)

		r.colType = 999
		_, err = r.GetInt32FromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetInt32Error2", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int32)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt32ToPayload([]int32{1, 2, 3})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Int32, buffer)
		assert.Nil(t, err)

		r.numRows = 99
		_, err = r.GetInt32FromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetInt64Error", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{false, true, true})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Int64, buffer)
		assert.Nil(t, err)

		_, err = r.GetInt64FromPayload()
		assert.NotNil(t, err)

		r.colType = 999
		_, err = r.GetInt64FromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetInt64Error2", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Int64)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddInt64ToPayload([]int64{1, 2, 3})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Int64, buffer)
		assert.Nil(t, err)

		r.numRows = 99
		_, err = r.GetInt64FromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetFloatError", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{false, true, true})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Float, buffer)
		assert.Nil(t, err)

		_, err = r.GetFloatFromPayload()
		assert.NotNil(t, err)

		r.colType = 999
		_, err = r.GetFloatFromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetFloatError2", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Float)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddFloatToPayload([]float32{1, 2, 3})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Float, buffer)
		assert.Nil(t, err)

		r.numRows = 99
		_, err = r.GetFloatFromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetDoubleError", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{false, true, true})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Double, buffer)
		assert.Nil(t, err)

		_, err = r.GetDoubleFromPayload()
		assert.NotNil(t, err)

		r.colType = 999
		_, err = r.GetDoubleFromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetDoubleError2", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Double)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddDoubleToPayload([]float64{1, 2, 3})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_Double, buffer)
		assert.Nil(t, err)

		r.numRows = 99
		_, err = r.GetDoubleFromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetStringError", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{false, true, true})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_String, buffer)
		assert.Nil(t, err)

		_, err = r.GetStringFromPayload()
		assert.NotNil(t, err)

		r.colType = 999
		_, err = r.GetStringFromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetStringError2", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_String)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddOneStringToPayload("hello0")
		assert.Nil(t, err)
		err = w.AddOneStringToPayload("hello1")
		assert.Nil(t, err)
		err = w.AddOneStringToPayload("hello2")
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_String, buffer)
		assert.Nil(t, err)

		r.numRows = 99
		_, err = r.GetStringFromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetBinaryVectorError", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{false, true, true})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_BinaryVector, buffer)
		assert.Nil(t, err)

		_, _, err = r.GetBinaryVectorFromPayload()
		assert.NotNil(t, err)

		r.colType = 999
		_, _, err = r.GetBinaryVectorFromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetBinaryVectorError2", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_BinaryVector)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBinaryVectorToPayload([]byte{1, 0, 0, 0, 0, 0, 0, 0}, 8)
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_BinaryVector, buffer)
		assert.Nil(t, err)

		r.numRows = 99
		_, _, err = r.GetBinaryVectorFromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetFloatVectorError", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_Bool)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddBoolToPayload([]bool{false, true, true})
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_FloatVector, buffer)
		assert.Nil(t, err)

		_, _, err = r.GetFloatVectorFromPayload()
		assert.NotNil(t, err)

		r.colType = 999
		_, _, err = r.GetFloatVectorFromPayload()
		assert.NotNil(t, err)
	})
	t.Run("TestGetFloatVectorError2", func(t *testing.T) {
		w, err := NewPayloadWriter(schemapb.DataType_FloatVector)
		require.Nil(t, err)
		require.NotNil(t, w)

		err = w.AddFloatVectorToPayload([]float32{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}, 8)
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		buffer, err := w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		r, err := NewPayloadReader(schemapb.DataType_FloatVector, buffer)
		assert.Nil(t, err)

		r.numRows = 99
		_, _, err = r.GetFloatVectorFromPayload()
		assert.NotNil(t, err)
	})

	t.Run("TestWriteLargeSizeData", func(t *testing.T) {
		t.Skip("Large data skip for online ut")
		size := 1 << 29 // 512M
		var vec []float32
		for i := 0; i < size/4; i++ {
			vec = append(vec, 1)
		}

		w, err := NewPayloadWriter(schemapb.DataType_FloatVector)
		assert.Nil(t, err)

		err = w.AddFloatVectorToPayload(vec, 128)
		assert.Nil(t, err)

		err = w.FinishPayloadWriter()
		assert.Nil(t, err)

		_, err = w.GetPayloadBufferFromWriter()
		assert.Nil(t, err)

		w.ReleasePayloadWriter()
	})
}
