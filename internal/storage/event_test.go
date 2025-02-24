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
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

/* #nosec G103 */
func TestDescriptorEvent(t *testing.T) {
	desc := newDescriptorEvent()

	var buf bytes.Buffer

	err := desc.Write(&buf)
	assert.Error(t, err)

	sizeTotal := 20 // not important
	desc.AddExtra(originalSizeKey, sizeTotal)

	// original size not in string format
	err = desc.Write(&buf)
	assert.Error(t, err)

	desc.AddExtra(originalSizeKey, "not in int format")

	err = desc.Write(&buf)
	assert.Error(t, err)

	// nullable not existed
	nullable, err := desc.GetNullable()
	assert.NoError(t, err)
	assert.False(t, nullable)

	desc.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))
	desc.AddExtra(nullableKey, "not bool format")

	err = desc.Write(&buf)
	// nullable not formatted
	assert.Error(t, err)

	desc.AddExtra(nullableKey, true)

	err = desc.Write(&buf)
	assert.NoError(t, err)

	nullable, err = desc.GetNullable()
	assert.NoError(t, err)
	assert.True(t, nullable)

	buffer := buf.Bytes()

	ts := UnsafeReadInt64(buffer, 0)
	assert.Greater(t, ts, int64(0))
	curTs := time.Now().UnixNano() / int64(time.Millisecond)
	curTs = int64(tsoutil.ComposeTS(curTs, 0))
	assert.GreaterOrEqual(t, curTs, ts)

	utc := UnsafeReadInt8(buffer, int(unsafe.Sizeof(ts)))
	assert.Equal(t, EventTypeCode(utc), DescriptorEventType)
	elen := UnsafeReadInt32(buffer, int(unsafe.Sizeof(ts)+unsafe.Sizeof(utc)))
	assert.Equal(t, elen, int32(len(buffer)))
	nPos := UnsafeReadInt32(buffer, int(unsafe.Sizeof(ts)+unsafe.Sizeof(utc)+unsafe.Sizeof(elen)))
	assert.GreaterOrEqual(t, nPos, int32(binary.Size(MagicNumber)+len(buffer)))
	t.Logf("next position = %d", nPos)

	collID := UnsafeReadInt64(buffer, binary.Size(eventHeader{}))
	assert.Equal(t, collID, int64(-1))
	partID := UnsafeReadInt64(buffer, binary.Size(eventHeader{})+
		int(unsafe.Sizeof(collID)))
	assert.Equal(t, partID, int64(-1))
	segID := UnsafeReadInt64(buffer, binary.Size(eventHeader{})+
		int(unsafe.Sizeof(collID))+
		int(unsafe.Sizeof(partID)))
	assert.Equal(t, segID, int64(-1))
	fieldID := UnsafeReadInt64(buffer, binary.Size(eventHeader{})+
		int(unsafe.Sizeof(collID))+
		int(unsafe.Sizeof(partID))+
		int(unsafe.Sizeof(segID)))
	assert.Equal(t, fieldID, int64(-1))
	startTs := UnsafeReadInt64(buffer, binary.Size(eventHeader{})+
		int(unsafe.Sizeof(collID))+
		int(unsafe.Sizeof(partID))+
		int(unsafe.Sizeof(segID))+
		int(unsafe.Sizeof(fieldID)))
	assert.Equal(t, startTs, int64(0))
	endTs := UnsafeReadInt64(buffer, binary.Size(eventHeader{})+
		int(unsafe.Sizeof(collID))+
		int(unsafe.Sizeof(partID))+
		int(unsafe.Sizeof(segID))+
		int(unsafe.Sizeof(fieldID))+
		int(unsafe.Sizeof(startTs)))
	assert.Equal(t, endTs, int64(0))
	colType := UnsafeReadInt32(buffer, binary.Size(eventHeader{})+
		int(unsafe.Sizeof(collID))+
		int(unsafe.Sizeof(partID))+
		int(unsafe.Sizeof(segID))+
		int(unsafe.Sizeof(fieldID))+
		int(unsafe.Sizeof(startTs))+
		int(unsafe.Sizeof(endTs)))
	assert.Equal(t, colType, int32(-1))

	postHeadOffset := binary.Size(eventHeader{}) +
		int(unsafe.Sizeof(collID)) +
		int(unsafe.Sizeof(partID)) +
		int(unsafe.Sizeof(segID)) +
		int(unsafe.Sizeof(fieldID)) +
		int(unsafe.Sizeof(startTs)) +
		int(unsafe.Sizeof(endTs)) +
		int(unsafe.Sizeof(colType))

	postHeadArray := buffer[postHeadOffset:]
	for i := DescriptorEventType; i < EventTypeEnd; i++ {
		hen := postHeadArray[i-DescriptorEventType]
		size := getEventFixPartSize(i)
		assert.Equal(t, hen, uint8(size))
	}
}

/* #nosec G103 */
func TestInsertEvent(t *testing.T) {
	insertT := func(t *testing.T,
		dt schemapb.DataType,
		w *insertEventWriter,
		ir1 func(w *insertEventWriter) error,
		ir2 func(w *insertEventWriter) error,
		iw func(w *insertEventWriter) error,
		ev interface{},
	) {
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))
		err := ir1(w)
		assert.NoError(t, err)
		err = iw(w)
		assert.Error(t, err)
		err = ir2(w)
		assert.NoError(t, err)
		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(insertEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(dt, pBuf, false)
		assert.NoError(t, err)
		values, _, _, err := pR.GetDataFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, values, ev)
		pR.Close()

		r, err := newEventReader(dt, bytes.NewBuffer(wBuf), false)
		assert.NoError(t, err)
		payload, nulls, _, err := r.GetDataFromPayload()
		assert.NoError(t, err)
		assert.Nil(t, nulls)
		assert.Equal(t, payload, ev)

		r.Close()
	}

	t.Run("insert_bool", func(t *testing.T) {
		w, err := newInsertEventWriter(schemapb.DataType_Bool)
		assert.NoError(t, err)
		insertT(t, schemapb.DataType_Bool, w,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]bool{true, false, true}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]bool{false, true, false}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5}, nil)
			},
			[]bool{true, false, true, false, true, false})
	})

	t.Run("insert_int8", func(t *testing.T) {
		w, err := newInsertEventWriter(schemapb.DataType_Int8)
		assert.NoError(t, err)
		insertT(t, schemapb.DataType_Int8, w,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int8{1, 2, 3}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int8{4, 5, 6}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5}, nil)
			},
			[]int8{1, 2, 3, 4, 5, 6})
	})

	t.Run("insert_int16", func(t *testing.T) {
		w, err := newInsertEventWriter(schemapb.DataType_Int16)
		assert.NoError(t, err)
		insertT(t, schemapb.DataType_Int16, w,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int16{1, 2, 3}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int16{4, 5, 6}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5}, nil)
			},
			[]int16{1, 2, 3, 4, 5, 6})
	})

	t.Run("insert_int32", func(t *testing.T) {
		w, err := newInsertEventWriter(schemapb.DataType_Int32)
		assert.NoError(t, err)
		insertT(t, schemapb.DataType_Int32, w,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int32{1, 2, 3}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int32{4, 5, 6}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5}, nil)
			},
			[]int32{1, 2, 3, 4, 5, 6})
	})

	t.Run("insert_int64", func(t *testing.T) {
		w, err := newInsertEventWriter(schemapb.DataType_Int64)
		assert.NoError(t, err)
		insertT(t, schemapb.DataType_Int64, w,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int64{1, 2, 3}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int64{4, 5, 6}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5}, nil)
			},
			[]int64{1, 2, 3, 4, 5, 6})
	})

	t.Run("insert_float32", func(t *testing.T) {
		w, err := newInsertEventWriter(schemapb.DataType_Float)
		assert.NoError(t, err)
		insertT(t, schemapb.DataType_Float, w,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]float32{1, 2, 3}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]float32{4, 5, 6}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5}, nil)
			},
			[]float32{1, 2, 3, 4, 5, 6})
	})

	t.Run("insert_float64", func(t *testing.T) {
		w, err := newInsertEventWriter(schemapb.DataType_Double)
		assert.NoError(t, err)
		insertT(t, schemapb.DataType_Double, w,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]float64{1, 2, 3}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]float64{4, 5, 6}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5}, nil)
			},
			[]float64{1, 2, 3, 4, 5, 6})
	})

	t.Run("insert_binary_vector", func(t *testing.T) {
		w, err := newInsertEventWriter(schemapb.DataType_BinaryVector, WithDim(16))
		assert.NoError(t, err)
		insertT(t, schemapb.DataType_BinaryVector, w,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]byte{1, 2, 3, 4}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]byte{5, 6, 7, 8}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5, 6}, nil)
			},
			[]byte{1, 2, 3, 4, 5, 6, 7, 8})
	})

	t.Run("insert_float_vector", func(t *testing.T) {
		w, err := newInsertEventWriter(schemapb.DataType_FloatVector, WithDim(2))
		assert.NoError(t, err)
		insertT(t, schemapb.DataType_FloatVector, w,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]float32{1, 2, 3, 4}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]float32{5, 6, 7, 8}, nil)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5, 6}, nil)
			},
			[]float32{1, 2, 3, 4, 5, 6, 7, 8})
	})

	t.Run("insert_string", func(t *testing.T) {
		w, err := newInsertEventWriter(schemapb.DataType_String)
		assert.NoError(t, err)
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload("1234", nil)
		assert.NoError(t, err)
		err = w.AddOneStringToPayload("567890", true)
		assert.NoError(t, err)
		err = w.AddOneStringToPayload("abcdefg", true)
		assert.NoError(t, err)
		err = w.AddDataToPayload([]int{1, 2, 3}, nil)
		assert.Error(t, err)
		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(insertEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_String, pBuf, false)
		assert.NoError(t, err)

		s, _, err := pR.GetStringFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, s[0], "1234")
		assert.Equal(t, s[1], "567890")
		assert.Equal(t, s[2], "abcdefg")
		pR.Close()

		r, err := newEventReader(schemapb.DataType_String, bytes.NewBuffer(wBuf), false)
		assert.NoError(t, err)

		s, _, err = pR.GetStringFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, s[0], "1234")
		assert.Equal(t, s[1], "567890")
		assert.Equal(t, s[2], "abcdefg")

		r.Close()
	})
}

/* #nosec G103 */
// delete data will always be saved as string(pk + ts) to binlog
func TestDeleteEvent(t *testing.T) {
	t.Run("delete_string", func(t *testing.T) {
		w, err := newDeleteEventWriter(schemapb.DataType_String)
		assert.NoError(t, err)
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload("1234", nil)
		assert.NoError(t, err)
		err = w.AddOneStringToPayload("567890", true)
		assert.NoError(t, err)
		err = w.AddOneStringToPayload("abcdefg", true)
		assert.NoError(t, err)
		err = w.AddDataToPayload([]int{1, 2, 3}, nil)
		assert.Error(t, err)
		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(insertEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_String, pBuf, false)
		assert.NoError(t, err)

		s, _, err := pR.GetStringFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, s[0], "1234")
		assert.Equal(t, s[1], "567890")
		assert.Equal(t, s[2], "abcdefg")

		pR.Close()

		r, err := newEventReader(schemapb.DataType_String, bytes.NewBuffer(wBuf), false)
		assert.NoError(t, err)

		s, _, err = pR.GetStringFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, s[0], "1234")
		assert.Equal(t, s[1], "567890")
		assert.Equal(t, s[2], "abcdefg")

		r.Close()
	})
}

/* #nosec G103 */
func TestCreateCollectionEvent(t *testing.T) {
	t.Run("create_event", func(t *testing.T) {
		w, err := newCreateCollectionEventWriter(schemapb.DataType_Float)
		assert.Error(t, err)
		assert.Nil(t, w)
	})

	t.Run("create_collection_timestamp", func(t *testing.T) {
		w, err := newCreateCollectionEventWriter(schemapb.DataType_Int64)
		assert.NoError(t, err)
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload([]int64{1, 2, 3}, nil)
		assert.NoError(t, err)
		err = w.AddDataToPayload([]int{4, 5, 6}, nil)
		assert.Error(t, err)
		err = w.AddDataToPayload([]int64{4, 5, 6}, nil)
		assert.NoError(t, err)
		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(createCollectionEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_Int64, pBuf, false)
		assert.NoError(t, err)
		values, _, _, err := pR.GetDataFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, values, []int64{1, 2, 3, 4, 5, 6})
		pR.Close()

		r, err := newEventReader(schemapb.DataType_Int64, bytes.NewBuffer(wBuf), false)
		assert.NoError(t, err)
		payload, _, _, err := r.GetDataFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, payload, []int64{1, 2, 3, 4, 5, 6})

		r.Close()
	})

	t.Run("create_collection_string", func(t *testing.T) {
		w, err := newCreateCollectionEventWriter(schemapb.DataType_String)
		assert.NoError(t, err)
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload("1234", nil)
		assert.NoError(t, err)
		err = w.AddOneStringToPayload("567890", true)
		assert.NoError(t, err)
		err = w.AddOneStringToPayload("abcdefg", true)
		assert.NoError(t, err)
		err = w.AddDataToPayload([]int{1, 2, 3}, nil)
		assert.Error(t, err)
		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(insertEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_String, pBuf, false)
		assert.NoError(t, err)

		s, _, err := pR.GetStringFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, s[0], "1234")
		assert.Equal(t, s[1], "567890")
		assert.Equal(t, s[2], "abcdefg")

		pR.Close()

		r, err := newEventReader(schemapb.DataType_String, bytes.NewBuffer(wBuf), true)
		assert.NoError(t, err)

		s, _, err = pR.GetStringFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, s[0], "1234")
		assert.Equal(t, s[1], "567890")
		assert.Equal(t, s[2], "abcdefg")

		r.Close()
	})
}

/* #nosec G103 */
func TestDropCollectionEvent(t *testing.T) {
	t.Run("drop_event", func(t *testing.T) {
		w, err := newDropCollectionEventWriter(schemapb.DataType_Float)
		assert.Error(t, err)
		assert.Nil(t, w)
	})

	t.Run("drop_collection_timestamp", func(t *testing.T) {
		w, err := newDropCollectionEventWriter(schemapb.DataType_Int64)
		assert.NoError(t, err)
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload([]int64{1, 2, 3}, nil)
		assert.NoError(t, err)
		err = w.AddDataToPayload([]int{4, 5, 6}, nil)
		assert.Error(t, err)
		err = w.AddDataToPayload([]int64{4, 5, 6}, nil)
		assert.NoError(t, err)
		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(createCollectionEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_Int64, pBuf, false)
		assert.NoError(t, err)
		values, _, _, err := pR.GetDataFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, values, []int64{1, 2, 3, 4, 5, 6})
		pR.Close()

		r, err := newEventReader(schemapb.DataType_Int64, bytes.NewBuffer(wBuf), false)
		assert.NoError(t, err)
		payload, _, _, err := r.GetDataFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, payload, []int64{1, 2, 3, 4, 5, 6})

		r.Close()
	})

	t.Run("drop_collection_string", func(t *testing.T) {
		w, err := newDropCollectionEventWriter(schemapb.DataType_String)
		assert.NoError(t, err)
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload("1234", nil)
		assert.NoError(t, err)
		err = w.AddOneStringToPayload("567890", true)
		assert.NoError(t, err)
		err = w.AddOneStringToPayload("abcdefg", true)
		assert.NoError(t, err)
		err = w.AddDataToPayload([]int{1, 2, 3}, nil)
		assert.Error(t, err)
		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(insertEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_String, pBuf, false)
		assert.NoError(t, err)

		s, _, err := pR.GetStringFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, s[0], "1234")
		assert.Equal(t, s[1], "567890")
		assert.Equal(t, s[2], "abcdefg")

		pR.Close()

		r, err := newEventReader(schemapb.DataType_String, bytes.NewBuffer(wBuf), false)
		assert.NoError(t, err)

		s, _, err = r.GetStringFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, s[0], "1234")
		assert.Equal(t, s[1], "567890")
		assert.Equal(t, s[2], "abcdefg")

		r.Close()
	})
}

/* #nosec G103 */
func TestCreatePartitionEvent(t *testing.T) {
	t.Run("create_event", func(t *testing.T) {
		w, err := newCreatePartitionEventWriter(schemapb.DataType_Float)
		assert.Error(t, err)
		assert.Nil(t, w)
	})

	t.Run("create_partition_timestamp", func(t *testing.T) {
		w, err := newCreatePartitionEventWriter(schemapb.DataType_Int64)
		assert.NoError(t, err)
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload([]int64{1, 2, 3}, nil)
		assert.NoError(t, err)
		err = w.AddDataToPayload([]int{4, 5, 6}, nil)
		assert.Error(t, err)
		err = w.AddDataToPayload([]int64{4, 5, 6}, nil)
		assert.NoError(t, err)
		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(createCollectionEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_Int64, pBuf, false)
		assert.NoError(t, err)
		values, _, _, err := pR.GetDataFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, values, []int64{1, 2, 3, 4, 5, 6})
		pR.Close()

		r, err := newEventReader(schemapb.DataType_Int64, bytes.NewBuffer(wBuf), false)
		assert.NoError(t, err)
		payload, _, _, err := r.GetDataFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, payload, []int64{1, 2, 3, 4, 5, 6})

		r.Close()
	})

	t.Run("create_partition_string", func(t *testing.T) {
		w, err := newCreatePartitionEventWriter(schemapb.DataType_String)
		assert.NoError(t, err)
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload("1234", nil)
		assert.NoError(t, err)
		err = w.AddOneStringToPayload("567890", true)
		assert.NoError(t, err)
		err = w.AddOneStringToPayload("abcdefg", true)
		assert.NoError(t, err)
		err = w.AddDataToPayload([]int{1, 2, 3}, nil)
		assert.Error(t, err)
		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(insertEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_String, pBuf, false)
		assert.NoError(t, err)

		s, _, err := pR.GetStringFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, s[0], "1234")
		assert.Equal(t, s[1], "567890")
		assert.Equal(t, s[2], "abcdefg")

		pR.Close()

		r, err := newEventReader(schemapb.DataType_String, bytes.NewBuffer(wBuf), false)
		assert.NoError(t, err)

		s, _, err = pR.GetStringFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, s[0], "1234")
		assert.Equal(t, s[1], "567890")
		assert.Equal(t, s[2], "abcdefg")

		r.Close()
	})
}

/* #nosec G103 */
func TestDropPartitionEvent(t *testing.T) {
	t.Run("drop_event", func(t *testing.T) {
		w, err := newDropPartitionEventWriter(schemapb.DataType_Float)
		assert.Error(t, err)
		assert.Nil(t, w)
	})

	t.Run("drop_partition_timestamp", func(t *testing.T) {
		w, err := newDropPartitionEventWriter(schemapb.DataType_Int64)
		assert.NoError(t, err)
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload([]int64{1, 2, 3}, nil)
		assert.NoError(t, err)
		err = w.AddDataToPayload([]int{4, 5, 6}, nil)
		assert.Error(t, err)
		err = w.AddDataToPayload([]int64{4, 5, 6}, nil)
		assert.NoError(t, err)
		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(createCollectionEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_Int64, pBuf, false)
		assert.NoError(t, err)
		values, _, _, err := pR.GetDataFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, values, []int64{1, 2, 3, 4, 5, 6})
		pR.Close()

		r, err := newEventReader(schemapb.DataType_Int64, bytes.NewBuffer(wBuf), false)
		assert.NoError(t, err)
		payload, _, _, err := r.GetDataFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, payload, []int64{1, 2, 3, 4, 5, 6})

		r.Close()
	})

	t.Run("drop_partition_string", func(t *testing.T) {
		w, err := newDropPartitionEventWriter(schemapb.DataType_String)
		assert.NoError(t, err)
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload("1234", nil)
		assert.NoError(t, err)
		err = w.AddOneStringToPayload("567890", true)
		assert.NoError(t, err)
		err = w.AddOneStringToPayload("abcdefg", true)
		assert.NoError(t, err)
		err = w.AddDataToPayload([]int{1, 2, 3}, nil)
		assert.Error(t, err)
		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(insertEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_String, pBuf, false)
		assert.NoError(t, err)

		s, _, err := pR.GetStringFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, s[0], "1234")
		assert.Equal(t, s[1], "567890")
		assert.Equal(t, s[2], "abcdefg")

		pR.Close()

		r, err := newEventReader(schemapb.DataType_String, bytes.NewBuffer(wBuf), false)
		assert.NoError(t, err)

		s, _, err = pR.GetStringFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, s[0], "1234")
		assert.Equal(t, s[1], "567890")
		assert.Equal(t, s[2], "abcdefg")

		r.Close()
	})
}

/* #nosec G103 */
func TestIndexFileEvent(t *testing.T) {
	t.Run("index_file_string", func(t *testing.T) {
		w, err := newIndexFileEventWriter(schemapb.DataType_String)
		assert.NoError(t, err)
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))

		payload := funcutil.GenRandomBytes()
		err = w.AddOneStringToPayload(typeutil.UnsafeBytes2str(payload), true)
		assert.NoError(t, err)

		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(indexFileEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_String, pBuf, false)
		assert.NoError(t, err)
		assert.Equal(t, pR.numRows, int64(1))
		value, _, err := pR.GetStringFromPayload()

		assert.Equal(t, len(value), 1)

		assert.NoError(t, err)
		assert.Equal(t, payload, typeutil.UnsafeStr2bytes(value[0]))
		pR.Close()
	})

	t.Run("index_file_int8", func(t *testing.T) {
		w, err := newIndexFileEventWriter(schemapb.DataType_Int8)
		assert.NoError(t, err)
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))

		payload := funcutil.GenRandomBytes()
		err = w.AddByteToPayload(payload, nil)
		assert.NoError(t, err)

		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(indexFileEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_Int8, pBuf, false)
		assert.Equal(t, pR.numRows, int64(len(payload)))
		assert.NoError(t, err)
		value, _, err := pR.GetByteFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, payload, value)
		pR.Close()
	})

	t.Run("index_file_int8_large", func(t *testing.T) {
		w, err := newIndexFileEventWriter(schemapb.DataType_Int8)
		assert.NoError(t, err)
		w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))

		payload := funcutil.GenRandomBytesWithLength(1000)
		err = w.AddByteToPayload(payload, nil)
		assert.NoError(t, err)

		err = w.Finish()
		assert.NoError(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.NoError(t, err)
		w.Close()

		wBuf := buf.Bytes()
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(indexFileEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_Int8, pBuf, false)
		assert.Equal(t, pR.numRows, int64(len(payload)))
		assert.NoError(t, err)
		value, _, err := pR.GetByteFromPayload()
		assert.NoError(t, err)
		assert.Equal(t, payload, value)
		pR.Close()
	})
}

func TestDescriptorEventTsError(t *testing.T) {
	insertData := &insertEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
	buf := new(bytes.Buffer)
	err := insertData.WriteEventData(buf)
	assert.Error(t, err)
	insertData.StartTimestamp = 1000
	err = insertData.WriteEventData(buf)
	assert.Error(t, err)

	deleteData := &deleteEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
	err = deleteData.WriteEventData(buf)
	assert.Error(t, err)
	deleteData.StartTimestamp = 1000
	err = deleteData.WriteEventData(buf)
	assert.Error(t, err)

	createCollectionData := &createCollectionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
	err = createCollectionData.WriteEventData(buf)
	assert.Error(t, err)
	createCollectionData.StartTimestamp = 1000
	err = createCollectionData.WriteEventData(buf)
	assert.Error(t, err)

	dropCollectionData := &dropCollectionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
	err = dropCollectionData.WriteEventData(buf)
	assert.Error(t, err)
	dropCollectionData.StartTimestamp = 1000
	err = dropCollectionData.WriteEventData(buf)
	assert.Error(t, err)

	createPartitionData := &createPartitionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
	err = createPartitionData.WriteEventData(buf)
	assert.Error(t, err)
	createPartitionData.StartTimestamp = 1000
	err = createPartitionData.WriteEventData(buf)
	assert.Error(t, err)

	dropPartitionData := &dropPartitionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
	err = dropPartitionData.WriteEventData(buf)
	assert.Error(t, err)
	dropPartitionData.StartTimestamp = 1000
	err = dropPartitionData.WriteEventData(buf)
	assert.Error(t, err)
}

func TestReadFixPartError(t *testing.T) {
	buf := new(bytes.Buffer)
	_, err := readEventHeader(buf)
	assert.Error(t, err)

	_, err = readInsertEventDataFixPart(buf)
	assert.Error(t, err)

	_, err = readDeleteEventDataFixPart(buf)
	assert.Error(t, err)

	_, err = readCreateCollectionEventDataFixPart(buf)
	assert.Error(t, err)

	_, err = readDropCollectionEventDataFixPart(buf)
	assert.Error(t, err)

	_, err = readCreatePartitionEventDataFixPart(buf)
	assert.Error(t, err)

	_, err = readDropPartitionEventDataFixPart(buf)
	assert.Error(t, err)

	_, err = readDescriptorEventData(buf)
	assert.Error(t, err)

	event := newDescriptorEventData()
	err = binary.Write(buf, common.Endian, event.DescriptorEventDataFixPart)
	assert.NoError(t, err)
	_, err = readDescriptorEventData(buf)
	assert.Error(t, err)

	size := getEventFixPartSize(EventTypeCode(10))
	assert.Equal(t, size, int32(-1))
}

func TestEventReaderError(t *testing.T) {
	buf := new(bytes.Buffer)
	r, err := newEventReader(schemapb.DataType_Int64, buf, false)
	assert.Nil(t, r)
	assert.Error(t, err)

	header := newEventHeader(DescriptorEventType)
	err = header.Write(buf)
	assert.NoError(t, err)

	r, err = newEventReader(schemapb.DataType_Int64, buf, false)
	assert.Nil(t, r)
	assert.Error(t, err)

	buf = new(bytes.Buffer)
	header = newEventHeader(InsertEventType)
	err = header.Write(buf)
	assert.NoError(t, err)

	r, err = newEventReader(schemapb.DataType_Int64, buf, false)
	assert.Nil(t, r)
	assert.Error(t, err)

	buf = new(bytes.Buffer)
	header = newEventHeader(InsertEventType)
	header.EventLength = getEventFixPartSize(InsertEventType) + int32(binary.Size(header))
	err = header.Write(buf)
	assert.NoError(t, err)

	insertData := &insertEventData{
		StartTimestamp: 1000,
		EndTimestamp:   2000,
	}
	err = binary.Write(buf, common.Endian, insertData)
	assert.NoError(t, err)

	r, err = newEventReader(schemapb.DataType_Int64, buf, false)
	assert.Nil(t, r)
	assert.Error(t, err)
}

func TestEventClose(t *testing.T) {
	w, err := newInsertEventWriter(schemapb.DataType_String)
	assert.NoError(t, err)
	w.SetEventTimestamp(tsoutil.ComposeTS(10, 0), tsoutil.ComposeTS(100, 0))
	err = w.AddDataToPayload("1234", nil)
	assert.NoError(t, err)
	err = w.Finish()
	assert.NoError(t, err)

	var buf bytes.Buffer
	err = w.Write(&buf)
	assert.NoError(t, err)
	w.Close()

	wBuf := buf.Bytes()
	r, err := newEventReader(schemapb.DataType_String, bytes.NewBuffer(wBuf), false)
	assert.NoError(t, err)

	r.Close()

	err = r.readHeader()
	assert.Error(t, err)
	err = r.readData()
	assert.Error(t, err)
}

func TestIndexFileEventDataError(t *testing.T) {
	var err error
	var buffer bytes.Buffer

	event := newIndexFileEventData()

	event.SetEventTimestamp(0, 1)
	// start timestamp not set
	err = event.WriteEventData(&buffer)
	assert.Error(t, err)

	event.SetEventTimestamp(1, 0)
	// end timestamp not set
	err = event.WriteEventData(&buffer)
	assert.Error(t, err)
}

func TestReadIndexFileEventDataFixPart(t *testing.T) {
	var err error
	var buffer bytes.Buffer
	// buffer is empty
	_, err = readIndexFileEventDataFixPart(&buffer)
	assert.Error(t, err)
}
