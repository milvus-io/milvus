package storage

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
)

func checkEventHeader(
	t *testing.T,
	buf []byte,
	tc EventTypeCode,
	svrID int32,
	length int32) {
	ts := UnsafeReadInt64(buf, 0)
	assert.Greater(t, ts, int64(0))
	curts := time.Now().UnixNano() / int64(time.Millisecond)
	curts = int64(tsoutil.ComposeTS(curts, 0))
	assert.GreaterOrEqual(t, curts, ts)
	utc := UnsafeReadInt8(buf, int(unsafe.Sizeof(ts)))
	assert.Equal(t, EventTypeCode(utc), tc)
	usid := UnsafeReadInt32(buf, int(unsafe.Sizeof(ts)+unsafe.Sizeof(utc)))
	assert.Equal(t, usid, svrID)
	elen := UnsafeReadInt32(buf, int(unsafe.Sizeof(ts)+unsafe.Sizeof(utc)+unsafe.Sizeof(usid)))
	assert.Equal(t, elen, length)
	npos := UnsafeReadInt32(buf, int(unsafe.Sizeof(ts)+unsafe.Sizeof(utc)+unsafe.Sizeof(usid)+unsafe.Sizeof(elen)))
	assert.Equal(t, npos, length)
}

func TestEventWriterAndReader(t *testing.T) {
	insertT := func(t *testing.T,
		dt schemapb.DataType,
		ir1 func(w *insertEventWriter) error,
		ir2 func(w *insertEventWriter) error,
		iw func(w *insertEventWriter) error,
		ev interface{},
	) {
		w, err := newInsertEventWriter(dt, 0)
		assert.Nil(t, err)
		w.SetStartTimestamp(tsoutil.ComposeTS(10, 0))
		w.SetEndTimestamp(tsoutil.ComposeTS(100, 0))
		err = ir1(w)
		assert.Nil(t, err)
		err = iw(w)
		assert.NotNil(t, err)
		err = ir2(w)
		assert.Nil(t, err)
		err = w.Finish()
		assert.Nil(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.Nil(t, err)
		err = w.Close()
		assert.Nil(t, err)

		wBuf := buf.Bytes()
		checkEventHeader(t, wBuf, InsertEventType, ServerID, int32(len(wBuf)))
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(insertEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(dt, pBuf)
		assert.Nil(t, err)
		vals, _, err := pR.GetDataFromPayload()
		assert.Nil(t, err)
		assert.Equal(t, vals, ev)
		err = pR.Close()
		assert.Nil(t, err)

		r, err := newEventReader(dt, bytes.NewBuffer(wBuf))
		assert.Nil(t, err)
		payload, _, err := r.GetDataFromPayload()
		assert.Nil(t, err)
		assert.Equal(t, payload, ev)

		err = r.Close()
		assert.Nil(t, err)
	}

	t.Run("insert_bool", func(t *testing.T) {
		insertT(t, schemapb.DataType_BOOL,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]bool{true, false, true})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]bool{false, true, false})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]bool{true, false, true, false, true, false})
	})

	t.Run("insert_int8", func(t *testing.T) {
		insertT(t, schemapb.DataType_INT8,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int8{1, 2, 3})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int8{4, 5, 6})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]int8{1, 2, 3, 4, 5, 6})
	})

	t.Run("insert_int16", func(t *testing.T) {
		insertT(t, schemapb.DataType_INT16,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int16{1, 2, 3})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int16{4, 5, 6})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]int16{1, 2, 3, 4, 5, 6})
	})

	t.Run("insert_int32", func(t *testing.T) {
		insertT(t, schemapb.DataType_INT32,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int32{1, 2, 3})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int32{4, 5, 6})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]int32{1, 2, 3, 4, 5, 6})
	})

	t.Run("insert_int64", func(t *testing.T) {
		insertT(t, schemapb.DataType_INT64,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int64{1, 2, 3})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int64{4, 5, 6})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]int64{1, 2, 3, 4, 5, 6})
	})

	t.Run("insert_float32", func(t *testing.T) {
		insertT(t, schemapb.DataType_FLOAT,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]float32{1, 2, 3})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]float32{4, 5, 6})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]float32{1, 2, 3, 4, 5, 6})
	})

	t.Run("insert_float64", func(t *testing.T) {
		insertT(t, schemapb.DataType_DOUBLE,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]float64{1, 2, 3})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]float64{4, 5, 6})
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]float64{1, 2, 3, 4, 5, 6})
	})

	t.Run("insert_binary_vector", func(t *testing.T) {
		insertT(t, schemapb.DataType_VECTOR_BINARY,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]byte{1, 2, 3, 4}, 16)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]byte{5, 6, 7, 8}, 16)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5, 6}, 16)
			},
			[]byte{1, 2, 3, 4, 5, 6, 7, 8})
	})

	t.Run("insert_float_vector", func(t *testing.T) {
		insertT(t, schemapb.DataType_VECTOR_FLOAT,
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]float32{1, 2, 3, 4}, 2)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]float32{5, 6, 7, 8}, 2)
			},
			func(w *insertEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5, 6}, 2)
			},
			[]float32{1, 2, 3, 4, 5, 6, 7, 8})
	})

	t.Run("insert_string", func(t *testing.T) {
		w, err := newInsertEventWriter(schemapb.DataType_STRING, 0)
		assert.Nil(t, err)
		w.SetStartTimestamp(tsoutil.ComposeTS(10, 0))
		w.SetEndTimestamp(tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload("1234")
		assert.Nil(t, err)
		err = w.AddOneStringToPayload("567890")
		assert.Nil(t, err)
		err = w.AddOneStringToPayload("abcdefg")
		assert.Nil(t, err)
		err = w.AddDataToPayload([]int{1, 2, 3})
		assert.NotNil(t, err)
		err = w.Finish()
		assert.Nil(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.Nil(t, err)
		err = w.Close()
		assert.Nil(t, err)

		wBuf := buf.Bytes()
		checkEventHeader(t, wBuf, InsertEventType, ServerID, int32(len(wBuf)))
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(insertEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_STRING, pBuf)
		assert.Nil(t, err)

		s0, err := pR.GetOneStringFromPayload(0)
		assert.Nil(t, err)
		assert.Equal(t, s0, "1234")

		s1, err := pR.GetOneStringFromPayload(1)
		assert.Nil(t, err)
		assert.Equal(t, s1, "567890")

		s2, err := pR.GetOneStringFromPayload(2)
		assert.Nil(t, err)
		assert.Equal(t, s2, "abcdefg")

		err = pR.Close()
		assert.Nil(t, err)

		r, err := newEventReader(schemapb.DataType_STRING, bytes.NewBuffer(wBuf))
		assert.Nil(t, err)

		s0, err = r.GetOneStringFromPayload(0)
		assert.Nil(t, err)
		assert.Equal(t, s0, "1234")

		s1, err = r.GetOneStringFromPayload(1)
		assert.Nil(t, err)
		assert.Equal(t, s1, "567890")

		s2, err = r.GetOneStringFromPayload(2)
		assert.Nil(t, err)
		assert.Equal(t, s2, "abcdefg")

		err = r.Close()
		assert.Nil(t, err)
	})
}
