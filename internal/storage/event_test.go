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

func TestDescriptorEvent(t *testing.T) {
	desc, err := newDescriptorEvent()
	assert.Nil(t, err)

	var buf bytes.Buffer

	err = desc.Write(&buf)
	assert.Nil(t, err)

	buffer := buf.Bytes()

	ts := UnsafeReadInt64(buffer, 0)
	assert.Greater(t, ts, int64(0))
	curts := time.Now().UnixNano() / int64(time.Millisecond)
	curts = int64(tsoutil.ComposeTS(curts, 0))
	assert.GreaterOrEqual(t, curts, ts)

	utc := UnsafeReadInt8(buffer, int(unsafe.Sizeof(ts)))
	assert.Equal(t, EventTypeCode(utc), DescriptorEventType)
	usid := UnsafeReadInt32(buffer, int(unsafe.Sizeof(ts)+unsafe.Sizeof(utc)))
	assert.Equal(t, usid, int32(ServerID))
	elen := UnsafeReadInt32(buffer, int(unsafe.Sizeof(ts)+unsafe.Sizeof(utc)+unsafe.Sizeof(usid)))
	assert.Equal(t, elen, int32(len(buffer)))
	npos := UnsafeReadInt32(buffer, int(unsafe.Sizeof(ts)+unsafe.Sizeof(utc)+unsafe.Sizeof(usid)+unsafe.Sizeof(elen)))
	assert.GreaterOrEqual(t, npos, int32(binary.Size(MagicNumber)+len(buffer)))
	t.Logf("next position = %d", npos)

	binVersion := UnsafeReadInt16(buffer, binary.Size(eventHeader{}))
	assert.Equal(t, binVersion, int16(BinlogVersion))
	svrVersion := UnsafeReadInt64(buffer, binary.Size(eventHeader{})+int(unsafe.Sizeof(binVersion)))
	assert.Equal(t, svrVersion, int64(ServerVersion))
	cmitID := UnsafeReadInt64(buffer, binary.Size(eventHeader{})+int(unsafe.Sizeof(binVersion))+int(unsafe.Sizeof(svrVersion)))
	assert.Equal(t, cmitID, int64(CommitID))
	headLen := UnsafeReadInt8(buffer, binary.Size(eventHeader{})+
		int(unsafe.Sizeof(binVersion))+
		int(unsafe.Sizeof(svrVersion))+
		int(unsafe.Sizeof(cmitID)))
	assert.Equal(t, headLen, int8(binary.Size(eventHeader{})))
	t.Logf("head len = %d", headLen)
	collID := UnsafeReadInt64(buffer, binary.Size(eventHeader{})+
		int(unsafe.Sizeof(binVersion))+
		int(unsafe.Sizeof(svrVersion))+
		int(unsafe.Sizeof(cmitID))+
		int(unsafe.Sizeof(headLen)))
	assert.Equal(t, collID, int64(-1))
	partID := UnsafeReadInt64(buffer, binary.Size(eventHeader{})+
		int(unsafe.Sizeof(binVersion))+
		int(unsafe.Sizeof(svrVersion))+
		int(unsafe.Sizeof(cmitID))+
		int(unsafe.Sizeof(headLen))+
		int(unsafe.Sizeof(collID)))
	assert.Equal(t, partID, int64(-1))
	segID := UnsafeReadInt64(buffer, binary.Size(eventHeader{})+
		int(unsafe.Sizeof(binVersion))+
		int(unsafe.Sizeof(svrVersion))+
		int(unsafe.Sizeof(cmitID))+
		int(unsafe.Sizeof(headLen))+
		int(unsafe.Sizeof(collID))+
		int(unsafe.Sizeof(partID)))
	assert.Equal(t, segID, int64(-1))
	startTs := UnsafeReadInt64(buffer, binary.Size(eventHeader{})+
		int(unsafe.Sizeof(binVersion))+
		int(unsafe.Sizeof(svrVersion))+
		int(unsafe.Sizeof(cmitID))+
		int(unsafe.Sizeof(headLen))+
		int(unsafe.Sizeof(collID))+
		int(unsafe.Sizeof(partID))+
		int(unsafe.Sizeof(segID)))
	assert.Equal(t, startTs, int64(0))
	endTs := UnsafeReadInt64(buffer, binary.Size(eventHeader{})+
		int(unsafe.Sizeof(binVersion))+
		int(unsafe.Sizeof(svrVersion))+
		int(unsafe.Sizeof(cmitID))+
		int(unsafe.Sizeof(headLen))+
		int(unsafe.Sizeof(collID))+
		int(unsafe.Sizeof(partID))+
		int(unsafe.Sizeof(segID))+
		int(unsafe.Sizeof(startTs)))
	assert.Equal(t, endTs, int64(0))
	colType := UnsafeReadInt32(buffer, binary.Size(eventHeader{})+
		int(unsafe.Sizeof(binVersion))+
		int(unsafe.Sizeof(svrVersion))+
		int(unsafe.Sizeof(cmitID))+
		int(unsafe.Sizeof(headLen))+
		int(unsafe.Sizeof(collID))+
		int(unsafe.Sizeof(partID))+
		int(unsafe.Sizeof(segID))+
		int(unsafe.Sizeof(startTs))+
		int(unsafe.Sizeof(endTs)))
	assert.Equal(t, colType, int32(-1))

	postHeadOffset := binary.Size(eventHeader{}) +
		int(unsafe.Sizeof(binVersion)) +
		int(unsafe.Sizeof(svrVersion)) +
		int(unsafe.Sizeof(cmitID)) +
		int(unsafe.Sizeof(headLen)) +
		int(unsafe.Sizeof(collID)) +
		int(unsafe.Sizeof(partID)) +
		int(unsafe.Sizeof(segID)) +
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

func TestInsertEvent(t *testing.T) {
	insertT := func(t *testing.T,
		dt schemapb.DataType,
		ir1 func(w *insertEventWriter) error,
		ir2 func(w *insertEventWriter) error,
		iw func(w *insertEventWriter) error,
		ev interface{},
	) {
		w, err := newInsertEventWriter(dt)
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
		w, err := newInsertEventWriter(schemapb.DataType_STRING)
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

func TestDeleteEvent(t *testing.T) {
	deleteT := func(t *testing.T,
		dt schemapb.DataType,
		ir1 func(w *deleteEventWriter) error,
		ir2 func(w *deleteEventWriter) error,
		iw func(w *deleteEventWriter) error,
		ev interface{},
	) {
		w, err := newDeleteEventWriter(dt)
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
		checkEventHeader(t, wBuf, DeleteEventType, ServerID, int32(len(wBuf)))
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

	t.Run("delete_bool", func(t *testing.T) {
		deleteT(t, schemapb.DataType_BOOL,
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]bool{true, false, true})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]bool{false, true, false})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]bool{true, false, true, false, true, false})
	})

	t.Run("delete_int8", func(t *testing.T) {
		deleteT(t, schemapb.DataType_INT8,
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int8{1, 2, 3})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int8{4, 5, 6})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]int8{1, 2, 3, 4, 5, 6})
	})

	t.Run("delete_int16", func(t *testing.T) {
		deleteT(t, schemapb.DataType_INT16,
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int16{1, 2, 3})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int16{4, 5, 6})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]int16{1, 2, 3, 4, 5, 6})
	})

	t.Run("delete_int32", func(t *testing.T) {
		deleteT(t, schemapb.DataType_INT32,
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int32{1, 2, 3})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int32{4, 5, 6})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]int32{1, 2, 3, 4, 5, 6})
	})

	t.Run("delete_int64", func(t *testing.T) {
		deleteT(t, schemapb.DataType_INT64,
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int64{1, 2, 3})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int64{4, 5, 6})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]int64{1, 2, 3, 4, 5, 6})
	})

	t.Run("delete_float32", func(t *testing.T) {
		deleteT(t, schemapb.DataType_FLOAT,
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]float32{1, 2, 3})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]float32{4, 5, 6})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]float32{1, 2, 3, 4, 5, 6})
	})

	t.Run("delete_float64", func(t *testing.T) {
		deleteT(t, schemapb.DataType_DOUBLE,
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]float64{1, 2, 3})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]float64{4, 5, 6})
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5})
			},
			[]float64{1, 2, 3, 4, 5, 6})
	})

	t.Run("delete_binary_vector", func(t *testing.T) {
		deleteT(t, schemapb.DataType_VECTOR_BINARY,
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]byte{1, 2, 3, 4}, 16)
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]byte{5, 6, 7, 8}, 16)
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5, 6}, 16)
			},
			[]byte{1, 2, 3, 4, 5, 6, 7, 8})
	})

	t.Run("delete_float_vector", func(t *testing.T) {
		deleteT(t, schemapb.DataType_VECTOR_FLOAT,
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]float32{1, 2, 3, 4}, 2)
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]float32{5, 6, 7, 8}, 2)
			},
			func(w *deleteEventWriter) error {
				return w.AddDataToPayload([]int{1, 2, 3, 4, 5, 6}, 2)
			},
			[]float32{1, 2, 3, 4, 5, 6, 7, 8})
	})

	t.Run("delete_string", func(t *testing.T) {
		w, err := newDeleteEventWriter(schemapb.DataType_STRING)
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
		checkEventHeader(t, wBuf, DeleteEventType, ServerID, int32(len(wBuf)))
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

func TestCreateCollectionEvent(t *testing.T) {
	t.Run("create_event", func(t *testing.T) {
		w, err := newCreateCollectionEventWriter(schemapb.DataType_FLOAT)
		assert.NotNil(t, err)
		assert.Nil(t, w)
	})

	t.Run("create_collection_timestamp", func(t *testing.T) {
		w, err := newCreateCollectionEventWriter(schemapb.DataType_INT64)
		assert.Nil(t, err)
		w.SetStartTimestamp(tsoutil.ComposeTS(10, 0))
		w.SetEndTimestamp(tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload([]int64{1, 2, 3})
		assert.Nil(t, err)
		err = w.AddDataToPayload([]int{4, 5, 6})
		assert.NotNil(t, err)
		err = w.AddDataToPayload([]int64{4, 5, 6})
		assert.Nil(t, err)
		err = w.Finish()
		assert.Nil(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.Nil(t, err)
		err = w.Close()
		assert.Nil(t, err)

		wBuf := buf.Bytes()
		checkEventHeader(t, wBuf, CreateCollectionEventType, ServerID, int32(len(wBuf)))
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(createCollectionEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_INT64, pBuf)
		assert.Nil(t, err)
		vals, _, err := pR.GetDataFromPayload()
		assert.Nil(t, err)
		assert.Equal(t, vals, []int64{1, 2, 3, 4, 5, 6})
		err = pR.Close()
		assert.Nil(t, err)

		r, err := newEventReader(schemapb.DataType_INT64, bytes.NewBuffer(wBuf))
		assert.Nil(t, err)
		payload, _, err := r.GetDataFromPayload()
		assert.Nil(t, err)
		assert.Equal(t, payload, []int64{1, 2, 3, 4, 5, 6})

		err = r.Close()
		assert.Nil(t, err)
	})

	t.Run("create_collection_string", func(t *testing.T) {
		w, err := newCreateCollectionEventWriter(schemapb.DataType_STRING)
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
		checkEventHeader(t, wBuf, CreateCollectionEventType, ServerID, int32(len(wBuf)))
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

func TestDropCollectionEvent(t *testing.T) {
	t.Run("drop_event", func(t *testing.T) {
		w, err := newDropCollectionEventWriter(schemapb.DataType_FLOAT)
		assert.NotNil(t, err)
		assert.Nil(t, w)
	})

	t.Run("drop_collection_timestamp", func(t *testing.T) {
		w, err := newDropCollectionEventWriter(schemapb.DataType_INT64)
		assert.Nil(t, err)
		w.SetStartTimestamp(tsoutil.ComposeTS(10, 0))
		w.SetEndTimestamp(tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload([]int64{1, 2, 3})
		assert.Nil(t, err)
		err = w.AddDataToPayload([]int{4, 5, 6})
		assert.NotNil(t, err)
		err = w.AddDataToPayload([]int64{4, 5, 6})
		assert.Nil(t, err)
		err = w.Finish()
		assert.Nil(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.Nil(t, err)
		err = w.Close()
		assert.Nil(t, err)

		wBuf := buf.Bytes()
		checkEventHeader(t, wBuf, DropCollectionEventType, ServerID, int32(len(wBuf)))
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(createCollectionEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_INT64, pBuf)
		assert.Nil(t, err)
		vals, _, err := pR.GetDataFromPayload()
		assert.Nil(t, err)
		assert.Equal(t, vals, []int64{1, 2, 3, 4, 5, 6})
		err = pR.Close()
		assert.Nil(t, err)

		r, err := newEventReader(schemapb.DataType_INT64, bytes.NewBuffer(wBuf))
		assert.Nil(t, err)
		payload, _, err := r.GetDataFromPayload()
		assert.Nil(t, err)
		assert.Equal(t, payload, []int64{1, 2, 3, 4, 5, 6})

		err = r.Close()
		assert.Nil(t, err)
	})

	t.Run("drop_collection_string", func(t *testing.T) {
		w, err := newDropCollectionEventWriter(schemapb.DataType_STRING)
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
		checkEventHeader(t, wBuf, DropCollectionEventType, ServerID, int32(len(wBuf)))
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

func TestCreatePartitionEvent(t *testing.T) {
	t.Run("create_event", func(t *testing.T) {
		w, err := newCreatePartitionEventWriter(schemapb.DataType_FLOAT)
		assert.NotNil(t, err)
		assert.Nil(t, w)
	})

	t.Run("create_partition_timestamp", func(t *testing.T) {
		w, err := newCreatePartitionEventWriter(schemapb.DataType_INT64)
		assert.Nil(t, err)
		w.SetStartTimestamp(tsoutil.ComposeTS(10, 0))
		w.SetEndTimestamp(tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload([]int64{1, 2, 3})
		assert.Nil(t, err)
		err = w.AddDataToPayload([]int{4, 5, 6})
		assert.NotNil(t, err)
		err = w.AddDataToPayload([]int64{4, 5, 6})
		assert.Nil(t, err)
		err = w.Finish()
		assert.Nil(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.Nil(t, err)
		err = w.Close()
		assert.Nil(t, err)

		wBuf := buf.Bytes()
		checkEventHeader(t, wBuf, CreatePartitionEventType, ServerID, int32(len(wBuf)))
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(createCollectionEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_INT64, pBuf)
		assert.Nil(t, err)
		vals, _, err := pR.GetDataFromPayload()
		assert.Nil(t, err)
		assert.Equal(t, vals, []int64{1, 2, 3, 4, 5, 6})
		err = pR.Close()
		assert.Nil(t, err)

		r, err := newEventReader(schemapb.DataType_INT64, bytes.NewBuffer(wBuf))
		assert.Nil(t, err)
		payload, _, err := r.GetDataFromPayload()
		assert.Nil(t, err)
		assert.Equal(t, payload, []int64{1, 2, 3, 4, 5, 6})

		err = r.Close()
		assert.Nil(t, err)
	})

	t.Run("create_partition_string", func(t *testing.T) {
		w, err := newCreatePartitionEventWriter(schemapb.DataType_STRING)
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
		checkEventHeader(t, wBuf, CreatePartitionEventType, ServerID, int32(len(wBuf)))
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

func TestDropPartitionEvent(t *testing.T) {
	t.Run("drop_event", func(t *testing.T) {
		w, err := newDropPartitionEventWriter(schemapb.DataType_FLOAT)
		assert.NotNil(t, err)
		assert.Nil(t, w)
	})

	t.Run("drop_partition_timestamp", func(t *testing.T) {
		w, err := newDropPartitionEventWriter(schemapb.DataType_INT64)
		assert.Nil(t, err)
		w.SetStartTimestamp(tsoutil.ComposeTS(10, 0))
		w.SetEndTimestamp(tsoutil.ComposeTS(100, 0))
		err = w.AddDataToPayload([]int64{1, 2, 3})
		assert.Nil(t, err)
		err = w.AddDataToPayload([]int{4, 5, 6})
		assert.NotNil(t, err)
		err = w.AddDataToPayload([]int64{4, 5, 6})
		assert.Nil(t, err)
		err = w.Finish()
		assert.Nil(t, err)

		var buf bytes.Buffer
		err = w.Write(&buf)
		assert.Nil(t, err)
		err = w.Close()
		assert.Nil(t, err)

		wBuf := buf.Bytes()
		checkEventHeader(t, wBuf, DropPartitionEventType, ServerID, int32(len(wBuf)))
		st := UnsafeReadInt64(wBuf, binary.Size(eventHeader{}))
		assert.Equal(t, Timestamp(st), tsoutil.ComposeTS(10, 0))
		et := UnsafeReadInt64(wBuf, binary.Size(eventHeader{})+int(unsafe.Sizeof(st)))
		assert.Equal(t, Timestamp(et), tsoutil.ComposeTS(100, 0))

		payloadOffset := binary.Size(eventHeader{}) + binary.Size(createCollectionEventData{})
		pBuf := wBuf[payloadOffset:]
		pR, err := NewPayloadReader(schemapb.DataType_INT64, pBuf)
		assert.Nil(t, err)
		vals, _, err := pR.GetDataFromPayload()
		assert.Nil(t, err)
		assert.Equal(t, vals, []int64{1, 2, 3, 4, 5, 6})
		err = pR.Close()
		assert.Nil(t, err)

		r, err := newEventReader(schemapb.DataType_INT64, bytes.NewBuffer(wBuf))
		assert.Nil(t, err)
		payload, _, err := r.GetDataFromPayload()
		assert.Nil(t, err)
		assert.Equal(t, payload, []int64{1, 2, 3, 4, 5, 6})

		err = r.Close()
		assert.Nil(t, err)
	})

	t.Run("drop_partition_string", func(t *testing.T) {
		w, err := newDropPartitionEventWriter(schemapb.DataType_STRING)
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
		checkEventHeader(t, wBuf, DropPartitionEventType, ServerID, int32(len(wBuf)))
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
