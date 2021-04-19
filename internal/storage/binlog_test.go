package storage

import (
	"encoding/binary"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
)

func TestInsertBinlog(t *testing.T) {
	w, err := NewInsertBinlogWriter(schemapb.DataType_INT64, 10, 20, 30, 40)
	assert.Nil(t, err)

	e1, err := w.NextInsertEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6})
	assert.NotNil(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6})
	assert.Nil(t, err)
	e1.SetStartTimestamp(100)
	e1.SetEndTimestamp(200)

	e2, err := w.NextInsertEventWriter()
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]int64{7, 8, 9})
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]bool{true, false, true})
	assert.NotNil(t, err)
	err = e2.AddDataToPayload([]int64{10, 11, 12})
	assert.Nil(t, err)
	e2.SetStartTimestamp(300)
	e2.SetEndTimestamp(400)

	w.SetStartTimeStamp(1000)
	w.SetEndTimeStamp(2000)

	_, err = w.GetBuffer()
	assert.NotNil(t, err)
	err = w.Close()
	assert.Nil(t, err)
	buf, err := w.GetBuffer()
	assert.Nil(t, err)

	//magic number
	magicNum := UnsafeReadInt32(buf, 0)
	assert.Equal(t, magicNum, MagicNumber)
	pos := int(unsafe.Sizeof(MagicNumber))

	//descriptor header, timestamp
	ts := UnsafeReadInt64(buf, pos)
	assert.Greater(t, ts, int64(0))
	curts := time.Now().UnixNano() / int64(time.Millisecond)
	curts = int64(tsoutil.ComposeTS(curts, 0))
	diffts := curts - ts
	maxdiff := int64(tsoutil.ComposeTS(1000, 0))
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(ts))

	//descriptor header, type code
	tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(tc), DescriptorEventType)
	pos += int(unsafe.Sizeof(tc))

	//descriptor header, server id
	svrID := UnsafeReadInt32(buf, pos)
	assert.Equal(t, svrID, int32(ServerID))
	pos += int(unsafe.Sizeof(svrID))

	//descriptor header, event length
	descEventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(descEventLen))

	//descriptor header, next position
	descNxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descEventLen+int32(unsafe.Sizeof(MagicNumber)), descNxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	//descriptor data fix, binlog version
	binLogVer := UnsafeReadInt16(buf, pos)
	assert.Equal(t, binLogVer, int16(BinlogVersion))
	pos += int(unsafe.Sizeof(binLogVer))

	//descriptor data fix, server version
	svrVer := UnsafeReadInt64(buf, pos)
	assert.Equal(t, svrVer, int64(ServerVersion))
	pos += int(unsafe.Sizeof(svrVer))

	//descriptor data fix, commit id
	cmitID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, cmitID, int64(CommitID))
	pos += int(unsafe.Sizeof(cmitID))

	//descriptor data fix, header length
	headLen := UnsafeReadInt8(buf, pos)
	assert.Equal(t, headLen, int8(binary.Size(eventHeader{})))
	pos += int(unsafe.Sizeof(headLen))

	//descriptor data fix, collection id
	collID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, collID, int64(10))
	pos += int(unsafe.Sizeof(collID))

	//descriptor data fix, partition id
	partID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, partID, int64(20))
	pos += int(unsafe.Sizeof(partID))

	//descriptor data fix, segment id
	segID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, segID, int64(30))
	pos += int(unsafe.Sizeof(segID))

	//descriptor data fix, field id
	fieldID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, fieldID, int64(40))
	pos += int(unsafe.Sizeof(fieldID))

	//descriptor data fix, start time stamp
	startts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, startts, int64(1000))
	pos += int(unsafe.Sizeof(startts))

	//descriptor data fix, end time stamp
	endts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, endts, int64(2000))
	pos += int(unsafe.Sizeof(endts))

	//descriptor data fix, payload type
	colType := UnsafeReadInt32(buf, pos)
	assert.Equal(t, schemapb.DataType(colType), schemapb.DataType_INT64)
	pos += int(unsafe.Sizeof(colType))

	//descriptor data, post header lengths
	for i := DescriptorEventType; i < EventTypeEnd; i++ {
		size := getEventFixPartSize(i)
		assert.Equal(t, uint8(size), buf[pos])
		pos++
	}

	//start of e1
	assert.Equal(t, pos, int(descNxtPos))

	//insert e1 header, Timestamp
	e1ts := UnsafeReadInt64(buf, pos)
	diffts = curts - e1ts
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(e1ts))

	//insert e1 header, type code
	e1tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(e1tc), InsertEventType)
	pos += int(unsafe.Sizeof(e1tc))

	//insert e1 header, Server id
	e1svrID := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e1svrID, int32(ServerID))
	pos += int(unsafe.Sizeof(e1svrID))

	//insert e1 header, event length
	e1EventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(e1EventLen))

	//insert e1 header, next position
	e1NxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descNxtPos+e1EventLen, e1NxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	//insert e1 data, start time stamp
	e1st := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e1st, int64(100))
	pos += int(unsafe.Sizeof(e1st))

	//insert e1 data, end time stamp
	e1et := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e1et, int64(200))
	pos += int(unsafe.Sizeof(e1et))

	//insert e1, payload
	e1Payload := buf[pos:e1NxtPos]
	e1r, err := NewPayloadReader(schemapb.DataType_INT64, e1Payload)
	assert.Nil(t, err)
	e1a, err := e1r.GetInt64FromPayload()
	assert.Nil(t, err)
	assert.Equal(t, e1a, []int64{1, 2, 3, 4, 5, 6})
	err = e1r.Close()
	assert.Nil(t, err)

	//start of e2
	pos = int(e1NxtPos)

	//insert e2 header, Timestamp
	e2ts := UnsafeReadInt64(buf, pos)
	diffts = curts - e2ts
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(e2ts))

	//insert e2 header, type code
	e2tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(e2tc), InsertEventType)
	pos += int(unsafe.Sizeof(e2tc))

	//insert e2 header, Server id
	e2svrID := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e2svrID, int32(ServerID))
	pos += int(unsafe.Sizeof(e2svrID))

	//insert e2 header, event length
	e2EventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(e2EventLen))

	//insert e2 header, next position
	e2NxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e1NxtPos+e2EventLen, e2NxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	//insert e2 data, start time stamp
	e2st := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e2st, int64(300))
	pos += int(unsafe.Sizeof(e2st))

	//insert e2 data, end time stamp
	e2et := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e2et, int64(400))
	pos += int(unsafe.Sizeof(e2et))

	//insert e2, payload
	e2Payload := buf[pos:]
	e2r, err := NewPayloadReader(schemapb.DataType_INT64, e2Payload)
	assert.Nil(t, err)
	e2a, err := e2r.GetInt64FromPayload()
	assert.Nil(t, err)
	assert.Equal(t, e2a, []int64{7, 8, 9, 10, 11, 12})
	err = e2r.Close()
	assert.Nil(t, err)

	assert.Equal(t, int(e2NxtPos), len(buf))

	//read binlog
	r, err := NewBinlogReader(buf)
	assert.Nil(t, err)
	event1, err := r.NextEventReader()
	assert.Nil(t, err)
	assert.NotNil(t, event1)
	p1, err := event1.GetInt64FromPayload()
	assert.Equal(t, p1, []int64{1, 2, 3, 4, 5, 6})
	assert.Nil(t, err)
	assert.Equal(t, event1.TypeCode, InsertEventType)
	ed1, ok := (event1.eventData).(*insertEventData)
	assert.True(t, ok)
	assert.Equal(t, ed1.StartTimestamp, Timestamp(100))
	assert.Equal(t, ed1.EndTimestamp, Timestamp(200))

	event2, err := r.NextEventReader()
	assert.Nil(t, err)
	assert.NotNil(t, event2)
	p2, err := event2.GetInt64FromPayload()
	assert.Nil(t, err)
	assert.Equal(t, p2, []int64{7, 8, 9, 10, 11, 12})
	assert.Equal(t, event2.TypeCode, InsertEventType)
	ed2, ok := (event2.eventData).(*insertEventData)
	assert.True(t, ok)
	_, ok = (event2.eventData).(*deleteEventData)
	assert.False(t, ok)
	assert.Equal(t, ed2.StartTimestamp, Timestamp(300))
	assert.Equal(t, ed2.EndTimestamp, Timestamp(400))
}

func TestDeleteBinlog(t *testing.T) {
	w, err := NewDeleteBinlogWriter(schemapb.DataType_INT64, 50)
	assert.Nil(t, err)

	e1, err := w.NextDeleteEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6})
	assert.NotNil(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6})
	assert.Nil(t, err)
	e1.SetStartTimestamp(100)
	e1.SetEndTimestamp(200)

	e2, err := w.NextDeleteEventWriter()
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]int64{7, 8, 9})
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]bool{true, false, true})
	assert.NotNil(t, err)
	err = e2.AddDataToPayload([]int64{10, 11, 12})
	assert.Nil(t, err)
	e2.SetStartTimestamp(300)
	e2.SetEndTimestamp(400)

	w.SetStartTimeStamp(1000)
	w.SetEndTimeStamp(2000)

	_, err = w.GetBuffer()
	assert.NotNil(t, err)
	err = w.Close()
	assert.Nil(t, err)
	buf, err := w.GetBuffer()
	assert.Nil(t, err)

	//magic number
	magicNum := UnsafeReadInt32(buf, 0)
	assert.Equal(t, magicNum, MagicNumber)
	pos := int(unsafe.Sizeof(MagicNumber))

	//descriptor header, timestamp
	ts := UnsafeReadInt64(buf, pos)
	assert.Greater(t, ts, int64(0))
	curts := time.Now().UnixNano() / int64(time.Millisecond)
	curts = int64(tsoutil.ComposeTS(curts, 0))
	diffts := curts - ts
	maxdiff := int64(tsoutil.ComposeTS(1000, 0))
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(ts))

	//descriptor header, type code
	tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(tc), DescriptorEventType)
	pos += int(unsafe.Sizeof(tc))

	//descriptor header, server id
	svrID := UnsafeReadInt32(buf, pos)
	assert.Equal(t, svrID, int32(ServerID))
	pos += int(unsafe.Sizeof(svrID))

	//descriptor header, event length
	descEventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(descEventLen))

	//descriptor header, next position
	descNxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descEventLen+int32(unsafe.Sizeof(MagicNumber)), descNxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	//descriptor data fix, binlog version
	binLogVer := UnsafeReadInt16(buf, pos)
	assert.Equal(t, binLogVer, int16(BinlogVersion))
	pos += int(unsafe.Sizeof(binLogVer))

	//descriptor data fix, server version
	svrVer := UnsafeReadInt64(buf, pos)
	assert.Equal(t, svrVer, int64(ServerVersion))
	pos += int(unsafe.Sizeof(svrVer))

	//descriptor data fix, commit id
	cmitID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, cmitID, int64(CommitID))
	pos += int(unsafe.Sizeof(cmitID))

	//descriptor data fix, header length
	headLen := UnsafeReadInt8(buf, pos)
	assert.Equal(t, headLen, int8(binary.Size(eventHeader{})))
	pos += int(unsafe.Sizeof(headLen))

	//descriptor data fix, collection id
	collID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, collID, int64(50))
	pos += int(unsafe.Sizeof(collID))

	//descriptor data fix, partition id
	partID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, partID, int64(-1))
	pos += int(unsafe.Sizeof(partID))

	//descriptor data fix, segment id
	segID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, segID, int64(-1))
	pos += int(unsafe.Sizeof(segID))

	//descriptor data fix, field id
	fieldID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, fieldID, int64(-1))
	pos += int(unsafe.Sizeof(fieldID))

	//descriptor data fix, start time stamp
	startts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, startts, int64(1000))
	pos += int(unsafe.Sizeof(startts))

	//descriptor data fix, end time stamp
	endts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, endts, int64(2000))
	pos += int(unsafe.Sizeof(endts))

	//descriptor data fix, payload type
	colType := UnsafeReadInt32(buf, pos)
	assert.Equal(t, schemapb.DataType(colType), schemapb.DataType_INT64)
	pos += int(unsafe.Sizeof(colType))

	//descriptor data, post header lengths
	for i := DescriptorEventType; i < EventTypeEnd; i++ {
		size := getEventFixPartSize(i)
		assert.Equal(t, uint8(size), buf[pos])
		pos++
	}

	//start of e1
	assert.Equal(t, pos, int(descNxtPos))

	//insert e1 header, Timestamp
	e1ts := UnsafeReadInt64(buf, pos)
	diffts = curts - e1ts
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(e1ts))

	//insert e1 header, type code
	e1tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(e1tc), DeleteEventType)
	pos += int(unsafe.Sizeof(e1tc))

	//insert e1 header, Server id
	e1svrID := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e1svrID, int32(ServerID))
	pos += int(unsafe.Sizeof(e1svrID))

	//insert e1 header, event length
	e1EventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(e1EventLen))

	//insert e1 header, next position
	e1NxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descNxtPos+e1EventLen, e1NxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	//insert e1 data, start time stamp
	e1st := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e1st, int64(100))
	pos += int(unsafe.Sizeof(e1st))

	//insert e1 data, end time stamp
	e1et := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e1et, int64(200))
	pos += int(unsafe.Sizeof(e1et))

	//insert e1, payload
	e1Payload := buf[pos:e1NxtPos]
	e1r, err := NewPayloadReader(schemapb.DataType_INT64, e1Payload)
	assert.Nil(t, err)
	e1a, err := e1r.GetInt64FromPayload()
	assert.Nil(t, err)
	assert.Equal(t, e1a, []int64{1, 2, 3, 4, 5, 6})
	err = e1r.Close()
	assert.Nil(t, err)

	//start of e2
	pos = int(e1NxtPos)

	//insert e2 header, Timestamp
	e2ts := UnsafeReadInt64(buf, pos)
	diffts = curts - e2ts
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(e2ts))

	//insert e2 header, type code
	e2tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(e2tc), DeleteEventType)
	pos += int(unsafe.Sizeof(e2tc))

	//insert e2 header, Server id
	e2svrID := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e2svrID, int32(ServerID))
	pos += int(unsafe.Sizeof(e2svrID))

	//insert e2 header, event length
	e2EventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(e2EventLen))

	//insert e2 header, next position
	e2NxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e1NxtPos+e2EventLen, e2NxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	//insert e2 data, start time stamp
	e2st := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e2st, int64(300))
	pos += int(unsafe.Sizeof(e2st))

	//insert e2 data, end time stamp
	e2et := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e2et, int64(400))
	pos += int(unsafe.Sizeof(e2et))

	//insert e2, payload
	e2Payload := buf[pos:]
	e2r, err := NewPayloadReader(schemapb.DataType_INT64, e2Payload)
	assert.Nil(t, err)
	e2a, err := e2r.GetInt64FromPayload()
	assert.Nil(t, err)
	assert.Equal(t, e2a, []int64{7, 8, 9, 10, 11, 12})
	err = e2r.Close()
	assert.Nil(t, err)

	assert.Equal(t, int(e2NxtPos), len(buf))

	//read binlog
	r, err := NewBinlogReader(buf)
	assert.Nil(t, err)
	event1, err := r.NextEventReader()
	assert.Nil(t, err)
	assert.NotNil(t, event1)
	p1, err := event1.GetInt64FromPayload()
	assert.Equal(t, p1, []int64{1, 2, 3, 4, 5, 6})
	assert.Nil(t, err)
	assert.Equal(t, event1.TypeCode, DeleteEventType)
	ed1, ok := (event1.eventData).(*deleteEventData)
	assert.True(t, ok)
	assert.Equal(t, ed1.StartTimestamp, Timestamp(100))
	assert.Equal(t, ed1.EndTimestamp, Timestamp(200))

	event2, err := r.NextEventReader()
	assert.Nil(t, err)
	assert.NotNil(t, event2)
	p2, err := event2.GetInt64FromPayload()
	assert.Nil(t, err)
	assert.Equal(t, p2, []int64{7, 8, 9, 10, 11, 12})
	assert.Equal(t, event2.TypeCode, DeleteEventType)
	ed2, ok := (event2.eventData).(*deleteEventData)
	assert.True(t, ok)
	_, ok = (event2.eventData).(*insertEventData)
	assert.False(t, ok)
	assert.Equal(t, ed2.StartTimestamp, Timestamp(300))
	assert.Equal(t, ed2.EndTimestamp, Timestamp(400))
}

func TestDDLBinlog1(t *testing.T) {
	w, err := NewDDLBinlogWriter(schemapb.DataType_INT64, 50)
	assert.Nil(t, err)

	e1, err := w.NextCreateCollectionEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6})
	assert.NotNil(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6})
	assert.Nil(t, err)
	e1.SetStartTimestamp(100)
	e1.SetEndTimestamp(200)

	e2, err := w.NextDropCollectionEventWriter()
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]int64{7, 8, 9})
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]bool{true, false, true})
	assert.NotNil(t, err)
	err = e2.AddDataToPayload([]int64{10, 11, 12})
	assert.Nil(t, err)
	e2.SetStartTimestamp(300)
	e2.SetEndTimestamp(400)

	w.SetStartTimeStamp(1000)
	w.SetEndTimeStamp(2000)

	_, err = w.GetBuffer()
	assert.NotNil(t, err)
	err = w.Close()
	assert.Nil(t, err)
	buf, err := w.GetBuffer()
	assert.Nil(t, err)

	//magic number
	magicNum := UnsafeReadInt32(buf, 0)
	assert.Equal(t, magicNum, MagicNumber)
	pos := int(unsafe.Sizeof(MagicNumber))

	//descriptor header, timestamp
	ts := UnsafeReadInt64(buf, pos)
	assert.Greater(t, ts, int64(0))
	curts := time.Now().UnixNano() / int64(time.Millisecond)
	curts = int64(tsoutil.ComposeTS(curts, 0))
	diffts := curts - ts
	maxdiff := int64(tsoutil.ComposeTS(1000, 0))
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(ts))

	//descriptor header, type code
	tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(tc), DescriptorEventType)
	pos += int(unsafe.Sizeof(tc))

	//descriptor header, server id
	svrID := UnsafeReadInt32(buf, pos)
	assert.Equal(t, svrID, int32(ServerID))
	pos += int(unsafe.Sizeof(svrID))

	//descriptor header, event length
	descEventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(descEventLen))

	//descriptor header, next position
	descNxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descEventLen+int32(unsafe.Sizeof(MagicNumber)), descNxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	//descriptor data fix, binlog version
	binLogVer := UnsafeReadInt16(buf, pos)
	assert.Equal(t, binLogVer, int16(BinlogVersion))
	pos += int(unsafe.Sizeof(binLogVer))

	//descriptor data fix, server version
	svrVer := UnsafeReadInt64(buf, pos)
	assert.Equal(t, svrVer, int64(ServerVersion))
	pos += int(unsafe.Sizeof(svrVer))

	//descriptor data fix, commit id
	cmitID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, cmitID, int64(CommitID))
	pos += int(unsafe.Sizeof(cmitID))

	//descriptor data fix, header length
	headLen := UnsafeReadInt8(buf, pos)
	assert.Equal(t, headLen, int8(binary.Size(eventHeader{})))
	pos += int(unsafe.Sizeof(headLen))

	//descriptor data fix, collection id
	collID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, collID, int64(50))
	pos += int(unsafe.Sizeof(collID))

	//descriptor data fix, partition id
	partID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, partID, int64(-1))
	pos += int(unsafe.Sizeof(partID))

	//descriptor data fix, segment id
	segID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, segID, int64(-1))
	pos += int(unsafe.Sizeof(segID))

	//descriptor data fix, field id
	fieldID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, fieldID, int64(-1))
	pos += int(unsafe.Sizeof(fieldID))

	//descriptor data fix, start time stamp
	startts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, startts, int64(1000))
	pos += int(unsafe.Sizeof(startts))

	//descriptor data fix, end time stamp
	endts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, endts, int64(2000))
	pos += int(unsafe.Sizeof(endts))

	//descriptor data fix, payload type
	colType := UnsafeReadInt32(buf, pos)
	assert.Equal(t, schemapb.DataType(colType), schemapb.DataType_INT64)
	pos += int(unsafe.Sizeof(colType))

	//descriptor data, post header lengths
	for i := DescriptorEventType; i < EventTypeEnd; i++ {
		size := getEventFixPartSize(i)
		assert.Equal(t, uint8(size), buf[pos])
		pos++
	}

	//start of e1
	assert.Equal(t, pos, int(descNxtPos))

	//insert e1 header, Timestamp
	e1ts := UnsafeReadInt64(buf, pos)
	diffts = curts - e1ts
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(e1ts))

	//insert e1 header, type code
	e1tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(e1tc), CreateCollectionEventType)
	pos += int(unsafe.Sizeof(e1tc))

	//insert e1 header, Server id
	e1svrID := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e1svrID, int32(ServerID))
	pos += int(unsafe.Sizeof(e1svrID))

	//insert e1 header, event length
	e1EventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(e1EventLen))

	//insert e1 header, next position
	e1NxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descNxtPos+e1EventLen, e1NxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	//insert e1 data, start time stamp
	e1st := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e1st, int64(100))
	pos += int(unsafe.Sizeof(e1st))

	//insert e1 data, end time stamp
	e1et := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e1et, int64(200))
	pos += int(unsafe.Sizeof(e1et))

	//insert e1, payload
	e1Payload := buf[pos:e1NxtPos]
	e1r, err := NewPayloadReader(schemapb.DataType_INT64, e1Payload)
	assert.Nil(t, err)
	e1a, err := e1r.GetInt64FromPayload()
	assert.Nil(t, err)
	assert.Equal(t, e1a, []int64{1, 2, 3, 4, 5, 6})
	err = e1r.Close()
	assert.Nil(t, err)

	//start of e2
	pos = int(e1NxtPos)

	//insert e2 header, Timestamp
	e2ts := UnsafeReadInt64(buf, pos)
	diffts = curts - e2ts
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(e2ts))

	//insert e2 header, type code
	e2tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(e2tc), DropCollectionEventType)
	pos += int(unsafe.Sizeof(e2tc))

	//insert e2 header, Server id
	e2svrID := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e2svrID, int32(ServerID))
	pos += int(unsafe.Sizeof(e2svrID))

	//insert e2 header, event length
	e2EventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(e2EventLen))

	//insert e2 header, next position
	e2NxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e1NxtPos+e2EventLen, e2NxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	//insert e2 data, start time stamp
	e2st := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e2st, int64(300))
	pos += int(unsafe.Sizeof(e2st))

	//insert e2 data, end time stamp
	e2et := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e2et, int64(400))
	pos += int(unsafe.Sizeof(e2et))

	//insert e2, payload
	e2Payload := buf[pos:]
	e2r, err := NewPayloadReader(schemapb.DataType_INT64, e2Payload)
	assert.Nil(t, err)
	e2a, err := e2r.GetInt64FromPayload()
	assert.Nil(t, err)
	assert.Equal(t, e2a, []int64{7, 8, 9, 10, 11, 12})
	err = e2r.Close()
	assert.Nil(t, err)

	assert.Equal(t, int(e2NxtPos), len(buf))

	//read binlog
	r, err := NewBinlogReader(buf)
	assert.Nil(t, err)
	event1, err := r.NextEventReader()
	assert.Nil(t, err)
	assert.NotNil(t, event1)
	p1, err := event1.GetInt64FromPayload()
	assert.Equal(t, p1, []int64{1, 2, 3, 4, 5, 6})
	assert.Nil(t, err)
	assert.Equal(t, event1.TypeCode, CreateCollectionEventType)
	ed1, ok := (event1.eventData).(*createCollectionEventData)
	assert.True(t, ok)
	assert.Equal(t, ed1.StartTimestamp, Timestamp(100))
	assert.Equal(t, ed1.EndTimestamp, Timestamp(200))

	event2, err := r.NextEventReader()
	assert.Nil(t, err)
	assert.NotNil(t, event2)
	p2, err := event2.GetInt64FromPayload()
	assert.Nil(t, err)
	assert.Equal(t, p2, []int64{7, 8, 9, 10, 11, 12})
	assert.Equal(t, event2.TypeCode, DropCollectionEventType)
	ed2, ok := (event2.eventData).(*dropCollectionEventData)
	assert.True(t, ok)
	_, ok = (event2.eventData).(*insertEventData)
	assert.False(t, ok)
	assert.Equal(t, ed2.StartTimestamp, Timestamp(300))
	assert.Equal(t, ed2.EndTimestamp, Timestamp(400))
}

func TestDDLBinlog2(t *testing.T) {
	w, err := NewDDLBinlogWriter(schemapb.DataType_INT64, 50)
	assert.Nil(t, err)

	e1, err := w.NextCreatePartitionEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6})
	assert.NotNil(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6})
	assert.Nil(t, err)
	e1.SetStartTimestamp(100)
	e1.SetEndTimestamp(200)

	e2, err := w.NextDropPartitionEventWriter()
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]int64{7, 8, 9})
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]bool{true, false, true})
	assert.NotNil(t, err)
	err = e2.AddDataToPayload([]int64{10, 11, 12})
	assert.Nil(t, err)
	e2.SetStartTimestamp(300)
	e2.SetEndTimestamp(400)

	w.SetStartTimeStamp(1000)
	w.SetEndTimeStamp(2000)

	_, err = w.GetBuffer()
	assert.NotNil(t, err)
	err = w.Close()
	assert.Nil(t, err)
	buf, err := w.GetBuffer()
	assert.Nil(t, err)

	//magic number
	magicNum := UnsafeReadInt32(buf, 0)
	assert.Equal(t, magicNum, MagicNumber)
	pos := int(unsafe.Sizeof(MagicNumber))

	//descriptor header, timestamp
	ts := UnsafeReadInt64(buf, pos)
	assert.Greater(t, ts, int64(0))
	curts := time.Now().UnixNano() / int64(time.Millisecond)
	curts = int64(tsoutil.ComposeTS(curts, 0))
	diffts := curts - ts
	maxdiff := int64(tsoutil.ComposeTS(1000, 0))
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(ts))

	//descriptor header, type code
	tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(tc), DescriptorEventType)
	pos += int(unsafe.Sizeof(tc))

	//descriptor header, server id
	svrID := UnsafeReadInt32(buf, pos)
	assert.Equal(t, svrID, int32(ServerID))
	pos += int(unsafe.Sizeof(svrID))

	//descriptor header, event length
	descEventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(descEventLen))

	//descriptor header, next position
	descNxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descEventLen+int32(unsafe.Sizeof(MagicNumber)), descNxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	//descriptor data fix, binlog version
	binLogVer := UnsafeReadInt16(buf, pos)
	assert.Equal(t, binLogVer, int16(BinlogVersion))
	pos += int(unsafe.Sizeof(binLogVer))

	//descriptor data fix, server version
	svrVer := UnsafeReadInt64(buf, pos)
	assert.Equal(t, svrVer, int64(ServerVersion))
	pos += int(unsafe.Sizeof(svrVer))

	//descriptor data fix, commit id
	cmitID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, cmitID, int64(CommitID))
	pos += int(unsafe.Sizeof(cmitID))

	//descriptor data fix, header length
	headLen := UnsafeReadInt8(buf, pos)
	assert.Equal(t, headLen, int8(binary.Size(eventHeader{})))
	pos += int(unsafe.Sizeof(headLen))

	//descriptor data fix, collection id
	collID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, collID, int64(50))
	pos += int(unsafe.Sizeof(collID))

	//descriptor data fix, partition id
	partID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, partID, int64(-1))
	pos += int(unsafe.Sizeof(partID))

	//descriptor data fix, segment id
	segID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, segID, int64(-1))
	pos += int(unsafe.Sizeof(segID))

	//descriptor data fix, field id
	fieldID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, fieldID, int64(-1))
	pos += int(unsafe.Sizeof(fieldID))

	//descriptor data fix, start time stamp
	startts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, startts, int64(1000))
	pos += int(unsafe.Sizeof(startts))

	//descriptor data fix, end time stamp
	endts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, endts, int64(2000))
	pos += int(unsafe.Sizeof(endts))

	//descriptor data fix, payload type
	colType := UnsafeReadInt32(buf, pos)
	assert.Equal(t, schemapb.DataType(colType), schemapb.DataType_INT64)
	pos += int(unsafe.Sizeof(colType))

	//descriptor data, post header lengths
	for i := DescriptorEventType; i < EventTypeEnd; i++ {
		size := getEventFixPartSize(i)
		assert.Equal(t, uint8(size), buf[pos])
		pos++
	}

	//start of e1
	assert.Equal(t, pos, int(descNxtPos))

	//insert e1 header, Timestamp
	e1ts := UnsafeReadInt64(buf, pos)
	diffts = curts - e1ts
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(e1ts))

	//insert e1 header, type code
	e1tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(e1tc), CreatePartitionEventType)
	pos += int(unsafe.Sizeof(e1tc))

	//insert e1 header, Server id
	e1svrID := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e1svrID, int32(ServerID))
	pos += int(unsafe.Sizeof(e1svrID))

	//insert e1 header, event length
	e1EventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(e1EventLen))

	//insert e1 header, next position
	e1NxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descNxtPos+e1EventLen, e1NxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	//insert e1 data, start time stamp
	e1st := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e1st, int64(100))
	pos += int(unsafe.Sizeof(e1st))

	//insert e1 data, end time stamp
	e1et := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e1et, int64(200))
	pos += int(unsafe.Sizeof(e1et))

	//insert e1, payload
	e1Payload := buf[pos:e1NxtPos]
	e1r, err := NewPayloadReader(schemapb.DataType_INT64, e1Payload)
	assert.Nil(t, err)
	e1a, err := e1r.GetInt64FromPayload()
	assert.Nil(t, err)
	assert.Equal(t, e1a, []int64{1, 2, 3, 4, 5, 6})
	err = e1r.Close()
	assert.Nil(t, err)

	//start of e2
	pos = int(e1NxtPos)

	//insert e2 header, Timestamp
	e2ts := UnsafeReadInt64(buf, pos)
	diffts = curts - e2ts
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(e2ts))

	//insert e2 header, type code
	e2tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(e2tc), DropPartitionEventType)
	pos += int(unsafe.Sizeof(e2tc))

	//insert e2 header, Server id
	e2svrID := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e2svrID, int32(ServerID))
	pos += int(unsafe.Sizeof(e2svrID))

	//insert e2 header, event length
	e2EventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(e2EventLen))

	//insert e2 header, next position
	e2NxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e1NxtPos+e2EventLen, e2NxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	//insert e2 data, start time stamp
	e2st := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e2st, int64(300))
	pos += int(unsafe.Sizeof(e2st))

	//insert e2 data, end time stamp
	e2et := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e2et, int64(400))
	pos += int(unsafe.Sizeof(e2et))

	//insert e2, payload
	e2Payload := buf[pos:]
	e2r, err := NewPayloadReader(schemapb.DataType_INT64, e2Payload)
	assert.Nil(t, err)
	e2a, err := e2r.GetInt64FromPayload()
	assert.Nil(t, err)
	assert.Equal(t, e2a, []int64{7, 8, 9, 10, 11, 12})
	err = e2r.Close()
	assert.Nil(t, err)

	assert.Equal(t, int(e2NxtPos), len(buf))

	//read binlog
	r, err := NewBinlogReader(buf)
	assert.Nil(t, err)
	event1, err := r.NextEventReader()
	assert.Nil(t, err)
	assert.NotNil(t, event1)
	p1, err := event1.GetInt64FromPayload()
	assert.Equal(t, p1, []int64{1, 2, 3, 4, 5, 6})
	assert.Nil(t, err)
	assert.Equal(t, event1.TypeCode, CreatePartitionEventType)
	ed1, ok := (event1.eventData).(*createPartitionEventData)
	assert.True(t, ok)
	assert.Equal(t, ed1.StartTimestamp, Timestamp(100))
	assert.Equal(t, ed1.EndTimestamp, Timestamp(200))

	event2, err := r.NextEventReader()
	assert.Nil(t, err)
	assert.NotNil(t, event2)
	p2, err := event2.GetInt64FromPayload()
	assert.Nil(t, err)
	assert.Equal(t, p2, []int64{7, 8, 9, 10, 11, 12})
	assert.Equal(t, event2.TypeCode, DropPartitionEventType)
	ed2, ok := (event2.eventData).(*dropPartitionEventData)
	assert.True(t, ok)
	_, ok = (event2.eventData).(*insertEventData)
	assert.False(t, ok)
	assert.Equal(t, ed2.StartTimestamp, Timestamp(300))
	assert.Equal(t, ed2.EndTimestamp, Timestamp(400))
}
