// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/stretchr/testify/assert"
)

/* #nosec G103 */
func TestInsertBinlog(t *testing.T) {
	w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)

	e1, err := w.NextInsertEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6})
	assert.NotNil(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6})
	assert.Nil(t, err)
	e1.SetEventTimestamp(100, 200)

	e2, err := w.NextInsertEventWriter()
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]int64{7, 8, 9})
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]bool{true, false, true})
	assert.NotNil(t, err)
	err = e2.AddDataToPayload([]int64{10, 11, 12})
	assert.Nil(t, err)
	e2.SetEventTimestamp(300, 400)

	w.SetEventTimeStamp(1000, 2000)

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
	assert.Equal(t, schemapb.DataType(colType), schemapb.DataType_Int64)
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
	e1r, err := NewPayloadReader(schemapb.DataType_Int64, e1Payload)
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
	e2r, err := NewPayloadReader(schemapb.DataType_Int64, e2Payload)
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

/* #nosec G103 */
func TestDeleteBinlog(t *testing.T) {
	w := NewDeleteBinlogWriter(schemapb.DataType_Int64, 50)

	e1, err := w.NextDeleteEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6})
	assert.NotNil(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6})
	assert.Nil(t, err)
	e1.SetEventTimestamp(100, 200)

	e2, err := w.NextDeleteEventWriter()
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]int64{7, 8, 9})
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]bool{true, false, true})
	assert.NotNil(t, err)
	err = e2.AddDataToPayload([]int64{10, 11, 12})
	assert.Nil(t, err)
	e2.SetEventTimestamp(300, 400)

	w.SetEventTimeStamp(1000, 2000)

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
	assert.Equal(t, schemapb.DataType(colType), schemapb.DataType_Int64)
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
	e1r, err := NewPayloadReader(schemapb.DataType_Int64, e1Payload)
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
	e2r, err := NewPayloadReader(schemapb.DataType_Int64, e2Payload)
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

/* #nosec G103 */
func TestDDLBinlog1(t *testing.T) {
	w := NewDDLBinlogWriter(schemapb.DataType_Int64, 50)

	e1, err := w.NextCreateCollectionEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6})
	assert.NotNil(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6})
	assert.Nil(t, err)
	e1.SetEventTimestamp(100, 200)

	e2, err := w.NextDropCollectionEventWriter()
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]int64{7, 8, 9})
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]bool{true, false, true})
	assert.NotNil(t, err)
	err = e2.AddDataToPayload([]int64{10, 11, 12})
	assert.Nil(t, err)
	e2.SetEventTimestamp(300, 400)

	w.SetEventTimeStamp(1000, 2000)

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
	assert.Equal(t, schemapb.DataType(colType), schemapb.DataType_Int64)
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
	e1r, err := NewPayloadReader(schemapb.DataType_Int64, e1Payload)
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
	e2r, err := NewPayloadReader(schemapb.DataType_Int64, e2Payload)
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

/* #nosec G103 */
func TestDDLBinlog2(t *testing.T) {
	w := NewDDLBinlogWriter(schemapb.DataType_Int64, 50)

	e1, err := w.NextCreatePartitionEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6})
	assert.NotNil(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6})
	assert.Nil(t, err)
	e1.SetEventTimestamp(100, 200)

	e2, err := w.NextDropPartitionEventWriter()
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]int64{7, 8, 9})
	assert.Nil(t, err)
	err = e2.AddDataToPayload([]bool{true, false, true})
	assert.NotNil(t, err)
	err = e2.AddDataToPayload([]int64{10, 11, 12})
	assert.Nil(t, err)
	e2.SetEventTimestamp(300, 400)

	w.SetEventTimeStamp(1000, 2000)

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
	assert.Equal(t, schemapb.DataType(colType), schemapb.DataType_Int64)
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
	e1r, err := NewPayloadReader(schemapb.DataType_Int64, e1Payload)
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
	e2r, err := NewPayloadReader(schemapb.DataType_Int64, e2Payload)
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

func TestNewBinlogReaderError(t *testing.T) {
	data := []byte{}
	reader, err := NewBinlogReader(data)
	assert.Nil(t, reader)
	assert.NotNil(t, err)

	data = []byte{0, 0, 0, 0}
	reader, err = NewBinlogReader(data)
	assert.Nil(t, reader)
	assert.NotNil(t, err)

	buffer := new(bytes.Buffer)
	err = binary.Write(buffer, binary.LittleEndian, int32(MagicNumber))
	assert.Nil(t, err)
	data = buffer.Bytes()

	reader, err = NewBinlogReader(data)
	assert.Nil(t, reader)
	assert.NotNil(t, err)

	err = binary.Write(buffer, binary.LittleEndian, int32(555))
	assert.Nil(t, err)
	data = buffer.Bytes()

	reader, err = NewBinlogReader(data)
	assert.Nil(t, reader)
	assert.NotNil(t, err)

	w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)

	w.SetEventTimeStamp(1000, 2000)

	e1, err := w.NextInsertEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6})
	assert.NotNil(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6})
	assert.Nil(t, err)
	e1.SetEventTimestamp(100, 200)

	_, err = w.GetBuffer()
	assert.NotNil(t, err)
	err = w.Close()
	assert.Nil(t, err)

	buf, err := w.GetBuffer()
	assert.Nil(t, err)

	reader, err = NewBinlogReader(buf)
	assert.Nil(t, err)
	reader.Close()

	event1, err := reader.NextEventReader()
	assert.Nil(t, event1)
	assert.NotNil(t, err)

	err = reader.Close()
	assert.Nil(t, err)
}

func TestNewBinlogWriterTsError(t *testing.T) {
	w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)

	_, err := w.GetBuffer()
	assert.NotNil(t, err)
	err = w.Close()
	assert.NotNil(t, err)

	w.SetEventTimeStamp(1000, 0)
	_, err = w.GetBuffer()
	assert.NotNil(t, err)
	err = w.Close()
	assert.NotNil(t, err)

	w.SetEventTimeStamp(1000, 2000)
	_, err = w.GetBuffer()
	assert.NotNil(t, err)
	err = w.Close()
	assert.Nil(t, err)

	_, err = w.GetBuffer()
	assert.Nil(t, err)
}

func TestInsertBinlogWriterCloseError(t *testing.T) {
	insertWriter := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
	e1, err := insertWriter.NextInsertEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	e1.SetEventTimestamp(100, 200)
	insertWriter.SetEventTimeStamp(1000, 2000)
	err = insertWriter.Close()
	assert.Nil(t, err)
	assert.NotNil(t, insertWriter.buffer)
	insertEventWriter, err := insertWriter.NextInsertEventWriter()
	assert.Nil(t, insertEventWriter)
	assert.NotNil(t, err)
}

func TestDeleteBinlogWriteCloseError(t *testing.T) {
	deleteWriter := NewDeleteBinlogWriter(schemapb.DataType_Int64, 10)
	e1, err := deleteWriter.NextDeleteEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	e1.SetEventTimestamp(100, 200)
	deleteWriter.SetEventTimeStamp(1000, 2000)
	err = deleteWriter.Close()
	assert.Nil(t, err)
	assert.NotNil(t, deleteWriter.buffer)
	deleteEventWriter, err := deleteWriter.NextDeleteEventWriter()
	assert.Nil(t, deleteEventWriter)
	assert.NotNil(t, err)
}

func TestDDBinlogWriteCloseError(t *testing.T) {
	ddBinlogWriter := NewDDLBinlogWriter(schemapb.DataType_Int64, 10)
	e1, err := ddBinlogWriter.NextCreateCollectionEventWriter()
	assert.Nil(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3})
	assert.Nil(t, err)
	e1.SetEventTimestamp(100, 200)

	ddBinlogWriter.SetEventTimeStamp(1000, 2000)
	err = ddBinlogWriter.Close()
	assert.Nil(t, err)
	assert.NotNil(t, ddBinlogWriter.buffer)

	createCollectionEventWriter, err := ddBinlogWriter.NextCreateCollectionEventWriter()
	assert.Nil(t, createCollectionEventWriter)
	assert.NotNil(t, err)

	dropCollectionEventWriter, err := ddBinlogWriter.NextDropCollectionEventWriter()
	assert.Nil(t, dropCollectionEventWriter)
	assert.NotNil(t, err)

	createPartitionEventWriter, err := ddBinlogWriter.NextCreatePartitionEventWriter()
	assert.Nil(t, createPartitionEventWriter)
	assert.NotNil(t, err)

	dropPartitionEventWriter, err := ddBinlogWriter.NextDropPartitionEventWriter()
	assert.Nil(t, dropPartitionEventWriter)
	assert.NotNil(t, err)
}

type testEvent struct {
	PayloadWriterInterface
	finishError           bool
	writeError            bool
	getMemoryError        bool
	getPayloadLengthError bool
	releasePayloadError   bool
}

func (e *testEvent) Finish() error {
	if e.finishError {
		return fmt.Errorf("finish error")
	}
	return nil
}

func (e *testEvent) Close() error {
	return nil
}

func (e *testEvent) Write(buffer *bytes.Buffer) error {
	if e.writeError {
		return fmt.Errorf("write error")
	}
	return nil
}

func (e *testEvent) GetMemoryUsageInBytes() (int32, error) {
	if e.getMemoryError {
		return -1, fmt.Errorf("getMemory error")
	}
	return 0, nil
}
func (e *testEvent) GetPayloadLengthFromWriter() (int, error) {
	if e.getPayloadLengthError {
		return -1, fmt.Errorf("getPayloadLength error")
	}
	return 0, nil
}

func (e *testEvent) ReleasePayloadWriter() error {
	if e.releasePayloadError {
		return fmt.Errorf("releasePayload error")
	}
	return nil
}

func (e *testEvent) SetOffset(offset int32) {

}

var _ EventWriter = (*testEvent)(nil)

func TestWriterListError(t *testing.T) {
	insertWriter := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40)
	errorEvent := &testEvent{}
	insertWriter.eventWriters = append(insertWriter.eventWriters, errorEvent)
	insertWriter.SetEventTimeStamp(1000, 2000)
	errorEvent.releasePayloadError = true
	err := insertWriter.Close()
	assert.NotNil(t, err)
	insertWriter.buffer = nil
	errorEvent.getPayloadLengthError = true
	err = insertWriter.Close()
	assert.NotNil(t, err)
	insertWriter.buffer = nil
	errorEvent.getMemoryError = true
	err = insertWriter.Close()
	assert.NotNil(t, err)
	insertWriter.buffer = nil
	errorEvent.writeError = true
	err = insertWriter.Close()
	assert.NotNil(t, err)
	insertWriter.buffer = nil
	errorEvent.finishError = true
	err = insertWriter.Close()
	assert.NotNil(t, err)
}
