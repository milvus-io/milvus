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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v2/util/uniquegenerator"
)

/* #nosec G103 */
func TestInsertBinlog(t *testing.T) {
	w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40, false)

	e1, err := w.NextInsertEventWriter()
	assert.NoError(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3}, nil)
	assert.NoError(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6}, nil)
	assert.Error(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6}, nil)
	assert.NoError(t, err)
	e1.SetEventTimestamp(100, 200)

	e2, err := w.NextInsertEventWriter()
	assert.NoError(t, err)
	err = e2.AddDataToPayload([]int64{7, 8, 9}, nil)
	assert.NoError(t, err)
	err = e2.AddDataToPayload([]bool{true, false, true}, nil)
	assert.Error(t, err)
	err = e2.AddDataToPayload([]int64{10, 11, 12}, nil)
	assert.NoError(t, err)
	e2.SetEventTimestamp(300, 400)

	w.SetEventTimeStamp(1000, 2000)

	w.baseBinlogWriter.descriptorEventData.AddExtra("test", "testExtra")
	sizeTotal := 2000000
	w.baseBinlogWriter.descriptorEventData.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))

	_, err = w.GetBuffer()
	assert.Error(t, err)
	err = w.Finish()
	assert.NoError(t, err)
	buf, err := w.GetBuffer()
	assert.NoError(t, err)

	w.Close()

	// magic number
	magicNum := UnsafeReadInt32(buf, 0)
	assert.Equal(t, magicNum, MagicNumber)
	pos := int(unsafe.Sizeof(MagicNumber))

	// descriptor header, timestamp
	ts := UnsafeReadInt64(buf, pos)
	assert.Greater(t, ts, int64(0))
	curts := time.Now().UnixNano() / int64(time.Millisecond)
	curts = int64(tsoutil.ComposeTS(curts, 0))
	diffts := curts - ts
	maxdiff := int64(tsoutil.ComposeTS(1000, 0))
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(ts))

	// descriptor header, type code
	tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(tc), DescriptorEventType)
	pos += int(unsafe.Sizeof(tc))

	// descriptor header, event length
	descEventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(descEventLen))

	// descriptor header, next position
	descNxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descEventLen+int32(unsafe.Sizeof(MagicNumber)), descNxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	// descriptor data fix, collection id
	collID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, collID, int64(10))
	pos += int(unsafe.Sizeof(collID))

	// descriptor data fix, partition id
	partID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, partID, int64(20))
	pos += int(unsafe.Sizeof(partID))

	// descriptor data fix, segment id
	segID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, segID, int64(30))
	pos += int(unsafe.Sizeof(segID))

	// descriptor data fix, field id
	fieldID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, fieldID, int64(40))
	pos += int(unsafe.Sizeof(fieldID))

	// descriptor data fix, start time stamp
	startts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, startts, int64(1000))
	pos += int(unsafe.Sizeof(startts))

	// descriptor data fix, end time stamp
	endts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, endts, int64(2000))
	pos += int(unsafe.Sizeof(endts))

	// descriptor data fix, payload type
	colType := UnsafeReadInt32(buf, pos)
	assert.Equal(t, schemapb.DataType(colType), schemapb.DataType_Int64)
	pos += int(unsafe.Sizeof(colType))

	// descriptor data, post header lengths
	for i := DescriptorEventType; i < EventTypeEnd; i++ {
		size := getEventFixPartSize(i)
		assert.Equal(t, uint8(size), buf[pos])
		pos++
	}

	// descriptor data, extra length
	extraLength := UnsafeReadInt32(buf, pos)
	assert.Equal(t, extraLength, w.baseBinlogWriter.descriptorEventData.ExtraLength)
	pos += int(unsafe.Sizeof(extraLength))

	multiBytes := make([]byte, extraLength)
	for i := 0; i < int(extraLength); i++ {
		singleByte := UnsafeReadByte(buf, pos)
		multiBytes[i] = singleByte
		pos++
	}
	var extra map[string]interface{}
	err = json.Unmarshal(multiBytes, &extra)
	assert.NoError(t, err)
	testExtra, ok := extra["test"]
	assert.True(t, ok)
	assert.Equal(t, "testExtra", fmt.Sprintf("%v", testExtra))
	size, ok := extra[originalSizeKey]
	assert.True(t, ok)
	assert.Equal(t, fmt.Sprintf("%v", sizeTotal), fmt.Sprintf("%v", size))

	// start of e1
	assert.Equal(t, pos, int(descNxtPos))

	// insert e1 header, Timestamp
	e1ts := UnsafeReadInt64(buf, pos)
	diffts = curts - e1ts
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(e1ts))

	// insert e1 header, type code
	e1tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(e1tc), InsertEventType)
	pos += int(unsafe.Sizeof(e1tc))

	// insert e1 header, event length
	e1EventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(e1EventLen))

	// insert e1 header, next position
	e1NxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descNxtPos+e1EventLen, e1NxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	// insert e1 data, start time stamp
	e1st := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e1st, int64(100))
	pos += int(unsafe.Sizeof(e1st))

	// insert e1 data, end time stamp
	e1et := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e1et, int64(200))
	pos += int(unsafe.Sizeof(e1et))

	// insert e1, payload
	e1Payload := buf[pos:e1NxtPos]
	e1r, err := NewPayloadReader(schemapb.DataType_Int64, e1Payload, false)
	assert.NoError(t, err)
	e1a, valids, err := e1r.GetInt64FromPayload()
	assert.NoError(t, err)
	assert.Equal(t, e1a, []int64{1, 2, 3, 4, 5, 6})
	assert.Nil(t, valids)
	e1r.Close()

	// start of e2
	pos = int(e1NxtPos)

	// insert e2 header, Timestamp
	e2ts := UnsafeReadInt64(buf, pos)
	diffts = curts - e2ts
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(e2ts))

	// insert e2 header, type code
	e2tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(e2tc), InsertEventType)
	pos += int(unsafe.Sizeof(e2tc))

	// insert e2 header, event length
	e2EventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(e2EventLen))

	// insert e2 header, next position
	e2NxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e1NxtPos+e2EventLen, e2NxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	// insert e2 data, start time stamp
	e2st := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e2st, int64(300))
	pos += int(unsafe.Sizeof(e2st))

	// insert e2 data, end time stamp
	e2et := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e2et, int64(400))
	pos += int(unsafe.Sizeof(e2et))

	// insert e2, payload
	e2Payload := buf[pos:]
	e2r, err := NewPayloadReader(schemapb.DataType_Int64, e2Payload, false)
	assert.NoError(t, err)
	e2a, valids, err := e2r.GetInt64FromPayload()
	assert.NoError(t, err)
	assert.Equal(t, e2a, []int64{7, 8, 9, 10, 11, 12})
	assert.Nil(t, valids)
	e2r.Close()

	assert.Equal(t, int(e2NxtPos), len(buf))

	// read binlog
	r, err := NewBinlogReader(buf)
	assert.NoError(t, err)
	event1, err := r.NextEventReader()
	assert.NoError(t, err)
	assert.NotNil(t, event1)
	p1, valids, err := event1.GetInt64FromPayload()
	assert.Equal(t, p1, []int64{1, 2, 3, 4, 5, 6})
	assert.Nil(t, valids)
	assert.NoError(t, err)
	assert.Equal(t, event1.TypeCode, InsertEventType)
	ed1, ok := (event1.eventData).(*insertEventData)
	assert.True(t, ok)
	assert.Equal(t, ed1.StartTimestamp, Timestamp(100))
	assert.Equal(t, ed1.EndTimestamp, Timestamp(200))

	event2, err := r.NextEventReader()
	assert.NoError(t, err)
	assert.NotNil(t, event2)
	p2, valids, err := event2.GetInt64FromPayload()
	assert.NoError(t, err)
	assert.Equal(t, p2, []int64{7, 8, 9, 10, 11, 12})
	assert.Nil(t, valids)
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
	w := NewDeleteBinlogWriter(schemapb.DataType_Int64, 50, 1, 1)

	e1, err := w.NextDeleteEventWriter()
	assert.NoError(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3}, nil)
	assert.NoError(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6}, nil)
	assert.Error(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6}, nil)
	assert.NoError(t, err)
	e1.SetEventTimestamp(100, 200)

	e2, err := w.NextDeleteEventWriter()
	assert.NoError(t, err)
	err = e2.AddDataToPayload([]int64{7, 8, 9}, nil)
	assert.NoError(t, err)
	err = e2.AddDataToPayload([]bool{true, false, true}, nil)
	assert.Error(t, err)
	err = e2.AddDataToPayload([]int64{10, 11, 12}, nil)
	assert.NoError(t, err)
	e2.SetEventTimestamp(300, 400)

	w.SetEventTimeStamp(1000, 2000)

	w.baseBinlogWriter.descriptorEventData.AddExtra("test", "testExtra")
	sizeTotal := 2000000
	w.baseBinlogWriter.descriptorEventData.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))

	_, err = w.GetBuffer()
	assert.Error(t, err)
	err = w.Finish()
	assert.NoError(t, err)
	buf, err := w.GetBuffer()
	assert.NoError(t, err)

	w.Close()

	// magic number
	magicNum := UnsafeReadInt32(buf, 0)
	assert.Equal(t, magicNum, MagicNumber)
	pos := int(unsafe.Sizeof(MagicNumber))

	// descriptor header, timestamp
	ts := UnsafeReadInt64(buf, pos)
	assert.Greater(t, ts, int64(0))
	curts := time.Now().UnixNano() / int64(time.Millisecond)
	curts = int64(tsoutil.ComposeTS(curts, 0))
	diffts := curts - ts
	maxdiff := int64(tsoutil.ComposeTS(1000, 0))
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(ts))

	// descriptor header, type code
	tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(tc), DescriptorEventType)
	pos += int(unsafe.Sizeof(tc))

	// descriptor header, event length
	descEventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(descEventLen))

	// descriptor header, next position
	descNxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descEventLen+int32(unsafe.Sizeof(MagicNumber)), descNxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	// descriptor data fix, collection id
	collID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, collID, int64(50))
	pos += int(unsafe.Sizeof(collID))

	// descriptor data fix, partition id
	partID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, partID, int64(1))
	pos += int(unsafe.Sizeof(partID))

	// descriptor data fix, segment id
	segID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, segID, int64(1))
	pos += int(unsafe.Sizeof(segID))

	// descriptor data fix, field id
	fieldID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, fieldID, int64(-1))
	pos += int(unsafe.Sizeof(fieldID))

	// descriptor data fix, start time stamp
	startts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, startts, int64(1000))
	pos += int(unsafe.Sizeof(startts))

	// descriptor data fix, end time stamp
	endts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, endts, int64(2000))
	pos += int(unsafe.Sizeof(endts))

	// descriptor data fix, payload type
	colType := UnsafeReadInt32(buf, pos)
	assert.Equal(t, schemapb.DataType(colType), schemapb.DataType_Int64)
	pos += int(unsafe.Sizeof(colType))

	// descriptor data, post header lengths
	for i := DescriptorEventType; i < EventTypeEnd; i++ {
		size := getEventFixPartSize(i)
		assert.Equal(t, uint8(size), buf[pos])
		pos++
	}

	// descriptor data, extra length
	extraLength := UnsafeReadInt32(buf, pos)
	assert.Equal(t, extraLength, w.baseBinlogWriter.descriptorEventData.ExtraLength)
	pos += int(unsafe.Sizeof(extraLength))

	multiBytes := make([]byte, extraLength)
	for i := 0; i < int(extraLength); i++ {
		singleByte := UnsafeReadByte(buf, pos)
		multiBytes[i] = singleByte
		pos++
	}
	var extra map[string]interface{}
	err = json.Unmarshal(multiBytes, &extra)
	assert.NoError(t, err)
	testExtra, ok := extra["test"]
	assert.True(t, ok)
	assert.Equal(t, "testExtra", fmt.Sprintf("%v", testExtra))
	size, ok := extra[originalSizeKey]
	assert.True(t, ok)
	assert.Equal(t, fmt.Sprintf("%v", sizeTotal), fmt.Sprintf("%v", size))

	// start of e1
	assert.Equal(t, pos, int(descNxtPos))

	// insert e1 header, Timestamp
	e1ts := UnsafeReadInt64(buf, pos)
	diffts = curts - e1ts
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(e1ts))

	// insert e1 header, type code
	e1tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(e1tc), DeleteEventType)
	pos += int(unsafe.Sizeof(e1tc))

	// insert e1 header, event length
	e1EventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(e1EventLen))

	// insert e1 header, next position
	e1NxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descNxtPos+e1EventLen, e1NxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	// insert e1 data, start time stamp
	e1st := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e1st, int64(100))
	pos += int(unsafe.Sizeof(e1st))

	// insert e1 data, end time stamp
	e1et := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e1et, int64(200))
	pos += int(unsafe.Sizeof(e1et))

	// insert e1, payload
	e1Payload := buf[pos:e1NxtPos]
	e1r, err := NewPayloadReader(schemapb.DataType_Int64, e1Payload, false)
	assert.NoError(t, err)
	e1a, valids, err := e1r.GetInt64FromPayload()
	assert.NoError(t, err)
	assert.Equal(t, e1a, []int64{1, 2, 3, 4, 5, 6})
	assert.Nil(t, valids)
	e1r.Close()

	// start of e2
	pos = int(e1NxtPos)

	// insert e2 header, Timestamp
	e2ts := UnsafeReadInt64(buf, pos)
	diffts = curts - e2ts
	assert.LessOrEqual(t, diffts, maxdiff)
	pos += int(unsafe.Sizeof(e2ts))

	// insert e2 header, type code
	e2tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(e2tc), DeleteEventType)
	pos += int(unsafe.Sizeof(e2tc))

	// insert e2 header, event length
	e2EventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(e2EventLen))

	// insert e2 header, next position
	e2NxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, e1NxtPos+e2EventLen, e2NxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	// insert e2 data, start time stamp
	e2st := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e2st, int64(300))
	pos += int(unsafe.Sizeof(e2st))

	// insert e2 data, end time stamp
	e2et := UnsafeReadInt64(buf, pos)
	assert.Equal(t, e2et, int64(400))
	pos += int(unsafe.Sizeof(e2et))

	// insert e2, payload
	e2Payload := buf[pos:]
	e2r, err := NewPayloadReader(schemapb.DataType_Int64, e2Payload, false)
	assert.NoError(t, err)
	e2a, valids, err := e2r.GetInt64FromPayload()
	assert.NoError(t, err)
	assert.Nil(t, valids)
	assert.Equal(t, e2a, []int64{7, 8, 9, 10, 11, 12})
	e2r.Close()

	assert.Equal(t, int(e2NxtPos), len(buf))

	// read binlog
	r, err := NewBinlogReader(buf)
	assert.NoError(t, err)
	event1, err := r.NextEventReader()
	assert.NoError(t, err)
	assert.NotNil(t, event1)
	p1, valids, err := event1.GetInt64FromPayload()
	assert.Nil(t, valids)
	assert.Equal(t, p1, []int64{1, 2, 3, 4, 5, 6})
	assert.NoError(t, err)
	assert.Equal(t, event1.TypeCode, DeleteEventType)
	ed1, ok := (event1.eventData).(*deleteEventData)
	assert.True(t, ok)
	assert.Equal(t, ed1.StartTimestamp, Timestamp(100))
	assert.Equal(t, ed1.EndTimestamp, Timestamp(200))

	event2, err := r.NextEventReader()
	assert.NoError(t, err)
	assert.NotNil(t, event2)
	p2, valids, err := event2.GetInt64FromPayload()
	assert.Nil(t, valids)
	assert.NoError(t, err)
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
func TestIndexFileBinlog(t *testing.T) {
	indexBuildID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	version := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	collectionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	partitionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	segmentID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	fieldID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	indexName := funcutil.GenRandomStr()
	indexID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	key := funcutil.GenRandomStr()

	timestamp := Timestamp(time.Now().UnixNano())
	payload := funcutil.GenRandomBytesWithLength(10000)

	w := NewIndexFileBinlogWriter(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexName, indexID, key)
	w.PayloadDataType = schemapb.DataType_Int8

	e, err := w.NextIndexFileEventWriter()
	assert.NoError(t, err)
	err = e.AddByteToPayload(payload, nil)
	assert.NoError(t, err)
	e.SetEventTimestamp(timestamp, timestamp)

	w.SetEventTimeStamp(timestamp, timestamp)

	sizeTotal := 2000000
	w.baseBinlogWriter.descriptorEventData.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))

	_, err = w.GetBuffer()
	assert.Error(t, err)
	err = w.Finish()
	assert.NoError(t, err)
	buf, err := w.GetBuffer()
	assert.NoError(t, err)

	w.Close()

	// magic number
	magicNum := UnsafeReadInt32(buf, 0)
	assert.Equal(t, magicNum, MagicNumber)
	pos := int(unsafe.Sizeof(MagicNumber))

	// descriptor header, timestamp
	ts := UnsafeReadInt64(buf, pos)
	assert.Greater(t, ts, int64(0))
	pos += int(unsafe.Sizeof(ts))

	// descriptor header, type code
	tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(tc), DescriptorEventType)
	pos += int(unsafe.Sizeof(tc))

	// descriptor header, event length
	descEventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(descEventLen))

	// descriptor header, next position
	descNxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descEventLen+int32(unsafe.Sizeof(MagicNumber)), descNxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	// descriptor data fix, collection id
	collID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, collID, collectionID)
	pos += int(unsafe.Sizeof(collID))

	// descriptor data fix, partition id
	partID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, partID, partitionID)
	pos += int(unsafe.Sizeof(partID))

	// descriptor data fix, segment id
	segID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, segID, segmentID)
	pos += int(unsafe.Sizeof(segID))

	// descriptor data fix, field id
	fID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, fieldID, fID)
	pos += int(unsafe.Sizeof(fID))

	// descriptor data fix, start time stamp
	startts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, startts, int64(timestamp))
	pos += int(unsafe.Sizeof(startts))

	// descriptor data fix, end time stamp
	endts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, endts, int64(timestamp))
	pos += int(unsafe.Sizeof(endts))

	// descriptor data fix, payload type
	colType := UnsafeReadInt32(buf, pos)
	assert.Equal(t, schemapb.DataType(colType), schemapb.DataType_Int8)
	pos += int(unsafe.Sizeof(colType))

	// descriptor data, post header lengths
	for i := DescriptorEventType; i < EventTypeEnd; i++ {
		size := getEventFixPartSize(i)
		assert.Equal(t, uint8(size), buf[pos])
		pos++
	}

	// descriptor data, extra length
	extraLength := UnsafeReadInt32(buf, pos)
	assert.Equal(t, extraLength, w.baseBinlogWriter.descriptorEventData.ExtraLength)
	pos += int(unsafe.Sizeof(extraLength))

	multiBytes := make([]byte, extraLength)
	for i := 0; i < int(extraLength); i++ {
		singleByte := UnsafeReadByte(buf, pos)
		multiBytes[i] = singleByte
		pos++
	}
	j := make(map[string]interface{})
	err = json.Unmarshal(multiBytes, &j)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%v", indexBuildID), fmt.Sprintf("%v", j["indexBuildID"]))
	assert.Equal(t, fmt.Sprintf("%v", version), fmt.Sprintf("%v", j["version"]))
	assert.Equal(t, fmt.Sprintf("%v", indexName), fmt.Sprintf("%v", j["indexName"]))
	assert.Equal(t, fmt.Sprintf("%v", indexID), fmt.Sprintf("%v", j["indexID"]))
	assert.Equal(t, fmt.Sprintf("%v", key), fmt.Sprintf("%v", j["key"]))
	assert.Equal(t, fmt.Sprintf("%v", sizeTotal), fmt.Sprintf("%v", j[originalSizeKey]))

	// NextIndexFileBinlogWriter after close
	_, err = w.NextIndexFileEventWriter()
	assert.Error(t, err)
}

/* #nosec G103 */
func TestIndexFileBinlogV2(t *testing.T) {
	indexBuildID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	version := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	collectionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	partitionID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	segmentID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	fieldID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	indexName := funcutil.GenRandomStr()
	indexID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	key := funcutil.GenRandomStr()

	timestamp := Timestamp(time.Now().UnixNano())
	payload := funcutil.GenRandomBytes()

	w := NewIndexFileBinlogWriter(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexName, indexID, key)

	e, err := w.NextIndexFileEventWriter()
	assert.NoError(t, err)
	err = e.AddOneStringToPayload(typeutil.UnsafeBytes2str(payload), true)
	assert.NoError(t, err)
	e.SetEventTimestamp(timestamp, timestamp)

	w.SetEventTimeStamp(timestamp, timestamp)

	sizeTotal := 2000000
	w.baseBinlogWriter.descriptorEventData.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))

	_, err = w.GetBuffer()
	assert.Error(t, err)
	err = w.Finish()
	assert.NoError(t, err)
	buf, err := w.GetBuffer()
	assert.NoError(t, err)

	w.Close()

	// magic number
	magicNum := UnsafeReadInt32(buf, 0)
	assert.Equal(t, magicNum, MagicNumber)
	pos := int(unsafe.Sizeof(MagicNumber))

	// descriptor header, timestamp
	ts := UnsafeReadInt64(buf, pos)
	assert.Greater(t, ts, int64(0))
	pos += int(unsafe.Sizeof(ts))

	// descriptor header, type code
	tc := UnsafeReadInt8(buf, pos)
	assert.Equal(t, EventTypeCode(tc), DescriptorEventType)
	pos += int(unsafe.Sizeof(tc))

	// descriptor header, event length
	descEventLen := UnsafeReadInt32(buf, pos)
	pos += int(unsafe.Sizeof(descEventLen))

	// descriptor header, next position
	descNxtPos := UnsafeReadInt32(buf, pos)
	assert.Equal(t, descEventLen+int32(unsafe.Sizeof(MagicNumber)), descNxtPos)
	pos += int(unsafe.Sizeof(descNxtPos))

	// descriptor data fix, collection id
	collID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, collID, collectionID)
	pos += int(unsafe.Sizeof(collID))

	// descriptor data fix, partition id
	partID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, partID, partitionID)
	pos += int(unsafe.Sizeof(partID))

	// descriptor data fix, segment id
	segID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, segID, segmentID)
	pos += int(unsafe.Sizeof(segID))

	// descriptor data fix, field id
	fID := UnsafeReadInt64(buf, pos)
	assert.Equal(t, fieldID, fID)
	pos += int(unsafe.Sizeof(fID))

	// descriptor data fix, start time stamp
	startts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, startts, int64(timestamp))
	pos += int(unsafe.Sizeof(startts))

	// descriptor data fix, end time stamp
	endts := UnsafeReadInt64(buf, pos)
	assert.Equal(t, endts, int64(timestamp))
	pos += int(unsafe.Sizeof(endts))

	// descriptor data fix, payload type
	colType := UnsafeReadInt32(buf, pos)
	assert.Equal(t, schemapb.DataType(colType), schemapb.DataType_String)
	pos += int(unsafe.Sizeof(colType))

	// descriptor data, post header lengths
	for i := DescriptorEventType; i < EventTypeEnd; i++ {
		size := getEventFixPartSize(i)
		assert.Equal(t, uint8(size), buf[pos])
		pos++
	}

	// descriptor data, extra length
	extraLength := UnsafeReadInt32(buf, pos)
	assert.Equal(t, extraLength, w.baseBinlogWriter.descriptorEventData.ExtraLength)
	pos += int(unsafe.Sizeof(extraLength))

	multiBytes := make([]byte, extraLength)
	for i := 0; i < int(extraLength); i++ {
		singleByte := UnsafeReadByte(buf, pos)
		multiBytes[i] = singleByte
		pos++
	}
	j := make(map[string]interface{})
	err = json.Unmarshal(multiBytes, &j)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%v", indexBuildID), fmt.Sprintf("%v", j["indexBuildID"]))
	assert.Equal(t, fmt.Sprintf("%v", version), fmt.Sprintf("%v", j["version"]))
	assert.Equal(t, fmt.Sprintf("%v", indexName), fmt.Sprintf("%v", j["indexName"]))
	assert.Equal(t, fmt.Sprintf("%v", indexID), fmt.Sprintf("%v", j["indexID"]))
	assert.Equal(t, fmt.Sprintf("%v", key), fmt.Sprintf("%v", j["key"]))
	assert.Equal(t, fmt.Sprintf("%v", sizeTotal), fmt.Sprintf("%v", j[originalSizeKey]))

	// NextIndexFileBinlogWriter after close
	_, err = w.NextIndexFileEventWriter()
	assert.Error(t, err)
}

func TestNewBinlogReaderError(t *testing.T) {
	data := []byte{}
	reader, err := NewBinlogReader(data)
	assert.Nil(t, reader)
	assert.Error(t, err)

	data = []byte{0, 0, 0, 0}
	reader, err = NewBinlogReader(data)
	assert.Nil(t, reader)
	assert.Error(t, err)

	buffer := new(bytes.Buffer)
	err = binary.Write(buffer, common.Endian, MagicNumber)
	assert.NoError(t, err)
	data = buffer.Bytes()

	reader, err = NewBinlogReader(data)
	assert.Nil(t, reader)
	assert.Error(t, err)

	err = binary.Write(buffer, common.Endian, int32(555))
	assert.NoError(t, err)
	data = buffer.Bytes()

	reader, err = NewBinlogReader(data)
	assert.Nil(t, reader)
	assert.Error(t, err)

	w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40, false)

	w.SetEventTimeStamp(1000, 2000)

	e1, err := w.NextInsertEventWriter()
	assert.NoError(t, err)
	err = e1.AddDataToPayload([]int64{1, 2, 3}, nil)
	assert.NoError(t, err)
	err = e1.AddDataToPayload([]int32{4, 5, 6}, nil)
	assert.Error(t, err)
	err = e1.AddDataToPayload([]int64{4, 5, 6}, nil)
	assert.NoError(t, err)
	e1.SetEventTimestamp(100, 200)

	_, err = w.GetBuffer()
	assert.Error(t, err)

	sizeTotal := 2000000
	w.baseBinlogWriter.descriptorEventData.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))

	err = w.Finish()
	assert.NoError(t, err)

	buf, err := w.GetBuffer()
	assert.NoError(t, err)
	w.Close()

	reader, err = NewBinlogReader(buf)
	assert.NoError(t, err)
	reader.Close()

	event1, err := reader.NextEventReader()
	assert.Nil(t, event1)
	assert.Error(t, err)

	reader.Close()
}

func TestNewBinlogWriterTsError(t *testing.T) {
	w := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40, false)

	_, err := w.GetBuffer()
	assert.Error(t, err)
	err = w.Finish()
	assert.Error(t, err)

	sizeTotal := 2000000
	w.baseBinlogWriter.descriptorEventData.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))

	w.SetEventTimeStamp(1000, 0)
	_, err = w.GetBuffer()
	assert.Error(t, err)
	err = w.Finish()
	assert.Error(t, err)

	w.SetEventTimeStamp(1000, 2000)
	_, err = w.GetBuffer()
	assert.Error(t, err)
	err = w.Finish()
	assert.NoError(t, err)

	_, err = w.GetBuffer()
	assert.NoError(t, err)
	w.Close()
}

func TestInsertBinlogWriterCloseError(t *testing.T) {
	insertWriter := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40, false)
	e1, err := insertWriter.NextInsertEventWriter()
	assert.NoError(t, err)

	sizeTotal := 2000000
	insertWriter.baseBinlogWriter.descriptorEventData.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))

	err = e1.AddDataToPayload([]int64{1, 2, 3}, nil)
	assert.NoError(t, err)
	e1.SetEventTimestamp(100, 200)
	insertWriter.SetEventTimeStamp(1000, 2000)
	err = insertWriter.Finish()
	assert.NoError(t, err)
	assert.NotNil(t, insertWriter.buffer)
	insertEventWriter, err := insertWriter.NextInsertEventWriter()
	assert.Nil(t, insertEventWriter)
	assert.Error(t, err)
	insertWriter.Close()
}

func TestDeleteBinlogWriteCloseError(t *testing.T) {
	deleteWriter := NewDeleteBinlogWriter(schemapb.DataType_Int64, 10, 1, 1)
	e1, err := deleteWriter.NextDeleteEventWriter()
	assert.NoError(t, err)
	sizeTotal := 2000000
	deleteWriter.baseBinlogWriter.descriptorEventData.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))
	err = e1.AddDataToPayload([]int64{1, 2, 3}, nil)
	assert.NoError(t, err)
	e1.SetEventTimestamp(100, 200)
	deleteWriter.SetEventTimeStamp(1000, 2000)
	err = deleteWriter.Finish()
	assert.NoError(t, err)
	assert.NotNil(t, deleteWriter.buffer)
	deleteEventWriter, err := deleteWriter.NextDeleteEventWriter()
	assert.Nil(t, deleteEventWriter)
	assert.Error(t, err)
	deleteWriter.Close()
}

type testEvent struct {
	PayloadWriterInterface
	finishError           bool
	writeError            bool
	getMemoryError        bool
	getPayloadLengthError bool
}

func (e *testEvent) Finish() error {
	if e.finishError {
		return errors.New("finish error")
	}
	return nil
}

func (e *testEvent) Close() {
}

func (e *testEvent) Write(buffer *bytes.Buffer) error {
	if e.writeError {
		return errors.New("write error")
	}
	return nil
}

func (e *testEvent) GetMemoryUsageInBytes() (int32, error) {
	if e.getMemoryError {
		return -1, errors.New("getMemory error")
	}
	return 0, nil
}

func (e *testEvent) GetPayloadLengthFromWriter() (int, error) {
	if e.getPayloadLengthError {
		return -1, errors.New("getPayloadLength error")
	}
	return 0, nil
}

func (e *testEvent) ReleasePayloadWriter() {
}

func (e *testEvent) SetOffset(offset int32) {
}

var _ EventWriter = (*testEvent)(nil)

func TestWriterListError(t *testing.T) {
	insertWriter := NewInsertBinlogWriter(schemapb.DataType_Int64, 10, 20, 30, 40, false)
	sizeTotal := 2000000
	insertWriter.baseBinlogWriter.descriptorEventData.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))
	errorEvent := &testEvent{}
	insertWriter.eventWriters = append(insertWriter.eventWriters, errorEvent)
	insertWriter.SetEventTimeStamp(1000, 2000)
	insertWriter.buffer = nil
	errorEvent.getPayloadLengthError = true
	err := insertWriter.Finish()
	assert.Error(t, err)
	insertWriter.buffer = nil
	errorEvent.getMemoryError = true
	err = insertWriter.Finish()
	assert.Error(t, err)
	insertWriter.buffer = nil
	errorEvent.writeError = true
	err = insertWriter.Finish()
	assert.Error(t, err)
	insertWriter.buffer = nil
	errorEvent.finishError = true
	err = insertWriter.Finish()
	assert.Error(t, err)
}
