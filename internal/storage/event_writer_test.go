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
	"testing"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
)

func TestEventTypeCode_String(t *testing.T) {
	var code EventTypeCode = 127
	res := code.String()
	assert.Equal(t, res, "InvalidEventType")

	code = DeleteEventType
	res = code.String()
	assert.Equal(t, res, "DeleteEventType")
}

func TestSizeofStruct(t *testing.T) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, baseEventHeader{})
	assert.Nil(t, err)
	s1 := binary.Size(baseEventHeader{})
	s2 := binary.Size(&baseEventHeader{})
	assert.Equal(t, s1, s2)
	assert.Equal(t, s1, buf.Len())
	buf.Reset()
	assert.Equal(t, 0, buf.Len())

	de := descriptorEventData{
		DescriptorEventDataFixPart: DescriptorEventDataFixPart{},
		PostHeaderLengths:          []uint8{0, 1, 2, 3},
	}
	err = de.Write(&buf)
	assert.Nil(t, err)
	s3 := binary.Size(de.DescriptorEventDataFixPart) + binary.Size(de.PostHeaderLengths)
	assert.Equal(t, s3, buf.Len())
}

func TestEventWriter(t *testing.T) {
	insertEvent, err := newInsertEventWriter(schemapb.DataType_Int32)
	assert.Nil(t, err)
	err = insertEvent.Close()
	assert.Nil(t, err)

	insertEvent, err = newInsertEventWriter(schemapb.DataType_Int32)
	assert.Nil(t, err)
	defer insertEvent.Close()

	err = insertEvent.AddInt64ToPayload([]int64{1, 1})
	assert.NotNil(t, err)
	err = insertEvent.AddInt32ToPayload([]int32{1, 2, 3})
	assert.Nil(t, err)
	nums, err := insertEvent.GetPayloadLengthFromWriter()
	assert.Nil(t, err)
	assert.EqualValues(t, 3, nums)
	err = insertEvent.Finish()
	assert.Nil(t, err)
	length, err := insertEvent.GetMemoryUsageInBytes()
	assert.Nil(t, err)
	assert.EqualValues(t, length, insertEvent.EventLength)
	err = insertEvent.AddInt32ToPayload([]int32{1})
	assert.NotNil(t, err)
	buffer := new(bytes.Buffer)
	insertEvent.SetEventTimestamp(100, 200)
	err = insertEvent.Write(buffer)
	assert.Nil(t, err)
	length, err = insertEvent.GetMemoryUsageInBytes()
	assert.Nil(t, err)
	assert.EqualValues(t, length, buffer.Len())
	err = insertEvent.Close()
	assert.Nil(t, err)
}
