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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
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
	err := binary.Write(&buf, common.Endian, baseEventHeader{})
	assert.NoError(t, err)
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
	assert.NoError(t, err)
	s3 := binary.Size(de.DescriptorEventDataFixPart) + binary.Size(de.PostHeaderLengths) + binary.Size(de.ExtraLength) + int(de.ExtraLength)
	assert.Equal(t, s3, buf.Len())
}

func TestEventWriter(t *testing.T) {
	insertEvent, err := newInsertEventWriter(schemapb.DataType_Int32)
	assert.NoError(t, err)
	insertEvent.Close()

	insertEvent, err = newInsertEventWriter(schemapb.DataType_Int32)
	assert.NoError(t, err)
	defer insertEvent.Close()

	err = insertEvent.AddInt64ToPayload([]int64{1, 1}, nil)
	assert.Error(t, err)
	err = insertEvent.AddInt32ToPayload([]int32{1, 2, 3}, nil)
	assert.NoError(t, err)
	nums, err := insertEvent.GetPayloadLengthFromWriter()
	assert.NoError(t, err)
	assert.EqualValues(t, 3, nums)
	err = insertEvent.Finish()
	assert.NoError(t, err)
	length, err := insertEvent.GetMemoryUsageInBytes()
	assert.NoError(t, err)
	assert.EqualValues(t, length, insertEvent.EventLength)
	err = insertEvent.AddInt32ToPayload([]int32{1}, nil)
	assert.Error(t, err)
	buffer := new(bytes.Buffer)
	insertEvent.SetEventTimestamp(100, 200)
	err = insertEvent.Write(buffer)
	assert.NoError(t, err)
	length, err = insertEvent.GetMemoryUsageInBytes()
	assert.NoError(t, err)
	assert.EqualValues(t, length, buffer.Len())
	insertEvent.Close()
}

func TestReadMagicNumber(t *testing.T) {
	var err error
	buf := bytes.Buffer{}

	// eof
	_, err = readMagicNumber(&buf)
	assert.Error(t, err)

	// not a magic number
	_ = binary.Write(&buf, common.Endian, MagicNumber+1)
	_, err = readMagicNumber(&buf)
	assert.Error(t, err)

	// normal case
	_ = binary.Write(&buf, common.Endian, MagicNumber)
	num, err := readMagicNumber(&buf)
	assert.NoError(t, err)
	assert.Equal(t, MagicNumber, num)
}
