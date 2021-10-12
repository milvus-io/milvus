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
	"fmt"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
)

func TestBinlogWriterReader(t *testing.T) {
	binlogWriter := NewInsertBinlogWriter(schemapb.DataType_Int32, 10, 20, 30, 40)
	tp := binlogWriter.GetBinlogType()
	assert.Equal(t, tp, InsertBinlog)

	binlogWriter.SetEventTimeStamp(1000, 2000)
	defer binlogWriter.Close()
	eventWriter, err := binlogWriter.NextInsertEventWriter()
	assert.Nil(t, err)
	err = eventWriter.AddInt32ToPayload([]int32{1, 2, 3})
	assert.Nil(t, err)
	_, err = binlogWriter.GetBuffer()
	assert.NotNil(t, err)
	eventWriter.SetEventTimestamp(1000, 2000)
	nums, err := binlogWriter.GetRowNums()
	assert.Nil(t, err)
	assert.EqualValues(t, 3, nums)
	sizeTotal := 20000000
	binlogWriter.baseBinlogWriter.descriptorEventData.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))
	err = binlogWriter.Close()
	assert.Nil(t, err)
	assert.EqualValues(t, 1, binlogWriter.GetEventNums())
	nums, err = binlogWriter.GetRowNums()
	assert.Nil(t, err)
	assert.EqualValues(t, 3, nums)
	err = eventWriter.AddInt32ToPayload([]int32{1, 2, 3})
	assert.NotNil(t, err)
	nums, err = binlogWriter.GetRowNums()
	assert.Nil(t, err)
	assert.EqualValues(t, 3, nums)

	buffer, err := binlogWriter.GetBuffer()
	assert.Nil(t, err)
	fmt.Println("reader offset : " + strconv.Itoa(len(buffer)))

	binlogReader, err := NewBinlogReader(buffer)
	assert.Nil(t, err)
	eventReader, err := binlogReader.NextEventReader()
	assert.Nil(t, err)
	_, err = eventReader.GetInt8FromPayload()
	assert.NotNil(t, err)
	payload, err := eventReader.GetInt32FromPayload()
	assert.Nil(t, err)
	assert.EqualValues(t, 3, len(payload))
	assert.EqualValues(t, 1, payload[0])
	assert.EqualValues(t, 2, payload[1])
	assert.EqualValues(t, 3, payload[2])

	reader, err := binlogReader.NextEventReader()
	assert.Nil(t, err)
	assert.Nil(t, reader)
}
