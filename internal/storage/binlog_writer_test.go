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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

func TestBinlogReaderWriterCipher(t *testing.T) {
	hookutil.InitTestCipher()

	encryptor, safeKey, err := hookutil.GetCipher().GetEncryptor(1, 1)
	require.NoError(t, err)
	require.NotNil(t, encryptor)
	cypherOpts := WithWriterEncryptionContext(1, safeKey, encryptor)

	binlogWriter := NewInsertBinlogWriter(schemapb.DataType_Int32, 10, 20, 30, 40, false, cypherOpts)
	binlogWriter.SetEventTimeStamp(1000, 2000)

	eventWriter, err := binlogWriter.NextInsertEventWriter()
	require.NoError(t, err)
	err = eventWriter.AddInt32ToPayload([]int32{1, 2, 3}, nil)
	assert.NoError(t, err)
	eventWriter.SetEventTimestamp(1000, 2000)
	nums, err := binlogWriter.GetRowNums()
	assert.NoError(t, err)
	assert.EqualValues(t, 3, nums)
	sizeTotal := 20000000
	binlogWriter.baseBinlogWriter.descriptorEventData.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))
	err = binlogWriter.Finish()
	assert.NoError(t, err)

	storedEdek, ok := binlogWriter.descriptorEvent.GetEdek()
	assert.True(t, ok)
	assert.EqualValues(t, safeKey, storedEdek)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, binlogWriter.GetEventNums())
	nums, err = binlogWriter.GetRowNums()
	assert.NoError(t, err)
	assert.EqualValues(t, 3, nums)

	buffer, err := binlogWriter.GetBuffer()
	assert.NoError(t, err)
	assert.NotEmpty(t, buffer)
	binlogWriter.Close()

	// Test reader
	binlogReader, err := NewBinlogReader(buffer, WithReaderDecryptionContext(1, 1))
	assert.NoError(t, err)

	log.Info("binlogReader", zap.Any("descriptorEvent", binlogReader.descriptorEvent))

	gotsafeKey, ok := binlogReader.descriptorEvent.GetEdek()
	assert.True(t, ok)
	assert.EqualValues(t, safeKey, gotsafeKey)

	eventReader, err := binlogReader.NextEventReader()
	assert.NoError(t, err)
	_, _, err = eventReader.GetInt8FromPayload()
	assert.Error(t, err)
	payload, _, err := eventReader.GetInt32FromPayload()
	assert.NoError(t, err)
	assert.EqualValues(t, 3, len(payload))
	assert.EqualValues(t, 1, payload[0])
	assert.EqualValues(t, 2, payload[1])
	assert.EqualValues(t, 3, payload[2])

	reader, err := binlogReader.NextEventReader()
	assert.NoError(t, err)
	assert.Nil(t, reader)
}

func TestBinlogWriterReader(t *testing.T) {
	binlogWriter := NewInsertBinlogWriter(schemapb.DataType_Int32, 10, 20, 30, 40, false)
	tp := binlogWriter.GetBinlogType()
	assert.Equal(t, tp, InsertBinlog)

	binlogWriter.SetEventTimeStamp(1000, 2000)
	defer binlogWriter.Close()
	eventWriter, err := binlogWriter.NextInsertEventWriter()
	assert.NoError(t, err)
	err = eventWriter.AddInt32ToPayload([]int32{1, 2, 3}, nil)
	assert.NoError(t, err)
	_, err = binlogWriter.GetBuffer()
	assert.Error(t, err)
	eventWriter.SetEventTimestamp(1000, 2000)
	nums, err := binlogWriter.GetRowNums()
	assert.NoError(t, err)
	assert.EqualValues(t, 3, nums)
	sizeTotal := 20000000
	binlogWriter.baseBinlogWriter.descriptorEventData.AddExtra(originalSizeKey, fmt.Sprintf("%v", sizeTotal))
	err = binlogWriter.Finish()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, binlogWriter.GetEventNums())
	nums, err = binlogWriter.GetRowNums()
	assert.NoError(t, err)
	assert.EqualValues(t, 3, nums)
	err = eventWriter.AddInt32ToPayload([]int32{1, 2, 3}, nil)
	assert.Error(t, err)
	nums, err = binlogWriter.GetRowNums()
	assert.NoError(t, err)
	assert.EqualValues(t, 3, nums)

	buffer, err := binlogWriter.GetBuffer()
	assert.NoError(t, err)
	binlogWriter.Close()

	binlogReader, err := NewBinlogReader(buffer)
	assert.NoError(t, err)
	eventReader, err := binlogReader.NextEventReader()
	assert.NoError(t, err)
	_, _, err = eventReader.GetInt8FromPayload()
	assert.Error(t, err)
	payload, _, err := eventReader.GetInt32FromPayload()
	assert.NoError(t, err)
	assert.EqualValues(t, 3, len(payload))
	assert.EqualValues(t, 1, payload[0])
	assert.EqualValues(t, 2, payload[1])
	assert.EqualValues(t, 3, payload[2])

	reader, err := binlogReader.NextEventReader()
	assert.NoError(t, err)
	assert.Nil(t, reader)
}
