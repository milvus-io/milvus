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

package writebuffer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
)

func TestSegmentTextTermBufferDeduplicatesAndAdvancesCoverage(t *testing.T) {
	buffer := newSegmentTextTermBuffer()
	buffer.Buffer([]*msgpb.TextTermBatch{
		{InputFieldId: 101, Terms: [][]byte{[]byte("world"), []byte("hello")}},
		{InputFieldId: 101, Terms: [][]byte{[]byte("fuzzy"), []byte("fuzzy")}},
	}, 100)
	require.EqualValues(t,
		textTermBufferOverhead+textTermFieldOverhead+3*textTermEntryOverhead+int64(len("world")+len("hello")+len("fuzzy")),
		buffer.MemorySize())
	buffer.Buffer([]*msgpb.TextTermBatch{
		{InputFieldId: 101, Terms: [][]byte{[]byte("hello"), []byte("again")}},
		{InputFieldId: 102},
	}, 120)
	buffer.Buffer(nil, 200)
	require.EqualValues(t,
		textTermBufferOverhead+2*textTermFieldOverhead+4*textTermEntryOverhead+int64(len("world")+len("hello")+len("fuzzy")+len("again")),
		buffer.MemorySize())

	result := buffer.Yield()
	require.NotNil(t, result)
	require.EqualValues(t, 120, result.CoverageTimestamp)
	require.ElementsMatch(t, [][]byte{[]byte("again"), []byte("fuzzy"), []byte("hello"), []byte("world")}, result.Fields[101])
	require.Empty(t, result.Fields[102])
}

func TestWriteBufferTextTermGenerationFreeze(t *testing.T) {
	wb := &writeBufferBase{textTermBuffers: make(map[int64]*segmentTextTermBuffer)}

	wb.bufferTextTerms(10, []*msgpb.TextTermBatch{
		{InputFieldId: 101, Terms: [][]byte{[]byte("first")}},
	}, 100)
	require.EqualValues(t,
		textTermBufferOverhead+textTermFieldOverhead+textTermEntryOverhead+int64(len("first")),
		wb.MemorySize())
	first := wb.yieldTextTerms(10)
	require.NotNil(t, first)
	require.EqualValues(t, 100, first.CoverageTimestamp)
	require.Equal(t, [][]byte{[]byte("first")}, first.Fields[101])
	require.Nil(t, wb.yieldTextTerms(10))
	require.Zero(t, wb.MemorySize())

	wb.bufferTextTerms(10, []*msgpb.TextTermBatch{
		{InputFieldId: 101, Terms: [][]byte{[]byte("second")}},
	}, 200)
	require.EqualValues(t,
		textTermBufferOverhead+textTermFieldOverhead+textTermEntryOverhead+int64(len("second")),
		wb.MemorySize())
	second := wb.yieldTextTerms(10)
	require.NotNil(t, second)
	require.EqualValues(t, 200, second.CoverageTimestamp)
	require.Equal(t, [][]byte{[]byte("second")}, second.Fields[101])
	require.Equal(t, [][]byte{[]byte("first")}, first.Fields[101])
}

func TestTextTermDataMemorySize(t *testing.T) {
	data := &syncmgr.TextTermData{Fields: map[int64][][]byte{
		101: {[]byte("first"), []byte("second")},
	}}
	require.EqualValues(t,
		textTermBufferOverhead+textTermFieldOverhead+2*textTermEntryOverhead+int64(len("first")+len("second")),
		textTermDataMemorySize(data))
}

func TestWriteBufferTextTermGenerationRestore(t *testing.T) {
	wb := &writeBufferBase{textTermBuffers: make(map[int64]*segmentTextTermBuffer)}
	wb.bufferTextTerms(10, []*msgpb.TextTermBatch{
		{InputFieldId: 101, Terms: [][]byte{[]byte("first"), []byte("shared")}},
	}, 100)
	frozen := wb.yieldTextTerms(10)
	wb.bufferTextTerms(10, []*msgpb.TextTermBatch{
		{InputFieldId: 101, Terms: [][]byte{[]byte("second"), []byte("shared")}},
	}, 200)

	wb.restoreTextTerms(10, frozen)
	restored := wb.yieldTextTerms(10)
	require.EqualValues(t, 200, restored.CoverageTimestamp)
	require.ElementsMatch(t, [][]byte{[]byte("first"), []byte("second"), []byte("shared")}, restored.Fields[101])
}
