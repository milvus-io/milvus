// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pyudf

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestClientIPCRejectsInvalidStringOffsets(t *testing.T) {
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]string{"a", "b", "c"}, nil)
	values := builder.NewArray()
	defer values.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "c0", Type: arrow.BinaryTypes.String}}, nil)
	record := array.NewRecord(schema, []arrow.Array{values}, 3)
	defer record.Release()
	var buffer bytes.Buffer
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(schema))
	// Keep an earlier valid batch to also verify partial output is discarded.
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	input := bytes.NewReader(buffer.Bytes())
	messages := ipc.NewMessageReader(input)
	defer messages.Release()
	for range 2 { // schema and first batch
		_, err := messages.Message()
		require.NoError(t, err)
	}
	message, err := messages.Message()
	require.NoError(t, err)
	require.Equal(t, ipc.MessageRecordBatch, message.Type())
	bodyStart := buffer.Len() - input.Len() - int(message.BodyLen())
	// With no null bitmap, the body starts with four int32 string offsets.
	for i := 0; i < 4; i++ {
		require.Equal(t, uint32(i), binary.LittleEndian.Uint32(buffer.Bytes()[bodyStart+4*i:]))
	}
	for _, test := range []struct {
		name    string
		offsets [4]int32
	}{
		{"non_monotonic", [4]int32{0, 2, 1, 3}},
		{"negative_first", [4]int32{-1, 1, 2, 3}},
		{"negative_middle", [4]int32{0, -1, 2, 3}},
		{"middle_out_of_bounds", [4]int32{0, 4, 2, 3}},
		{"last_out_of_bounds", [4]int32{0, 1, 2, 4}},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := bytes.Clone(buffer.Bytes())
			for i, offset := range test.offsets {
				binary.LittleEndian.PutUint32(payload[bodyStart+4*i:], uint32(offset))
			}
			outputs, err := decodeOutputs(context.Background(), payload)
			defer releaseColumns(outputs)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Nil(t, outputs, "invalid later batches must not publish partial results")
		})
	}
}

func TestClientIPCValidStringOffsets(t *testing.T) {
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]string{"prefix", "", "ignored", "abc"}, []bool{true, true, false, true})
	values := builder.NewArray()
	defer values.Release()
	sliced := array.NewSlice(values, 1, 4)
	defer sliced.Release()
	empty := array.NewSlice(values, 0, 0)
	defer empty.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "c0", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	var buffer bytes.Buffer
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(schema))
	for _, input := range []arrow.Array{empty, sliced} {
		record := array.NewRecord(schema, []arrow.Array{input}, int64(input.Len()))
		err := writer.Write(record)
		record.Release()
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	outputs, err := decodeOutputs(context.Background(), buffer.Bytes())
	require.NoError(t, err)
	defer releaseColumns(outputs)
	require.Len(t, outputs, 1)
	require.Len(t, outputs[0].Chunks(), 2)
	require.Zero(t, outputs[0].Chunk(0).Len())
	chunk := outputs[0].Chunk(1).(*array.String)
	require.Equal(t, "", chunk.Value(0))
	require.True(t, chunk.IsNull(1))
	require.Equal(t, "abc", chunk.Value(2))
}
