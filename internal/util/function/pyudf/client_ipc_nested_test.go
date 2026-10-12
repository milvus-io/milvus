// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package pyudf

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestDecodeNestedSlicedBinaryAndLargeString(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	binaryBuilder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	binaryBuilder.AppendValues([][]byte{[]byte("long-prefix"), []byte("b")}, nil)
	binaryValues := binaryBuilder.NewArray()
	binaryBuilder.Release()
	defer binaryValues.Release()
	textBuilder := array.NewLargeStringBuilder(pool)
	textBuilder.AppendValues([]string{"long-prefix", "中文"}, nil)
	textValues := textBuilder.NewArray()
	textBuilder.Release()
	defer textValues.Release()
	largeBinaryBuilder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.LargeBinary)
	largeBinaryBuilder.AppendValues([][]byte{[]byte("long-prefix"), []byte("c")}, nil)
	largeBinaryValues := largeBinaryBuilder.NewArray()
	largeBinaryBuilder.Release()
	defer largeBinaryValues.Release()
	for _, column := range []arrow.Array{binaryValues, textValues, largeBinaryValues} {
		sliced := array.NewSlice(column, 1, 2)
		require.NoError(t, validateOutputArray(sliced))
		sliced.Release()
	}
	structs, err := array.NewStructArray([]arrow.Array{binaryValues, textValues, largeBinaryValues}, []string{"bytes", "text", "large_bytes"})
	require.NoError(t, err)
	defer structs.Release()
	sliced := array.NewSlice(structs, 1, 2)
	defer sliced.Release()
	require.NoError(t, validateOutputArray(sliced))
	schema := arrow.NewSchema([]arrow.Field{{Name: "c0", Type: sliced.DataType()}}, nil)
	record := array.NewRecord(schema, []arrow.Array{sliced}, 1)
	defer record.Release()
	var buffer bytes.Buffer
	writer := ipc.NewWriter(&buffer, ipc.WithSchema(schema))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	outputs, err := decodeOutputs(context.Background(), buffer.Bytes())
	require.NoError(t, err)
	defer releaseColumns(outputs)
	result := outputs[0].Chunk(0).(*array.Struct)
	require.Equal(t, []byte("b"), result.Field(0).(*array.Binary).Value(0))
	require.Equal(t, "中文", result.Field(1).(*array.LargeString).Value(0))
	require.Equal(t, []byte("c"), result.Field(2).(*array.LargeBinary).Value(0))
}
