// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segcore

import (
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/arrowconv"
	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const grepResultHit = `{"items":[{"type":"match","starts":[0],"ends":[7],"text":"timeout","line_number":1,"absolute_offset":0}]}`

func grepJSONRecord(t *testing.T, pool memory.Allocator, positions string, documents ...[]byte) arrow.Record {
	t.Helper()
	pos, _, err := array.FromJSON(pool, arrow.PrimitiveTypes.Int64, strings.NewReader(positions))
	require.NoError(t, err)
	defer pos.Release()
	b := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	for _, doc := range documents {
		if doc == nil {
			b.AppendNull()
		} else {
			b.Append(doc)
		}
	}
	values := b.NewArray()
	b.Release()
	defer values.Release()
	return array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "positions", Type: pos.DataType()}, {Name: "details", Type: values.DataType()}}, nil), []arrow.Array{pos, values}, int64(pos.Len()))
}

func TestGrepScanResultRejectsInvalidNativeResults(t *testing.T) {
	for name, positions := range map[string]string{"negative": "[-1]", "out of range": "[4]", "duplicate": "[1,1]", "unordered": "[1,0]", "too many": "[0,1,2]", "null": "[null]"} {
		t.Run(name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			pos, _, err := array.FromJSON(pool, arrow.PrimitiveTypes.Int64, strings.NewReader(positions))
			require.NoError(t, err)
			docs := make([][]byte, pos.Len())
			pos.Release()
			for i := range docs {
				docs[i] = []byte(grepResultHit)
			}
			record := grepJSONRecord(t, pool, positions, docs...)
			defer record.Release()
			result, err := newGrepScanResult(record, 4, 2)
			require.Nil(t, result)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}
	t.Run("null binary", func(t *testing.T) {
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		record := grepJSONRecord(t, pool, "[0]", nil)
		defer record.Release()
		result, err := newGrepScanResult(record, 4, 2)
		require.Nil(t, result)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	})
}

func TestGrepScanResultOwnsJSONBuffers(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	payload := `{"items":[{"type":"match","starts":[0,8],"ends":[3,11],"text":"foo bar foo","line_number":1,"absolute_offset":0},{"type":"match","starts":[6],"ends":[9],"text":"hello foo","line_number":1,"absolute_offset":0}]}`
	record := grepJSONRecord(t, pool, "[3,0,2]", []byte(grepResultHit), []byte(payload), []byte(grepResultHit))
	sliced := record.NewSlice(1, 3)
	record.Release()
	result, err := newGrepScanResult(sliced, 4, 2)
	sliced.Release()
	require.NoError(t, err)
	require.Equal(t, []int{0, 2}, result.Positions)
	column := arrow.NewChunked(native.GrepDetailsType(), []arrow.Array{result.Details})
	result.Release()
	exported, err := arrowconv.ArrowChunkedColumnToFieldData(column, &schemapb.FieldSchema{Name: "matches", DataType: schemapb.DataType_JSON})
	column.Release()
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte(payload), []byte(grepResultHit)}, exported.GetScalars().GetJsonData().Data)
	empty := grepJSONRecord(t, pool, "[]")
	result, err = newGrepScanResult(empty, 4, 2)
	empty.Release()
	require.NoError(t, err)
	require.Zero(t, result.Details.Len())
	result.Release()
}
