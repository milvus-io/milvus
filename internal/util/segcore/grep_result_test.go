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

const grepResultHit = `[{"type":"match","content":"timeout","starts":[0],"ends":[7]}]`

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

func TestGrepScanResultPreservesPartialCharacterSpans(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	payload := []byte(`[{"type":"match","content":"é中","starts":[1,3],"ends":[2,4]}]`)
	record := grepJSONRecord(t, pool, "[0]", payload)
	defer record.Release()
	result, err := newGrepScanResult(record, 1, 1)
	require.NoError(t, err)
	defer result.Release()
	require.Equal(t, payload, result.Details.(*array.Binary).Value(0))
}

func TestGrepScanResultKeepsMatchingRowsWithoutSubmatches(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	payload := []byte(`[{"type":"match","content":"error","line_number":2,"line_start_offset":7,"starts":[],"ends":[]}]`)
	record := grepJSONRecord(t, pool, "[0]", payload)
	defer record.Release()
	result, err := newGrepScanResult(record, 1, 1)
	require.NoError(t, err)
	defer result.Release()
	require.Equal(t, []int{0}, result.Positions)
	require.Equal(t, payload, result.Details.(*array.Binary).Value(0))
}

func TestGrepScanResultRejectsInvalidNativeResults(t *testing.T) {
	for name, payload := range map[string]string{
		"invalid json": `[`, "not array": `{}`, "empty": `[]`, "null array": `null`, "null line": `[null]`,
		"missing type":        `[{"content":"x","starts":[],"ends":[]}]`,
		"unknown type":        `[{"type":"other","content":"x","starts":[],"ends":[]}]`,
		"context only":        `[{"type":"context","content":"x"}]`,
		"context spans":       `[{"type":"match","content":"x","starts":[],"ends":[]},{"type":"context","content":"y","starts":[],"ends":[]}]`,
		"context null spans":  `[{"type":"match","content":"x","starts":[],"ends":[]},{"type":"context","content":"y","starts":null}]`,
		"match missing spans": `[{"type":"match","content":"x"}]`,
		"out of bounds":       `[{"type":"match","content":"x","starts":[0],"ends":[2]}]`,
		"missing content":     `[{"type":"match","starts":[0],"ends":[1]}]`,
		"null content":        `[{"type":"match","content":null,"starts":[0],"ends":[1]}]`,
		"null starts":         `[{"type":"match","content":"x","starts":null,"ends":[1]}]`,
		"null ends":           `[{"type":"match","content":"x","starts":[0],"ends":null}]`,
		"negative start":      `[{"type":"match","content":"x","starts":[-1],"ends":[1]}]`,
		"negative end":        `[{"type":"match","content":"x","starts":[0],"ends":[-1]}]`,
		"reversed":            `[{"type":"match","content":"x","starts":[2],"ends":[1]}]`,
		"span counts":         `[{"type":"match","content":"x","starts":[0],"ends":[]}]`,
		"null span":           `[{"type":"match","content":"x","starts":[null],"ends":[1]}]`,
		"overflow":            `[{"type":"match","content":"x","starts":[9223372036854775808],"ends":[9223372036854775808]}]`,
		"fraction":            `[{"type":"match","content":"x","starts":[0.5],"ends":[1]}]`,
		"bad utf8":            string([]byte{'[', '"', 0xff, '"', ']'}),
	} {
		t.Run(name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			record := grepJSONRecord(t, pool, "[0]", []byte(payload))
			defer record.Release()
			result, err := newGrepScanResult(record, 4, 2)
			require.Nil(t, result)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
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
	payload := `[{"type":"match","content":"foo bar foo","starts":[0,8],"ends":[3,11]},{"type":"match","content":"hello foo","starts":[6],"ends":[9]}]`
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

func TestGrepScanResultDistinguishesContextFromEmptyMatch(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	payload := []byte(`[{"type":"context","content":"before","line_number":1,"line_start_offset":0},{"type":"match","content":"error","line_number":2,"line_start_offset":7,"starts":[],"ends":[]}]`)
	record := grepJSONRecord(t, pool, "[0]", payload)
	defer record.Release()
	result, err := newGrepScanResult(record, 1, 1)
	require.NoError(t, err)
	defer result.Release()
	require.Equal(t, payload, result.Details.(*array.Binary).Value(0))
}
