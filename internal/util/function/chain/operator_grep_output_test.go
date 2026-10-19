// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chain

import (
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type grepJSONLine struct {
	Content string  `json:"content"`
	Starts  []int64 `json:"starts"`
	Ends    []int64 `json:"ends"`
}

// The outer fixture array denotes documents. Each document is appended as one
// Binary value, exactly as the native runner returns it (no base64 JSON wrapper).
func grepJSONColumn(pool memory.Allocator, input io.Reader) (arrow.Array, int64, error) {
	var docs []json.RawMessage
	if err := json.NewDecoder(input).Decode(&docs); err != nil {
		return nil, 0, err
	}
	builder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	defer builder.Release()
	for _, doc := range docs {
		if string(doc) == "null" {
			builder.AppendNull()
		} else {
			builder.Append(doc)
		}
	}
	return builder.NewArray(), 0, nil
}

func decodeGrepJSON(t *testing.T, payload []byte) []grepJSONLine {
	t.Helper()
	var lines []grepJSONLine
	require.NoError(t, json.Unmarshal(payload, &lines))
	return lines
}

func TestQueryScanJSONExportOwnership(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	source, _, err := grepJSONColumn(pool, strings.NewReader(`[[{"content":"skip","starts":[0],"ends":[4]}],[{"content":"foo bar foo","starts":[0,8],"ends":[3,11]},{"content":"hello foo","starts":[6],"ends":[9]}],[]]`))
	require.NoError(t, err)
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{1, 0, 2})
	defer builder.Release()
	require.NoError(t, builder.SetComputedField(grepOutputField("matches")))
	require.NoError(t, builder.AddColumnFromChunks("matches", []arrow.Array{array.NewSlice(source, 1, 2), array.NewSlice(source, 0, 0), array.NewSlice(source, 1, 3)}))
	frame := builder.Build()
	result, err := exportFieldData(frame, "matches")
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_JSON, result.Type)
	require.Zero(t, result.FieldId)
	payloads := result.GetScalars().GetJsonData().Data
	require.Len(t, payloads, 3)
	require.NotSame(t, &source.(*array.Binary).Value(1)[0], &payloads[0][0])
	source.Release()
	frame.Release()
	want := []grepJSONLine{{"foo bar foo", []int64{0, 8}, []int64{3, 11}}, {"hello foo", []int64{6}, []int64{9}}}
	require.Equal(t, want, decodeGrepJSON(t, payloads[0]))
	require.Equal(t, want, decodeGrepJSON(t, payloads[1]))
	require.Empty(t, decodeGrepJSON(t, payloads[2]))
	wire, err := proto.Marshal(&segcorepb.RetrieveResults{ComputedResults: []*schemapb.FieldData{result}})
	require.NoError(t, err)
	decoded := &segcorepb.RetrieveResults{}
	require.NoError(t, proto.Unmarshal(wire, decoded))
	require.True(t, proto.Equal(result, decoded.ComputedResults[0]))
}

func TestQueryScanFinalOutputProjection(t *testing.T) {
	for _, names := range [][]string{{"second", "first"}, {"second"}, nil, {"missing"}} {
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		builder := NewDataFrameBuilder().SetChunkSizes([]int64{1})
		for _, name := range []string{"first", "second"} {
			b := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
			payload, err := json.Marshal([]grepJSONLine{{name, []int64{0}, []int64{1}}})
			require.NoError(t, err)
			b.Append(payload)
			require.NoError(t, builder.SetComputedField(grepOutputField(name)))
			require.NoError(t, builder.AddColumnFromChunks(name, []arrow.Array{b.NewArray()}))
			b.Release()
		}
		for _, name := range []string{"$id", "$timestamp"} {
			b := array.NewInt64Builder(pool)
			b.Append(1)
			require.NoError(t, builder.AddColumnFromChunks(name, []arrow.Array{b.NewArray()}))
			b.Release()
		}
		frame := builder.Build()
		builder.Release()
		projected := NewDataFrameBuilder().SetChunkSizes([]int64{1})
		require.NoError(t, projected.AddColumnFrom(frame, "$id"))
		require.NoError(t, projected.AddColumnFrom(frame, "$timestamp"))
		var err error
		for _, name := range names {
			if err = projected.AddColumnFrom(frame, name); err != nil {
				break
			}
		}
		frame.Release()
		if len(names) == 1 && names[0] == "missing" {
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			projected.Release()
			pool.AssertSize(t, 0)
			continue
		}
		out := projected.Build()
		projected.Release()
		result, err := ToRetrieveResults(out)
		require.NoError(t, err)
		out.Release()
		require.Len(t, result.ComputedResults, len(names))
		for i, name := range names {
			require.Equal(t, name, result.ComputedResults[i].FieldName)
			require.Equal(t, name, decodeGrepJSON(t, result.ComputedResults[i].GetScalars().GetJsonData().Data[0])[0].Content)
		}
		pool.AssertSize(t, 0)
	}
}

func TestQueryScanEmptyJSONExport(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{0})
	defer builder.Release()
	require.NoError(t, builder.SetComputedField(grepOutputField("matches")))
	b := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	require.NoError(t, builder.AddColumnFromChunks("matches", []arrow.Array{b.NewArray()}))
	b.Release()
	frame := builder.Build()
	defer frame.Release()
	result, err := exportFieldData(frame, "matches")
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_JSON, result.Type)
	require.NotNil(t, result.GetScalars().GetJsonData())
	require.Empty(t, result.GetScalars().GetJsonData().Data)
}
