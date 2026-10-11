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

package transformio

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestStructPayloadIPCAndJSONDecoder(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	span := arrow.StructOf(arrow.Field{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true}, arrow.Field{Name: "start", Type: arrow.PrimitiveTypes.Int64})
	highlight := arrow.StructOf(span.Fields()[0], span.Fields()[1],
		arrow.Field{Name: "details", Type: arrow.StructOf(arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Float64}), Nullable: true},
		arrow.Field{Name: "spans", Type: arrow.ListOf(span)})
	schema := arrow.NewSchema([]arrow.Field{{Name: "highlight", Type: highlight, Nullable: true}}, nil)
	record, _, err := array.RecordFromJSON(alloc, schema, strings.NewReader(`[
		{"highlight":{"text":"中文","start":9007199254740993,"details":{"score":0.75},"spans":[{"text":"中","start":0},null]}},
		{"highlight":null},
		{"highlight":{"text":null,"start":0,"details":null,"spans":[]}}
	]`), array.WithUseNumber())
	require.NoError(t, err)
	var wire bytes.Buffer
	writer := ipc.NewWriter(&wire, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	record.Release()
	reader, err := ipc.NewReader(bytes.NewReader(wire.Bytes()), ipc.WithAllocator(alloc))
	require.NoError(t, err)
	require.True(t, reader.Next())
	table := array.NewTableFromRecords(reader.Schema(), []arrow.Record{reader.Record()})
	fields, err := ToFieldData(table, nil)
	require.NoError(t, err)
	table.Release()
	reader.Release()
	alloc.AssertSize(t, 0)
	require.Equal(t, schemapb.DataType_JSON, fields[0].GetType())
	require.Zero(t, fields[0].GetFieldId(), "generated payload does not inherit a storage field ID")
	require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(fields[0]))
	data := fields[0].GetScalars().GetJsonData().GetData()
	var decoded struct {
		Text    *string `json:"text"`
		Start   int64   `json:"start"`
		Details *struct {
			Score float64 `json:"score"`
		} `json:"details"`
		Spans []*struct {
			Text  string `json:"text"`
			Start int64  `json:"start"`
		} `json:"spans"`
	}
	require.NoError(t, json.Unmarshal(data[0], &decoded))
	require.Equal(t, "中文", *decoded.Text)
	require.Equal(t, int64(9007199254740993), decoded.Start, "JSON wire retains integer precision")
	require.Equal(t, 0.75, decoded.Details.Score)
	require.Equal(t, "中", decoded.Spans[0].Text)
	require.Nil(t, decoded.Spans[1])
	require.Empty(t, data[1], "null rows use FieldData validity")
	require.NoError(t, json.Unmarshal(data[2], &decoded))
	require.Nil(t, decoded.Text)
	require.Nil(t, decoded.Details)
	require.NotNil(t, decoded.Spans)
	require.Empty(t, decoded.Spans)
}

func TestEmptyStructPayloadKeepsJSONType(t *testing.T) {
	typeOf := arrow.StructOf(arrow.Field{Name: "text", Type: arrow.BinaryTypes.String})
	field := arrow.Field{Name: "highlight", Type: typeOf, Nullable: true}
	chunks := arrow.NewChunked(typeOf, nil)
	column := arrow.NewColumn(field, chunks)
	chunks.Release()
	table := array.NewTable(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Column{*column}, 0)
	column.Release()
	defer table.Release()
	fields, err := ToFieldData(table, nil)
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_JSON, fields[0].GetType())
	require.Empty(t, fields[0].GetScalars().GetJsonData().GetData())
}

func TestStructJSONRejectsNonFiniteAndDuplicateFields(t *testing.T) {
	for _, number := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		builder := array.NewFloat64Builder(memory.DefaultAllocator)
		builder.Append(number)
		values := builder.NewArray()
		builder.Release()
		structured, err := array.NewStructArray([]arrow.Array{values}, []string{"score"})
		values.Release()
		require.NoError(t, err)
		field := arrow.Field{Name: "highlight", Type: structured.DataType()}
		record := array.NewRecord(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{structured}, 1)
		structured.Release()
		table := array.NewTableFromRecords(record.Schema(), []arrow.Record{record})
		record.Release()
		_, err = ToFieldData(table, nil)
		table.Release()
		require.ErrorIs(t, err, merr.ErrFunctionFailed)
	}
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	builder.Append("text")
	values := builder.NewArray()
	builder.Release()
	structured, err := array.NewStructArray([]arrow.Array{values, values}, []string{"text", "text"})
	values.Release()
	require.NoError(t, err)
	defer structured.Release()
	_, err = encodeJSONOutput(structured, 0)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
}

func TestMalformedJSONOutputFailsBeforeClientDecode(t *testing.T) {
	for _, value := range []string{`{"score":NaN}`, `{"text":`, "\xff"} {
		table, err := FromFieldData([]*schemapb.FieldData{stringField("payload", value)}, 1, memory.DefaultAllocator)
		require.NoError(t, err)
		_, err = ToFieldData(table, []*schemapb.FieldSchema{{Name: "payload", DataType: schemapb.DataType_JSON}})
		table.Release()
		require.ErrorIs(t, err, merr.ErrFunctionFailed)
	}
}
