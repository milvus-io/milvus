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

package segcore_test

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/grep"
	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Keep the encoded result alive without retaining every benchmark iteration.
var grepJSONBenchmarkWire []byte

// BenchmarkGrepJSON measures a single growing segment with preselected offsets.
// pipeline includes native compile/matching/JSON encoding, C Data import, JSON
// validation, DataFrame assembly and protobuf encoding. export_only reuses the
// same Arrow frame to isolate the final protobuf conversion/encoding cost.
func BenchmarkGrepJSON(b *testing.B) {
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(b.TempDir())
	require.NoError(b, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
	const rows = 1024
	for _, tc := range []struct {
		name string
		hits int
		text string
	}{
		{"no_hits", 0, strings.Repeat("x", 505) + " needle"},
		{"ten_percent", 102, strings.Repeat("x", 505) + " needle"},
		{"all_hits", rows, strings.Repeat("x", 505) + " needle"},
		{"eight_lines_four_spans", rows, strings.TrimSuffix(strings.Repeat(strings.Repeat("needle ", 4)+strings.Repeat("x", 36)+"\n", 8), "\n")},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			schema := &schemapb.CollectionSchema{Name: "grep_bench", Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "65535"}}},
			}}
			collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{CollectionID: 900, Schema: schema})
			require.NoError(b, err)
			defer collection.Release()
			segment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{Collection: collection, SegmentID: 900, SegmentType: segcore.SegmentTypeGrowing})
			require.NoError(b, err)
			defer segment.Release()
			ids, timestamps, texts := make([]int64, rows), make([]uint64, rows), make([]string, rows)
			miss := strings.Repeat("x", len(tc.text))
			for i := range ids {
				ids[i] = int64(i)
				timestamps[i] = 1
				texts[i] = miss
				if i < tc.hits {
					texts[i] = tc.text
				}
			}
			_, err = segment.Insert(ctx, &segcore.InsertRequest{RowIDs: ids, Timestamps: timestamps, Record: &segcorepb.InsertRecord{NumRows: rows, FieldsData: []*schemapb.FieldData{
				{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ids}}}}},
				{FieldId: 101, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: texts}}}}},
			}}})
			require.NoError(b, err)
			offsetsBuilder := array.NewInt64Builder(memory.DefaultAllocator)
			offsetsBuilder.AppendValues(ids, nil)
			offsets := offsetsBuilder.NewInt64Array()
			offsetsBuilder.Release()
			defer offsets.Release()
			spec, err := grep.Parse("-F -e needle", 65536)
			require.NoError(b, err)
			request := native.ScanRequest{
				FieldID: 101, Limit: rows, Spec: *spec,
				CompileLimits:   native.CompileLimits{MaxRegexBytes: 1 << 20, MaxDFACacheBytes: 1 << 20},
				ExecutionLimits: native.ExecutionLimits{BatchSize: 128, MaxResultBytes: 32 << 20},
			}
			runner := segcore.NewGrepSegment(segment)
			probe, err := runner.ScanGrep(ctx, offsets, request)
			require.NoError(b, err)
			require.Len(b, probe.Positions, tc.hits)
			require.Equal(b, arrow.BINARY, probe.Details.DataType().ID())
			if tc.hits > 0 {
				var lines []struct {
					Starts []int64 `json:"starts"`
					Ends   []int64 `json:"ends"`
				}
				require.NoError(b, json.Unmarshal(probe.Details.(*array.Binary).Value(0), &lines))
				wantLines, wantSpans := 1, 1
				if tc.name == "eight_lines_four_spans" {
					wantLines, wantSpans = 8, 4
				}
				require.Len(b, lines, wantLines)
				for _, line := range lines {
					require.Len(b, line.Starts, wantSpans)
					require.Len(b, line.Ends, wantSpans)
				}
			}
			jsonBytes := 0
			for i := 0; i < probe.Details.Len(); i++ {
				jsonBytes += len(probe.Details.(*array.Binary).Value(i))
			}
			frame, err := grepBenchmarkFrame(probe)
			probe.Release()
			require.NoError(b, err)
			defer frame.Release()
			exported, err := chain.ToRetrieveResults(frame)
			require.NoError(b, err)
			require.Equal(b, schemapb.DataType_JSON, exported.ComputedResults[0].Type)
			require.Len(b, exported.ComputedResults[0].GetScalars().GetJsonData().Data, tc.hits)

			b.Run("pipeline", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(rows * len(tc.text)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := runner.ScanGrep(ctx, offsets, request)
					if err != nil {
						result.Release()
						b.Fatal(err)
					}
					current, err := grepBenchmarkFrame(result)
					result.Release()
					if err != nil {
						b.Fatal(err)
					}
					output, err := chain.ToRetrieveResults(current)
					current.Release()
					if err != nil {
						b.Fatal(err)
					}
					grepJSONBenchmarkWire, err = proto.Marshal(output)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(tc.hits), "hits/op")
				b.ReportMetric(float64(jsonBytes), "JSON-B/op")
			})
			b.Run("export_only", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					output, err := chain.ToRetrieveResults(frame)
					if err != nil {
						b.Fatal(err)
					}
					grepJSONBenchmarkWire, err = proto.Marshal(output)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(tc.hits), "hits/op")
				b.ReportMetric(float64(jsonBytes), "JSON-B/op")
			})
		})
	}
}

func grepBenchmarkFrame(result *native.ScanResult) (*chain.DataFrame, error) {
	builder := chain.NewDataFrameBuilder().SetChunkSizes([]int64{int64(len(result.Positions))})
	defer builder.Release()
	if err := builder.SetComputedField(arrow.Field{Name: "matches", Type: arrow.BinaryTypes.Binary, Metadata: arrow.NewMetadata(
		[]string{chain.ArrowDataTypeMetadataKey}, []string{strconv.Itoa(int(schemapb.DataType_JSON))},
	)}); err != nil {
		return nil, err
	}
	result.Details.Retain()
	if err := builder.AddColumnFromChunks("matches", []arrow.Array{result.Details}); err != nil {
		return nil, err
	}
	for _, name := range []string{"$id", "$timestamp"} {
		values := array.NewInt64Builder(memory.DefaultAllocator)
		values.Reserve(len(result.Positions))
		for _, position := range result.Positions {
			if name == "$id" {
				values.Append(int64(position))
			} else {
				values.Append(1)
			}
		}
		column := values.NewArray()
		values.Release()
		if err := builder.AddColumnFromChunks(name, []arrow.Array{column}); err != nil {
			return nil, err
		}
	}
	return builder.Build(), nil
}
