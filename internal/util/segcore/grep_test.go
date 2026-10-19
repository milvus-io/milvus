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

package segcore_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/grep"
	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestGrepSegmentAdapter(t *testing.T) {
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole))
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
	for _, dataType := range []schemapb.DataType{schemapb.DataType_VarChar, schemapb.DataType_Text} {
		t.Run(dataType.String(), func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Name: "grep", Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "text", DataType: dataType, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "65535"}}},
			}}
			collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{CollectionID: 100, Schema: schema})
			require.NoError(t, err)
			defer collection.Release()
			segment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{Collection: collection, SegmentID: 100, SegmentType: segcore.SegmentTypeGrowing})
			require.NoError(t, err)
			defer segment.Release()
			_, err = segment.Insert(context.Background(), &segcore.InsertRequest{RowIDs: []int64{0, 1, 2, 3}, Timestamps: []uint64{1, 1, 1, 1}, Record: &segcorepb.InsertRecord{NumRows: 4, FieldsData: []*schemapb.FieldData{
				{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{0, 1, 2, 3}}}}}},
				{FieldId: 101, Type: dataType, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{ValidData: []bool{true, false, true, true}, Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"nothing", "timeout", "超时 timeout", "timeout"}}}}}},
			}}})
			require.NoError(t, err)
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			b := array.NewInt64Builder(pool)
			b.AppendValues([]int64{0, 1, 2, 3}, nil)
			offsets := b.NewInt64Array()
			b.Release()
			defer offsets.Release()
			adapter := segcore.NewGrepSegment(segment)
			spec, err := grep.Parse("-e timeout", 65536)
			require.NoError(t, err)
			req := native.ScanRequest{FieldID: 101, Limit: 1, Spec: *spec, CompileLimits: native.CompileLimits{MaxRegexBytes: 1048576, MaxDFACacheBytes: 1048576}, ExecutionLimits: native.ExecutionLimits{BatchSize: 128, MaxResultBytes: 1048576}}
			result, err := adapter.ScanGrep(context.Background(), offsets, req)
			require.NoError(t, err)
			require.Equal(t, []int{2}, result.Positions)
			require.True(t, arrow.TypeEqual(native.GrepDetailsType(), result.Details.DataType()))
			result.Release()
			t.Run("errors", func(t *testing.T) {
				zeroSpec := req
				zeroSpec.Spec = grep.Spec{}
				result, err := adapter.ScanGrep(context.Background(), offsets, zeroSpec)
				require.Nil(t, result)
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				invalid := req
				invalidSpec, err := grep.Parse("-e '['", 65536)
				require.NoError(t, err)
				invalid.Spec = *invalidSpec
				result, err = adapter.ScanGrep(context.Background(), offsets, invalid)
				require.Nil(t, result)
				require.Error(t, err)
				require.Equal(t, merr.InputError, merr.GetErrorType(err))
				invalid = req
				invalid.ExecutionLimits.MaxResultBytes = 1
				result, err = adapter.ScanGrep(context.Background(), offsets, invalid)
				require.Nil(t, result)
				require.Error(t, err)
				require.Equal(t, merr.InputError, merr.GetErrorType(err))
				require.Equal(t, int32(merr.CodeInvalidParameter), merr.Code(err))
				require.Contains(t, err.Error(), "quotaAndLimits.limits.maxOutputSize=1 bytes")
				require.Contains(t, err.Error(), "resource limit exceeded: ResultBytes")
				invalid = req
				compileSpec, err := grep.Parse("-e '[a-z]+'", 65536)
				require.NoError(t, err)
				invalid.Spec = *compileSpec
				invalid.CompileLimits.MaxRegexBytes = 1
				result, err = adapter.ScanGrep(context.Background(), offsets, invalid)
				require.Nil(t, result)
				require.Error(t, err)
				require.Equal(t, merr.InputError, merr.GetErrorType(err))
				require.Equal(t, int32(merr.CodeInvalidParameter), merr.Code(err))
				require.Contains(t, err.Error(), "function.grep.maxRegexBytes=1 bytes")
				// Preserve classification through the status serialization used by RPCs.
				remote := merr.Error(merr.Status(err))
				require.Equal(t, merr.InputError, merr.GetErrorType(remote))
				require.False(t, merr.IsRetryableErr(remote))
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				result, err = adapter.ScanGrep(ctx, offsets, req)
				require.Nil(t, result)
				require.ErrorIs(t, err, context.Canceled)
				deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer deadlineCancel()
				result, err = adapter.ScanGrep(deadlineCtx, offsets, req)
				require.Nil(t, result)
				require.ErrorIs(t, err, context.DeadlineExceeded)
			})
			t.Run("invalid offsets", func(t *testing.T) {
				for _, null := range []bool{false, true} {
					builder := array.NewInt64Builder(pool)
					if null {
						builder.AppendNull()
					} else {
						builder.Append(-1)
					}
					invalidOffsets := builder.NewInt64Array()
					builder.Release()
					result, err := adapter.ScanGrep(context.Background(), invalidOffsets, req)
					invalidOffsets.Release()
					require.Nil(t, result)
					require.Error(t, err)
					require.Equal(t, merr.SystemError, merr.GetErrorType(err))
					if null {
						require.ErrorIs(t, err, merr.ErrServiceInternal)
						require.Contains(t, err.Error(), "non-null Int64")
					} else {
						require.Equal(t, int32(merr.CodeUnexpectedError), merr.Code(err))
						require.Contains(t, err.Error(), "grep offset out of range")
					}
				}
			})
			t.Run("request cancellation after entry", func(t *testing.T) {
				builder := array.NewInt64Builder(pool)
				builder.AppendValues(make([]int64, 1000000), nil) // Repeated nonmatching row.
				manyOffsets := builder.NewInt64Array()
				builder.Release()
				defer manyOffsets.Release()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				// The guard starts observing Done only after ScanGrep's entry
				// check. Cancel there so this exercises the native error path,
				// rather than racing a timer against the Go entry check.
				result, err := adapter.ScanGrep(grepCancelOnDoneContext{Context: ctx, cancel: cancel}, manyOffsets, req)
				require.Nil(t, result)
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.ErrorIs(t, err, merr.ErrSegcoreFollyCancel)
				result, err = adapter.ScanGrep(context.Background(), offsets, req)
				require.NoError(t, err)
				result.Release()
			})
			t.Run("parallel calls have independent limits", func(t *testing.T) {
				const count = 4
				results := make([]*native.ScanResult, count)
				errors := make([]error, count)
				var wg sync.WaitGroup
				for i := 0; i < count; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						results[i], errors[i] = adapter.ScanGrep(context.Background(), offsets, req)
					}(i)
				}
				wg.Wait()
				for i := range results {
					if results[i] != nil {
						defer results[i].Release()
					}
					require.NoError(t, errors[i])
					require.Equal(t, []int{2}, results[i].Positions)
				}
			})
			// Exercise the real GrepOp/row-selection path, not only the native bridge.
			repr := &chain.OperatorRepr{Type: types.OpTypeGrep, Inputs: []string{"text"}, Outputs: []string{"matches"}, Params: map[string]*schemapb.FunctionParamValue{
				"arguments": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "-e timeout"}},
			}}
			op, err := chain.NewGrepOpFromReprWithContext(repr, types.FunctionBuildContext{QueryScan: &types.QueryScanBuildContext{FieldID: 101, QueryLimit: 1, Segment: adapter}})
			require.NoError(t, err)
			builder := chain.NewDataFrameBuilder().SetChunkSizes([]int64{4})
			require.NoError(t, builder.AddColumnFromChunks(types.SegOffsetFieldName, []arrow.Array{offsets}))
			input := builder.Build()
			builder.Release()
			defer input.Release()
			fc := chain.NewFuncChainWithAllocator(pool).SetStage(types.StageQueryScan).Add(op)
			output, err := fc.Execute(input)
			require.NoError(t, err)
			defer output.Release()
			require.Equal(t, int64(1), output.NumRows())
			require.Equal(t, []int64{2}, output.Column(types.SegOffsetFieldName).Chunk(0).(*array.Int64).Int64Values())
			details := output.Column("matches").Chunk(0).(*array.Binary)
			var lines []struct {
				Content string `json:"content"`
			}
			require.NoError(t, json.Unmarshal(details.Value(0), &lines))
			require.Equal(t, "超时 timeout", lines[0].Content)
			// Exercise native Binary -> Op logical metadata -> JSON FieldData.
			finalBuilder := chain.NewDataFrameBuilder().SetChunkSizes(output.ChunkSizes())
			require.NoError(t, finalBuilder.AddColumnFrom(output, "matches"))
			for _, name := range []string{types.IDFieldName, types.TimestampFieldName} {
				values := array.NewInt64Builder(pool)
				values.Append(2)
				require.NoError(t, finalBuilder.AddColumnFromChunks(name, []arrow.Array{values.NewArray()}))
				values.Release()
			}
			finalFrame := finalBuilder.Build()
			finalBuilder.Release()
			exported, err := chain.ToRetrieveResults(finalFrame)
			finalFrame.Release()
			require.NoError(t, err)
			require.Len(t, exported.ComputedResults, 1)
			require.Equal(t, schemapb.DataType_JSON, exported.ComputedResults[0].Type)
			require.Zero(t, exported.ComputedResults[0].FieldId)
			require.JSONEq(t, `[{"content":"超时 timeout","starts":[7],"ends":[14]}]`,
				string(exported.ComputedResults[0].GetScalars().GetJsonData().Data[0]))
			t.Run("JSON output budgets", func(t *testing.T) {
				// Escaping makes the C++ JSON output larger than the Rust result.
				text := strings.Repeat("\\", 1024) + "timeout"
				_, err := segment.Insert(context.Background(), &segcore.InsertRequest{
					RowIDs: []int64{4}, Timestamps: []uint64{1},
					Record: &segcorepb.InsertRecord{NumRows: 1, FieldsData: []*schemapb.FieldData{
						{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{4}}}}}},
						{FieldId: 101, Type: dataType, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{ValidData: []bool{true}, Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{text}}}}}},
					}},
				})
				require.NoError(t, err)
				b := array.NewInt64Builder(pool)
				b.AppendValues([]int64{4, 3}, nil)
				candidates := b.NewInt64Array()
				b.Release()
				defer candidates.Release()
				scan := req
				scan.ExecutionLimits.BatchSize = 1
				first, err := adapter.ScanGrep(context.Background(), candidates, scan)
				require.NoError(t, err)
				jsonBytes := uint64(len(first.Details.(*array.Binary).Value(0))) + 16
				first.Release()
				for _, tc := range []struct {
					name   string
					budget uint64
					reason string
				}{
					{"JSON expansion exceeds budget", 1800, "actual="},
					{"no budget for next batch", jsonBytes, "remaining=0 bytes"},
				} {
					t.Run(tc.name, func(t *testing.T) {
						scan.Limit = 2
						scan.ExecutionLimits.MaxResultBytes = tc.budget
						result, err := adapter.ScanGrep(context.Background(), candidates, scan)
						defer result.Release()
						require.Nil(t, result)
						require.Error(t, err)
						require.Contains(t, err.Error(), tc.reason)
						require.Equal(t, int32(merr.CodeInvalidParameter), merr.Code(err))
						remote := merr.Error(merr.Status(err))
						require.Equal(t, merr.InputError, merr.GetErrorType(remote))
						require.False(t, merr.IsRetryableErr(remote))
					})
				}
			})
		})
	}
}

// Cancel when the native cancellation guard subscribes, after the Go entry
// check has observed a live context. The underlying context owns all state.
type grepCancelOnDoneContext struct {
	context.Context
	cancel context.CancelFunc
}

func (ctx grepCancelOnDoneContext) Done() <-chan struct{} {
	ctx.cancel()
	return ctx.Context.Done()
}
