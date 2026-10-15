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
	"path/filepath"
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
			req := native.ScanRequest{FieldID: 101, Limit: 1, Spec: grep.Spec{Patterns: []string{"timeout"}}, CompileLimits: native.CompileLimits{MaxRegexBytes: 1048576, MaxDFACacheBytes: 1048576}, ExecutionLimits: native.ExecutionLimits{BatchSize: 128, MaxResultBytes: 1048576}}
			result, err := adapter.ScanGrep(context.Background(), offsets, req)
			require.NoError(t, err)
			require.Equal(t, []int64{2}, result.Positions)
			require.True(t, arrow.TypeEqual(native.GrepDetailsType(), result.Details.DataType()))
			result.Release()
			t.Run("errors", func(t *testing.T) {
				invalid := req
				invalid.Spec.Patterns = []string{"["}
				result, err := adapter.ScanGrep(context.Background(), offsets, invalid)
				require.Nil(t, result)
				require.Error(t, err)
				require.Equal(t, merr.InputError, merr.GetErrorType(err))
				invalid = req
				invalid.ExecutionLimits.MaxResultBytes = 1
				result, err = adapter.ScanGrep(context.Background(), offsets, invalid)
				require.Nil(t, result)
				require.Error(t, err)
				require.Equal(t, merr.SystemError, merr.GetErrorType(err))
				require.Equal(t, int32(merr.CodeOutOfRange), merr.Code(err))
				require.Contains(t, err.Error(), "quotaAndLimits.limits.maxOutputSize=1 bytes")
				require.Contains(t, err.Error(), "remaining=1 bytes, requested=16 bytes")
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
			t.Run("request deadline during scan", func(t *testing.T) {
				builder := array.NewInt64Builder(pool)
				builder.AppendValues(make([]int64, 1000000), nil) // Repeated nonmatching row.
				manyOffsets := builder.NewInt64Array()
				builder.Release()
				defer manyOffsets.Release()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				result, err := adapter.ScanGrep(ctx, manyOffsets, req)
				require.Nil(t, result)
				require.ErrorIs(t, err, context.DeadlineExceeded)
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
					require.Equal(t, []int64{2}, results[i].Positions)
				}
			})
			// Exercise the real GrepOp/row-selection path, not only the native bridge.
			repr := &chain.OperatorRepr{Type: types.OpTypeGrep, Inputs: []string{"text"}, Outputs: []string{"matches"}, Params: map[string]*schemapb.FunctionParamValue{
				"arguments": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "-e timeout"}},
				"limit":     {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 1}},
			}}
			op, err := chain.NewGrepOpFromReprWithContext(repr, types.FunctionBuildContext{QueryScan: &types.QueryScanBuildContext{FieldID: 101, Segment: adapter}})
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
			details := output.Column("matches").Chunk(0).(*array.List)
			lines := details.ListValues().(*array.Struct)
			require.Equal(t, "超时 timeout", lines.Field(0).(*array.String).Value(0))
		})
	}
}
