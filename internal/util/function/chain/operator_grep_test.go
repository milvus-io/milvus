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
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/grep"
	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func grepTestRepr(t *testing.T, limit int64) *OperatorRepr {
	t.Helper()
	pb := queryChainFixture().Ops[0]
	pb.Params["limit"].Value = &schemapb.FunctionParamValue_Int64Value{Int64Value: limit}
	repr, err := ProtoOpToRepr(pb)
	require.NoError(t, err)
	return repr
}

type grepTestRunner func(context.Context, *array.Int64, int64) (*native.ScanResult, error)
type grepTestSegment struct{ run grepTestRunner }

func (s grepTestSegment) ScanGrep(ctx context.Context, offsets *array.Int64, req native.ScanRequest) (*native.ScanResult, error) {
	return s.run(ctx, offsets, req.Limit)
}

type grepBinding struct {
	Build types.FunctionBuildContext
}

func grepTestBinding(runner grepTestRunner) grepBinding {
	return grepBinding{
		Build: types.FunctionBuildContext{QueryScan: &types.QueryScanBuildContext{
			FieldID: 101, Segment: grepTestSegment{run: runner},
		}},
	}
}
func grepTestLimits() native.ExecutionLimits {
	return native.ExecutionLimits{BatchSize: 128, MaxResultBytes: uint64(paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64())}
}
func (b grepBinding) execution(ctx context.Context, pool memory.Allocator) *types.FuncContext {
	return types.NewFuncContextFull(ctx, pool, types.StageQueryScan)
}

func grepTestDetails(t *testing.T, pool memory.Allocator, json string) arrow.Array {
	t.Helper()
	arr, _, err := array.FromJSON(pool, native.GrepDetailsType(), strings.NewReader(json))
	require.NoError(t, err)
	return arr
}

const grepHit = `[{"content":"超时 timeout","spans":[{"start":7,"end":14}]}]`

func grepTestFrame(t *testing.T, pool memory.Allocator, values string) *DataFrame {
	t.Helper()
	column := selectionTestColumn(t, pool, arrow.PrimitiveTypes.Int64, values)
	b := NewDataFrameBuilder()
	defer b.Release()
	b.SetChunkSizes([]int64{int64(column.Len())})
	require.NoError(t, b.addColumn(types.SegOffsetFieldName, column))
	return b.Build()
}

func TestGrepSelectionDetailsAndLimitSnapshot(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := grepTestFrame(t, pool, "[7,2,90,41]")
	defer func() {
		if input != nil {
			input.Release()
		}
	}()
	called := false
	binding := grepTestBinding(func(_ context.Context, offsets *array.Int64, limit int64) (*native.ScanResult, error) {
		called = true
		require.Equal(t, []int64{7, 2, 90, 41}, offsets.Int64Values())
		require.Equal(t, int64(2), limit)
		return &native.ScanResult{Positions: []int64{1, 3}, Details: grepTestDetails(t, pool, "["+grepHit+","+grepHit+"]")}, nil
	})
	op, err := NewGrepOpFromReprWithContext(grepTestRepr(t, 2), binding.Build)
	require.NoError(t, err)
	require.Equal(t, []string{types.SegOffsetFieldName}, op.Inputs())
	item := &paramtable.Get().FunctionCfg.GrepMaxLimit
	old := item.GetValue()
	defer paramtable.Get().Save(item.Key, old)
	require.NoError(t, paramtable.Get().Save(item.Key, "1"))
	result, err := ExecuteOperator(binding.execution(context.Background(), pool), input, op)
	require.NoError(t, err)
	defer result.Release()
	require.True(t, called)
	require.Equal(t, int64(4), input.NumRows())
	input.Release()
	input = nil
	require.Equal(t, []int64{2, 41}, result.Column(types.SegOffsetFieldName).Chunk(0).(*array.Int64).Int64Values())
	require.Equal(t, int64(2), result.NumRows())
	details := result.Column("matches").Chunk(0).(*array.List)
	lines := details.ListValues().(*array.Struct)
	require.Equal(t, "超时 timeout", lines.Field(0).(*array.String).Value(0))
	spans := lines.Field(1).(*array.List).ListValues().(*array.Struct)
	require.Equal(t, uint64(7), spans.Field(0).(*array.Uint64).Value(0))
	require.Equal(t, uint64(14), spans.Field(1).(*array.Uint64).Value(0))
}

func TestGrepPreparationAndCapability(t *testing.T) {
	repr := grepTestRepr(t, 1)
	_, err := NewGrepOpFromReprWithContext(repr, types.FunctionBuildContext{})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	binding := grepTestBinding(nil)
	repr.Params["arguments"].Value = &schemapb.FunctionParamValue_StringValue{StringValue: `-e '['`}

	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := grepTestFrame(t, pool, "[7]")
	defer input.Release()
	syntaxErr := merr.WrapErrParameterInvalidMsg("native regex syntax")
	binding.Build.QueryScan.Segment = grepTestSegment{run: func(context.Context, *array.Int64, int64) (*native.ScanResult, error) { return nil, syntaxErr }}
	op, err := NewGrepOpFromReprWithContext(repr, binding.Build)
	require.NoError(t, err) // Engine compilation is local to each native call.
	result, err := ExecuteOperator(binding.execution(context.Background(), pool), input, op)
	require.Nil(t, result)
	require.ErrorIs(t, err, syntaxErr)
	repr.Params["arguments"].Value = &schemapb.FunctionParamValue_StringValue{StringValue: `-C 2 -e x`}
	_, err = NewGrepOpFromReprWithContext(repr, binding.Build)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)

}

func TestGrepMaximumValidation(t *testing.T) {
	const defaultLimit int64 = 10000
	repr := grepTestRepr(t, 10)
	item := &paramtable.Get().FunctionCfg.GrepMaxLimit
	old := item.GetValue()
	defer paramtable.Get().Save(item.Key, old)
	binding := grepTestBinding(func(context.Context, *array.Int64, int64) (*native.ScanResult, error) { return nil, nil })
	for _, value := range []string{"", "0", "-1", "bad", "1.5", "9223372036854775808"} {
		t.Run(value, func(t *testing.T) {
			require.NoError(t, paramtable.Get().Save(item.Key, value))
			require.Equal(t, defaultLimit, paramtable.Get().FunctionCfg.GrepMaxLimit.GetAsInt64())
			for _, limit := range []int64{defaultLimit, defaultLimit + 1} {
				repr.Params["limit"].Value = &schemapb.FunctionParamValue_Int64Value{Int64Value: limit}
				_, opErr := NewGrepOpFromReprWithContext(repr, binding.Build)
				pb := queryChainFixture()
				pb.Ops[0].Params["limit"].Value = &schemapb.FunctionParamValue_Int64Value{Int64Value: limit}
				_, planErr := PrepareQueryChain([]*schemapb.FunctionChain{pb}, querySchemaFixture())
				if limit == defaultLimit {
					require.NoError(t, opErr)
					require.NoError(t, planErr)
				} else {
					require.ErrorIs(t, opErr, merr.ErrParameterInvalid)
					require.ErrorIs(t, planErr, merr.ErrParameterInvalid)
				}
			}
		})
	}
	require.NoError(t, paramtable.Get().Save(item.Key, "9"))
	require.Equal(t, int64(9), paramtable.Get().FunctionCfg.GrepMaxLimit.GetAsInt64())
	repr.Params["limit"].Value = &schemapb.FunctionParamValue_Int64Value{Int64Value: 10}
	_, err := NewGrepOpFromReprWithContext(repr, binding.Build)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	_, err = PrepareQueryChain([]*schemapb.FunctionChain{queryChainFixture()}, querySchemaFixture())
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestGrepEmptyAndRunnerFailureOwnership(t *testing.T) {
	for _, fail := range []bool{false, true} {
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		input := grepTestFrame(t, pool, "[]")
		upstream := merr.Wrap(merr.ErrServiceRateLimit, "test runner budget")
		binding := grepTestBinding(func(context.Context, *array.Int64, int64) (*native.ScanResult, error) {
			r := &native.ScanResult{Details: grepTestDetails(t, pool, "[]")}
			if fail {
				return r, upstream
			}
			return r, nil
		})
		op, err := NewGrepOpFromReprWithContext(grepTestRepr(t, 1), binding.Build)
		require.NoError(t, err)
		result, err := ExecuteOperator(binding.execution(context.Background(), pool), input, op)
		if fail {
			require.Nil(t, result)
			require.ErrorIs(t, err, upstream)
			require.Equal(t, merr.Code(upstream), merr.Code(err))
		} else {
			require.NoError(t, err)
			require.Equal(t, int64(0), result.NumRows())
			require.True(t, result.HasColumn("matches"))
			result.Release()
		}
		input.Release()
		pool.AssertSize(t, 0)
	}
}

func TestGrepCancellationAfterRunner(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := grepTestFrame(t, pool, "[1]")
	defer input.Release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	binding := grepTestBinding(func(context.Context, *array.Int64, int64) (*native.ScanResult, error) {
		cancel()
		return &native.ScanResult{Positions: []int64{0}, Details: grepTestDetails(t, pool, "["+grepHit+"]")}, nil
	})
	op, err := NewGrepOpFromReprWithContext(grepTestRepr(t, 1), binding.Build)
	require.NoError(t, err)
	result, err := ExecuteOperator(binding.execution(ctx, pool), input, op)
	require.Nil(t, result)
	require.ErrorIs(t, err, context.Canceled)
}

func TestGrepChainRegistrationAndStage(t *testing.T) {
	pb := queryChainFixture()
	repr, err := ProtoChainToRepr(pb)
	require.NoError(t, err)
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	binding := grepTestBinding(func(context.Context, *array.Int64, int64) (*native.ScanResult, error) {
		return &native.ScanResult{Details: grepTestDetails(t, pool, "[]")}, nil
	})
	fc, err := FuncChainFromReprWithContext(repr, pool, binding.Build)
	require.NoError(t, err)
	input := grepTestFrame(t, pool, "[7,2]")
	defer input.Release()
	result, err := fc.Execute(input)
	require.NoError(t, err)
	defer result.Release()
	require.Equal(t, int64(0), result.NumRows())
	repr.Stage = types.StageL0Rerank
	_, err = FuncChainFromReprWithContext(repr, pool, binding.Build)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestGrepNilOperatorAndNormalizedStage(t *testing.T) {
	fc := NewFuncChainWithAllocator(memory.DefaultAllocator).SetStage(types.StageQueryScan).Add(nil)
	require.ErrorIs(t, fc.Validate(), merr.ErrServiceInternal)
	var op *GrepOp
	fc = NewFuncChainWithAllocator(memory.DefaultAllocator).SetStage(types.StageQueryScan).Add(op)
	require.ErrorIs(t, fc.Validate(), merr.ErrServiceInternal)
	pb := queryChainFixture()
	pb.OutputColumns = nil
	pb.Stage = schemapb.FunctionChainStage_FunctionChainStageL0Rerank
	pb.Ops[0].Op = " grep "
	_, err := ProtoChainToRepr(pb)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

// Deterministic segment adapter for concurrency tests. It validates orchestration
// and isolation, not native regex semantics or actual segment storage reads.
type nativeTextTestSegment struct {
	pool    memory.Allocator
	label   string
	entered chan<- native.ScanRequest
	gate    <-chan struct{}
	reads   atomic.Int64
}

func (s *nativeTextTestSegment) ScanGrep(ctx context.Context, offsets *array.Int64, req native.ScanRequest) (*native.ScanResult, error) {
	s.entered <- req
	if req.FieldID != 101 || req.Spec.PatternCount() != 1 || req.Spec.Pattern(0) != "timeout" || req.Spec.CaseMode() != grep.CaseInsensitive {
		return nil, merr.WrapErrServiceInternalMsg("wrong grep preparation or shared pattern mutation")
	}
	// Adapters may replace their local copy, but cannot mutate parsed patterns.
	req.Spec = grep.Spec{}
	select {
	case <-s.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	builder := array.NewBuilder(s.pool, native.GrepDetailsType()).(*array.ListBuilder)
	defer builder.Release()
	lines := builder.ValueBuilder().(*array.StructBuilder)
	content := lines.FieldBuilder(0).(*array.StringBuilder)
	spans := lines.FieldBuilder(1).(*array.ListBuilder)
	span := spans.ValueBuilder().(*array.StructBuilder)
	starts := span.FieldBuilder(0).(*array.Uint64Builder)
	ends := span.FieldBuilder(1).(*array.Uint64Builder)
	selected := make([]int64, 0)
	for i, offset := range offsets.Int64Values() {
		if int64(len(selected)) >= req.Limit {
			break
		}
		s.reads.Add(1)
		if offset == 90 || offset == 2 {
			continue
		} // predetermined NULL / non-match
		text := s.label + " timeout"
		selected = append(selected, int64(i))
		builder.Append(true)
		lines.Append(true)
		content.Append(text)
		spans.Append(true)
		span.Append(true)
		starts.Append(uint64(len(s.label) + 1))
		ends.Append(uint64(len(text)))
	}
	return &native.ScanResult{Positions: selected, Details: builder.NewArray()}, nil
}

func TestGrepSegmentChainsParallel(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	pb := queryChainFixture()
	pb.Ops[0].Params["limit"].Value = &schemapb.FunctionParamValue_Int64Value{Int64Value: 2}
	repr, err := ProtoChainToRepr(pb)
	require.NoError(t, err)
	binding := grepTestBinding(nil)
	const n = 8
	chains := make([]*FuncChain, n)
	entered := make(chan native.ScanRequest, n)
	gate := make(chan struct{})
	inputs := make([]*DataFrame, n)
	outputs := make([]*DataFrame, n)
	errs := make([]error, n)
	segments := make([]*nativeTextTestSegment, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		inputs[i] = grepTestFrame(t, pool, "[7,2,90,41,99]")
		segments[i] = &nativeTextTestSegment{pool: pool, label: strconv.Itoa(i), entered: entered, gate: gate}
		binding.Build.QueryScan.Segment = segments[i]
		chains[i], err = FuncChainFromReprWithContext(repr, pool, binding.Build)
		require.NoError(t, err)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outputs[i], errs[i] = chains[i].ExecuteWithOptions(context.Background(), ExecuteOptions{
				EnableColumnPruning: true, Downstream: DownstreamSpec{RequiredColumns: []string{"matches"}},
			}, inputs[i])
		}(i)
	}
	for i := 0; i < n; i++ {
		req := <-entered
		require.Equal(t, int64(2), req.Limit)
		require.Equal(t, native.CompileLimits{MaxRegexBytes: 32 << 20, MaxDFACacheBytes: 8 << 20}, req.CompileLimits)
		require.Equal(t, grepTestLimits(), req.ExecutionLimits)
	}
	close(gate)
	wg.Wait()
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		require.Equal(t, int64(2), outputs[i].NumRows())
		require.Equal(t, []int64{7, 41}, outputs[i].Column(types.SegOffsetFieldName).Chunk(0).(*array.Int64).Int64Values())
		lines := outputs[i].Column("matches").Chunk(0).(*array.List).ListValues().(*array.Struct)
		require.Equal(t, strconv.Itoa(i)+" timeout", lines.Field(0).(*array.String).Value(0))
		require.Equal(t, int64(4), segments[i].reads.Load())
		outputs[i].Release()
		inputs[i].Release()
	}
	for i, fc := range chains {
		require.Equal(t, "timeout", fc.operators[0].(*GrepOp).request.Spec.Pattern(0))
		if i > 0 {
			require.NotSame(t, chains[0].operators[0], fc.operators[0])
		}
	}
}

func TestGrepSegmentChainsCancellationIsolation(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	repr, err := ProtoChainToRepr(queryChainFixture())
	require.NoError(t, err)
	entered := make(chan native.ScanRequest, 2)
	gate := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	contexts := []context.Context{ctx, context.Background()}
	inputs := []*DataFrame{grepTestFrame(t, pool, "[7]"), grepTestFrame(t, pool, "[7]")}
	outputs := make([]*DataFrame, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		binding := grepTestBinding(nil)
		binding.Build.QueryScan.Segment = &nativeTextTestSegment{pool: pool, label: strconv.Itoa(i), entered: entered, gate: gate}
		fc, err := FuncChainFromReprWithContext(repr, pool, binding.Build)
		require.NoError(t, err)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outputs[i], errs[i] = fc.ExecuteWithContext(contexts[i], inputs[i])
		}(i)
	}
	<-entered
	<-entered
	cancel()
	close(gate)
	wg.Wait()
	require.ErrorIs(t, errs[0], context.Canceled)
	require.Nil(t, outputs[0])
	require.NoError(t, errs[1])
	require.Equal(t, int64(1), outputs[1].NumRows())
	outputs[1].Release()
	for _, input := range inputs {
		input.Release()
	}
}

func TestGrepRequiresSegmentAndExecutionBudgets(t *testing.T) {
	binding := grepTestBinding(nil)
	binding.Build.QueryScan.Segment = nil
	op, err := NewGrepOpFromReprWithContext(grepTestRepr(t, 1), binding.Build)
	require.Nil(t, op)
	require.ErrorIs(t, err, merr.ErrServiceInternal)

	item := &paramtable.Get().QuotaConfig.MaxOutputSize
	previous := item.GetValue()
	defer paramtable.Get().Save(item.Key, previous)
	for _, value := range []string{"0", "-1", "bad"} {
		require.NoError(t, paramtable.Get().Save(item.Key, value))
		op, err := NewGrepOpFromReprWithContext(grepTestRepr(t, 1), grepTestBinding(nil).Build)
		require.Nil(t, op)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
	}
}

func TestGrepExecutionConfigurationSnapshot(t *testing.T) {
	item := &paramtable.Get().FunctionCfg.GrepBatchSize
	previous := item.GetValue()
	defer paramtable.Get().Save(item.Key, previous)
	for _, value := range []string{"", "0", "-1", "bad", "9223372036854775808"} {
		require.NoError(t, paramtable.Get().Save(item.Key, value))
		require.Equal(t, int64(128), paramtable.Get().FunctionCfg.GrepBatchSize.GetAsInt64())
	}
	require.NoError(t, paramtable.Get().Save(item.Key, "7"))
	resultItem := &paramtable.Get().QuotaConfig.MaxOutputSize
	previousResult := resultItem.GetValue()
	defer paramtable.Get().Save(resultItem.Key, previousResult)
	require.NoError(t, paramtable.Get().Save(resultItem.Key, "65536"))
	regexItem := &paramtable.Get().FunctionCfg.GrepMaxRegexBytes
	cacheItem := &paramtable.Get().FunctionCfg.GrepMaxDFACacheBytes
	oldRegex, oldCache := regexItem.GetValue(), cacheItem.GetValue()
	defer paramtable.Get().Save(regexItem.Key, oldRegex)
	defer paramtable.Get().Save(cacheItem.Key, oldCache)
	require.NoError(t, paramtable.Get().Save(regexItem.Key, "4096"))
	require.NoError(t, paramtable.Get().Save(cacheItem.Key, "8192"))
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	entered := make(chan native.ScanRequest, 1)
	gate := make(chan struct{})
	close(gate)
	binding := grepTestBinding(nil)
	binding.Build.QueryScan.Segment = &nativeTextTestSegment{pool: pool, label: "batch", entered: entered, gate: gate}
	op, err := NewGrepOpFromReprWithContext(grepTestRepr(t, 1), binding.Build)
	require.NoError(t, err)
	binding.Build.QueryScan.FieldID = 999
	require.NoError(t, paramtable.Get().Save(regexItem.Key, "16384"))
	require.NoError(t, paramtable.Get().Save(cacheItem.Key, "32768"))
	require.NoError(t, paramtable.Get().Save(item.Key, "11"))
	require.NoError(t, paramtable.Get().Save(resultItem.Key, "131072"))
	input := grepTestFrame(t, pool, "[7]")
	defer input.Release()
	result, err := ExecuteOperator(binding.execution(context.Background(), pool), input, op)
	require.NoError(t, err)
	defer result.Release()
	req := <-entered
	require.Equal(t, int64(7), req.ExecutionLimits.BatchSize)
	require.Equal(t, uint64(65536), req.ExecutionLimits.MaxResultBytes)
	require.Equal(t, native.CompileLimits{MaxRegexBytes: 4096, MaxDFACacheBytes: 8192}, req.CompileLimits)
	require.Equal(t, int64(1), req.Limit)
	require.Equal(t, int64(11), paramtable.Get().FunctionCfg.GrepBatchSize.GetAsInt64())
}

func TestGrepArgumentSizeConfiguration(t *testing.T) {
	repr := grepTestRepr(t, 1)
	repr.Params["arguments"].Value = &schemapb.FunctionParamValue_StringValue{StringValue: "-e a"}
	item := &paramtable.Get().FunctionCfg.GrepMaxArgumentBytes
	previous := item.GetValue()
	defer paramtable.Get().Save(item.Key, previous)
	require.NoError(t, paramtable.Get().Save(item.Key, "4"))
	_, err := NewGrepOpFromReprWithContext(repr, grepTestBinding(nil).Build)
	require.NoError(t, err)
	require.NoError(t, paramtable.Get().Save(item.Key, "3"))
	_, err = NewGrepOpFromReprWithContext(repr, grepTestBinding(nil).Build)
	require.ErrorIs(t, err, merr.ErrParameterTooLarge)
	require.NoError(t, paramtable.Get().Save(item.Key, "4"))
	_, err = NewGrepOpFromReprWithContext(repr, grepTestBinding(nil).Build)
	require.NoError(t, err)
}
