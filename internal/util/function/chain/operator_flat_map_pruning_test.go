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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

type flatMapPeakAllocator struct {
	checked *memory.CheckedAllocator
	mu      sync.Mutex
	peak    int
}

func (a *flatMapPeakAllocator) Allocate(size int) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	data := a.checked.Allocate(size)
	a.peak = max(a.peak, a.checked.CurrentAlloc())
	return data
}

func (a *flatMapPeakAllocator) Reallocate(size int, data []byte) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	data = a.checked.Reallocate(size, data)
	a.peak = max(a.peak, a.checked.CurrentAlloc())
	return data
}

func (a *flatMapPeakAllocator) Free(data []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.checked.Free(data)
}

func flatMapProjectionExpr() *operatorTransformExpr {
	contentType := arrow.StructOf(arrow.Field{Name: "text", Type: arrow.BinaryTypes.String})
	chunkType := arrow.StructOf(arrow.Field{Name: "content", Type: contentType})
	return &operatorTransformExpr{
		name: "test_flatmap_projection", stages: []string{types.StageIngestion},
		outputs: []arrow.DataType{arrow.ListOf(chunkType)},
		execute: func(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
			chunks := make([]arrow.Array, len(inputs[0].Chunks()))
			for group, input := range inputs[0].Chunks() {
				list := array.NewListBuilder(ctx.Pool(), chunkType)
				chunk := list.ValueBuilder().(*array.StructBuilder)
				content := chunk.FieldBuilder(0).(*array.StructBuilder)
				text := content.FieldBuilder(0).(*array.StringBuilder)
				for row := range input.Len() {
					list.Append(true)
					if input.(*array.String).Value(row) == "" {
						continue
					}
					for range 256 {
						chunk.Append(true)
						content.Append(true)
						text.Append("x")
					}
				}
				chunks[group] = list.NewArray()
				list.Release()
			}
			output := arrow.NewChunked(arrow.ListOf(chunkType), chunks)
			for _, chunk := range chunks {
				chunk.Release()
			}
			return []*arrow.Chunked{output}, nil
		},
	}
}

func flatMapProjectionInput(t *testing.T, pool memory.Allocator, document string) *DataFrame {
	t.Helper()
	id := array.NewInt64Builder(pool)
	id.Append(100)
	idData := id.NewArray()
	id.Release()
	text := array.NewStringBuilder(pool)
	text.Append(document)
	documentData := text.NewArray()
	text.Append("tenant-a")
	tenantData := text.NewArray()
	text.Release()
	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes([]int64{1}).SetIdentityLineage()
	require.NoError(t, builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idData}))
	require.NoError(t, builder.AddColumnFromChunks("document", []arrow.Array{documentData}))
	require.NoError(t, builder.AddColumnFromChunks("tenant", []arrow.Array{tenantData}))
	return builder.Build()
}

func TestFlatMapProjectionReducesActualArrowPeak(t *testing.T) {
	run := func(prune bool) int {
		pool := &flatMapPeakAllocator{checked: memory.NewCheckedAllocator(memory.DefaultAllocator)}
		defer pool.checked.AssertSize(t, 0)
		input := flatMapProjectionInput(t, pool, strings.Repeat("d", 4096))
		defer input.Release()
		fc := NewFuncChainWithAllocator(pool).SetStage(types.StageIngestion).
			FlatMap(flatMapProjectionExpr(), []string{"document"}, "chunk").
			SelectAs([]string{"chunk.content"}, []string{"snippet"}).
			SelectAs([]string{"snippet.text"}, []string{"text"})
		output, err := fc.ExecuteWithOptions(context.Background(), ExecuteOptions{
			EnableColumnPruning: prune, Downstream: DownstreamSpec{RequiredColumns: []string{"text"}},
		}, input)
		require.NoError(t, err)
		defer output.Release()
		require.EqualValues(t, 256, output.NumRows())
		require.Equal(t, "x", output.Column("text").Chunk(0).(*array.String).Value(255))
		require.ElementsMatch(t, []string{types.IDFieldName, "text"}, output.ColumnNames())
		for _, lineage := range output.Lineage()[0] {
			require.Equal(t, transform.RowLineage{{Group: 0, Row: 0}}, lineage)
		}
		return pool.peak
	}
	unpruned, pruned := run(false), run(true)
	t.Logf("Arrow peak allocation: unpruned=%dB, pruned=%dB", unpruned, pruned)
	require.GreaterOrEqual(t, unpruned, 4096*256)
	require.Less(t, pruned, unpruned/16)
}

func TestFlatMapProjectionRetainsInputsOfLaterMap(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := flatMapProjectionInput(t, pool, strings.Repeat("d", 4096))
	defer input.Release()
	label := &operatorTransformExpr{
		name: "test_parent_label", stages: []string{types.StageIngestion}, outputs: []arrow.DataType{arrow.BinaryTypes.String},
		execute: func(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
			builder := array.NewStringBuilder(ctx.Pool())
			defer builder.Release()
			document, tenant := inputs[0].Chunk(0).(*array.String), inputs[1].Chunk(0).(*array.String)
			for row := range document.Len() {
				builder.Append(strconv.Itoa(len(document.Value(row))) + ":" + tenant.Value(row))
			}
			data := builder.NewArray()
			defer data.Release()
			return []*arrow.Chunked{arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{data})}, nil
		},
	}
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StageIngestion).
		FlatMap(flatMapProjectionExpr(), []string{"document"}, "chunk").
		Map(label, []string{"document", "tenant"}, []string{"label"}).
		SelectAs([]string{"chunk.content.text", "label"}, []string{"text", "label"}).
		ExecuteWithOptions(context.Background(), ExecuteOptions{
			EnableColumnPruning: true, Downstream: DownstreamSpec{RequiredColumns: []string{"text", "label"}},
		}, input)
	require.NoError(t, err)
	defer output.Release()
	labels := output.Column("label").Chunk(0).(*array.String)
	for row := range labels.Len() {
		require.Equal(t, "4096:tenant-a", labels.Value(row))
	}
}

var registerFlatMapProjection sync.Once

func TestTransformExecutorPrunesOnlyWithExplicitTerminalSelect(t *testing.T) {
	registerFlatMapProjection.Do(func() {
		types.MustRegisterFunction("test_flatmap_projection", func(types.FunctionBuildContext, types.FunctionConfig) (types.FunctionExpr, error) {
			return flatMapProjectionExpr(), nil
		})
	})
	for _, tc := range []struct {
		name, document string
		selectOutput   bool
		rows           int64
	}{
		{name: "project", document: strings.Repeat("d", 4096), selectOutput: true, rows: 256},
		{name: "empty", selectOutput: true, rows: 0},
		{name: "keep parent", document: strings.Repeat("d", 4096), rows: 256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := &flatMapPeakAllocator{checked: memory.NewCheckedAllocator(memory.DefaultAllocator)}
			defer pool.checked.AssertSize(t, 0)
			frame := flatMapProjectionInput(t, pool, tc.document)
			defer frame.Release()
			input, err := ToArrowBatch(frame)
			require.NoError(t, err)
			defer ReleaseArrowBatch(input)
			flat, err := transform.FlatMap("chunk", operatorTransformInputExpr("test_flatmap_projection", "document"), transform.ExplodeOptions{})
			require.NoError(t, err)
			ops := []*schemapb.FunctionChainOp{flat}
			if tc.selectOutput {
				selection, err := transform.Select([]string{"chunk.content.text"}, []string{"text"})
				require.NoError(t, err)
				ops = append(ops, selection)
			}
			plan, err := transform.PreInsert(tc.name, ops...)
			require.NoError(t, err)
			result, err := NewTransformExecutor(pool, types.FunctionBuildContext{}).Execute(context.Background(), plan, input)
			require.NoError(t, err)
			defer ReleaseArrowBatch(result.Data)
			require.Len(t, result.Data.Groups, 1)
			require.Equal(t, tc.rows, result.Data.Groups[0].NumRows())
			require.Len(t, result.Lineage[0], int(tc.rows))
			_, hasDocument := result.Data.Groups[0].Schema().FieldsByName("document")
			require.Equal(t, !tc.selectOutput, hasDocument)
			if tc.selectOutput {
				require.Less(t, pool.peak, 128*1024)
			} else {
				require.GreaterOrEqual(t, pool.peak, 4096*256)
			}
		})
	}
}
