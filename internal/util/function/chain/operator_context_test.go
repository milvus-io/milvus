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
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func operatorContextFields(ctx context.Context) map[string]string {
	result := make(map[string]string)
	for _, field := range mlog.FieldsFromContext(ctx) {
		result[field.Key] = field.String
	}
	return result
}

func TestCachedChainRequestScopesRemainIsolatedAcrossConcurrentCalls(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := preQueryFrame(t, pool, []string{"query"})
	defer input.Release()
	type observed struct {
		scope    types.RequestContext
		fields   map[string]string
		deadline time.Time
		query    *types.QueryContext
		limit    int64
		span     trace.SpanContext
	}
	seen := make(chan observed, 2)
	function := &operatorTransformExpr{
		name: "shared model", stages: []string{types.StagePreProcess}, outputs: []arrow.DataType{arrow.BinaryTypes.String},
		execute: func(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
			deadline, _ := ctx.Context().Deadline()
			seen <- observed{ctx.RequestContext(), operatorContextFields(ctx.Context()), deadline, ctx.QueryContext(), ctx.OutputRowLimit(), trace.SpanContextFromContext(ctx.Context())}
			inputs[0].Retain()
			return []*arrow.Chunked{inputs[0]}, nil
		},
	}
	fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePreProcess).SetName("shared_chain").Map(function, []string{"query"}, []string{"query"})
	traceID, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)
	span := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID})
	parent, cancel := context.WithDeadline(trace.ContextWithSpanContext(context.Background(), span), time.Now().Add(time.Minute))
	defer cancel()
	deadline, _ := parent.Deadline()
	queries := preQuerySnapshot("query")
	var group sync.WaitGroup
	errors := make(chan error, 2)
	for _, user := range []string{"alice", "bob"} {
		ctx := types.WithRequestContext(parent, types.RequestContext{Database: "db_" + user, Collection: "docs_" + user, User: user, ResourceName: "parent"})
		group.Add(1)
		go func() {
			defer group.Done()
			output, err := fc.ExecuteWithOptions(ctx, ExecuteOptions{QueryContext: queries, OutputRowLimit: 8}, input)
			if output != nil && output != input {
				output.Release()
			}
			errors <- err
		}()
	}
	group.Wait()
	for range 2 {
		require.NoError(t, <-errors)
		result := <-seen
		require.Contains(t, []string{"alice", "bob"}, result.scope.User)
		require.Equal(t, "db_"+result.scope.User, result.scope.Database)
		require.Equal(t, "docs_"+result.scope.User, result.scope.Collection)
		require.Equal(t, "shared_chain/step/0/Map", result.scope.ResourceName)
		require.Equal(t, result.scope.ResourceName, result.fields["resource_name"])
		require.Equal(t, result.scope.Database, result.fields["dbName"])
		require.Equal(t, result.scope.Collection, result.fields["collectionName"])
		require.Equal(t, result.scope.User, result.fields["user"])
		require.Equal(t, deadline, result.deadline)
		require.Equal(t, span, result.span)
		require.Same(t, queries, result.query)
		require.EqualValues(t, 8, result.limit)
	}
	require.Zero(t, types.GetRequestContext(parent))
	require.Nil(t, mlog.FieldsFromContext(parent))
}

func TestFlatMapRetainsOuterResourceAndExpandedQueryScope(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := preQueryFrame(t, pool, []string{"a|b", "c"})
	defer input.Release()
	split := operatorTransformChunkExpr()
	split.stages = []string{types.StagePreProcess}
	original := split.execute
	split.execute = func(ctx *types.FuncContext, columns []*arrow.Chunked) ([]*arrow.Chunked, error) {
		require.Equal(t, "expand/step/0/FlatMap", ctx.RequestContext().ResourceName)
		require.Equal(t, "alice", ctx.RequestContext().User)
		return original(ctx, columns)
	}
	inspect := &operatorTransformExpr{
		name: "inspect expanded scope", stages: []string{types.StagePreProcess}, outputs: []arrow.DataType{arrow.BinaryTypes.String},
		execute: func(ctx *types.FuncContext, columns []*arrow.Chunked) ([]*arrow.Chunked, error) {
			require.Equal(t, "expand/step/2/Map", ctx.RequestContext().ResourceName)
			require.Equal(t, "alice", ctx.RequestContext().User)
			require.Equal(t, []int64{0, 0, 1}, ctx.QueryContext().RowQueryOrigins)
			require.Equal(t, []string{"expand/step/2/Map"}, []string{operatorContextFields(ctx.Context())["resource_name"]})
			columns[0].Retain()
			return []*arrow.Chunked{columns[0]}, nil
		},
	}
	fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePreProcess).SetName("expand").
		FlatMap(split, []string{"query"}, "child").SelectAs([]string{`child["text"]`}, []string{"query"}).Map(inspect, []string{"query"}, []string{"query"})
	parent := types.WithRequestContext(context.Background(), types.RequestContext{User: "alice"})
	output, err := fc.ExecuteWithOptions(parent, ExecuteOptions{QueryContext: preQuerySnapshot("a|b", "c")}, input)
	require.NoError(t, err)
	output.Release()
	require.Empty(t, types.GetRequestContext(parent).ResourceName)
}

func TestOperatorFailureSchemaAndCancellationLogsUseCurrentScope(t *testing.T) {
	for _, mode := range []string{"function", "schema", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			input := preQueryFrame(t, pool, []string{"a"})
			defer input.Release()
			ctx, cancel := context.WithCancel(types.WithRequestContext(context.Background(), types.RequestContext{Database: "db", Collection: "docs", User: "alice"}))
			defer cancel()
			seen := false
			capture := func(ctx context.Context, message string, _ ...mlog.Field) {
				if mode == "cancel" && message != "function chain execution cancelled" {
					return
				}
				seen = true
				require.Equal(t, "failure/step/0/Map", types.GetRequestContext(ctx).ResourceName)
				require.Equal(t, "failure/step/0/Map", operatorContextFields(ctx)["resource_name"])
				require.Equal(t, "alice", operatorContextFields(ctx)["user"])
			}
			if mode == "cancel" {
				defer mockey.Mock(mlog.Debug).To(capture).Build().UnPatch()
			} else {
				defer mockey.Mock(mlog.Error).To(capture).Build().UnPatch()
			}
			function := &operatorTransformExpr{
				name: "failure", stages: []string{types.StagePreProcess}, outputs: []arrow.DataType{arrow.BinaryTypes.String},
				execute: func(_ *types.FuncContext, columns []*arrow.Chunked) ([]*arrow.Chunked, error) {
					if mode == "function" {
						return nil, merr.ErrFunctionFailed
					}
					cancel()
					columns[0].Retain()
					return []*arrow.Chunked{columns[0]}, nil
				},
			}
			column := "query"
			if mode == "schema" {
				column = "unavailable"
			}
			fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePreProcess).SetName("failure").Map(function, []string{column}, []string{"query"})
			output, err := fc.ExecuteWithContext(ctx, input)
			require.Error(t, err)
			require.Nil(t, output)
			require.True(t, seen)
		})
	}
}
