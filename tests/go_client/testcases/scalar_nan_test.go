// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testcases

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// Exercise the same persisted rows before and after scalar indexes are built.
// Different NaN signs/payloads must compare equal, above infinity; signed zeros
// must share membership. Vectors stay finite because vector NaNs are invalid.
func TestScalarNaNScanAndIndexes(t *testing.T) {
	ctx := hp.CreateContext(t, 4*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("scalar_nan", 6)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("f32").WithDataType(entity.FieldTypeFloat)).
		WithField(entity.NewField().WithName("f64").WithDataType(entity.FieldTypeDouble)).
		WithField(entity.NewField().WithName("arr_low").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeDouble).WithMaxCapacity(4)).
		WithField(entity.NewField().WithName("arr_high").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeDouble).WithMaxCapacity(4)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(2))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong)))

	nan1 := math.Float64frombits(0x7ff8000000000001)
	nan2 := math.Float64frombits(0xfff8000000000042)
	scalar64 := []float64{math.Inf(-1), math.Copysign(0, -1), 0, math.Inf(1), nan1, nan2}
	scalar32 := []float32{float32(math.Inf(-1)), float32(math.Copysign(0, -1)), 0, float32(math.Inf(1)), math.Float32frombits(0x7fc00001), math.Float32frombits(0xffc12345)}
	arrayValues := [][]float64{{nan1, 1}, {nan2, 1, nan1}, {}, {3}, {math.Inf(1)}, {1}}
	const rowCount = 1200 // Exceed the minimum sealed-segment indexing size.
	ids := make([]int64, rowCount)
	values32 := make([]float32, rowCount)
	values64 := make([]float64, rowCount)
	arrays := make([][]float64, rowCount)
	vectors := make([][]float32, rowCount)
	for i := range ids {
		ids[i] = int64(i)
		values32[i], values64[i] = scalar32[i%6], scalar64[i%6]
		arrays[i] = arrayValues[i%6]
		vectors[i] = []float32{float32(i), 1}
	}
	inserted, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName,
		column.NewColumnInt64("id", ids), column.NewColumnFloat("f32", values32), column.NewColumnDouble("f64", values64),
		column.NewColumnDoubleArray("arr_low", arrays), column.NewColumnDoubleArray("arr_high", arrays),
		column.NewColumnFloatVector("vector", 2, vectors)))
	require.NoError(t, err)
	require.EqualValues(t, rowCount, inserted.InsertCount)
	flushTask, err := mc.Flush(ctx, client.NewFlushOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, flushTask.Await(ctx))

	buildIndex := func(field string, idx index.Index) {
		t.Helper()
		task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, field, idx))
		require.NoError(t, err)
		require.NoError(t, task.Await(ctx))
		description, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(collectionName, field))
		require.NoError(t, err)
		require.EqualValues(t, rowCount, description.IndexedRows, field)
	}
	load := func() {
		t.Helper()
		task, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
		require.NoError(t, err)
		require.NoError(t, task.Await(ctx))
	}
	buildIndex("vector", index.NewFlatIndex(entity.L2))
	load()
	t.Run("scan", func(t *testing.T) {
		checkScalarNaNQueries(t, ctx, mc, collectionName, nan1)
	})

	require.NoError(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(collectionName)))
	buildIndex("f32", index.NewSortedIndex())
	buildIndex("f64", index.NewSortedIndex())
	// The two cardinality limits exercise both HYBRID families without changing
	// the separate ARRAY SORT software gate used by the cluster.
	for field, limit := range map[string]string{"arr_low": "1000", "arr_high": "1"} {
		buildIndex(field, index.NewGenericIndex(field, map[string]string{
			index.IndexTypeKey: "AUTOINDEX", "bitmap_cardinality_limit": limit,
		}))
	}
	load()
	t.Run("indexes", func(t *testing.T) {
		checkScalarNaNQueries(t, ctx, mc, collectionName, nan2)
	})
}

func checkScalarNaNQueries(t *testing.T, ctx context.Context, mc *base.MilvusClient, collectionName string, nan float64) {
	t.Helper()
	check := func(expr string, expected []int64, params map[string]any) {
		t.Helper()
		opt := client.NewQueryOption(collectionName).WithFilter("id < 6 && (" + expr + ")").
			WithOutputFields("id").WithConsistencyLevel(entity.ClStrong)
		for key, value := range params {
			opt.WithTemplateParam(key, value)
		}
		result, err := mc.Query(ctx, opt)
		require.NoError(t, err, expr)
		require.ElementsMatch(t, expected, result.GetColumn("id").(*column.ColumnInt64).Data(), expr)
	}
	for _, field := range []string{"f32", "f64"} {
		check(field+" > 1", []int64{3, 4, 5}, nil)
		check(field+" < 1", []int64{0, 1, 2}, nil)
		check(field+" == {nan}", []int64{4, 5}, map[string]any{"nan": nan})
		check(field+" != {nan}", []int64{0, 1, 2, 3}, map[string]any{"nan": nan})
		check(field+" < {nan}", []int64{0, 1, 2, 3}, map[string]any{"nan": nan})
		check(field+" > {nan}", nil, map[string]any{"nan": nan})
		check(field+" in {values}", []int64{1, 2, 4, 5}, map[string]any{"values": []float64{nan, math.Copysign(0, -1)}})
		check(field+" in {values}", []int64{1, 2}, map[string]any{"values": []float64{0}})
	}
	for _, field := range []string{"arr_low", "arr_high"} {
		check(fmt.Sprintf("array_contains(%s, {nan})", field), []int64{0, 1}, map[string]any{"nan": nan})
		check(fmt.Sprintf("array_contains_any(%s, {values})", field), []int64{0, 1, 3}, map[string]any{"values": []float64{nan, 3}})
		check(fmt.Sprintf("array_contains_all(%s, {values})", field), []int64{0, 1}, map[string]any{"values": []float64{nan, 1, nan}})
		check(fmt.Sprintf("not array_contains(%s, {nan})", field), []int64{2, 3, 4, 5}, map[string]any{"nan": nan})
		check(fmt.Sprintf("array_contains(%s, 1)", field), []int64{0, 1, 5}, nil)
		check(field+"[0] > 1", []int64{0, 1, 3, 4}, nil)
		check("array_length("+field+") == 0", []int64{2}, nil)
	}

	// A secondary primary-key order makes ties deterministic, while descending
	// scalar order differs from the default primary-key order.
	for _, field := range []string{"f32", "f64"} {
		result, err := mc.Query(ctx, client.NewQueryOption(collectionName).WithFilter("id < 6").WithLimit(6).
			WithOutputFields("id", "f32", "f64", "arr_low").WithConsistencyLevel(entity.ClStrong).
			WithOrderByFields(field+":desc", "id:asc"))
		require.NoError(t, err)
		require.Equal(t, []int64{4, 5, 3, 1, 2, 0}, result.GetColumn("id").(*column.ColumnInt64).Data(), field)
		floats := result.GetColumn("f32").(*column.ColumnFloat).Data()
		doubles := result.GetColumn("f64").(*column.ColumnDouble).Data()
		for i := 0; i < 2; i++ {
			require.True(t, math.IsNaN(float64(floats[i])), field)
			require.True(t, math.IsNaN(doubles[i]), field)
		}
	}
	result, err := mc.Query(ctx, client.NewQueryOption(collectionName).WithFilter("id == 1").WithOutputFields("arr_low").
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	array := result.GetColumn("arr_low").(*column.ColumnDoubleArray).Data()[0]
	require.Len(t, array, 3)
	require.True(t, math.IsNaN(array[0]))
	require.Equal(t, float64(1), array[1])
	require.True(t, math.IsNaN(array[2]))
}
