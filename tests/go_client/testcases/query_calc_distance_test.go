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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// calcDistanceCD is the calc_distance query param JSON string for a float/dense
// reference against the "vec" field.
func calcDistanceCD() string {
	return `{"metric_type":"IP","field":"vec","vector":[1,2,3,4],"output_name":"distance"}`
}

// calcDistanceCollName returns a unique collection name for calc_distance tests.
func calcDistanceCollName(t *testing.T, tag string) string {
	return common.GenRandomString(fmt.Sprintf("CalcDistance_%s", tag), 6)
}

// calcDistanceCreateAndLoad creates a collection of pk + the given vec field
// (plus the optional scalar fields), indexes "vec" with an AUTOINDEX on the
// metric, and loads it.
func calcDistanceCreateAndLoad(ctx context.Context, t *testing.T, mc *base.MilvusClient,
	n string, vecField *entity.Field, metric index.MetricType, extraFields ...*entity.Field,
) {
	t.Helper()
	schema := entity.NewSchema().WithDynamicFieldEnabled(false).
		WithField(entity.NewField().WithName("pk").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true))
	for _, f := range extraFields {
		schema.WithField(f)
	}
	schema.WithField(vecField)
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(n, schema).
		WithConsistencyLevel(entity.ClStrong)))

	task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(n, "vec", index.NewAutoIndex(metric)))
	require.NoError(t, err)
	require.NoError(t, task.Await(ctx))
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(n))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))
}

// calcDistanceDistancesByPK returns the distance column keyed by the pk column.
func calcDistanceDistancesByPK(t *testing.T, result client.ResultSet) map[int64]float32 {
	t.Helper()
	pkColumn, ok := result.GetColumn("pk").(*column.ColumnInt64)
	require.True(t, ok, "pk column type")
	distColumn, ok := result.GetColumn("distance").(*column.ColumnFloat)
	require.True(t, ok, "distance column type")
	require.Equal(t, pkColumn.Len(), distColumn.Len())

	out := make(map[int64]float32, pkColumn.Len())
	for i := 0; i < pkColumn.Len(); i++ {
		out[pkColumn.Data()[i]] = distColumn.Data()[i]
	}
	return out
}

// TestCalcDistance exercises calc_distance on a FloatVector collection: a
// 500-row IP batch, COSINE (zero-norm and collinear rows), the expression
// reference form, a zero-row filter, and an output_name collision.
func TestCalcDistance(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	n := calcDistanceCollName(t, "base")
	defer mc.DropCollection(ctx, client.NewDropCollectionOption(n))

	const rows = 500 // row i has vec [i,i,i,i]
	calcDistanceCreateAndLoad(ctx, t, mc, n,
		entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(4), entity.IP)

	pkCol := make([]int64, rows)
	vecCol := make([][]float32, rows)
	for i := 0; i < rows; i++ {
		pkCol[i] = int64(i)
		vecCol[i] = []float32{float32(i), float32(i), float32(i), float32(i)}
	}
	insertRes, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(n).
		WithInt64Column("pk", pkCol).
		WithFloatVectorColumn("vec", 4, vecCol))
	require.NoError(t, err)
	require.EqualValues(t, rows, insertRes.InsertCount)

	// partition p1 holds two rows so the expression-form query below can also
	// exercise partition scope (the reference resolves inside p1).
	require.NoError(t, mc.CreatePartition(ctx, client.NewCreatePartitionOption(n, "p1")))
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(n).
		WithPartition("p1").
		WithInt64Column("pk", []int64{1000, 1001}).
		WithFloatVectorColumn("vec", 4, [][]float32{{1, 1, 1, 1}, {2, 2, 2, 2}}))
	require.NoError(t, err)

	// IP against ref [1,2,3,4]: row i -> 10*i. Filtered to the default
	// partition (pk < 1000; pks 1000/1001 live in p1). The vector field is not
	// in output_fields, so it must be dropped from the result.
	result, err := mc.Query(ctx, client.NewQueryOption(n).
		WithFilter("pk >= 0 && pk < 1000").
		WithOutputFields("pk").
		WithCalcDistance(map[string]any{
			"metric_type": "IP",
			"field":       "vec",
			"vector":      []float32{1, 2, 3, 4},
			"output_name": "distance",
		}))
	require.NoError(t, err)
	require.Equal(t, rows, result.Len())
	require.Nil(t, result.GetColumn("vec"), "unrequested vector field must be dropped")
	d := calcDistanceDistancesByPK(t, result)
	require.Zero(t, d[0])
	require.InEpsilon(t, 1000, d[100], 1e-3)
	require.InEpsilon(t, 10*float32(rows-1), d[rows-1], 1e-3)

	// COSINE with the vector field also requested (kept), under ORDER BY so the
	// distance column must survive the remap; rows come back in pk order.
	result, err = mc.Query(ctx, client.NewQueryOption(n).
		WithFilter("pk in [0,1,2]").
		WithOutputFields("pk", "vec").
		WithOrderByFields("pk:asc").
		WithLimit(3).
		WithCalcDistance(map[string]any{
			"metric_type": "COSINE",
			"field":       "vec",
			"vector":      []float32{1, 2, 3, 4},
			"output_name": "distance",
		}))
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2}, result.GetColumn("pk").(*column.ColumnInt64).Data())
	require.NotNil(t, result.GetColumn("vec"), "requested vector field must be kept")
	d = calcDistanceDistancesByPK(t, result)
	require.Zero(t, d[0])
	require.InEpsilon(t, 10/math.Sqrt(120), d[1], 1e-3)
	require.InEpsilon(t, 20/math.Sqrt(480), d[2], 1e-3)

	// expression form within a partition, L2 metric: reference = vector of the
	// row pk==1001 in partition p1 = [2,2,2,2]
	result, err = mc.Query(ctx, client.NewQueryOption(n).
		WithPartitions("p1").
		WithFilter("pk in [1000, 1001]").
		WithOutputFields("pk").
		WithCalcDistance(map[string]any{
			"metric_type": "L2",
			"field":       "vec",
			"vector":      "pk == 1001",
			"output_name": "distance",
		}))
	require.NoError(t, err)
	d = calcDistanceDistancesByPK(t, result)
	require.InEpsilon(t, 4, d[1000], 1e-3) // [1,1,1,1] vs [2,2,2,2]
	require.Zero(t, d[1001])

	// zero-row filter -> empty result
	result, err = mc.Query(ctx, client.NewQueryOption(n).
		WithFilter("pk < 0").
		WithOutputFields("pk").
		WithCalcDistance(map[string]any{
			"metric_type": "IP",
			"field":       "vec",
			"vector":      []float32{1, 2, 3, 4},
			"output_name": "distance",
		}))
	require.NoError(t, err)
	require.Zero(t, result.Len())

	// output_name colliding with the primary key -> rejected
	_, err = mc.Query(ctx, client.NewQueryOption(n).
		WithFilter("pk >= 0").
		WithOutputFields("pk").
		WithCalcDistance(map[string]any{
			"metric_type": "IP",
			"field":       "vec",
			"vector":      []float32{1, 2, 3, 4},
			"output_name": "pk",
		}))
	require.Error(t, err)
}

// TestCalcDistanceVectorTypes covers calc_distance across the non-float vector
// field types (int8 / float16 / bfloat16 / binary / sparse) and the nullable
// float-vector null-row semantics.
func TestCalcDistanceVectorTypes(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	queryCD := func(t *testing.T, n string, metric, vector any) map[int64]float32 {
		t.Helper()
		result, err := mc.Query(ctx, client.NewQueryOption(n).
			WithFilter("pk >= 0").
			WithOutputFields("pk").
			WithCalcDistance(map[string]any{
				"metric_type": "IP",
				"field":       "vec",
				"vector":      vector,
				"output_name": "distance",
			}))
		require.NoError(t, err)
		return calcDistanceDistancesByPK(t, result)
	}

	t.Run("int8", func(t *testing.T) {
		n := calcDistanceCollName(t, "i8")
		defer mc.DropCollection(ctx, client.NewDropCollectionOption(n))
		calcDistanceCreateAndLoad(ctx, t, mc, n,
			entity.NewField().WithName("vec").WithDataType(entity.FieldTypeInt8Vector).WithDim(4), entity.IP)
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(n).
			WithInt64Column("pk", []int64{0, 1}).
			WithInt8VectorColumn("vec", 4, [][]int8{{1, 2, 3, 4}, {2, 4, 6, 8}}))
		require.NoError(t, err)
		d := queryCD(t, n, entity.IP, []float32{1, 2, 3, 4})
		require.InEpsilon(t, 30, d[0], 1e-3)
		require.InEpsilon(t, 60, d[1], 1e-3)
	})

	t.Run("float16", func(t *testing.T) {
		n := calcDistanceCollName(t, "fp16")
		defer mc.DropCollection(ctx, client.NewDropCollectionOption(n))
		calcDistanceCreateAndLoad(ctx, t, mc, n,
			entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloat16Vector).WithDim(4), entity.IP)
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(n).
			WithInt64Column("pk", []int64{0, 1}).
			WithFloat16VectorColumn("vec", 4, [][]float32{{1, 2, 3, 4}, {2, 4, 6, 8}}))
		require.NoError(t, err)
		d := queryCD(t, n, entity.IP, []float32{1, 2, 3, 4})
		require.InEpsilon(t, 30, d[0], 1e-3)
		require.InEpsilon(t, 60, d[1], 1e-3)
	})

	t.Run("bfloat16", func(t *testing.T) {
		n := calcDistanceCollName(t, "bf16")
		defer mc.DropCollection(ctx, client.NewDropCollectionOption(n))
		calcDistanceCreateAndLoad(ctx, t, mc, n,
			entity.NewField().WithName("vec").WithDataType(entity.FieldTypeBFloat16Vector).WithDim(4), entity.IP)
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(n).
			WithInt64Column("pk", []int64{0, 1}).
			WithBFloat16VectorColumn("vec", 4, [][]float32{{1, 2, 3, 4}, {2, 4, 6, 8}}))
		require.NoError(t, err)
		d := queryCD(t, n, entity.IP, []float32{1, 2, 3, 4})
		require.InEpsilon(t, 30, d[0], 1e-3)
		require.InEpsilon(t, 60, d[1], 1e-3)
	})

	t.Run("binary", func(t *testing.T) {
		n := calcDistanceCollName(t, "bin")
		defer mc.DropCollection(ctx, client.NewDropCollectionOption(n))
		calcDistanceCreateAndLoad(ctx, t, mc, n,
			entity.NewField().WithName("vec").WithDataType(entity.FieldTypeBinaryVector).WithDim(16), entity.HAMMING)
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(n).
			WithInt64Column("pk", []int64{0, 1}).
			WithBinaryVectorColumn("vec", 16, [][]byte{{15, 170}, {255, 85}}))
		require.NoError(t, err)
		// HAMMING against [0x0F,0xAA]: row0 identical -> 0; row1 popcount = 12
		result, err := mc.Query(ctx, client.NewQueryOption(n).
			WithFilter("pk >= 0").
			WithOutputFields("pk").
			WithCalcDistance(map[string]any{
				"metric_type": "HAMMING",
				"field":       "vec",
				"vector":      []float32{15, 170},
				"output_name": "distance",
			}))
		require.NoError(t, err)
		d := calcDistanceDistancesByPK(t, result)
		require.Zero(t, d[0])
		require.InEpsilon(t, 12, d[1], 1e-3)

		// JACCARD against the same rows: row0 identical -> 0;
		// row1: intersect=4, union=16 -> 1 - 4/16 = 0.75
		result, err = mc.Query(ctx, client.NewQueryOption(n).
			WithFilter("pk >= 0").
			WithOutputFields("pk").
			WithCalcDistance(map[string]any{
				"metric_type": "JACCARD",
				"field":       "vec",
				"vector":      []float32{15, 170},
				"output_name": "distance",
			}))
		require.NoError(t, err)
		d = calcDistanceDistancesByPK(t, result)
		require.Zero(t, d[0])
		require.InEpsilon(t, 0.75, d[1], 1e-3)
	})

	t.Run("sparse", func(t *testing.T) {
		n := calcDistanceCollName(t, "sparse")
		defer mc.DropCollection(ctx, client.NewDropCollectionOption(n))
		calcDistanceCreateAndLoad(ctx, t, mc, n,
			entity.NewField().WithName("vec").WithDataType(entity.FieldTypeSparseVector), entity.IP)
		row0, err := entity.NewSliceSparseEmbedding([]uint32{0, 2, 5}, []float32{1, 2, 3})
		require.NoError(t, err)
		row1, err := entity.NewSliceSparseEmbedding([]uint32{2, 7}, []float32{2, 10})
		require.NoError(t, err)
		_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(n).
			WithInt64Column("pk", []int64{0, 1}).
			WithColumns(column.NewColumnSparseVectors("vec", []entity.SparseEmbedding{row0, row1})))
		require.NoError(t, err)
		// IP against {0:1,2:2,5:3}: row0=14; row1=4
		result, err := mc.Query(ctx, client.NewQueryOption(n).
			WithFilter("pk >= 0").
			WithOutputFields("pk").
			WithCalcDistance(map[string]any{
				"metric_type": "IP",
				"field":       "vec",
				"vector":      map[string]any{"0": 1, "2": 2, "5": 3},
				"output_name": "distance",
			}))
		require.NoError(t, err)
		d := calcDistanceDistancesByPK(t, result)
		require.InEpsilon(t, 14, d[0], 1e-3)
		require.InEpsilon(t, 4, d[1], 1e-3)
	})

	t.Run("nullable float", func(t *testing.T) {
		n := calcDistanceCollName(t, "null")
		defer mc.DropCollection(ctx, client.NewDropCollectionOption(n))
		calcDistanceCreateAndLoad(ctx, t, mc, n,
			entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(4).WithNullable(true), entity.IP)
		nullableVec, err := column.NewNullableColumnFloatVector("vec", 4, [][]float32{{1, 2, 3, 4}, {2, 4, 6, 8}}, []bool{true, false, true})
		require.NoError(t, err)
		_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(n).
			WithInt64Column("pk", []int64{0, 1, 2}).
			WithColumns(nullableVec))
		require.NoError(t, err)

		result, err := mc.Query(ctx, client.NewQueryOption(n).
			WithFilter("pk >= 0").
			WithOutputFields("pk").
			WithCalcDistance(map[string]any{
				"metric_type": "IP",
				"field":       "vec",
				"vector":      []float32{1, 2, 3, 4},
				"output_name": "distance",
			}))
		require.NoError(t, err)
		pkCol := result.GetColumn("pk").(*column.ColumnInt64)
		distCol := result.GetColumn("distance").(*column.ColumnFloat)
		require.Equal(t, pkCol.Len(), distCol.Len())
		nulls := make(map[int64]bool)
		for i := 0; i < pkCol.Len(); i++ {
			isNull, err := distCol.IsNull(i)
			require.NoError(t, err)
			nulls[pkCol.Data()[i]] = isNull
		}
		require.False(t, nulls[0])
		require.True(t, nulls[1]) // null vec row -> null distance
		require.False(t, nulls[2])
	})
}

// TestCalcDistanceRejections covers the combinations and inputs calc_distance
// rejects: aggregation / GROUP BY / function chains, and the malformed
// reference-vector inputs (NaN/Inf, dimension mismatch, empty vector).
func TestCalcDistanceRejections(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	n := calcDistanceCollName(t, "reject")
	defer mc.DropCollection(ctx, client.NewDropCollectionOption(n))

	// collection with a scalar tag field so GROUP BY on it is valid up to the
	// calc_distance rejection. Two rows are inserted so the expression-form
	// rejection subtests have something to match.
	calcDistanceCreateAndLoad(ctx, t, mc, n,
		entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(4), entity.IP,
		entity.NewField().WithName("tag").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64))
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(n).
		WithInt64Column("pk", []int64{0, 1}).
		WithVarcharColumn("tag", []string{"a", "b"}).
		WithFloatVectorColumn("vec", 4, [][]float32{{1, 2, 3, 4}, {2, 4, 6, 8}}))
	require.NoError(t, err)

	const cdJSON = `{"metric_type":"IP","field":"vec","vector":[1,2,3,4],"output_name":"distance"}`

	t.Run("expression matches multiple rows", func(t *testing.T) {
		// "pk >= 0" matches both rows: the reference must be a single row.
		_, err := mc.Query(ctx, client.NewQueryOption(n).
			WithFilter("pk >= 0").
			WithOutputFields("pk").
			WithCalcDistance(map[string]any{
				"metric_type": "IP",
				"field":       "vec",
				"vector":      "pk >= 0",
				"output_name": "distance",
			}))
		require.Error(t, err)
	})

	t.Run("expression matches no rows", func(t *testing.T) {
		_, err := mc.Query(ctx, client.NewQueryOption(n).
			WithFilter("pk >= 0").
			WithOutputFields("pk").
			WithCalcDistance(map[string]any{
				"metric_type": "IP",
				"field":       "vec",
				"vector":      "pk == 999",
				"output_name": "distance",
			}))
		require.Error(t, err)
	})

	t.Run("aggregation", func(t *testing.T) {
		_, err := mc.Query(ctx, client.NewQueryOption(n).
			WithFilter("pk >= 0").
			WithOutputFields("count(*)").
			WithCalcDistance(map[string]any{
				"metric_type": "IP",
				"field":       "vec",
				"vector":      []float32{1, 2, 3, 4},
				"output_name": "distance",
			}))
		require.Error(t, err)
	})

	t.Run("group by", func(t *testing.T) {
		resp, err := mc.GetService().Query(ctx, &milvuspb.QueryRequest{
			CollectionName: n,
			Expr:           "pk >= 0",
			OutputFields:   []string{"tag"},
			QueryParams: []*commonpb.KeyValuePair{
				{Key: "group_by_fields", Value: "tag"},
				{Key: "calc_distance", Value: cdJSON},
			},
		})
		require.NoError(t, err)
		require.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode(),
			"calc_distance + GROUP BY must be rejected")
	})

	t.Run("function chain", func(t *testing.T) {
		resp, err := mc.GetService().Query(ctx, &milvuspb.QueryRequest{
			CollectionName: n,
			Expr:           "pk >= 0",
			OutputFields:   []string{"pk"},
			FunctionChains: []*schemapb.FunctionChain{{}},
			QueryParams: []*commonpb.KeyValuePair{
				{Key: "calc_distance", Value: cdJSON},
			},
		})
		require.NoError(t, err)
		require.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode(),
			"calc_distance + function chains must be rejected")
	})

	t.Run("NaN reference", func(t *testing.T) {
		_, err := mc.Query(ctx, client.NewQueryOption(n).
			WithFilter("pk >= 0").
			WithOutputFields("pk").
			WithCalcDistance(map[string]any{
				"metric_type": "IP",
				"field":       "vec",
				"vector":      []float32{1, 2, 3, float32(math.NaN())},
				"output_name": "distance",
			}))
		require.Error(t, err)
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		_, err := mc.Query(ctx, client.NewQueryOption(n).
			WithFilter("pk >= 0").
			WithOutputFields("pk").
			WithCalcDistance(map[string]any{
				"metric_type": "IP",
				"field":       "vec",
				"vector":      []float32{1, 2},
				"output_name": "distance",
			}))
		require.Error(t, err)
	})

	t.Run("empty vector", func(t *testing.T) {
		_, err := mc.Query(ctx, client.NewQueryOption(n).
			WithFilter("pk >= 0").
			WithOutputFields("pk").
			WithCalcDistance(map[string]any{
				"metric_type": "IP",
				"field":       "vec",
				"vector":      []float32{},
				"output_name": "distance",
			}))
		require.Error(t, err)
	})
}
