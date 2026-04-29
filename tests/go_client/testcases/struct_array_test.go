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

// Tests in this file mirror the L0 (smoke / must-pass) cases from
// tests/python_client/milvus_client/test_milvus_client_struct_array.py.
// Each Go test function is named after the original Python test it ports.
package testcases

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const structArrayTestNb = 200 // shrunk from python's default_nb=3000 for faster Go SDK runs

// canonicalStructArrayCollection creates the canonical schema (id + normal_vector + clips with
// clip_str/clip_embedding1/clip_embedding2), inserts numRows of random data, builds indexes on
// normal_vector and the two struct sub-vectors, and loads the collection.
//
// Returns the collection name, struct schema (needed for WithStructArrayColumn), and the
// generated test data so callers can run further assertions.
func canonicalStructArrayCollection(t *testing.T, ctx CtxT, mc MC, numRows int) (string, *entity.StructSchema, hp.StructArrayTestData) {
	collName := common.GenRandomString(hp.StructArrayPrefix, 6)
	opt := hp.DefaultStructArraySchemaOption(collName)
	schema, structSchema := hp.CreateStructArraySchema(opt)

	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)

	data := hp.GenerateStructArrayData(numRows, opt)
	insertOpt := client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", data.IDs).
		WithFloatVectorColumn("normal_vector", data.Dim, data.NormalVectors).
		WithStructArrayColumn("clips", structSchema, data.ClipsRows)
	_, err = mc.Insert(ctx, insertOpt)
	common.CheckErr(t, err, true)

	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	indexAndLoad(t, ctx, mc, collName, data.Dim)
	return collName, structSchema, data
}

// indexAndLoad builds the canonical 3 indexes (normal_vector + 2 sub-vectors) and loads.
func indexAndLoad(t *testing.T, ctx CtxT, mc MC, collName string, dim int) {
	_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewIvfFlatIndex(entity.L2, 128)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "clips[clip_embedding1]",
		index.NewHNSWIndex(entity.MaxSimCosine, 16, 200)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "clips[clip_embedding2]",
		index.NewHNSWIndex(entity.MaxSimCosine, 16, 200)))
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)
}

// type aliases to keep test signatures readable
type (
	CtxT = context.Context
	MC   = *base.MilvusClient
)

// TestStructArrayCreateWithClipEmbedding1 ports test_create_struct_array_with_clip_embedding1.
func TestStructArrayCreateWithClipEmbedding1(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(hp.StructArrayPrefix+"_basic", 6)
	schema, _ := hp.CreateStructArraySchema(hp.DefaultStructArraySchemaOption(collName))

	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema)), true)

	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.True(t, has)
}

// TestStructArrayCreateWithScalarFields ports test_create_struct_array_with_scalar_fields.
func TestStructArrayCreateWithScalarFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(hp.StructArrayPrefix+"_basic", 6)
	dim := hp.StructArrayDefaultDim

	structSchema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("int_field").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("float_field").WithDataType(entity.FieldTypeFloat)).
		WithField(entity.NewField().WithName("string_field").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512)).
		WithField(entity.NewField().WithName("bool_field").WithDataType(entity.FieldTypeBool))

	schema := entity.NewSchema().WithName(collName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("normal_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dim))).
		WithField(entity.NewField().WithName("metadata").
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).
			WithMaxCapacity(int64(hp.StructArrayDefaultCapacity)).
			WithStructSchema(structSchema))

	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema)), true)
	has, err := mc.HasCollection(ctx, client.NewHasCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.True(t, has)
}

// TestStructArrayInsertBasic ports test_insert_struct_array_basic.
func TestStructArrayInsertBasic(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(hp.StructArrayPrefix+"_basic", 6)
	opt := hp.DefaultStructArraySchemaOption(collName)
	schema, structSchema := hp.CreateStructArraySchema(opt)
	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema)), true)

	data := hp.GenerateStructArrayData(structArrayTestNb, opt)
	res, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", data.IDs).
		WithFloatVectorColumn("normal_vector", data.Dim, data.NormalVectors).
		WithStructArrayColumn("clips", structSchema, data.ClipsRows))
	common.CheckErr(t, err, true)
	require.EqualValues(t, structArrayTestNb, res.InsertCount)
}

// TestStructArrayCreateEmbListHNSWIndexCosine ports test_create_emb_list_hnsw_index_cosine.
func TestStructArrayCreateEmbListHNSWIndexCosine(t *testing.T) {
	runEmbListHNSWIndex(t, entity.MaxSimCosine)
}

// TestStructArrayCreateEmbListHNSWIndexIp ports test_create_emb_list_hnsw_index_ip.
// Python test name says _ip but the body actually uses MAX_SIM_COSINE; we mirror that.
func TestStructArrayCreateEmbListHNSWIndexIp(t *testing.T) {
	runEmbListHNSWIndex(t, entity.MaxSimCosine)
}

func runEmbListHNSWIndex(t *testing.T, metric entity.MetricType) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(hp.StructArrayPrefix+"_index", 6)
	opt := hp.DefaultStructArraySchemaOption(collName)
	schema, structSchema := hp.CreateStructArraySchema(opt)
	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema)), true)

	data := hp.GenerateStructArrayData(structArrayTestNb, opt)
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", data.IDs).
		WithFloatVectorColumn("normal_vector", data.Dim, data.NormalVectors).
		WithStructArrayColumn("clips", structSchema, data.ClipsRows))
	common.CheckErr(t, err, true)

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewIvfFlatIndex(entity.L2, 128)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "clips[clip_embedding1]",
		index.NewHNSWIndex(metric, 16, 200)))
	common.CheckErr(t, err, true)
}

// TestStructArraySearchVectorSingle ports test_search_struct_array_vector_single.
// Original uses EmbeddingList with one vector — we use entity.FloatVectorArray with one element.
func TestStructArraySearchVectorSingle(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName, _, data := canonicalStructArrayCollection(t, ctx, mc, structArrayTestNb)

	// baseline: search normal vector field
	queryVec := hp.RandFloatVector(data.Dim)
	normalRS, err := mc.Search(ctx, client.NewSearchOption(collName, 10,
		[]entity.Vector{entity.FloatVector(queryVec)}).
		WithANNSField("normal_vector").
		WithSearchParam("nprobe", "10").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Greater(t, normalRS[0].ResultCount, 0)

	// MAX_SIM search on struct sub-vector with EmbList(=FloatVectorArray) of one vector
	embList := entity.FloatVectorArray{entity.FloatVector(queryVec)}
	rs, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{embList}).
		WithANNSField("clips[clip_embedding1]").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Greater(t, rs[0].ResultCount, 0)
}

// TestStructArraySearchVectorMultiple ports test_search_struct_array_vector_multiple.
func TestStructArraySearchVectorMultiple(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName, _, data := canonicalStructArrayCollection(t, ctx, mc, structArrayTestNb)

	embList := entity.FloatVectorArray{
		entity.FloatVector(hp.RandFloatVector(data.Dim)),
		entity.FloatVector(hp.RandFloatVector(data.Dim)),
		entity.FloatVector(hp.RandFloatVector(data.Dim)),
	}
	rs, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{embList}).
		WithANNSField("clips[clip_embedding1]").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Greater(t, rs[0].ResultCount, 0)
}

// TestStructArrayHybridSearchWithNormalVector ports
// test_hybrid_search_struct_array_with_normal_vector.
func TestStructArrayHybridSearchWithNormalVector(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(hp.StructArrayPrefix+"_hybrid", 6)
	dim := hp.StructArrayDefaultDim

	structSchema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("clip_str").WithDataType(entity.FieldTypeVarChar).WithMaxLength(65535)).
		WithField(entity.NewField().WithName("clip_embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dim)))

	schema := entity.NewSchema().WithName(collName).
		WithField(entity.NewField().WithName("pk").WithDataType(entity.FieldTypeVarChar).WithMaxLength(100).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("random").WithDataType(entity.FieldTypeDouble)).
		WithField(entity.NewField().WithName("embeddings").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dim))).
		WithField(entity.NewField().WithName("clips").
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).
			WithMaxCapacity(100).
			WithStructSchema(structSchema))

	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema)), true)

	const numEntities = 100
	pks := make([]string, numEntities)
	randoms := make([]float64, numEntities)
	embeddings := make([][]float32, numEntities)
	rows := make([]map[string]any, numEntities)
	for i := 0; i < numEntities; i++ {
		pks[i] = fmt.Sprintf("%d", i)
		randoms[i] = rand.Float64()
		embeddings[i] = hp.RandFloatVector(dim)
		count := 2 + rand.Intn(2) // 2 or 3
		strs := make([]string, count)
		embs := make([][]float32, count)
		for j := 0; j < count; j++ {
			strs[j] = fmt.Sprintf("item_%d_%d", i, j)
			embs[j] = hp.RandFloatVector(dim)
		}
		rows[i] = map[string]any{"clip_str": strs, "clip_embedding": embs}
	}

	pkCol := column.NewColumnVarChar("pk", pks)
	randomCol := column.NewColumnDouble("random", randoms)
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(pkCol, randomCol).
		WithFloatVectorColumn("embeddings", dim, embeddings).
		WithStructArrayColumn("clips", structSchema, rows))
	common.CheckErr(t, err, true)

	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "embeddings",
		index.NewIvfFlatIndex(entity.L2, 128)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "clips[clip_embedding]",
		index.NewHNSWIndex(entity.MaxSimL2, 16, 200)))
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)

	queryVec := entity.FloatVector(hp.RandFloatVector(dim))
	queryEmbList := entity.FloatVectorArray{entity.FloatVector(hp.RandFloatVector(dim))}
	rs, err := mc.HybridSearch(ctx, client.NewHybridSearchOption(collName, 5,
		client.NewAnnRequest("embeddings", 5, queryVec),
		client.NewAnnRequest("clips[clip_embedding]", 5, queryEmbList),
	).WithReranker(client.NewRRFReranker()).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.GreaterOrEqual(t, len(rs), 1)
	require.Greater(t, rs[0].ResultCount, 0)
}

// TestStructArrayQueryAllFields ports test_query_struct_array_all_fields.
func TestStructArrayQueryAllFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName, _, _ := canonicalStructArrayCollection(t, ctx, mc, structArrayTestNb)

	rs, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter("id >= 0").WithLimit(10).
		WithOutputFields("*").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Greater(t, rs.ResultCount, 0)

	clipsCol := rs.GetColumn("clips")
	require.NotNil(t, clipsCol, "clips column must be present in query results")
	for i := 0; i < rs.ResultCount; i++ {
		v, err := clipsCol.Get(i)
		require.NoError(t, err)
		_, ok := v.(map[string]any)
		require.True(t, ok, "struct array element must decode as map[string]any")
	}
}

// TestStructArrayQuerySpecificFields ports test_query_struct_array_specific_fields.
func TestStructArrayQuerySpecificFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName, _, _ := canonicalStructArrayCollection(t, ctx, mc, structArrayTestNb)

	rs, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter("id >= 0").WithLimit(10).
		WithOutputFields("id", "clips").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.Greater(t, rs.ResultCount, 0)
	require.NotNil(t, rs.GetColumn("id"))
	require.NotNil(t, rs.GetColumn("clips"))
}

// TestStructArrayUpsertData ports test_upsert_struct_array_data.
// Scaled-down: 200 flushed + 100 growing + 5 upsert per segment, vs python's 2000 + 1000 + 10.
func TestStructArrayUpsertData(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName, structSchema, _ := crudCollection(t, ctx, mc)

	dim := hp.StructArrayDefaultDim
	insertSegment := func(start, count int, label string) {
		ids := make([]int64, count)
		vecs := make([][]float32, count)
		rows := make([]map[string]any, count)
		for i := 0; i < count; i++ {
			ids[i] = int64(start + i)
			vecs[i] = hp.RandFloatVector(dim)
			rows[i] = map[string]any{
				"clip_embedding1": [][]float32{hp.RandFloatVector(dim)},
				"scalar_field":    []int64{int64(start + i)},
				"label":           []string{fmt.Sprintf("%s_%d", label, start+i)},
			}
		}
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
			WithInt64Column("id", ids).
			WithFloatVectorColumn("normal_vector", dim, vecs).
			WithStructArrayColumn("clips", structSchema, rows))
		common.CheckErr(t, err, true)
	}

	insertSegment(0, 200, "flushed")
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	insertSegment(200, 100, "growing")

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewIvfFlatIndex(entity.L2, 128)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "clips[clip_embedding1]",
		index.NewHNSWIndex(entity.MaxSimCosine, 16, 200)))
	common.CheckErr(t, err, true)
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)

	// Upsert 5 from flushed (ids 0..4) and 5 from growing (200..204).
	upsertIDs := []int64{0, 1, 2, 3, 4, 200, 201, 202, 203, 204}
	upsertVecs := make([][]float32, len(upsertIDs))
	upsertRows := make([]map[string]any, len(upsertIDs))
	for i, id := range upsertIDs {
		upsertVecs[i] = hp.RandFloatVector(dim)
		var prefix string
		if id < 200 {
			prefix = "updated_flushed"
		} else {
			prefix = "updated_growing"
		}
		upsertRows[i] = map[string]any{
			"clip_embedding1": [][]float32{hp.RandFloatVector(dim)},
			"scalar_field":    []int64{id + 10000},
			"label":           []string{fmt.Sprintf("%s_%d", prefix, id)},
		}
	}
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", upsertIDs).
		WithFloatVectorColumn("normal_vector", dim, upsertVecs).
		WithStructArrayColumn("clips", structSchema, upsertRows))
	common.CheckErr(t, err, true)

	// Skip second flush: target instance has flush rate limited at 0.1/s. Rely on Strong
	// consistency in the query to see upsert results regardless of segment state.
	rs, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter("id < 5").WithOutputFields("id", "clips").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 5, rs.ResultCount)
	clips := rs.GetColumn("clips")
	for i := 0; i < rs.ResultCount; i++ {
		v, err := clips.Get(i)
		require.NoError(t, err)
		m := v.(map[string]any)
		labels := m["label"].([]string)
		require.Len(t, labels, 1)
		require.True(t, strings.Contains(labels[0], "updated_flushed"),
			"row %d label=%s does not contain updated_flushed", i, labels[0])
	}

	rs2, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter("id >= 200 and id < 205").WithOutputFields("id", "clips").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 5, rs2.ResultCount)
}

// TestStructArrayDeleteData ports test_delete_struct_array_data.
func TestStructArrayDeleteData(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName, structSchema, _ := crudCollection(t, ctx, mc)

	dim := hp.StructArrayDefaultDim
	insertSegment := func(start, count int, label string) {
		ids := make([]int64, count)
		vecs := make([][]float32, count)
		rows := make([]map[string]any, count)
		for i := 0; i < count; i++ {
			ids[i] = int64(start + i)
			vecs[i] = hp.RandFloatVector(dim)
			rows[i] = map[string]any{
				"clip_embedding1": [][]float32{hp.RandFloatVector(dim)},
				"scalar_field":    []int64{int64(start + i)},
				"label":           []string{fmt.Sprintf("%s_%d", label, start+i)},
			}
		}
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
			WithInt64Column("id", ids).
			WithFloatVectorColumn("normal_vector", dim, vecs).
			WithStructArrayColumn("clips", structSchema, rows))
		common.CheckErr(t, err, true)
	}

	insertSegment(0, 200, "flushed")
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	insertSegment(200, 100, "growing")

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewIvfFlatIndex(entity.L2, 128)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "clips[clip_embedding1]",
		index.NewHNSWIndex(entity.MaxSimCosine, 16, 200)))
	common.CheckErr(t, err, true)
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)

	// delete 5 from flushed (ids 0..4) and 5 from growing (200..204)
	_, err = mc.Delete(ctx, client.NewDeleteOption(collName).WithExpr("id in [0,1,2,3,4,200,201,202,203,204]"))
	common.CheckErr(t, err, true)
	// Skip second flush: target instance has flush rate limited at 0.1/s. Rely on Strong
	// consistency in the subsequent query.

	rs, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter("id in [0,1,2,3,4,200,201,202,203,204]").
		WithOutputFields("id").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 0, rs.ResultCount, "deleted rows should not be returned")
}

// crudCollection creates the CRUD-suite collection (clips with clip_embedding1 + scalar_field +
// label, normal_vector nullable).
func crudCollection(t *testing.T, ctx CtxT, mc MC) (string, *entity.StructSchema, *entity.Schema) {
	collName := common.GenRandomString(hp.StructArrayPrefix+"_crud", 6)
	dim := hp.StructArrayDefaultDim

	structSchema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("clip_embedding1").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dim))).
		WithField(entity.NewField().WithName("scalar_field").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("label").WithDataType(entity.FieldTypeVarChar).WithMaxLength(128))

	schema := entity.NewSchema().WithName(collName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("normal_vector").
			WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dim))).
		WithField(entity.NewField().WithName("clips").
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).
			WithMaxCapacity(100).
			WithStructSchema(structSchema))

	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema)), true)
	return collName, structSchema, schema
}

// TestStructArrayRangeSearchNotSupported ports test_struct_array_range_search_not_supported.
func TestStructArrayRangeSearchNotSupported(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName, _, data := canonicalStructArrayCollection(t, ctx, mc, structArrayTestNb)

	queryEmb := entity.FloatVectorArray{entity.FloatVector(hp.RandFloatVector(data.Dim))}
	// Range params must be embedded in the "params" JSON; the server rejects range search on
	// struct sub-vector regardless of metric.
	_, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{queryEmb}).
		WithANNSField("clips[clip_embedding1]").
		WithSearchParam("params", `{"radius": 0.1, "range_filter": 0.5}`).
		WithConsistencyLevel(entity.ClStrong))
	require.Error(t, err, "range search on struct sub-vector should be rejected")
	require.Contains(t, err.Error(), "range search")
}
