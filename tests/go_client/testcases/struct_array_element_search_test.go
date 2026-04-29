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

// L0 ports of tests/python_client/milvus_client/
// test_milvus_client_struct_array_element_search.py.
//
// Three Python tests are explicitly @pytest.mark.xfail and we mirror that with t.Skip:
//   - test_element_filter_search_basic_cosine        (flaky element_indices on growing segment)
//   - test_element_filter_search_basic_l2            (same root cause)
//   - test_element_filter_search_verify_in_struct_offset (pymilvus element_indices not exposed)
package testcases

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const elemSearchPrefix = "struct_elem_search"

// =============================================================================
// 1. TestMilvusClientStructArrayElementFilterSearch  (5 L0, 3 skipped as xfail)
// =============================================================================

func TestStructArrayElementFilterSearchBasicCosine(t *testing.T) {
	t.Skip("xfail in python: flaky element-level search on growing segment returns wrong element-to-row mapping")
}

func TestStructArrayElementFilterSearchBasicL2(t *testing.T) {
	t.Skip("xfail in python: same flaky element-to-row mapping issue")
}

func TestStructArrayElementFilterSearchVerifyInStructOffset(t *testing.T) {
	t.Skip("xfail in python: element_indices not yet re-exposed after PR #3240 refactoring")
}

// TestStructArrayElementFilterSearchWithDocLevelFilter ports
// test_element_filter_search_with_doc_level_filter.
func TestStructArrayElementFilterSearchWithDocLevelFilter(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(elemSearchPrefix+"_ef_doc", 6)
	opt := hp.DefaultStructAElementSchemaOption(collName)
	opt.IncludeSize = true
	opt.IncludeCategory = false
	opt.IncludeFloatVal = true
	schema, structSchema := hp.CreateStructAElementSchema(opt)
	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)

	// 500 rows is enough to validate the doc filter without bloating runtime.
	ds := hp.GenerateStructAElementData(500, 0, opt)
	insertElemDataset(t, ctx, mc, collName, structSchema, ds, opt)
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	indexAndLoadElem(t, ctx, mc, collName)

	// Use row 200's first element embedding as query; filter pins doc_int>100 + str_val match.
	// Single-vector search (not EmbList) — element_filter+vector search on struct sub-vector
	// works with regular FloatVector when only one query vector is involved.
	queryVec := ds.Rows[200].StructA[0].Embedding
	// Plain-vector search against an EmbList-indexed field must override metric_type to the
	// underlying COSINE (the index's MAX_SIM_COSINE is reserved for embedding-list queries).
	rs, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{entity.FloatVector(queryVec)}).
		WithANNSField("structA[embedding]").
		WithSearchParam("metric_type", "COSINE").
		WithFilter(`doc_int > 100 && element_filter(structA, $[str_val] == "row_200_elem_0")`).
		WithOutputFields("id", "doc_int").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.GreaterOrEqual(t, len(rs), 1)
	require.Greater(t, rs[0].ResultCount, 0)

	// Top-1 must be row 200 (queried its own vector).
	idCol := rs[0].GetColumn("id")
	docCol := rs[0].GetColumn("doc_int")
	for i := 0; i < rs[0].ResultCount; i++ {
		v, _ := docCol.Get(i)
		require.Greater(t, v.(int64), int64(100))
	}
	first, _ := idCol.Get(0)
	require.EqualValues(t, int64(200), first.(int64))
}

// TestStructArrayElementFilterSearchCompoundSameElementSemantic ports
// test_element_filter_search_compound_same_element_semantic.
func TestStructArrayElementFilterSearchCompoundSameElementSemantic(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(elemSearchPrefix+"_ef_semantic", 6)
	opt := hp.DefaultStructAElementSchemaOption(collName)
	opt.IncludeSize = true
	opt.IncludeCategory = false
	opt.IncludeFloatVal = true
	schema, structSchema := hp.CreateStructAElementSchema(opt)
	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)

	targetVec := hp.SeedVector(77777, opt.Dim)
	rows := []hp.StructARow{
		{
			ID: 0, DocInt: 0, DocVarChar: "cat_0",
			NormalVector: hp.SeedVector(99990, opt.Dim),
			StructA: []hp.StructAElement{
				{Embedding: hp.SeedVector(0, opt.Dim), IntVal: 1, StrVal: "a", FloatVal: 0.1, Color: "Red", Size: "S"},
				{Embedding: targetVec, IntVal: 2, StrVal: "b", FloatVal: 0.2, Color: "Blue", Size: "L"},
			},
		},
		{
			ID: 1, DocInt: 1, DocVarChar: "cat_1",
			NormalVector: hp.SeedVector(99991, opt.Dim),
			StructA: []hp.StructAElement{
				{Embedding: targetVec, IntVal: 10, StrVal: "x", FloatVal: 1.0, Color: "Red", Size: "L"},
			},
		},
		{
			ID: 2, DocInt: 2, DocVarChar: "cat_2",
			NormalVector: hp.SeedVector(99992, opt.Dim),
			StructA: []hp.StructAElement{
				{Embedding: hp.SeedVector(20, opt.Dim), IntVal: 20, StrVal: "p", FloatVal: 2.0, Color: "Blue", Size: "S"},
			},
		},
	}
	insertCustomRows(t, ctx, mc, collName, structSchema, rows, opt)
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	indexAndLoadElem(t, ctx, mc, collName)

	rs, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{entity.FloatVector(targetVec)}).
		WithANNSField("structA[embedding]").
		WithSearchParam("metric_type", "COSINE").
		WithFilter(`element_filter(structA, $[color] == "Red" && $[size] == "L")`).
		WithOutputFields("id").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	matched := map[int64]bool{}
	idCol := rs[0].GetColumn("id")
	for i := 0; i < rs[0].ResultCount; i++ {
		v, _ := idCol.Get(i)
		matched[v.(int64)] = true
	}
	require.False(t, matched[0], "row 0: Red and L are on different elements; must NOT match")
	require.True(t, matched[1], "row 1: elem[0]={Red,L} satisfies same-element semantic")
}

// indexAndLoadElem builds the canonical 2 indexes for plain-vector struct sub-search.
// Use entity.COSINE on the sub-vector (NOT MaxSimCosine) so plain FloatVector searches work.
// Tests that need EmbList/MaxSim semantics build their own indexes.
func indexAndLoadElem(t *testing.T, ctx CtxT, mc MC, collName string) {
	_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewHNSWIndex(entity.COSINE, 16, 200)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "structA[embedding]",
		index.NewHNSWIndex(entity.COSINE, 16, 200)))
	common.CheckErr(t, err, true)
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)
}

// =============================================================================
// 2. TestMilvusClientStructArrayElementMatchSearch  (4 L0)
// =============================================================================

// matchSearchSetup creates the canonical match-search collection (no doc_varchar, has size).
func matchSearchSetup(t *testing.T, ctx CtxT, mc MC, namePrefix string, rows []hp.StructARow) (string, hp.StructAElementSchemaOption) {
	collName := common.GenRandomString(namePrefix, 6)
	opt := hp.DefaultStructAElementSchemaOption(collName)
	opt.IncludeDocVChar = false
	opt.IncludeCategory = false
	opt.IncludeSize = true
	opt.IncludeFloatVal = true
	schema, structSchema := hp.CreateStructAElementSchema(opt)
	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)
	insertCustomRows(t, ctx, mc, collName, structSchema, rows, opt)
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	indexAndLoadElem(t, ctx, mc, collName)
	return collName, opt
}

func semanticRows(opt hp.StructAElementSchemaOption) []hp.StructARow {
	return []hp.StructARow{
		{
			ID: 0, DocInt: 0,
			NormalVector: hp.SeedVector(99990, opt.Dim),
			StructA: []hp.StructAElement{
				{Embedding: hp.SeedVector(0, opt.Dim), IntVal: 1, StrVal: "a", Color: "Red", Size: "S"},
				{Embedding: hp.SeedVector(1, opt.Dim), IntVal: 2, StrVal: "b", Color: "Blue", Size: "L"},
				{Embedding: hp.SeedVector(2, opt.Dim), IntVal: 3, StrVal: "c", Color: "Green", Size: "M"},
			},
		},
		{
			ID: 1, DocInt: 1,
			NormalVector: hp.SeedVector(99991, opt.Dim),
			StructA: []hp.StructAElement{
				{Embedding: hp.SeedVector(10, opt.Dim), IntVal: 1, StrVal: "x", Color: "Red", Size: "L"},
				{Embedding: hp.SeedVector(11, opt.Dim), IntVal: 2, StrVal: "y", Color: "Red", Size: "L"},
			},
		},
		{
			ID: 2, DocInt: 2,
			NormalVector: hp.SeedVector(99992, opt.Dim),
			StructA: []hp.StructAElement{
				{Embedding: hp.SeedVector(20, opt.Dim), IntVal: 1, StrVal: "p", Color: "Blue", Size: "S"},
				{Embedding: hp.SeedVector(21, opt.Dim), IntVal: 2, StrVal: "q", Color: "Green", Size: "XL"},
			},
		},
	}
}

func TestStructArrayElementMatchSearch(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	opt := hp.DefaultStructAElementSchemaOption("")
	opt.IncludeDocVChar = false
	opt.IncludeCategory = false
	opt.IncludeSize = true
	opt.IncludeFloatVal = true

	t.Run("match_all_basic", func(t *testing.T) {
		// MATCH_ALL is not supported by the 2.6 base server used by this backport.
		t.Skip("MATCH_ALL is not supported by the 2.6 base server")

		ds := hp.GenerateStructAElementData(500, 0, opt)
		collName := common.GenRandomString(elemSearchPrefix+"_ma_basic", 6)
		o2 := opt
		o2.CollectionName = collName
		schema, structSchema := hp.CreateStructAElementSchema(o2)
		common.CheckErr(t, mc.CreateCollection(ctx,
			client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)
		insertElemDataset(t, ctx, mc, collName, structSchema, ds, o2)
		_, err := mc.Flush(ctx, client.NewFlushOption(collName))
		common.CheckErr(t, err, true)
		indexAndLoadElem(t, ctx, mc, collName)

		ids := queryAllIDs(t, ctx, mc, collName, `MATCH_ALL(structA, $[color] == "Red")`, 100)
		gt := hp.GtMatch(ds.Rows, "MATCH_ALL", func(e hp.StructAElement) bool { return e.Color == "Red" }, 0, nil)
		require.True(t, subset(ids, hp.IDSetToSorted(gt)),
			"got %v not subset of gt %v", ids, hp.IDSetToSorted(gt))
	})

	t.Run("match_all_compound_same_element", func(t *testing.T) {
		// MATCH_ALL is not supported by the 2.6 base server used by this backport.
		t.Skip("MATCH_ALL is not supported by the 2.6 base server")

		collName, _ := matchSearchSetup(t, ctx, mc, elemSearchPrefix+"_ma_compound", semanticRows(opt))
		ids := queryAllIDs(t, ctx, mc, collName, `MATCH_ALL(structA, $[color] == "Red" && $[size] == "L")`, 100)
		require.Contains(t, ids, int64(1), "row 1: all elements are Red+L")
		require.NotContains(t, ids, int64(0), "row 0: not all elements are Red+L")
	})

	t.Run("match_any_basic", func(t *testing.T) {
		ds := hp.GenerateStructAElementData(500, 0, opt)
		collName := common.GenRandomString(elemSearchPrefix+"_many_basic", 6)
		o2 := opt
		o2.CollectionName = collName
		schema, structSchema := hp.CreateStructAElementSchema(o2)
		common.CheckErr(t, mc.CreateCollection(ctx,
			client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)
		insertElemDataset(t, ctx, mc, collName, structSchema, ds, o2)
		_, err := mc.Flush(ctx, client.NewFlushOption(collName))
		common.CheckErr(t, err, true)
		indexAndLoadElem(t, ctx, mc, collName)

		ids := queryAllIDs(t, ctx, mc, collName, `MATCH_ANY(structA, $[color] == "Blue")`, 100)
		gt := hp.GtMatch(ds.Rows, "MATCH_ANY", func(e hp.StructAElement) bool { return e.Color == "Blue" }, 0, nil)
		require.True(t, subset(ids, hp.IDSetToSorted(gt)),
			"got %v not subset of gt %v", ids, hp.IDSetToSorted(gt))
	})

	t.Run("match_nested_semantic_verification", func(t *testing.T) {
		collName, _ := matchSearchSetup(t, ctx, mc, elemSearchPrefix+"_semantic", semanticRows(opt))
		ids := queryAllIDs(t, ctx, mc, collName, `MATCH_ANY(structA, $[color] == "Red" && $[size] == "L")`, 100)
		require.NotContains(t, ids, int64(0), "row 0: Red and L on different elements should NOT match")
		require.Contains(t, ids, int64(1), "row 1: elem[0]={Red,L} should match")
	})
}

// =============================================================================
// 3. TestMilvusClientStructArrayElementNestedIndex  (3 L0)
// =============================================================================

// nestedIndexSetup mirrors python `_setup_base_collection` with one extra index on the requested
// struct sub-field, then queries one row to confirm the load+index path works.
func nestedIndexSetup(t *testing.T, ctx CtxT, mc MC, namePrefix, subField, indexType string) {
	collName := common.GenRandomString(namePrefix, 6)
	opt := hp.DefaultStructAElementSchemaOption(collName)
	opt.IncludeDocVChar = false
	opt.IncludeCategory = false
	opt.IncludeSize = true
	opt.IncludeFloatVal = true
	schema, structSchema := hp.CreateStructAElementSchema(opt)
	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)

	ds := hp.GenerateStructAElementData(200, 0, opt)
	insertElemDataset(t, ctx, mc, collName, structSchema, ds, opt)
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewHNSWIndex(entity.COSINE, 16, 200)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "structA[embedding]",
		index.NewHNSWIndex(entity.MaxSimCosine, 16, 200)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "structA["+subField+"]",
		index.NewGenericIndex("nested_idx", map[string]string{"index_type": indexType})))
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)

	// Sanity query
	rs, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter("id < 5").WithOutputFields("id").WithLimit(5).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 5, rs.ResultCount)
}

func TestStructArrayElementNestedIndexInvertedInt(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	nestedIndexSetup(t, ctx, mc, elemSearchPrefix+"_ni_inv_int", "int_val", "INVERTED")
}

func TestStructArrayElementNestedIndexInvertedVarchar(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	nestedIndexSetup(t, ctx, mc, elemSearchPrefix+"_ni_inv_str", "str_val", "INVERTED")
}

func TestStructArrayElementNestedIndexSTLSortInt(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	nestedIndexSetup(t, ctx, mc, elemSearchPrefix+"_ni_stl_int", "int_val", "STL_SORT")
}

// =============================================================================
// 4. TestMilvusClientStructArrayElementNonFloatVectors  (2 L0 — schema create only)
// =============================================================================

func runNonFloatVectorCreate(t *testing.T, ctx CtxT, mc MC, namePrefix string, vecType entity.FieldType, metric entity.MetricType) {
	collName := common.GenRandomString(namePrefix, 6)
	dim := hp.StructAElemDim
	structSchema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("embedding").WithDataType(vecType).WithDim(int64(dim))).
		WithField(entity.NewField().WithName("int_val").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("str_val").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256))

	schema := entity.NewSchema().WithName(collName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("doc_int").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("normal_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dim))).
		WithField(entity.NewField().WithName("structA").
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).
			WithMaxCapacity(int64(hp.StructAElemCapacity)).
			WithStructSchema(structSchema))

	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)

	_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewHNSWIndex(entity.COSINE, 16, 200)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "structA[embedding]",
		index.NewHNSWIndex(metric, 16, 200)))
	common.CheckErr(t, err, true)
}

func TestStructArrayElementNonFloatVectorsFloat16(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	runNonFloatVectorCreate(t, ctx, mc, elemSearchPrefix+"_nf_f16",
		entity.FieldTypeFloat16Vector, entity.MaxSimL2)
}

func TestStructArrayElementNonFloatVectorsBFloat16(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	runNonFloatVectorCreate(t, ctx, mc, elemSearchPrefix+"_nf_bf16",
		entity.FieldTypeBFloat16Vector, entity.MaxSimIP)
}

// =============================================================================
// 5. TestMilvusClientStructArrayElementGroupBySearch  (2 L0)
// =============================================================================

// groupByCollection inlines the GroupBy schema (id + doc_int + doc_category(VarChar) + doc_group(Int32)
// + normal_vector + structA{embedding,int_val,str_val,float_val,color}) since it's unique to this
// suite.
func groupByCollection(t *testing.T, ctx CtxT, mc MC) (string, []hp.StructARow) {
	collName := common.GenRandomString(elemSearchPrefix+"_gb", 6)
	dim := hp.StructAElemDim

	structSchema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dim))).
		WithField(entity.NewField().WithName("int_val").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("str_val").WithDataType(entity.FieldTypeVarChar).WithMaxLength(65535)).
		WithField(entity.NewField().WithName("float_val").WithDataType(entity.FieldTypeFloat)).
		WithField(entity.NewField().WithName("color").WithDataType(entity.FieldTypeVarChar).WithMaxLength(128))

	schema := entity.NewSchema().WithName(collName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("doc_int").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("doc_category").WithDataType(entity.FieldTypeVarChar).WithMaxLength(128)).
		WithField(entity.NewField().WithName("doc_group").WithDataType(entity.FieldTypeInt32)).
		WithField(entity.NewField().WithName("normal_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dim))).
		WithField(entity.NewField().WithName("structA").
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).
			WithMaxCapacity(int64(hp.StructAElemCapacity)).
			WithStructSchema(structSchema))

	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)

	const nb = 500
	opt := hp.DefaultStructAElementSchemaOption(collName)
	opt.IncludeDocVChar = false
	opt.IncludeCategory = false
	opt.IncludeSize = false
	opt.IncludeFloatVal = true
	ds := hp.GenerateStructAElementData(nb, 0, opt)
	rows := ds.Rows

	ids := make([]int64, nb)
	docInts := make([]int64, nb)
	docCats := make([]string, nb)
	docGroups := make([]int32, nb)
	vectors := make([][]float32, nb)
	structRows := make([]map[string]any, nb)
	for i, r := range rows {
		ids[i] = r.ID
		docInts[i] = r.DocInt
		docCats[i] = hp.StructAElemCategories[r.ID%4]
		docGroups[i] = int32(r.ID % 5)
		vectors[i] = r.NormalVector
		// build sub-field rows directly (matches GroupBy schema sub-fields)
		embs := make([][]float32, len(r.StructA))
		intVals := make([]int64, len(r.StructA))
		strVals := make([]string, len(r.StructA))
		floatVals := make([]float32, len(r.StructA))
		colors := make([]string, len(r.StructA))
		for j, e := range r.StructA {
			embs[j] = e.Embedding
			intVals[j] = e.IntVal
			strVals[j] = e.StrVal
			floatVals[j] = e.FloatVal
			colors[j] = e.Color
		}
		structRows[i] = map[string]any{
			"embedding": embs,
			"int_val":   intVals,
			"str_val":   strVals,
			"float_val": floatVals,
			"color":     colors,
		}
	}

	docCatCol := column.NewColumnVarChar("doc_category", docCats)
	docGroupCol := column.NewColumnInt32("doc_group", docGroups)
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", ids).
		WithInt64Column("doc_int", docInts).
		WithColumns(docCatCol, docGroupCol).
		WithFloatVectorColumn("normal_vector", dim, vectors).
		WithStructArrayColumn("structA", structSchema, structRows))
	common.CheckErr(t, err, true)

	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewHNSWIndex(entity.COSINE, 16, 200)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "structA[embedding]",
		index.NewHNSWIndex(entity.MaxSimCosine, 16, 200)))
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)

	return collName, rows
}

func TestStructArrayElementGroupByElementFilterBasic(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collName, rows := groupByCollection(t, ctx, mc)

	queryVec := rows[0].NormalVector
	rs, err := mc.Search(ctx, client.NewSearchOption(collName, 10,
		[]entity.Vector{entity.FloatVector(queryVec)}).
		WithANNSField("normal_vector").
		WithFilter(`MATCH_ANY(structA, $[int_val] > 100)`).
		WithGroupByField("doc_category").
		WithOutputFields("id", "doc_category").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.GreaterOrEqual(t, len(rs), 1)
	require.Greater(t, rs[0].ResultCount, 0)

	// no duplicate doc_category in returned rows
	seen := map[string]bool{}
	catCol := rs[0].GetColumn("doc_category")
	for i := 0; i < rs[0].ResultCount; i++ {
		v, _ := catCol.Get(i)
		c := v.(string)
		require.False(t, seen[c], "duplicate doc_category %q in grouped results", c)
		seen[c] = true
	}
}

func TestStructArrayElementGroupByMatchAll(t *testing.T) {
	// MATCH_ALL is not supported by the 2.6 base server used by this backport.
	t.Skip("MATCH_ALL is not supported by the 2.6 base server")

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collName, rows := groupByCollection(t, ctx, mc)

	queryVec := rows[0].NormalVector
	rs, err := mc.Search(ctx, client.NewSearchOption(collName, 10,
		[]entity.Vector{entity.FloatVector(queryVec)}).
		WithANNSField("normal_vector").
		WithFilter(`MATCH_ALL(structA, $[int_val] > 0)`).
		WithGroupByField("doc_category").
		WithOutputFields("id", "doc_category").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.GreaterOrEqual(t, len(rs), 1)
	require.Greater(t, rs[0].ResultCount, 0)
}

// =============================================================================
// 6. TestMilvusClientStructArrayElementSearchNoFilter  (4 L0)
// =============================================================================

// noFilterCollection inlines the NoFilter schema (id + doc_int + doc_category + normal_vector +
// structA{embedding,int_val,str_val,color}).
func noFilterCollection(t *testing.T, ctx CtxT, mc MC) (string, []hp.StructARow) {
	collName := common.GenRandomString(elemSearchPrefix+"_nf", 6)
	dim := hp.StructAElemDim
	const nb = 500

	structSchema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dim))).
		WithField(entity.NewField().WithName("int_val").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("str_val").WithDataType(entity.FieldTypeVarChar).WithMaxLength(65535)).
		WithField(entity.NewField().WithName("color").WithDataType(entity.FieldTypeVarChar).WithMaxLength(128))

	schema := entity.NewSchema().WithName(collName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("doc_int").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("doc_category").WithDataType(entity.FieldTypeVarChar).WithMaxLength(128)).
		WithField(entity.NewField().WithName("normal_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dim))).
		WithField(entity.NewField().WithName("structA").
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).
			WithMaxCapacity(int64(hp.StructAElemCapacity)).
			WithStructSchema(structSchema))

	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)

	opt := hp.DefaultStructAElementSchemaOption(collName)
	opt.IncludeDocVChar = false
	opt.IncludeCategory = false
	opt.IncludeSize = false
	opt.IncludeFloatVal = false
	ds := hp.GenerateStructAElementData(nb, 0, opt)
	rows := ds.Rows

	ids := make([]int64, nb)
	docInts := make([]int64, nb)
	docCats := make([]string, nb)
	vectors := make([][]float32, nb)
	structRows := make([]map[string]any, nb)
	for i, r := range rows {
		ids[i] = r.ID
		docInts[i] = r.DocInt
		docCats[i] = hp.StructAElemCategories[r.ID%4]
		vectors[i] = r.NormalVector
		embs := make([][]float32, len(r.StructA))
		intVals := make([]int64, len(r.StructA))
		strVals := make([]string, len(r.StructA))
		colors := make([]string, len(r.StructA))
		for j, e := range r.StructA {
			embs[j] = e.Embedding
			intVals[j] = e.IntVal
			strVals[j] = e.StrVal
			colors[j] = e.Color
		}
		structRows[i] = map[string]any{
			"embedding": embs,
			"int_val":   intVals,
			"str_val":   strVals,
			"color":     colors,
		}
	}

	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", ids).
		WithInt64Column("doc_int", docInts).
		WithVarcharColumn("doc_category", docCats).
		WithFloatVectorColumn("normal_vector", dim, vectors).
		WithStructArrayColumn("structA", structSchema, structRows))
	common.CheckErr(t, err, true)

	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewHNSWIndex(entity.COSINE, 16, 200)))
	common.CheckErr(t, err, true)
	// Plain COSINE on sub-vector so single-vector searches work directly.
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "structA[embedding]",
		index.NewHNSWIndex(entity.COSINE, 16, 200)))
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)
	return collName, rows
}

func TestStructArrayElementSearchNoFilter(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collName, rows := noFilterCollection(t, ctx, mc)

	t.Run("basic", func(t *testing.T) {
		queryVec := rows[0].StructA[0].Embedding
		rs, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{entity.FloatVector(queryVec)}).
			WithANNSField("structA[embedding]").
			WithSearchParam("metric_type", "COSINE").
			WithOutputFields("id").
			WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		require.EqualValues(t, 10, rs[0].ResultCount, "expected exactly limit=10 rows")
		first, _ := rs[0].GetColumn("id").Get(0)
		require.EqualValues(t, int64(0), first.(int64), "self-match top-1 should be row 0")
	})

	t.Run("ground_truth", func(t *testing.T) {
		queryVec := rows[42].StructA[1].Embedding
		const limit = 20
		rs, err := mc.Search(ctx, client.NewSearchOption(collName, limit, []entity.Vector{entity.FloatVector(queryVec)}).
			WithANNSField("structA[embedding]").
			WithSearchParam("metric_type", "COSINE").
			WithOutputFields("id").
			WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		// HNSW recall on a small dataset can return slightly fewer than limit; accept ≥ 90%.
		require.GreaterOrEqual(t, rs[0].ResultCount, limit*9/10,
			"got %d results, expected at least %d", rs[0].ResultCount, limit*9/10)

		gtIDs := hp.GtElementSearchNoFilter(rows, queryVec, "COSINE", limit)
		idCol := rs[0].GetColumn("id")
		got := make([]int64, rs[0].ResultCount)
		for i := 0; i < rs[0].ResultCount; i++ {
			v, _ := idCol.Get(i)
			got[i] = v.(int64)
		}
		require.EqualValues(t, gtIDs[0], got[0], "top-1 must match ground truth")
		// top-K recall ≥ 0.85 (HNSW recall + small-dataset tolerance)
		gtSet := map[int64]bool{}
		for _, id := range gtIDs {
			gtSet[id] = true
		}
		overlap := 0
		for _, id := range got {
			if gtSet[id] {
				overlap++
			}
		}
		require.GreaterOrEqual(t, float64(overlap)/float64(limit), 0.85,
			"recall too low: %d/%d", overlap, limit)
	})

	t.Run("distance_order", func(t *testing.T) {
		queryVec := rows[0].StructA[0].Embedding
		const limit = 50
		rs, err := mc.Search(ctx, client.NewSearchOption(collName, limit, []entity.Vector{entity.FloatVector(queryVec)}).
			WithANNSField("structA[embedding]").
			WithSearchParam("metric_type", "COSINE").
			WithOutputFields("id").
			WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		require.GreaterOrEqual(t, rs[0].ResultCount, limit*9/10,
			"got %d results, expected at least %d", rs[0].ResultCount, limit*9/10)
		// COSINE: distances should be monotonically non-increasing
		for i := 0; i < rs[0].ResultCount-1; i++ {
			require.GreaterOrEqual(t, float64(rs[0].Scores[i]+1e-3), float64(rs[0].Scores[i+1]),
				"distance not monotonic at pos %d: %f < %f", i, rs[0].Scores[i], rs[0].Scores[i+1])
		}
	})

	t.Run("group_by_pk", func(t *testing.T) {
		queryVec := rows[0].StructA[0].Embedding
		const limit = 20
		rs, err := mc.Search(ctx, client.NewSearchOption(collName, limit, []entity.Vector{entity.FloatVector(queryVec)}).
			WithANNSField("structA[embedding]").
			WithSearchParam("metric_type", "COSINE").
			WithGroupByField("id").
			WithOutputFields("id").
			WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		require.EqualValues(t, limit, rs[0].ResultCount)

		seen := map[int64]bool{}
		idCol := rs[0].GetColumn("id")
		for i := 0; i < rs[0].ResultCount; i++ {
			v, _ := idCol.Get(i)
			id := v.(int64)
			require.False(t, seen[id], "duplicate PK %d under group_by=id", id)
			seen[id] = true
		}
		first, _ := idCol.Get(0)
		require.EqualValues(t, int64(0), first.(int64), "self-match top-1 should be row 0 even with group_by_pk")
	})
}
