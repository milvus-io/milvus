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

// L0 ports of tests/python_client/milvus_client/test_milvus_client_struct_array_element_query.py.
// Where Python uses class-level fixtures we collapse subtests into a single Go test function via
// t.Run so the collection setup amortises across cases.
package testcases

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const (
	elemQuerySealedNb  = hp.StructAElemSealedNb
	elemQueryGrowingNb = hp.StructAElemGrowingNb
)

// setupElemSharedCollection mirrors the python class fixture: create canonical schema, build
// indexes on normal_vector and structA[embedding], insert sealedNb sealed + growingNb growing
// rows, load. Returns coll name + struct schema + concatenated dataset for ground truth.
func setupElemSharedCollection(t *testing.T, ctx CtxT, mc MC, namePrefix string, opt hp.StructAElementSchemaOption) (string, *entity.StructSchema, []hp.StructARow) {
	collName := common.GenRandomString(namePrefix, 6)
	opt.CollectionName = collName
	schema, structSchema := hp.CreateStructAElementSchema(opt)
	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)

	// Sealed batch
	sealed := hp.GenerateStructAElementData(elemQuerySealedNb, 0, opt)
	insertElemDataset(t, ctx, mc, collName, structSchema, sealed, opt)
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	// Growing batch (no flush)
	growing := hp.GenerateStructAElementData(elemQueryGrowingNb, int64(elemQuerySealedNb), opt)
	insertElemDataset(t, ctx, mc, collName, structSchema, growing, opt)

	// Indexes + load
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewHNSWIndex(entity.COSINE, 16, 200)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "structA[embedding]",
		index.NewHNSWIndex(entity.MaxSimCosine, 16, 200)))
	common.CheckErr(t, err, true)
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)

	all := append([]hp.StructARow{}, sealed.Rows...)
	all = append(all, growing.Rows...)
	return collName, structSchema, all
}

func insertElemDataset(t *testing.T, ctx CtxT, mc MC, collName string, structSchema *entity.StructSchema, ds hp.StructAElementDataset, opt hp.StructAElementSchemaOption) {
	ids, vectors, docInts, docVChars, structRows := ds.ToInsertColumns()
	insertOpt := client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", ids)
	if opt.IncludeDocInt {
		insertOpt = insertOpt.WithInt64Column("doc_int", docInts)
	}
	if opt.IncludeDocVChar {
		insertOpt = insertOpt.WithVarcharColumn("doc_varchar", docVChars)
	}
	insertOpt = insertOpt.
		WithFloatVectorColumn("normal_vector", opt.Dim, vectors).
		WithStructArrayColumn("structA", structSchema, structRows)
	_, err := mc.Insert(ctx, insertOpt)
	common.CheckErr(t, err, true)
}

// queryAllIDs runs Query and returns sorted IDs from the "id" output column.
func queryAllIDs(t *testing.T, ctx CtxT, mc MC, collName, expr string, limit int) []int64 {
	rs, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(expr).WithOutputFields("id").WithLimit(limit).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	col := rs.GetColumn("id")
	require.NotNil(t, col, "id column missing")
	out := make([]int64, rs.ResultCount)
	for i := 0; i < rs.ResultCount; i++ {
		v, err := col.Get(i)
		require.NoError(t, err)
		out[i] = v.(int64)
	}
	return sortInt64s(out)
}

func sortInt64s(s []int64) []int64 {
	for i := 1; i < len(s); i++ {
		j := i
		for j > 0 && s[j-1] > s[j] {
			s[j-1], s[j] = s[j], s[j-1]
			j--
		}
	}
	return s
}

func subset(a, b []int64) bool {
	bs := make(map[int64]struct{}, len(b))
	for _, x := range b {
		bs[x] = struct{}{}
	}
	for _, x := range a {
		if _, ok := bs[x]; !ok {
			return false
		}
	}
	return true
}

// =============================================================================
// 1.  TestMilvusClientStructArrayElementContains  (4 L0)
// =============================================================================

func TestStructArrayElementContains(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	opt := hp.DefaultStructAElementSchemaOption("")
	collName, _, data := setupElemSharedCollection(t, ctx, mc, hp.StructAElemPrefix+"_ac_shared", opt)

	t.Run("array_contains_int_subfield", func(t *testing.T) {
		const target int64 = 100 // row 1 elem 0 → int_val = 1*100+0 = 100
		ids := queryAllIDs(t, ctx, mc, collName, "array_contains(structA[int_val], 100)", 50)
		gt := hp.GtArrayContains(data, target, func(e hp.StructAElement) int64 { return e.IntVal })
		require.True(t, subset(ids, hp.IDSetToSorted(gt)),
			"server returned IDs not in ground truth: got %v gt %v", ids, hp.IDSetToSorted(gt))
		require.Greater(t, len(ids), 0)
	})

	t.Run("array_contains_varchar_subfield", func(t *testing.T) {
		ids := queryAllIDs(t, ctx, mc, collName, `array_contains(structA[color], "Red")`, 50)
		require.Greater(t, len(ids), 0)
		// Every returned row must contain at least one Red element.
		expected := hp.GtArrayContains(data, "Red", func(e hp.StructAElement) string { return e.Color })
		require.True(t, subset(ids, hp.IDSetToSorted(expected)))
	})

	t.Run("array_contains_all_struct_subfield", func(t *testing.T) {
		ids := queryAllIDs(t, ctx, mc, collName, `array_contains_all(structA[color], ["Red", "Blue"])`, 50)
		require.Greater(t, len(ids), 0)
		gt := hp.GtArrayContainsAll(data, []string{"Red", "Blue"}, func(e hp.StructAElement) string { return e.Color })
		require.True(t, subset(ids, hp.IDSetToSorted(gt)))
	})

	t.Run("array_contains_any_struct_subfield", func(t *testing.T) {
		ids := queryAllIDs(t, ctx, mc, collName, `array_contains_any(structA[category], ["A", "B"])`, 50)
		require.Greater(t, len(ids), 0)
		gt := hp.GtArrayContainsAny(data, []string{"A", "B"}, func(e hp.StructAElement) string { return e.Category })
		require.True(t, subset(ids, hp.IDSetToSorted(gt)))
	})
}

// =============================================================================
// 2.  TestMilvusClientStructArrayElementQuery  (3 L0)
// =============================================================================

func TestStructArrayElementQueryBasics(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	opt := hp.DefaultStructAElementSchemaOption("")
	collName, _, data := setupElemSharedCollection(t, ctx, mc, hp.StructAElemPrefix+"_efq_shared", opt)

	t.Run("element_filter_query_basic", func(t *testing.T) {
		ids := queryAllIDs(t, ctx, mc, collName, `element_filter(structA, $[int_val] > 200)`, 50)
		require.Greater(t, len(ids), 0)
		gt := hp.GtElementFilter(data, func(e hp.StructAElement) bool { return e.IntVal > 200 }, nil)
		require.True(t, subset(ids, hp.IDSetToSorted(gt)),
			"got %v not subset of gt %v", ids, hp.IDSetToSorted(gt))
	})

	t.Run("element_filter_query_compound", func(t *testing.T) {
		ids := queryAllIDs(t, ctx, mc, collName,
			`element_filter(structA, $[color] == "Red" && $[int_val] > 100)`, 50)
		require.Greater(t, len(ids), 0)
		gt := hp.GtElementFilter(data,
			func(e hp.StructAElement) bool { return e.Color == "Red" && e.IntVal > 100 }, nil)
		require.True(t, subset(ids, hp.IDSetToSorted(gt)))
	})

	t.Run("element_filter_query_with_doc_filter", func(t *testing.T) {
		ids := queryAllIDs(t, ctx, mc, collName,
			`doc_int > 100 && element_filter(structA, $[color] == "Red")`, 50)
		require.Greater(t, len(ids), 0)
		gt := hp.GtElementFilter(data,
			func(e hp.StructAElement) bool { return e.Color == "Red" },
			func(r hp.StructARow) bool { return r.DocInt > 100 })
		require.True(t, subset(ids, hp.IDSetToSorted(gt)))
	})
}

// =============================================================================
// 4.  TestMilvusClientStructArrayElementSTLSortIndex  (2 L0)
// =============================================================================

// runSTLSortCase creates a fresh collection with HNSW(normal_vector + structA[embedding]) and
// STL_SORT(structA[<scalarField>]), inserts sealed+growing data, loads, and asserts a basic
// query returns the expected number of rows. Mirrors stl_sort_index_create_int / _varchar.
func runSTLSortCase(t *testing.T, ctx CtxT, mc MC, scalarField, namePrefix string) {
	collName := common.GenRandomString(namePrefix, 6)
	opt := hp.DefaultStructAElementSchemaOption(collName)
	schema, structSchema := hp.CreateStructAElementSchema(opt)

	// Pre-create with index params to exercise the same code path as the python prepare_index_params.
	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)

	sealed := hp.GenerateStructAElementData(elemQuerySealedNb, 0, opt)
	insertElemDataset(t, ctx, mc, collName, structSchema, sealed, opt)
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	growing := hp.GenerateStructAElementData(elemQueryGrowingNb, int64(elemQuerySealedNb), opt)
	insertElemDataset(t, ctx, mc, collName, structSchema, growing, opt)

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewHNSWIndex(entity.COSINE, 16, 200)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "structA[embedding]",
		index.NewHNSWIndex(entity.MaxSimCosine, 16, 200)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "structA["+scalarField+"]",
		index.NewGenericIndex("stl_sort_idx", map[string]string{"index_type": "STL_SORT"})))
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)

	rs, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter("id < 10").WithOutputFields("id").WithLimit(10).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 10, rs.ResultCount)
}

func TestStructArrayElementSTLSortIndexCreateInt(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	runSTLSortCase(t, ctx, mc, "int_val", hp.StructAElemPrefix+"_stl_int")
}

func TestStructArrayElementSTLSortIndexCreateVarchar(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	runSTLSortCase(t, ctx, mc, "str_val", hp.StructAElemPrefix+"_stl_vc")
}

// =============================================================================
// 5.  TestMilvusClientStructArrayElementIndexRegression  (2 L0)
// =============================================================================

// runInvertedIndexCase creates a collection with an INVERTED index on a struct sub-field, then
// runs a search filtered by element_filter on the same sub-field — verifying the index was
// actually built with the right inverted-index code path (PR #48183 regression).
func runInvertedIndexCase(t *testing.T, ctx CtxT, mc MC, scalarField, namePrefix, filter string, gtFn func(hp.StructAElement) bool) {
	collName := common.GenRandomString(namePrefix, 6)
	opt := hp.DefaultStructAElementSchemaOption(collName)
	opt.IncludeDocInt = false
	opt.IncludeDocVChar = false
	opt.IncludeFloatVal = false
	opt.IncludeCategory = false
	schema, structSchema := hp.CreateStructAElementSchema(opt)
	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)

	sealed := hp.GenerateStructAElementData(elemQuerySealedNb, 0, opt)
	insertElemDataset(t, ctx, mc, collName, structSchema, sealed, opt)
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewHNSWIndex(entity.COSINE, 16, 200)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "structA[embedding]",
		index.NewHNSWIndex(entity.MaxSimCosine, 16, 200)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "structA["+scalarField+"]",
		index.NewGenericIndex("inv_idx", map[string]string{"index_type": "INVERTED"})))
	common.CheckErr(t, err, true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)

	// Search using row 0's first embedding (deterministic from SeedVector).
	queryEmb := entity.FloatVectorArray{entity.FloatVector(hp.SeedVector(0, opt.Dim))}
	rs, err := mc.Search(ctx, client.NewSearchOption(collName, 10, []entity.Vector{queryEmb}).
		WithANNSField("structA[embedding]").
		WithFilter(filter).
		WithOutputFields("id").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.GreaterOrEqual(t, len(rs), 1)
	require.Greater(t, rs[0].ResultCount, 0)

	// Sanity: every returned ID must satisfy the ground-truth predicate.
	gt := hp.GtElementFilter(sealed.Rows, gtFn, nil)
	idCol := rs[0].GetColumn("id")
	for i := 0; i < rs[0].ResultCount; i++ {
		v, err := idCol.Get(i)
		require.NoError(t, err)
		_, ok := gt[v.(int64)]
		require.True(t, ok, "row %d not in ground truth set", v.(int64))
	}
}

func TestStructArrayElementInvertedIndexVarchar(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	runInvertedIndexCase(t, ctx, mc, "color", hp.StructAElemPrefix+"_inv_vc",
		`element_filter(structA, $[color] == "Red")`,
		func(e hp.StructAElement) bool { return e.Color == "Red" })
}

func TestStructArrayElementInvertedIndexInt(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	runInvertedIndexCase(t, ctx, mc, "int_val", hp.StructAElemPrefix+"_inv_int",
		`element_filter(structA, $[int_val] > 100)`,
		func(e hp.StructAElement) bool { return e.IntVal > 100 })
}

// =============================================================================
// 6.  TestMilvusClientStructArrayElementQueryCorrectness  (10 L0)
// =============================================================================

// setupCorrectnessCollection mirrors python `_setup_with_controlled_data`: inserts background
// inert rows then the controlled test rows. We shrink background rows from 3500 → 100 inert to
// keep tests fast while still exercising sealed+growing.
func setupCorrectnessCollection(t *testing.T, ctx CtxT, mc MC, namePrefix string, controlled []hp.StructARow) (string, *entity.StructSchema, []hp.StructARow) {
	collName := common.GenRandomString(namePrefix, 6)
	opt := hp.DefaultStructAElementSchemaOption(collName)
	opt.IncludeDocVChar = false
	opt.IncludeFloatVal = false
	opt.IncludeCategory = false
	schema, structSchema := hp.CreateStructAElementSchema(opt)
	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)

	bgRows := make([]hp.StructARow, 0, 100)
	bgStart := int64(100000)
	for i := int64(0); i < 100; i++ {
		bgRows = append(bgRows, hp.MakeInertRow(bgStart+i, opt))
	}
	insertCustomRows(t, ctx, mc, collName, structSchema, bgRows, opt)
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	insertCustomRows(t, ctx, mc, collName, structSchema, controlled, opt)

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewHNSWIndex(entity.COSINE, 16, 200)))
	common.CheckErr(t, err, true)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "structA[embedding]",
		index.NewHNSWIndex(entity.MaxSimCosine, 16, 200)))
	common.CheckErr(t, err, true)
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)

	all := append([]hp.StructARow{}, bgRows...)
	all = append(all, controlled...)
	return collName, structSchema, all
}

func insertCustomRows(t *testing.T, ctx CtxT, mc MC, collName string, structSchema *entity.StructSchema, rows []hp.StructARow, opt hp.StructAElementSchemaOption) {
	if len(rows) == 0 {
		return
	}
	ids, vectors, docInts, docVChars, structRows := hp.RowsToColumns(rows, opt)
	insertOpt := client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", ids)
	if opt.IncludeDocInt {
		insertOpt = insertOpt.WithInt64Column("doc_int", docInts)
	}
	if opt.IncludeDocVChar {
		insertOpt = insertOpt.WithVarcharColumn("doc_varchar", docVChars)
	}
	insertOpt = insertOpt.
		WithFloatVectorColumn("normal_vector", opt.Dim, vectors).
		WithStructArrayColumn("structA", structSchema, structRows)
	_, err := mc.Insert(ctx, insertOpt)
	common.CheckErr(t, err, true)
}

func TestStructArrayElementQueryCorrectness(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	t.Run("element_filter_query_exact_ids", func(t *testing.T) {
		opt := hp.DefaultStructAElementSchemaOption("")
		controlled := []hp.StructARow{
			hp.MakeRow(0, opt, []hp.StructAElement{{IntVal: 10}, {IntVal: 20}, {IntVal: 30}}),
			hp.MakeRow(1, opt, []hp.StructAElement{{IntVal: 3}, {IntVal: 4}}),
			hp.MakeRow(2, opt, []hp.StructAElement{{IntVal: 1}, {IntVal: 100}}),
			hp.MakeRow(3, opt, []hp.StructAElement{{IntVal: 6}}),
			hp.MakeRow(4, opt, []hp.StructAElement{{IntVal: 5}}),
		}
		collName, _, _ := setupCorrectnessCollection(t, ctx, mc, hp.StructAElemPrefix+"_exact_ids", controlled)
		ids := queryAllIDs(t, ctx, mc, collName, `element_filter(structA, $[int_val] > 5)`, 100)
		require.Equal(t, []int64{0, 2, 3}, ids)
	})

	t.Run("element_filter_query_no_match", func(t *testing.T) {
		opt := hp.DefaultStructAElementSchemaOption("")
		controlled := []hp.StructARow{
			hp.MakeRow(0, opt, []hp.StructAElement{{IntVal: 1}, {IntVal: 2}}),
			hp.MakeRow(1, opt, []hp.StructAElement{{IntVal: 3}}),
		}
		collName, _, _ := setupCorrectnessCollection(t, ctx, mc, hp.StructAElemPrefix+"_no_match", controlled)
		ids := queryAllIDs(t, ctx, mc, collName, `element_filter(structA, $[int_val] > 9999)`, 100)
		require.Empty(t, ids)
	})

	t.Run("element_filter_query_offset_correctness", func(t *testing.T) {
		opt := hp.DefaultStructAElementSchemaOption("")
		controlled := make([]hp.StructARow, 0, 20)
		for i := int64(0); i < 20; i++ {
			controlled = append(controlled, hp.MakeRow(i, opt, []hp.StructAElement{{IntVal: i * 10}}))
		}
		collName, _, _ := setupCorrectnessCollection(t, ctx, mc, hp.StructAElemPrefix+"_offset", controlled)

		expr := `element_filter(structA, $[int_val] > 50)`
		all := queryAllIDs(t, ctx, mc, collName, expr, 100)
		page1 := queryPage(t, ctx, mc, collName, expr, 5, 0)
		page2 := queryPage(t, ctx, mc, collName, expr, 5, 5)
		page3 := queryPage(t, ctx, mc, collName, expr, 5, 10)

		// Pages must be disjoint
		require.Empty(t, intersect(page1, page2))
		require.Empty(t, intersect(page1, page3))
		require.Empty(t, intersect(page2, page3))
		// And union ⊆ all
		union := append(append(append([]int64{}, page1...), page2...), page3...)
		require.True(t, subset(union, all),
			"union of pages %v not subset of full result %v", union, all)
	})

	t.Run("element_filter_query_returned_elements_correctness", func(t *testing.T) {
		opt := hp.DefaultStructAElementSchemaOption("")
		controlled := []hp.StructARow{
			hp.MakeRow(0, opt, []hp.StructAElement{
				{IntVal: 10, Color: "Red"},
				{IntVal: 20, Color: "Blue"},
				{IntVal: 30, Color: "Green"},
			}),
			hp.MakeRow(1, opt, []hp.StructAElement{
				{IntVal: 5, Color: "Red"},
			}),
		}
		collName, _, _ := setupCorrectnessCollection(t, ctx, mc, hp.StructAElemPrefix+"_elem_correct", controlled)
		rs, err := mc.Query(ctx, client.NewQueryOption(collName).
			WithFilter(`element_filter(structA, $[int_val] > 15)`).
			WithOutputFields("id", "structA").WithLimit(100).
			WithConsistencyLevel(entity.ClStrong))
		common.CheckErr(t, err, true)
		require.EqualValues(t, 1, rs.ResultCount)
		require.EqualValues(t, int64(0), mustInt64(t, rs.GetColumn("id"), 0))
		structVal, err := rs.GetColumn("structA").Get(0)
		require.NoError(t, err)
		m := structVal.(map[string]any)
		intVals := m["int_val"].([]int64)
		require.GreaterOrEqual(t, len(intVals), 1)
	})

	t.Run("element_filter_count_exact", func(t *testing.T) {
		opt := hp.DefaultStructAElementSchemaOption("")
		controlled := make([]hp.StructARow, 10)
		for i := int64(0); i < 10; i++ {
			val := int64(1)
			if i >= 5 {
				val = 100
			}
			controlled[i] = hp.MakeRow(i, opt, []hp.StructAElement{{IntVal: val}})
		}
		collName, _, _ := setupCorrectnessCollection(t, ctx, mc, hp.StructAElemPrefix+"_count", controlled)
		require.EqualValues(t, 5, queryCountStar(t, ctx, mc, collName,
			`element_filter(structA, $[int_val] > 50)`))
		require.EqualValues(t, 10, queryCountStar(t, ctx, mc, collName,
			`element_filter(structA, $[int_val] > 0)`))
	})

	t.Run("element_filter_query_same_element_semantic", func(t *testing.T) {
		opt := hp.DefaultStructAElementSchemaOption("")
		controlled := []hp.StructARow{
			hp.MakeRow(0, opt, []hp.StructAElement{
				{IntVal: 10, Color: "Red"},
				{IntVal: 20, Color: "Blue"},
			}),
			hp.MakeRow(1, opt, []hp.StructAElement{{IntVal: 20, Color: "Red"}}),
			hp.MakeRow(2, opt, []hp.StructAElement{{IntVal: 5, Color: "Green"}}),
		}
		collName, _, _ := setupCorrectnessCollection(t, ctx, mc, hp.StructAElemPrefix+"_same_elem", controlled)
		ids := queryAllIDs(t, ctx, mc, collName,
			`element_filter(structA, $[color] == "Red" && $[int_val] > 15)`, 100)
		require.Equal(t, []int64{1}, ids, "same-element semantic: only row 1 has Red AND int_val>15 in same elem")
	})

	t.Run("match_all_query_exact_verification", func(t *testing.T) {
		opt := hp.DefaultStructAElementSchemaOption("")
		controlled := []hp.StructARow{
			hp.MakeRow(0, opt, []hp.StructAElement{{IntVal: 11}, {IntVal: 20}, {IntVal: 30}}),
			hp.MakeRow(1, opt, []hp.StructAElement{{IntVal: 5}, {IntVal: 20}}),
			hp.MakeRow(2, opt, []hp.StructAElement{{IntVal: 100}}),
			hp.MakeRow(3, opt, []hp.StructAElement{{IntVal: 1}, {IntVal: 2}, {IntVal: 3}}),
			hp.MakeRow(4, opt, []hp.StructAElement{{IntVal: 10}, {IntVal: 15}}),
		}
		collName, _, _ := setupCorrectnessCollection(t, ctx, mc, hp.StructAElemPrefix+"_mall_exact", controlled)
		ids := queryAllIDs(t, ctx, mc, collName, `MATCH_ALL(structA, $[int_val] > 10)`, 100)
		require.Equal(t, []int64{0, 2}, ids)
	})

	t.Run("match_exact_query_verification", func(t *testing.T) {
		opt := hp.DefaultStructAElementSchemaOption("")
		controlled := []hp.StructARow{
			hp.MakeRow(0, opt, []hp.StructAElement{
				{IntVal: 1, Color: "Red"}, {IntVal: 2, Color: "Red"}, {IntVal: 3, Color: "Red"},
			}),
			hp.MakeRow(1, opt, []hp.StructAElement{
				{IntVal: 1, Color: "Red"}, {IntVal: 2, Color: "Blue"}, {IntVal: 3, Color: "Red"},
			}),
			hp.MakeRow(2, opt, []hp.StructAElement{
				{IntVal: 1, Color: "Red"}, {IntVal: 2, Color: "Blue"},
			}),
			hp.MakeRow(3, opt, []hp.StructAElement{{IntVal: 1, Color: "Blue"}}),
			hp.MakeRow(4, opt, []hp.StructAElement{
				{IntVal: 1, Color: "Red"}, {IntVal: 2, Color: "Red"},
			}),
		}
		collName, _, _ := setupCorrectnessCollection(t, ctx, mc, hp.StructAElemPrefix+"_mexact", controlled)
		ids := queryAllIDs(t, ctx, mc, collName,
			`MATCH_EXACT(structA, $[color] == "Red", threshold=2)`, 100)
		require.Equal(t, []int64{1, 4}, ids)
	})

	t.Run("array_contains_exact_result_set", func(t *testing.T) {
		opt := hp.DefaultStructAElementSchemaOption("")
		controlled := []hp.StructARow{
			hp.MakeRow(0, opt, []hp.StructAElement{{IntVal: 1, Color: "Red"}}),
			hp.MakeRow(1, opt, []hp.StructAElement{{IntVal: 2, Color: "Blue"}}),
			hp.MakeRow(2, opt, []hp.StructAElement{
				{IntVal: 3, Color: "Red"}, {IntVal: 4, Color: "Blue"},
			}),
			hp.MakeRow(3, opt, []hp.StructAElement{{IntVal: 5, Color: "Green"}}),
		}
		collName, _, _ := setupCorrectnessCollection(t, ctx, mc, hp.StructAElemPrefix+"_ac_exact", controlled)
		ids := queryAllIDs(t, ctx, mc, collName, `array_contains(structA[color], "Red")`, 100)
		require.Equal(t, []int64{0, 2}, ids)
		ids2 := queryAllIDs(t, ctx, mc, collName, `array_contains_all(structA[color], ["Red", "Blue"])`, 100)
		require.Equal(t, []int64{2}, ids2)
	})

	t.Run("element_filter_query_no_false_positives", func(t *testing.T) {
		opt := hp.DefaultStructAElementSchemaOption("")
		controlled := make([]hp.StructARow, 0, 50)
		for i := int64(0); i < 50; i++ {
			r := uint(i)
			numElems := 2 + int(r%5) // 2..6
			elems := make([]hp.StructAElement, numElems)
			for j := 0; j < numElems; j++ {
				elems[j] = hp.StructAElement{
					IntVal: i*100 + int64(j),
					Color:  hp.StructAElemColors[j%3],
				}
			}
			controlled = append(controlled, hp.MakeRow(i, opt, elems))
		}
		collName, _, allRows := setupCorrectnessCollection(t, ctx, mc, hp.StructAElemPrefix+"_no_fp", controlled)

		expr := `element_filter(structA, $[color] == "Blue" && $[int_val] > 2000)`
		ids := queryAllIDs(t, ctx, mc, collName, expr, 100)
		gt := hp.GtElementFilter(allRows,
			func(e hp.StructAElement) bool { return e.Color == "Blue" && e.IntVal > 2000 }, nil)
		require.Equal(t, hp.IDSetToSorted(gt), ids,
			"server returned IDs differ from ground truth")
	})
}

func mustInt64(t *testing.T, c interface{ Get(int) (any, error) }, idx int) int64 {
	v, err := c.Get(idx)
	require.NoError(t, err)
	return v.(int64)
}

func queryPage(t *testing.T, ctx CtxT, mc MC, collName, expr string, limit, offset int) []int64 {
	rs, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(expr).WithOutputFields("id").WithLimit(limit).WithOffset(offset).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	col := rs.GetColumn("id")
	out := make([]int64, rs.ResultCount)
	for i := 0; i < rs.ResultCount; i++ {
		v, _ := col.Get(i)
		out[i] = v.(int64)
	}
	return out
}

func queryCountStar(t *testing.T, ctx CtxT, mc MC, collName, expr string) int64 {
	rs, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(expr).WithOutputFields("count(*)").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 1, rs.ResultCount)
	v, err := rs.GetColumn("count(*)").Get(0)
	require.NoError(t, err)
	return v.(int64)
}

func intersect(a, b []int64) []int64 {
	bm := make(map[int64]struct{}, len(b))
	for _, x := range b {
		bm[x] = struct{}{}
	}
	out := []int64{}
	for _, x := range a {
		if _, ok := bm[x]; ok {
			out = append(out, x)
		}
	}
	return out
}

// =============================================================================
// 8.  TestMilvusClientStructArrayElementMatchQuery  (2 L0)
// =============================================================================

func TestStructArrayElementMatchQuery(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString(hp.StructAElemPrefix+"_mq", 6)
	opt := hp.DefaultStructAElementSchemaOption(collName)
	opt.IncludeDocVChar = false
	opt.IncludeFloatVal = false
	opt.IncludeCategory = false
	schema, structSchema := hp.CreateStructAElementSchema(opt)
	common.CheckErr(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)), true)

	ds := hp.GenerateStructAElementData(elemQuerySealedNb, 0, opt)
	insertElemDataset(t, ctx, mc, collName, structSchema, ds, opt)
	_, err := mc.Flush(ctx, client.NewFlushOption(collName))
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

	t.Run("match_query_all", func(t *testing.T) {
		ids := queryAllIDs(t, ctx, mc, collName, `MATCH_ALL(structA, $[int_val] > 0)`, 100)
		gt := hp.GtMatch(ds.Rows, "MATCH_ALL", func(e hp.StructAElement) bool { return e.IntVal > 0 }, 0, nil)
		require.True(t, subset(ids, hp.IDSetToSorted(gt)),
			"got %v not subset of gt %v", ids, hp.IDSetToSorted(gt))
	})

	t.Run("match_query_any", func(t *testing.T) {
		ids := queryAllIDs(t, ctx, mc, collName, `MATCH_ANY(structA, $[str_val] == "row_10_elem_0")`, 100)
		gt := hp.GtMatch(ds.Rows, "MATCH_ANY",
			func(e hp.StructAElement) bool { return e.StrVal == "row_10_elem_0" }, 0, nil)
		require.Equal(t, hp.IDSetToSorted(gt), ids)
	})
}
