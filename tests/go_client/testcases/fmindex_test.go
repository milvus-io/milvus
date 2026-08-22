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

// L0 coverage for the FMINDEX scalar index through the public Go SDK.
package testcases

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const (
	fmIndexContentField = "content"
	fmIndexNoIndexField = "content_no_index"
	fmIndexVectorField  = "vector"
	fmIndexVectorDim    = 8
	fmIndexKeywordCount = 8
	fmIndexTotalRows    = 1600
)

var fmIndexKeywords = []string{"stadium", "park", "school", "library", "hospital", "restaurant", "office", "store"}

func fmIndexSchema(collectionName string) *entity.Schema {
	return entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(fmIndexNoIndexField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fmIndexContentField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fmIndexVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(fmIndexVectorDim))
}

func createFMIndexCollection(t *testing.T, ctx CtxT, mc MC, collectionName string, nullable bool) {
	t.Helper()

	schema := fmIndexSchema(collectionName)
	if nullable {
		schema = entity.NewSchema().WithName(collectionName).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
			WithField(entity.NewField().WithName(fmIndexContentField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64).WithNullable(true)).
			WithField(entity.NewField().WithName(fmIndexVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(fmIndexVectorDim))
	}

	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})
}

func insertFMIndexRows(t *testing.T, ctx CtxT, mc MC, collectionName string, start, count int, nullable bool) int {
	t.Helper()

	ids := make([]int64, count)
	values := make([]string, 0, count)
	valid := make([]bool, count)
	vectors := make([][]float32, count)
	nonNull := 0
	for i := 0; i < count; i++ {
		ids[i] = int64(start + i)
		v := make([]float32, fmIndexVectorDim)
		v[0] = float32(start + i)
		vectors[i] = v
		if nullable && i%8 == 7 {
			valid[i] = false
			continue
		}
		valid[i] = true
		values = append(values, fmIndexKeywords[i%fmIndexKeywordCount])
		nonNull++
	}

	opt := client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, vectors)
	if nullable {
		col, err := column.NewNullableColumnVarChar(fmIndexContentField, values, valid)
		require.NoError(t, err)
		opt.WithColumns(col)
	} else {
		opt.WithVarcharColumn(fmIndexContentField, values)
	}

	result, err := mc.Insert(ctx, opt)
	require.NoError(t, err)
	require.EqualValues(t, count, result.InsertCount)
	return nonNull
}

func flushFMIndexRows(t *testing.T, ctx CtxT, mc MC, collectionName string) {
	t.Helper()
	flushTask, err := mc.Flush(ctx, client.NewFlushOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, flushTask.Await(ctx))
}

func indexAndLoadFMIndex(t *testing.T, ctx CtxT, mc MC, collectionName string) {
	t.Helper()

	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, fmIndexContentField,
		index.NewFMIndex().WithIndexName("fm_content").WithSaSampleRate(32)))
	require.NoError(t, err)
	require.NoError(t, idxTask.Await(ctx))

	vecTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, fmIndexVectorField,
		index.NewFlatIndex(entity.L2)))
	require.NoError(t, err)
	require.NoError(t, vecTask.Await(ctx))

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))
}

// createFMIndex issues CreateIndex and waits for the build to reach a terminal
// state. Build-param validation may be reported either synchronously (in the
// CreateIndex RPC) or asynchronously (as an IndexState_Failed during the build),
// so the error is surfaced from whichever path reports it. The returned task is
// nil on a synchronous error, hence Await is only called when CreateIndex succeeds.
func createFMIndex(ctx CtxT, mc MC, collectionName, field string, idx index.Index) error {
	task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, field, idx))
	if err != nil {
		return err
	}
	return task.Await(ctx)
}

func queryFMIndexIDs(t *testing.T, ctx CtxT, mc MC, collectionName, expr string) []int64 {
	t.Helper()

	rs, err := mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(expr).WithOutputFields("id").
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err, "query %q", expr)

	col, ok := rs.GetColumn("id").(*column.ColumnInt64)
	require.True(t, ok)
	out := make([]int64, 0, col.Len())
	for i := 0; i < col.Len(); i++ {
		v, err := col.GetAsInt64(i)
		require.NoError(t, err)
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestFMIndexAnchoredLike verifies exact prefix / infix / suffix LIKE on the
// FMINDEX field matches the brute-force scan on the un-indexed twin field, and
// that exact equality (not accelerated by FMINDEX) stays correct.
func TestFMIndexAnchoredLike(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_like", 6)

	createFMIndexCollection(t, ctx, mc, collectionName, false)

	// Both fields carry the same keyword per row; FMINDEX accelerates `content`,
	// `content_no_index` forces the raw-data scan.
	ids := make([]int64, fmIndexTotalRows)
	values := make([]string, fmIndexTotalRows)
	vectors := make([][]float32, fmIndexTotalRows)
	for i := 0; i < fmIndexTotalRows; i++ {
		ids[i] = int64(i)
		values[i] = fmIndexKeywords[i%fmIndexKeywordCount]
		v := make([]float32, fmIndexVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithVarcharColumn(fmIndexNoIndexField, values).
		WithVarcharColumn(fmIndexContentField, values).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, vectors))
	require.NoError(t, err)

	flushFMIndexRows(t, ctx, mc, collectionName)
	indexAndLoadFMIndex(t, ctx, mc, collectionName)

	expected := fmIndexTotalRows / fmIndexKeywordCount
	assertSame := func(idxExpr, scanExpr string) {
		t.Helper()
		idxIDs := queryFMIndexIDs(t, ctx, mc, collectionName, idxExpr)
		scanIDs := queryFMIndexIDs(t, ctx, mc, collectionName, scanExpr)
		require.Equal(t, scanIDs, idxIDs, "FMINDEX vs scan mismatch: %s", idxExpr)
	}

	// prefix
	assertSame(
		fmt.Sprintf(`%s like "sta%%"`, fmIndexContentField),
		fmt.Sprintf(`%s like "sta%%"`, fmIndexNoIndexField))
	require.Len(t, queryFMIndexIDs(t, ctx, mc, collectionName,
		fmt.Sprintf(`%s like "sta%%"`, fmIndexContentField)), expected)
	// suffix
	assertSame(
		fmt.Sprintf(`%s like "%%ium"`, fmIndexContentField),
		fmt.Sprintf(`%s like "%%ium"`, fmIndexNoIndexField))
	// infix
	assertSame(
		fmt.Sprintf(`%s like "%%adi%%"`, fmIndexContentField),
		fmt.Sprintf(`%s like "%%adi%%"`, fmIndexNoIndexField))
	// no match
	assertSame(
		fmt.Sprintf(`%s like "zzz%%"`, fmIndexContentField),
		fmt.Sprintf(`%s like "zzz%%"`, fmIndexNoIndexField))
	// equality falls back to raw scan but must stay correct
	assertSame(
		fmt.Sprintf(`%s == "park"`, fmIndexContentField),
		fmt.Sprintf(`%s == "park"`, fmIndexNoIndexField))
}

// TestFMIndexBuildParamErrors verifies invalid build params are rejected by the
// server with a range/format error rather than silently applying a default.
func TestFMIndexBuildParamErrors(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_param", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(fmIndexContentField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fmIndexVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(fmIndexVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	ids := make([]int64, 16)
	content := make([]string, 16)
	vectors := make([][]float32, 16)
	for i := 0; i < 16; i++ {
		ids[i] = int64(i)
		content[i] = fmIndexKeywords[i%fmIndexKeywordCount]
		v := make([]float32, fmIndexVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithVarcharColumn(fmIndexContentField, content).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, vectors))
	require.NoError(t, err)
	flushFMIndexRows(t, ctx, mc, collectionName)

	// fm_sa_sample_rate out of [4, 256]
	err = createFMIndex(ctx, mc, collectionName, fmIndexContentField,
		index.NewFMIndex().WithIndexName("fm_bad_rate").WithSaSampleRate(257))
	common.CheckErr(t, err, false, "fm_sa_sample_rate for FM-index must be in [4, 256]")

	// fm_block_bytes not a power of two in [8, 128]
	err = createFMIndex(ctx, mc, collectionName, fmIndexContentField,
		index.NewFMIndex().WithIndexName("fm_bad_block").WithBlockBytes(24))
	common.CheckErr(t, err, false, "fm_block_bytes for FM-index must be a power of two in [8, 128]")
}

// TestFMIndexNonVarcharRejected verifies building FMINDEX on a non-VARCHAR field
// is rejected (FMINDEX is VARCHAR-only in this release).
func TestFMIndexNonVarcharRejected(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_nonvarchar", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("int_field").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(fmIndexVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(fmIndexVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, "int_field",
		index.NewFMIndex().WithIndexName("fm_bad_field")))
	common.CheckErr(t, err, false, "FM-index can only be created on VARCHAR field")
}

// TestFMIndexStructSubFieldRejected verifies building FMINDEX on a struct-array
// sub-field (e.g. structA[str_val]) is rejected: the sub-field is an ARRAY type
// regardless of the element type, and FMINDEX is VARCHAR-only in this release.
func TestFMIndexStructSubFieldRejected(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_struct_sub", 6)

	structSchema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("str_val").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName("int_val").WithDataType(entity.FieldTypeInt64))
	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(fmIndexVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(fmIndexVectorDim)).
		WithField(entity.NewField().WithName("structA").WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).WithMaxCapacity(10).WithStructSchema(structSchema))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	// Even a VARCHAR-typed struct sub-field is an ARRAY field to the checker.
	_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, "structA[str_val]",
		index.NewFMIndex().WithIndexName("fm_struct_str")))
	common.CheckErr(t, err, false, "FM-index can only be created on VARCHAR field")

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, "structA[int_val]",
		index.NewFMIndex().WithIndexName("fm_struct_int")))
	common.CheckErr(t, err, false, "FM-index can only be created on VARCHAR field")
}

// TestFMIndexNullRowsNotMatched verifies nullable VARCHAR NULL rows are treated
// as an empty document: no pattern, not even LIKE '%', may match them.
func TestFMIndexNullRowsNotMatched(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_null", 6)

	createFMIndexCollection(t, ctx, mc, collectionName, true)
	nonNull := insertFMIndexRows(t, ctx, mc, collectionName, 0, fmIndexTotalRows, true)
	flushFMIndexRows(t, ctx, mc, collectionName)
	indexAndLoadFMIndex(t, ctx, mc, collectionName)

	all := queryFMIndexIDs(t, ctx, mc, collectionName, fmt.Sprintf(`%s like "%%"`, fmIndexContentField))
	require.Len(t, all, nonNull, "LIKE '%%' must return exactly the non-NULL rows")

	sta := queryFMIndexIDs(t, ctx, mc, collectionName, fmt.Sprintf(`%s like "sta%%"`, fmIndexContentField))
	require.NotEmpty(t, sta)
	for _, id := range sta {
		require.NotEqualf(t, int64(7), id%8, "LIKE 'sta%%' must never match a NULL row id=%d", id)
	}
}

// TestFMIndexAcceleratedPathLongTextLowHit exercises FMINDEX's own execution
// path (not the brute-force scan). The short-keyword corpus in
// TestFMIndexAnchoredLike makes every keyword hit ~1/8 of the rows, so
// FMINDEX's count-first cost guard declines those patterns and they fall back
// to the scan. Here the corpus is long text (~500 bytes/row) with a rare marker
// in only a handful of rows: total tokens are large and the marker's occurrence
// count is tiny, so the guard ACCEPTS the pattern and the query is actually
// answered by FMINDEX. The result must still equal the brute-force scan on the
// twin field and be exactly the marked rows.
func TestFMIndexAcceleratedPathLongTextLowHit(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_accel", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(fmIndexNoIndexField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(600)).
		WithField(entity.NewField().WithName(fmIndexContentField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(600)).
		WithField(entity.NewField().WithName(fmIndexVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(fmIndexVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	const (
		totalRows  = 2000
		filler     = "y"
		marker     = "ZEBRA"
		fillerLen  = 500
		markerStep = 500
	)
	ids := make([]int64, totalRows)
	content := make([]string, totalRows)
	noIndex := make([]string, totalRows)
	vectors := make([][]float32, totalRows)
	marked := map[int64]bool{}
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		v := make([]float32, fmIndexVectorDim)
		v[0] = float32(i)
		vectors[i] = v

		text := ""
		for j := 0; j < fillerLen; j++ {
			text += filler
		}
		if i%markerStep == 0 {
			text += marker
			marked[ids[i]] = true
		}
		content[i] = text
		noIndex[i] = text
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithVarcharColumn(fmIndexNoIndexField, noIndex).
		WithVarcharColumn(fmIndexContentField, content).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, vectors))
	require.NoError(t, err)

	flushFMIndexRows(t, ctx, mc, collectionName)
	indexAndLoadFMIndex(t, ctx, mc, collectionName)

	expr := fmt.Sprintf(`%s like "%%%s%%"`, fmIndexContentField, marker)
	idxIDs := queryFMIndexIDs(t, ctx, mc, collectionName, expr)
	scanIDs := queryFMIndexIDs(t, ctx, mc, collectionName,
		fmt.Sprintf(`%s like "%%%s%%"`, fmIndexNoIndexField, marker))

	require.Equal(t, scanIDs, idxIDs, "FMINDEX accelerated path vs scan mismatch")
	require.Len(t, idxIDs, len(marked))
	for _, id := range idxIDs {
		require.Truef(t, marked[id], "FMINDEX returned unexpected id=%d", id)
	}
}

// TestFMIndexNonASCII verifies byte-exact substring matching over multi-byte
// UTF-8 content (CJK / emoji), where a byte-level index must not mis-align.
func TestFMIndexNonASCII(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_utf8", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(fmIndexNoIndexField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fmIndexContentField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fmIndexVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(fmIndexVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	values := []string{"中文测试数据", "测试中文", "emoji😀测试", "纯英文english", "中文😀中文"}
	// Keep the row count above the segment index threshold (1024) so FMINDEX is really built.
	totalRows := len(values) * 300
	ids := make([]int64, totalRows)
	content := make([]string, totalRows)
	noIndex := make([]string, totalRows)
	vectors := make([][]float32, totalRows)
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		content[i] = values[i%len(values)]
		noIndex[i] = values[i%len(values)]
		v := make([]float32, fmIndexVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithVarcharColumn(fmIndexNoIndexField, noIndex).
		WithVarcharColumn(fmIndexContentField, content).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, vectors))
	require.NoError(t, err)
	flushFMIndexRows(t, ctx, mc, collectionName)
	indexAndLoadFMIndex(t, ctx, mc, collectionName)

	assertSame := func(pattern string) {
		t.Helper()
		idxIDs := queryFMIndexIDs(t, ctx, mc, collectionName, fmt.Sprintf(`%s like "%s"`, fmIndexContentField, pattern))
		scanIDs := queryFMIndexIDs(t, ctx, mc, collectionName, fmt.Sprintf(`%s like "%s"`, fmIndexNoIndexField, pattern))
		require.Equal(t, scanIDs, idxIDs, "FMINDEX vs scan mismatch for %q", pattern)
	}
	// CJK infix, emoji infix, mixed CJK+emoji infix
	assertSame("%测试%")
	assertSame("%😀%")
	assertSame("%中文😀%")
}

// TestFMIndexGrowingAndSealedMixed verifies a LIKE query returns both sealed
// (FMINDEX-served) and growing (brute-force scan) rows in one query.
func TestFMIndexGrowingAndSealedMixed(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_growing", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(fmIndexNoIndexField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fmIndexContentField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fmIndexVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(fmIndexVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	// Sealed batch: keyword "stadium".
	sealedN := fmIndexTotalRows
	ids := make([]int64, sealedN)
	content := make([]string, sealedN)
	noIndex := make([]string, sealedN)
	vectors := make([][]float32, sealedN)
	for i := 0; i < sealedN; i++ {
		ids[i] = int64(i)
		content[i] = fmIndexKeywords[i%fmIndexKeywordCount]
		noIndex[i] = fmIndexKeywords[i%fmIndexKeywordCount]
		v := make([]float32, fmIndexVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithVarcharColumn(fmIndexNoIndexField, noIndex).
		WithVarcharColumn(fmIndexContentField, content).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, vectors))
	require.NoError(t, err)
	flushFMIndexRows(t, ctx, mc, collectionName)
	indexAndLoadFMIndex(t, ctx, mc, collectionName)

	// Growing batch (NOT flushed): disjoint keyword "growing" only.
	growingN := 200
	gids := make([]int64, growingN)
	gcontent := make([]string, growingN)
	gnoIndex := make([]string, growingN)
	gvectors := make([][]float32, growingN)
	for i := 0; i < growingN; i++ {
		gids[i] = int64(sealedN + i)
		gcontent[i] = "growing"
		gnoIndex[i] = "growing"
		v := make([]float32, fmIndexVectorDim)
		v[0] = float32(sealedN + i)
		gvectors[i] = v
	}
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", gids).
		WithVarcharColumn(fmIndexNoIndexField, gnoIndex).
		WithVarcharColumn(fmIndexContentField, gcontent).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, gvectors))
	require.NoError(t, err)

	// Pattern "growing" hits only the growing batch; a returned id >= sealedN can
	// only come from a growing segment.
	got := queryFMIndexIDs(t, ctx, mc, collectionName,
		fmt.Sprintf(`%s like "growing%%"`, fmIndexContentField))
	require.Len(t, got, growingN, "LIKE must see the un-flushed growing rows")

	// Pattern "sta%" hits only the sealed batch.
	sealed := queryFMIndexIDs(t, ctx, mc, collectionName,
		fmt.Sprintf(`%s like "sta%%"`, fmIndexContentField))
	require.NotEmpty(t, sealed)
	for _, id := range sealed {
		require.Less(t, id, int64(sealedN), "sealed LIKE must not return growing rows")
	}
}

// TestFMIndexEmptyPatternAndGeneralFallback verifies `LIKE '%'` (empty pattern,
// == IsNotNull) and the general-LIKE / regex fallback paths stay correct.
func TestFMIndexEmptyPatternAndGeneralFallback(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_fallback", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(fmIndexNoIndexField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fmIndexContentField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fmIndexVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(fmIndexVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	// Keep the row count above the segment index threshold (1024) so FMINDEX is really built.
	totalRows := 2000
	ids := make([]int64, totalRows)
	content := make([]string, totalRows)
	noIndex := make([]string, totalRows)
	vectors := make([][]float32, totalRows)
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		// "req-<n>-error-<n>" shape so general LIKE and regex have something to match.
		content[i] = fmt.Sprintf("req-%d-error-%d", i%4, i%10)
		noIndex[i] = content[i]
		v := make([]float32, fmIndexVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithVarcharColumn(fmIndexNoIndexField, noIndex).
		WithVarcharColumn(fmIndexContentField, content).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, vectors))
	require.NoError(t, err)
	flushFMIndexRows(t, ctx, mc, collectionName)
	indexAndLoadFMIndex(t, ctx, mc, collectionName)

	assertSame := func(idxExpr, scanExpr string) {
		t.Helper()
		idxIDs := queryFMIndexIDs(t, ctx, mc, collectionName, idxExpr)
		scanIDs := queryFMIndexIDs(t, ctx, mc, collectionName, scanExpr)
		require.Equal(t, scanIDs, idxIDs, "mismatch: %s", idxExpr)
	}

	// Empty pattern returns every non-NULL row (== IsNotNull).
	all := queryFMIndexIDs(t, ctx, mc, collectionName, fmt.Sprintf(`%s like "%%"`, fmIndexContentField))
	require.Len(t, all, totalRows)

	// General LIKE with an interior wildcard (falls back to scan, still exact).
	assertSame(
		fmt.Sprintf(`%s like "req-%%error%%"`, fmIndexContentField),
		fmt.Sprintf(`%s like "req-%%error%%"`, fmIndexNoIndexField))

	// LIKE with a single-char `_` wildcard (also general, falls back, still exact).
	assertSame(
		fmt.Sprintf(`%s like "req-_error-_"`, fmIndexContentField),
		fmt.Sprintf(`%s like "req-_error-_"`, fmIndexNoIndexField))

	// Regex via `=~` (RegexMatch is not accelerated by FMINDEX, falls back to the
	// RE2 scan, still exact).
	assertSame(
		fmt.Sprintf(`%s =~ "req-.error-."`, fmIndexContentField),
		fmt.Sprintf(`%s =~ "req-.error-."`, fmIndexNoIndexField))
}

// TestFMIndexBuildParamBoundaries verifies boundary build params: 4 and 256 are
// valid for fm_sa_sample_rate, 8 and 128 valid for fm_block_bytes; 3 and 4 are
// rejected respectively.
func TestFMIndexBuildParamBoundaries(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	newCollection := func(suffix string) string {
		collectionName := common.GenRandomString("fmindex_bound_"+suffix, 6)
		schema := entity.NewSchema().WithName(collectionName).
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
			WithField(entity.NewField().WithName(fmIndexContentField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
			WithField(entity.NewField().WithName(fmIndexVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(fmIndexVectorDim))
		require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema)))
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
		})
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
			WithInt64Column("id", []int64{1}).
			WithVarcharColumn(fmIndexContentField, []string{"abc"}).
			WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}))
		require.NoError(t, err)
		// Flush so the index build reaches a terminal state (rather than staying
		// InProgress on an empty growing segment) and createFMIndex can Await it.
		flushFMIndexRows(t, ctx, mc, collectionName)
		return collectionName
	}

	// Valid boundaries for fm_sa_sample_rate.
	for _, rate := range []int{4, 256} {
		coll := newCollection(fmt.Sprintf("rate%d", rate))
		err := createFMIndex(ctx, mc, coll, fmIndexContentField,
			index.NewFMIndex().WithIndexName("fm_rate").WithSaSampleRate(rate))
		common.CheckErr(t, err, true)
	}
	// 3 is below the valid range.
	coll := newCollection("rate3")
	err := createFMIndex(ctx, mc, coll, fmIndexContentField,
		index.NewFMIndex().WithIndexName("fm_rate_bad").WithSaSampleRate(3))
	common.CheckErr(t, err, false, "fm_sa_sample_rate for FM-index must be in [4, 256]")

	// Valid boundaries for fm_block_bytes.
	for _, bb := range []int{8, 128} {
		coll := newCollection(fmt.Sprintf("bb%d", bb))
		err := createFMIndex(ctx, mc, coll, fmIndexContentField,
			index.NewFMIndex().WithIndexName("fm_bb").WithBlockBytes(bb))
		common.CheckErr(t, err, true)
	}
	// 4 is below the valid range.
	coll = newCollection("bb4")
	err = createFMIndex(ctx, mc, coll, fmIndexContentField,
		index.NewFMIndex().WithIndexName("fm_bb_bad").WithBlockBytes(4))
	common.CheckErr(t, err, false, "fm_block_bytes for FM-index must be a power of two in [8, 128]")
}

// TestFMIndexEscapedWildcards verifies LIKE escape handling: `\%` and `\_` match
// the literal characters, `\\` matches a backslash, and a dangling trailing `\`
// is rejected. Escaped literals inside the pattern keep the anchored lowering
// (a pattern with only leading/trailing `%` plus escaped bytes is still a
// prefix/infix/suffix), so this exercises the escape model against FMINDEX and
// the scan, which must agree.
func TestFMIndexEscapedWildcards(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_escape", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(fmIndexNoIndexField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fmIndexContentField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fmIndexVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(fmIndexVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	// Rows carrying the literal special characters, cycled.
	values := []string{"100%done", "under_score", "back\\slash", "plain", "50%_mixed"}
	// Keep the row count above the segment index threshold (1024) so FMINDEX is really built.
	totalRows := len(values) * 300
	ids := make([]int64, totalRows)
	content := make([]string, totalRows)
	noIndex := make([]string, totalRows)
	vectors := make([][]float32, totalRows)
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		content[i] = values[i%len(values)]
		noIndex[i] = values[i%len(values)]
		v := make([]float32, fmIndexVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithVarcharColumn(fmIndexNoIndexField, noIndex).
		WithVarcharColumn(fmIndexContentField, content).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, vectors))
	require.NoError(t, err)
	flushFMIndexRows(t, ctx, mc, collectionName)
	indexAndLoadFMIndex(t, ctx, mc, collectionName)

	assertSame := func(idxExpr, scanExpr string) {
		t.Helper()
		idxIDs := queryFMIndexIDs(t, ctx, mc, collectionName, idxExpr)
		scanIDs := queryFMIndexIDs(t, ctx, mc, collectionName, scanExpr)
		require.Equal(t, scanIDs, idxIDs, "mismatch: %s", idxExpr)
	}
	// LIKE escapes must be carried in a raw string literal (r"..."), because a
	// normal "..." literal is processed by strconv.Unquote first, which rejects
	// `\%`/`\_`. In r"..." the backslash reaches the LIKE layer verbatim, where
	// `\%` matches a literal '%', `\_` a literal '_', `\\` a literal backslash.
	assertSame(
		fmt.Sprintf(`%s like r"%%\%%%%"`, fmIndexContentField),
		fmt.Sprintf(`%s like r"%%\%%%%"`, fmIndexNoIndexField))
	assertSame(
		fmt.Sprintf(`%s like r"%%\_%%"`, fmIndexContentField),
		fmt.Sprintf(`%s like r"%%\_%%"`, fmIndexNoIndexField))
	assertSame(
		fmt.Sprintf(`%s like r"%%\\%%"`, fmIndexContentField),
		fmt.Sprintf(`%s like r"%%\\%%"`, fmIndexNoIndexField))

	// A dangling trailing backslash inside a raw string is an invalid LIKE
	// pattern and must be rejected.
	_, err = mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf(`%s like r"abc\"`, fmIndexContentField)).WithOutputFields("id").
		WithConsistencyLevel(entity.ClStrong))
	require.Error(t, err, "a dangling trailing backslash must be rejected")
}
