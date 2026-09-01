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
	"strings"
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
	fmIndexFillerLen    = 500
	fmIndexMarkerStep   = 500
	fmIndexSASampleRate = 32
)

var fmIndexKeywords = []string{"stadium", "park", "school", "library", "hospital", "restaurant", "office", "store"}

func fmIndexSchema(collectionName string) *entity.Schema {
	return entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(fmIndexNoIndexField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(600)).
		WithField(entity.NewField().WithName(fmIndexContentField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(600)).
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
		opt.WithVarcharColumn(fmIndexNoIndexField, values)
	}

	result, err := mc.Insert(ctx, opt)
	require.NoError(t, err)
	require.EqualValues(t, count, result.InsertCount)
	return nonNull
}

func flushFMIndexRows(t *testing.T, ctx CtxT, mc MC, collectionName string) {
	t.Helper()
	// The shared standalone test instance limits Flush to 0.1 requests/sec.
	// Keep the helper deterministic when two lifecycle phases flush the same
	// collection within one token-bucket interval.
	const maxAttempts = 5
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		flushTask, err := mc.Flush(ctx, client.NewFlushOption(collectionName))
		if err == nil {
			if err = flushTask.Await(ctx); err == nil {
				return
			}
		}
		lastErr = err
		if attempt+1 < maxAttempts {
			t.Logf("Flush for %s failed on attempt %d/%d, retrying: %v", collectionName, attempt+1, maxAttempts, err)
			time.Sleep(5 * time.Second)
		}
	}
	require.NoError(t, lastErr)
}

func indexAndLoadFMIndex(t *testing.T, ctx CtxT, mc MC, collectionName string, expectedRows int64) {
	t.Helper()

	require.NoError(t, createFMIndex(ctx, mc, collectionName, fmIndexContentField,
		index.NewFMIndex().WithIndexName("fm_content").WithSaSampleRate(fmIndexSASampleRate)))
	requireFMIndexReady(t, ctx, mc, collectionName, "fm_content", expectedRows)

	vecTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, fmIndexVectorField,
		index.NewFlatIndex(entity.L2)))
	require.NoError(t, err)
	require.NoError(t, vecTask.Await(ctx))

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))
}

func requireFMIndexCostGuard(t *testing.T, values []string, occurrences int, context string) {
	t.Helper()
	totalTokens := 0
	for _, value := range values {
		totalTokens += len(value)
	}
	require.Lessf(t, occurrences*fmIndexSASampleRate, totalTokens/1000,
		"fixture must satisfy the default FMINDEX cost guard for %s: occurrences=%d totalTokens=%d",
		context, occurrences, totalTokens)
}

// createFMIndex issues CreateIndex and waits for the build to reach a terminal
// state. Build-param validation may be reported either synchronously (in the
// CreateIndex RPC) or asynchronously (as an IndexState_Failed during the build),
// so the error is surfaced from whichever path reports it. The returned task is
// nil on a synchronous error, hence Await is only called when CreateIndex succeeds.
func createFMIndex(ctx CtxT, mc MC, collectionName, field string, idx index.Index) error {
	opt := client.NewCreateIndexOption(collectionName, field, idx)
	if idx.Name() != "" {
		opt.WithIndexName(idx.Name())
	}
	task, err := mc.CreateIndex(ctx, opt)
	if err != nil {
		return err
	}
	return task.Await(ctx)
}

func requireFMIndexReady(
	t *testing.T,
	ctx CtxT,
	mc MC,
	collectionName string,
	indexName string,
	expectedRows int64,
) client.IndexDescription {
	t.Helper()

	desc, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(collectionName, indexName))
	require.NoError(t, err)
	require.Equal(t, indexName, desc.Name())
	require.Equal(t, index.FMINDEX, desc.IndexType())
	require.Zero(t, desc.PendingIndexRows)
	if expectedRows >= 0 {
		require.Equal(t, expectedRows, desc.TotalRows)
		require.Equal(t, expectedRows, desc.IndexedRows)
	} else {
		require.Positive(t, desc.IndexedRows)
		require.Equal(t, desc.TotalRows, desc.IndexedRows)
	}
	return desc
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

func searchFMIndexIDs(t *testing.T, ctx CtxT, mc MC, collectionName, expr, expectedContent string) []int64 {
	t.Helper()

	results, err := mc.Search(ctx, client.NewSearchOption(collectionName, fmIndexTotalRows,
		[]entity.Vector{entity.FloatVector(make([]float32, fmIndexVectorDim))}).
		WithANNSField(fmIndexVectorField).
		WithFilter(expr).
		WithOutputFields(fmIndexNoIndexField).
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err, "search %q", expr)
	require.Len(t, results, 1)
	require.NoError(t, results[0].Err)

	result := results[0]
	pkColumn, ok := result.IDs.(*column.ColumnInt64)
	require.True(t, ok, "search result primary key must be INT64")
	require.Equal(t, result.ResultCount, pkColumn.Len(), "Search result count and PK count differ")
	require.Len(t, result.Scores, result.ResultCount, "Search score count and result count differ")
	contentColumn, ok := result.GetColumn(fmIndexNoIndexField).(*column.ColumnVarChar)
	require.True(t, ok, "requested unindexed VARCHAR output field is missing")
	require.Equal(t, result.ResultCount, contentColumn.Len(), "output field and result counts differ")

	ids := make([]int64, 0, result.ResultCount)
	for i := 0; i < pkColumn.Len(); i++ {
		id, err := pkColumn.GetAsInt64(i)
		require.NoError(t, err)
		content, err := contentColumn.GetAsString(i)
		require.NoError(t, err)
		require.Equal(t, expectedContent, content,
			"projected content does not match PK %d", id)
		if i > 0 {
			require.LessOrEqual(t, result.Scores[i-1], result.Scores[i], "L2 Search scores must be nondecreasing")
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// TestFMIndexAnchoredLike verifies exact prefix / infix / suffix LIKE on the
// FMINDEX field matches the brute-force scan on the un-indexed twin field, and
// that exact equality (not accelerated by FMINDEX) stays correct.
func TestFMIndexAnchoredLike(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_like", 6)

	createFMIndexCollection(t, ctx, mc, collectionName, false)

	// Long non-matching rows dominate the token count while a sparse marker keeps
	// occurrence enumeration below the FMINDEX cost guard. The indexed field and
	// raw-scan twin still carry identical values.
	ids := make([]int64, fmIndexTotalRows)
	values := make([]string, fmIndexTotalRows)
	vectors := make([][]float32, fmIndexTotalRows)
	filler := strings.Repeat("y", fmIndexFillerLen)
	marked := make([]int64, 0, fmIndexTotalRows/fmIndexMarkerStep+1)
	for i := 0; i < fmIndexTotalRows; i++ {
		ids[i] = int64(i)
		values[i] = filler
		if i%fmIndexMarkerStep == 0 {
			values[i] = "stadium"
			marked = append(marked, int64(i))
		}
		v := make([]float32, fmIndexVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}
	requireFMIndexCostGuard(t, values, len(marked), "anchored LIKE")
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithVarcharColumn(fmIndexNoIndexField, values).
		WithVarcharColumn(fmIndexContentField, values).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, vectors))
	require.NoError(t, err)

	flushFMIndexRows(t, ctx, mc, collectionName)
	indexAndLoadFMIndex(t, ctx, mc, collectionName, fmIndexTotalRows)

	assertSame := func(idxExpr, scanExpr string) []int64 {
		t.Helper()
		idxIDs := queryFMIndexIDs(t, ctx, mc, collectionName, idxExpr)
		scanIDs := queryFMIndexIDs(t, ctx, mc, collectionName, scanExpr)
		require.Equal(t, scanIDs, idxIDs, "FMINDEX vs scan mismatch: %s", idxExpr)
		return idxIDs
	}

	// prefix
	prefixIDs := assertSame(
		fmt.Sprintf(`%s like "sta%%"`, fmIndexContentField),
		fmt.Sprintf(`%s like "sta%%"`, fmIndexNoIndexField))
	require.Equal(t, marked, prefixIDs)
	// Search and Query deliberately reuse this same loaded collection and index.
	// With topK covering the fixture and a FLAT vector index, the filter result
	// must be identical to the un-indexed twin-field query.
	searchIDs := searchFMIndexIDs(t, ctx, mc, collectionName,
		fmt.Sprintf(`%s like "sta%%"`, fmIndexContentField), "stadium")
	require.Equal(t, queryFMIndexIDs(t, ctx, mc, collectionName,
		fmt.Sprintf(`%s like "sta%%"`, fmIndexNoIndexField)), searchIDs,
		"FMINDEX Search filter result must match the raw-scan twin")
	require.Equal(t, marked, searchIDs, "anchored LIKE must return the sparse marker rows")
	// suffix
	suffixIDs := assertSame(
		fmt.Sprintf(`%s like "%%ium"`, fmIndexContentField),
		fmt.Sprintf(`%s like "%%ium"`, fmIndexNoIndexField))
	require.Equal(t, marked, suffixIDs)
	// infix
	infixIDs := assertSame(
		fmt.Sprintf(`%s like "%%adi%%"`, fmIndexContentField),
		fmt.Sprintf(`%s like "%%adi%%"`, fmIndexNoIndexField))
	require.Equal(t, marked, infixIDs)
	// no match
	noMatchIDs := assertSame(
		fmt.Sprintf(`%s like "zzz%%"`, fmIndexContentField),
		fmt.Sprintf(`%s like "zzz%%"`, fmIndexNoIndexField))
	require.Empty(t, noMatchIDs)
	// equality falls back to raw scan but must stay correct
	equalIDs := assertSame(
		fmt.Sprintf(`%s == "stadium"`, fmIndexContentField),
		fmt.Sprintf(`%s == "stadium"`, fmIndexNoIndexField))
	require.Equal(t, marked, equalIDs)
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
	indexAndLoadFMIndex(t, ctx, mc, collectionName, fmIndexTotalRows)

	all := queryFMIndexIDs(t, ctx, mc, collectionName, fmt.Sprintf(`%s like "%%"`, fmIndexContentField))
	expectedAll := make([]int64, 0, nonNull)
	expectedSta := make([]int64, 0, fmIndexTotalRows/fmIndexKeywordCount)
	for id := 0; id < fmIndexTotalRows; id++ {
		if id%8 != 7 {
			expectedAll = append(expectedAll, int64(id))
		}
		if id%8 == 0 {
			expectedSta = append(expectedSta, int64(id))
		}
	}
	require.Equal(t, expectedAll, all, "LIKE '%%' must return the exact non-NULL PK set")

	sta := queryFMIndexIDs(t, ctx, mc, collectionName, fmt.Sprintf(`%s like "sta%%"`, fmIndexContentField))
	require.Equal(t, expectedSta, sta, "LIKE 'sta%%' must return the exact non-NULL stadium PK set")
}

// TestFMIndexLowHitDifferentialCorrectness verifies exact results for a
// long-text, low-hit fixture where the FMINDEX count-first guard is eligible.
// The public Go test API has no stable execution-path metric, so this test
// intentionally proves differential correctness only, not physical index use.
func TestFMIndexLowHitDifferentialCorrectness(t *testing.T) {
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

		text := strings.Repeat(filler, fillerLen)
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
	indexAndLoadFMIndex(t, ctx, mc, collectionName, totalRows)

	expr := fmt.Sprintf(`%s like "%%%s%%"`, fmIndexContentField, marker)
	idxIDs := queryFMIndexIDs(t, ctx, mc, collectionName, expr)
	scanIDs := queryFMIndexIDs(t, ctx, mc, collectionName,
		fmt.Sprintf(`%s like "%%%s%%"`, fmIndexNoIndexField, marker))

	require.Equal(t, scanIDs, idxIDs, "low-hit indexed-field query vs scan mismatch")
	require.Len(t, idxIDs, len(marked))
	for _, id := range idxIDs {
		require.Truef(t, marked[id], "FMINDEX returned unexpected id=%d", id)
	}
	totalTokens := totalRows*fillerLen + len(marked)*len(marker)
	markerOccurrences := len(marked)
	require.Lessf(t, markerOccurrences*fmIndexSASampleRate, totalTokens/1000,
		"fixture must satisfy the FMINDEX count-first guard: occurrences=%d totalTokens=%d", markerOccurrences, totalTokens)
}

// TestFMIndexNonASCII verifies byte-exact substring matching over multi-byte
// UTF-8 content (CJK / emoji), where a byte-level index must not mis-align.
func TestFMIndexNonASCII(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_utf8", 6)

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

	values := []string{"中文测试数据", "测试中文", "emoji😀测试", "纯英文english", "中文😀中文"}
	// Keep the row count above the segment index threshold (1024) so FMINDEX is really built.
	totalRows := 2000
	ids := make([]int64, totalRows)
	content := make([]string, totalRows)
	noIndex := make([]string, totalRows)
	vectors := make([][]float32, totalRows)
	filler := strings.Repeat("y", fmIndexFillerLen)
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		text := filler
		if valueIndex := i % fmIndexMarkerStep; valueIndex < len(values) {
			text = values[valueIndex]
		}
		content[i] = text
		noIndex[i] = text
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
	indexAndLoadFMIndex(t, ctx, mc, collectionName, int64(totalRows))

	assertSame := func(pattern string, matchingValueIndexes map[int]struct{}) {
		t.Helper()
		idxIDs := queryFMIndexIDs(t, ctx, mc, collectionName, fmt.Sprintf(`%s like "%s"`, fmIndexContentField, pattern))
		scanIDs := queryFMIndexIDs(t, ctx, mc, collectionName, fmt.Sprintf(`%s like "%s"`, fmIndexNoIndexField, pattern))
		require.Equal(t, scanIDs, idxIDs, "FMINDEX vs scan mismatch for %q", pattern)
		expected := make([]int64, 0, totalRows)
		for id := 0; id < totalRows; id++ {
			if _, ok := matchingValueIndexes[id%fmIndexMarkerStep]; ok {
				expected = append(expected, int64(id))
			}
		}
		requireFMIndexCostGuard(t, content, len(expected), "UTF-8 pattern "+pattern)
		require.Equal(t, expected, idxIDs, "unexpected UTF-8 LIKE ground truth for %q", pattern)
	}
	// CJK infix, emoji infix, mixed CJK+emoji infix
	assertSame("%测试%", map[int]struct{}{0: {}, 1: {}, 2: {}})
	assertSame("%😀%", map[int]struct{}{2: {}, 4: {}})
	assertSame("%中文😀%", map[int]struct{}{4: {}})
}

// TestFMIndexGrowingAndSealedMixed verifies a LIKE query returns both sealed
// (FMINDEX-served) and growing (brute-force scan) rows in one query.
func TestFMIndexGrowingAndSealedMixed(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_growing", 6)

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

	// Sealed batch: sparse "stadium" rows in a long filler corpus. The four
	// occurrences satisfy the cost guard and are served by the sealed FMINDEX.
	sealedN := fmIndexTotalRows
	ids := make([]int64, sealedN)
	content := make([]string, sealedN)
	noIndex := make([]string, sealedN)
	vectors := make([][]float32, sealedN)
	filler := strings.Repeat("y", fmIndexFillerLen)
	expected := make([]int64, 0, sealedN/fmIndexMarkerStep+2)
	for i := 0; i < sealedN; i++ {
		ids[i] = int64(i)
		text := filler
		if i%fmIndexMarkerStep == 0 {
			text = "stadium"
			expected = append(expected, int64(i))
		}
		content[i] = text
		noIndex[i] = text
		v := make([]float32, fmIndexVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}
	requireFMIndexCostGuard(t, content, len(expected), "sealed/growing anchored LIKE")
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithVarcharColumn(fmIndexNoIndexField, noIndex).
		WithVarcharColumn(fmIndexContentField, content).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, vectors))
	require.NoError(t, err)
	flushFMIndexRows(t, ctx, mc, collectionName)
	indexAndLoadFMIndex(t, ctx, mc, collectionName, int64(sealedN))

	// Growing batch (NOT flushed): include one sealed keyword "stadium" so one
	// filter result must combine FMINDEX-served sealed rows and raw-scanned
	// growing rows.
	growingN := 200
	gids := make([]int64, growingN)
	gcontent := make([]string, growingN)
	gnoIndex := make([]string, growingN)
	gvectors := make([][]float32, growingN)
	for i := 0; i < growingN; i++ {
		gids[i] = int64(sealedN + i)
		gcontent[i] = filler
		if i == 0 {
			gcontent[i] = "stadium"
			expected = append(expected, gids[i])
		}
		gnoIndex[i] = gcontent[i]
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

	// Pattern "sta%" must return both the sealed and growing copies. Compare it
	// with the twin field so this checks completeness, not only segment ranges.
	got := queryFMIndexIDs(t, ctx, mc, collectionName,
		fmt.Sprintf(`%s like "sta%%"`, fmIndexContentField))
	scanIDs := queryFMIndexIDs(t, ctx, mc, collectionName,
		fmt.Sprintf(`%s like "sta%%"`, fmIndexNoIndexField))
	require.Equal(t, scanIDs, got)
	require.Equal(t, expected, got, "LIKE must combine every sealed-index and growing-scan match")
}

// TestFMIndexEmptyPatternAndGeneralLike verifies `LIKE '%'` (empty pattern,
// == IsNotNull), general-LIKE candidate recheck, and regex fallback behavior.
func TestFMIndexEmptyPatternAndGeneralLike(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_fallback", 6)

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

	// Keep the row count above the segment index threshold (1024) so FMINDEX is really built.
	totalRows := 2000
	ids := make([]int64, totalRows)
	content := make([]string, totalRows)
	noIndex := make([]string, totalRows)
	vectors := make([][]float32, totalRows)
	filler := strings.Repeat("y", fmIndexFillerLen)
	matching := make([]int64, 0, totalRows/fmIndexMarkerStep)
	qopOnly := make(map[int64]struct{})
	zebraOnly := make(map[int64]struct{})
	fallbackMatches := make([]int64, 0, 2*totalRows/fmIndexMarkerStep)
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		text := filler
		switch i % fmIndexMarkerStep {
		case 0:
			text = "QOP" + filler + "ZEBRA"
			matching = append(matching, int64(i))
		case 1:
			text = "QOP" + filler
			qopOnly[int64(i)] = struct{}{}
		case 2:
			text = filler + "ZEBRA"
			zebraOnly[int64(i)] = struct{}{}
		case 3:
			text = "req-0-error-0"
			fallbackMatches = append(fallbackMatches, int64(i))
		case 4:
			text = "req-1-error-1"
			fallbackMatches = append(fallbackMatches, int64(i))
		}
		content[i] = text
		noIndex[i] = text
		v := make([]float32, fmIndexVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}
	requireFMIndexCostGuard(t, content, len(matching)+len(qopOnly), "general LIKE QOP fragment")
	requireFMIndexCostGuard(t, content, len(matching)+len(zebraOnly), "general LIKE ZEBRA fragment")
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithVarcharColumn(fmIndexNoIndexField, noIndex).
		WithVarcharColumn(fmIndexContentField, content).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, vectors))
	require.NoError(t, err)
	flushFMIndexRows(t, ctx, mc, collectionName)
	indexAndLoadFMIndex(t, ctx, mc, collectionName, int64(totalRows))

	assertSame := func(idxExpr, scanExpr string) []int64 {
		t.Helper()
		idxIDs := queryFMIndexIDs(t, ctx, mc, collectionName, idxExpr)
		scanIDs := queryFMIndexIDs(t, ctx, mc, collectionName, scanExpr)
		require.Equal(t, scanIDs, idxIDs, "mismatch: %s", idxExpr)
		return idxIDs
	}

	// Empty pattern returns every non-NULL row (== IsNotNull).
	all := queryFMIndexIDs(t, ctx, mc, collectionName, fmt.Sprintf(`%s like "%%"`, fmIndexContentField))
	allExpected := make([]int64, totalRows)
	for id := range totalRows {
		allExpected[id] = int64(id)
	}
	require.Equal(t, allExpected, all, "LIKE '%' must return every non-NULL row")

	// Both rare fragments pass the cost guard. Rows containing only QOP or only
	// ZEBRA are phase-1 candidates but must be removed by the exact recheck.
	generalIDs := assertSame(
		fmt.Sprintf(`%s like "QOP%%ZEBRA"`, fmIndexContentField),
		fmt.Sprintf(`%s like "QOP%%ZEBRA"`, fmIndexNoIndexField))
	require.Equal(t, matching, generalIDs, "general LIKE must keep only full-pattern matches")
	for _, id := range generalIDs {
		_, isQOPOnly := qopOnly[id]
		_, isZebraOnly := zebraOnly[id]
		require.Falsef(t, isQOPOnly || isZebraOnly, "fragment-only candidate id=%d leaked past recheck", id)
	}

	// LIKE with a single-char `_` wildcard is also general LIKE and remains exact.
	underscoreIDs := assertSame(
		fmt.Sprintf(`%s like "req-_-error-_"`, fmIndexContentField),
		fmt.Sprintf(`%s like "req-_-error-_"`, fmIndexNoIndexField))
	require.Equal(t, fallbackMatches, underscoreIDs)

	// Regex via `=~` (RegexMatch is not accelerated by FMINDEX, falls back to the
	// RE2 scan, still exact).
	regexIDs := assertSame(
		fmt.Sprintf(`%s =~ "req-.-error-."`, fmIndexContentField),
		fmt.Sprintf(`%s =~ "req-.-error-."`, fmIndexNoIndexField))
	require.Equal(t, fallbackMatches, regexIDs)
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
		ids := make([]int64, fmIndexTotalRows)
		content := make([]string, fmIndexTotalRows)
		vectors := make([][]float32, fmIndexTotalRows)
		for i := 0; i < fmIndexTotalRows; i++ {
			ids[i] = int64(i)
			content[i] = fmt.Sprintf("boundary-%d", i)
			vectors[i] = []float32{1, 0, 0, 0, 0, 0, 0, 0}
		}
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
			WithInt64Column("id", ids).
			WithVarcharColumn(fmIndexContentField, content).
			WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, vectors))
		require.NoError(t, err)
		// Keep the segment above minSegmentNumRowsToEnableIndex so valid boundary
		// values exercise a real FMINDEX build instead of datacoord fake-finished.
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
		WithField(entity.NewField().WithName(fmIndexNoIndexField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(600)).
		WithField(entity.NewField().WithName(fmIndexContentField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(600)).
		WithField(entity.NewField().WithName(fmIndexVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(fmIndexVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	// Sparse rows carry literal special characters; long filler rows keep each
	// literal below the FMINDEX cost guard.
	values := []string{"100%done", "under_score", "back\\slash", "plain", "50%_mixed"}
	// Keep the row count above the segment index threshold (1024) so FMINDEX is really built.
	totalRows := 2000
	ids := make([]int64, totalRows)
	content := make([]string, totalRows)
	noIndex := make([]string, totalRows)
	vectors := make([][]float32, totalRows)
	filler := strings.Repeat("y", fmIndexFillerLen)
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		text := filler
		if valueIndex := i % fmIndexMarkerStep; valueIndex < len(values) {
			text = values[valueIndex]
		}
		content[i] = text
		noIndex[i] = text
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
	indexAndLoadFMIndex(t, ctx, mc, collectionName, int64(totalRows))

	assertSame := func(idxExpr, scanExpr string, matchingValueIndexes map[int]struct{}) {
		t.Helper()
		idxIDs := queryFMIndexIDs(t, ctx, mc, collectionName, idxExpr)
		scanIDs := queryFMIndexIDs(t, ctx, mc, collectionName, scanExpr)
		require.Equal(t, scanIDs, idxIDs, "mismatch: %s", idxExpr)
		expected := make([]int64, 0, totalRows)
		for id := 0; id < totalRows; id++ {
			if _, ok := matchingValueIndexes[id%fmIndexMarkerStep]; ok {
				expected = append(expected, int64(id))
			}
		}
		requireFMIndexCostGuard(t, content, len(expected), "escaped LIKE")
		require.Equal(t, expected, idxIDs, "escaped LIKE ground truth mismatch: %s", idxExpr)
	}
	// LIKE escapes must be carried in a raw string literal (r"..."), because a
	// normal "..." literal is processed by strconv.Unquote first, which rejects
	// `\%`/`\_`. In r"..." the backslash reaches the LIKE layer verbatim, where
	// `\%` matches a literal '%', `\_` a literal '_', `\\` a literal backslash.
	assertSame(
		fmt.Sprintf(`%s like r"%%\%%%%"`, fmIndexContentField),
		fmt.Sprintf(`%s like r"%%\%%%%"`, fmIndexNoIndexField),
		map[int]struct{}{0: {}, 4: {}})
	assertSame(
		fmt.Sprintf(`%s like r"%%\_%%"`, fmIndexContentField),
		fmt.Sprintf(`%s like r"%%\_%%"`, fmIndexNoIndexField),
		map[int]struct{}{1: {}, 4: {}})
	assertSame(
		fmt.Sprintf(`%s like r"%%\\%%"`, fmIndexContentField),
		fmt.Sprintf(`%s like r"%%\\%%"`, fmIndexNoIndexField),
		map[int]struct{}{2: {}})

	// A dangling trailing backslash inside a raw string is an invalid LIKE
	// pattern and must be rejected.
	_, err = mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf(`%s like r"abc\"`, fmIndexContentField)).WithOutputFields("id").
		WithConsistencyLevel(entity.ClStrong))
	require.Error(t, err, "a dangling trailing backslash must be rejected")
}

// TestFMIndexCompoundMutationAndRebuild verifies the lifecycle paths that are
// easy to miss when only testing a freshly built sealed segment: compound
// filters, delete/upsert visibility, dropping the scalar index, and rebuilding
// it without changing query results.
func TestFMIndexCompoundMutationAndRebuild(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("fmindex_lifecycle", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(fmIndexNoIndexField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fmIndexContentField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName(fmIndexVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(fmIndexVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	const totalRows = 2000
	ids := make([]int64, totalRows)
	content := make([]string, totalRows)
	noIndex := make([]string, totalRows)
	vectors := make([][]float32, totalRows)
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		content[i] = fmt.Sprintf("req-%d-error-%d", i%4, i%10)
		noIndex[i] = content[i]
		vectors[i] = []float32{float32(i), 0, 0, 0, 0, 0, 0, 0}
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithVarcharColumn(fmIndexNoIndexField, noIndex).
		WithVarcharColumn(fmIndexContentField, content).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, vectors))
	require.NoError(t, err)
	flushFMIndexRows(t, ctx, mc, collectionName)
	indexAndLoadFMIndex(t, ctx, mc, collectionName, totalRows)

	assertSame := func(idxExpr, scanExpr string) []int64 {
		t.Helper()
		scanIDs := queryFMIndexIDs(t, ctx, mc, collectionName, scanExpr)
		idxIDs := queryFMIndexIDs(t, ctx, mc, collectionName, idxExpr)
		require.Equal(t, scanIDs, idxIDs, "mismatch: %s", idxExpr)
		return idxIDs
	}
	assertSame(
		fmt.Sprintf(`%s like "%%error%%" && id >= 1000`, fmIndexContentField),
		fmt.Sprintf(`%s like "%%error%%" && id >= 1000`, fmIndexNoIndexField))
	assertSame(
		fmt.Sprintf(`%s like "req-0%%" || %s like "req-1%%"`, fmIndexContentField, fmIndexContentField),
		fmt.Sprintf(`%s like "req-0%%" || %s like "req-1%%"`, fmIndexNoIndexField, fmIndexNoIndexField))
	assertSame(
		fmt.Sprintf(`not (%s like "req-0%%")`, fmIndexContentField),
		fmt.Sprintf(`not (%s like "req-0%%")`, fmIndexNoIndexField))

	deleteResult, err := mc.Delete(ctx, client.NewDeleteOption(collectionName).WithExpr("id in [1000]"))
	require.NoError(t, err)
	require.EqualValues(t, 1, deleteResult.DeleteCount)

	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", []int64{1000}).
		WithVarcharColumn(fmIndexNoIndexField, []string{"restored"}).
		WithVarcharColumn(fmIndexContentField, []string{"restored"}).
		WithFloatVectorColumn(fmIndexVectorField, fmIndexVectorDim, [][]float32{{1000, 0, 0, 0, 0, 0, 0, 0}}))
	require.NoError(t, err)
	// Seal the replacement before rebuilding so the rebuilt index, rather than
	// only the growing-segment scan, is responsible for the post-upsert check.
	flushFMIndexRows(t, ctx, mc, collectionName)
	restored := assertSame(`content like "restored%"`, `content_no_index like "restored%"`)
	require.Equal(t, []int64{1000}, restored, "upserted PK must be visible with its replacement value")
	expectedOriginal := make([]int64, 0, totalRows/20)
	for id := 0; id < totalRows; id += 20 {
		if id != 1000 {
			expectedOriginal = append(expectedOriginal, int64(id))
		}
	}
	original := assertSame(`content like "req-0-error-0"`, `content_no_index like "req-0-error-0"`)
	require.Equal(t, expectedOriginal, original, "the upserted PK must no longer match its original value")

	require.NoError(t, mc.DropIndex(ctx, client.NewDropIndexOption(collectionName, "fm_content")))
	_, err = mc.DescribeIndex(ctx, client.NewDescribeIndexOption(collectionName, "fm_content"))
	require.Error(t, err, "named FMINDEX must be absent after DropIndex")
	require.NoError(t, createFMIndex(ctx, mc, collectionName, fmIndexContentField,
		index.NewFMIndex().WithIndexName("fm_content").WithSaSampleRate(32)))
	requireFMIndexReady(t, ctx, mc, collectionName, "fm_content", -1)
	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))
	restored = assertSame(`content like "restored%"`, `content_no_index like "restored%"`)
	require.Equal(t, []int64{1000}, restored, "rebuilt FMINDEX must retain the replacement value")
	original = assertSame(`content like "req-0-error-0"`, `content_no_index like "req-0-error-0"`)
	require.Equal(t, expectedOriginal, original, "rebuilt FMINDEX must not resurrect the deleted value")
}
