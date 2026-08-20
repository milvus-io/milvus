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

// L0 coverage for the roaring_match exact membership filter through the public Go SDK.
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
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const (
	roaringCreatorField = "creator_id"
	roaringVectorField  = "vector"
	roaringVectorDim    = 8
	// Keep the row count above indexCoord.segment.minSegmentNumRowsToEnableIndex
	// (1024) so scalar indexes are really built, not fake-finished.
	roaringTotalRows = 2000
	roaringDomain    = 50
)

func roaringIntSchema(collectionName string, nullable bool) *entity.Schema {
	creatorField := entity.NewField().WithName(roaringCreatorField).WithDataType(entity.FieldTypeInt64)
	if nullable {
		creatorField.WithNullable(true)
	}
	return entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(creatorField).
		WithField(entity.NewField().WithName(roaringVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(roaringVectorDim))
}

func createRoaringCollection(t *testing.T, ctx CtxT, mc MC, collectionName string, nullable bool) {
	t.Helper()

	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, roaringIntSchema(collectionName, nullable)).
		WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})
}

func insertRoaringRows(t *testing.T, ctx CtxT, mc MC, collectionName string, nullable bool) {
	t.Helper()

	ids := make([]int64, roaringTotalRows)
	vectors := make([][]float32, roaringTotalRows)
	for i := 0; i < roaringTotalRows; i++ {
		ids[i] = int64(i)
		v := make([]float32, roaringVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}

	opt := client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithFloatVectorColumn(roaringVectorField, roaringVectorDim, vectors)

	if nullable {
		values := make([]int64, 0, roaringTotalRows)
		valid := make([]bool, roaringTotalRows)
		for i := 0; i < roaringTotalRows; i++ {
			if i%8 == 7 {
				valid[i] = false
				continue
			}
			valid[i] = true
			values = append(values, int64(i%roaringDomain))
		}
		col, err := column.NewNullableColumnInt64(roaringCreatorField, values, valid)
		require.NoError(t, err)
		opt.WithColumns(col)
	} else {
		creators := make([]int64, roaringTotalRows)
		for i := 0; i < roaringTotalRows; i++ {
			creators[i] = int64(i % roaringDomain)
		}
		opt.WithInt64Column(roaringCreatorField, creators)
	}

	_, err := mc.Insert(ctx, opt)
	require.NoError(t, err)
}

func flushLoadRoaring(t *testing.T, ctx CtxT, mc MC, collectionName string) {
	t.Helper()

	flushTask, err := mc.Flush(ctx, client.NewFlushOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, flushTask.Await(ctx))

	vecTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, roaringVectorField,
		index.NewFlatIndex(entity.L2)))
	require.NoError(t, err)
	require.NoError(t, vecTask.Await(ctx))

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))
}

func queryRoaringIDs(t *testing.T, ctx CtxT, mc MC, collectionName, expr string, blob any) []int64 {
	t.Helper()

	opt := client.NewQueryOption(collectionName).
		WithFilter(expr).WithOutputFields("id").
		WithConsistencyLevel(entity.ClStrong)
	if blob != nil {
		opt.WithTemplateParam("rb", blob)
	}
	rs, err := mc.Query(ctx, opt)
	require.NoError(t, err, "query %q", expr)

	col, ok := rs.GetColumn("id").(*column.ColumnInt64)
	require.True(t, ok)
	out := make([]int64, 0, col.Len())
	for i := 0; i < col.Len(); i++ {
		v, err := col.GetAsInt64(i)
		require.NoError(t, err)
		out = append(out, v)
	}
	return out
}

// TestRoaringMatchExactMembership verifies roaring_match selects exactly the
// requested ids that exist — no false positives and no false negatives.
func TestRoaringMatchExactMembership(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_exact", 6)

	createRoaringCollection(t, ctx, mc, collectionName, false)
	insertRoaringRows(t, ctx, mc, collectionName, false)
	flushLoadRoaring(t, ctx, mc, collectionName)

	members := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1000000} // last one absent
	blob, err := client.NewRoaringBitmapBlob(members)
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField), blob)
	require.NotEmpty(t, got)
	seen := map[int64]bool{}
	for _, id := range got {
		creator := id % roaringDomain
		seen[creator] = true
		require.Truef(t, creator >= 0 && creator <= 9, "roaring_match returned non-member id=%d", id)
	}
	for _, m := range []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} {
		require.Truef(t, seen[m], "roaring_match missed member %d", m)
	}
}

// TestRoaringMatchNotExactComplement verifies `not roaring_match` returns the
// exact complement over non-NULL rows.
func TestRoaringMatchNotExactComplement(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_not", 6)

	createRoaringCollection(t, ctx, mc, collectionName, false)
	insertRoaringRows(t, ctx, mc, collectionName, false)
	flushLoadRoaring(t, ctx, mc, collectionName)

	blob, err := client.NewRoaringBitmapBlob([]int64{0, 1, 2, 3, 4})
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("not roaring_match(%s, {rb})", roaringCreatorField), blob)
	require.NotEmpty(t, got)
	for _, id := range got {
		creator := id % roaringDomain
		require.GreaterOrEqualf(t, creator, int64(5), "not roaring_match returned a member id=%d", id)
	}
}

// TestRoaringMatchDelete verifies roaring_match is accepted in a delete
// expression (bloom_match is not, since it has false positives) and removes
// exactly the requested ids.
func TestRoaringMatchDelete(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_delete", 6)

	createRoaringCollection(t, ctx, mc, collectionName, false)
	insertRoaringRows(t, ctx, mc, collectionName, false)
	flushLoadRoaring(t, ctx, mc, collectionName)

	victims := []int64{0, 1, 2}
	blob, err := client.NewRoaringBitmapBlob(victims)
	require.NoError(t, err)

	dr, err := mc.Delete(ctx, client.NewDeleteOption(collectionName).
		WithExpr(fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField)).
		WithTemplateParam("rb", blob))
	require.NoError(t, err)
	require.EqualValues(t, int64(len(victims)*roaringTotalRows/roaringDomain), dr.DeleteCount)

	remaining := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField), blob)
	require.Empty(t, remaining, "victim rows must be deleted")
}

// TestRoaringMatchGrowingAndSealedMixed verifies roaring_match evaluates both
// sealed and growing segments in one query, exactly.
func TestRoaringMatchGrowingAndSealedMixed(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_growing", 6)

	createRoaringCollection(t, ctx, mc, collectionName, false)
	insertRoaringRows(t, ctx, mc, collectionName, false)
	flushLoadRoaring(t, ctx, mc, collectionName)

	const growingN = 200
	gids := make([]int64, growingN)
	gcreators := make([]int64, growingN)
	gvectors := make([][]float32, growingN)
	for i := 0; i < growingN; i++ {
		gids[i] = int64(roaringTotalRows + i)
		gcreators[i] = int64(500 + i%10)
		v := make([]float32, roaringVectorDim)
		v[0] = float32(roaringTotalRows + i)
		gvectors[i] = v
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", gids).
		WithInt64Column(roaringCreatorField, gcreators).
		WithFloatVectorColumn(roaringVectorField, roaringVectorDim, gvectors))
	require.NoError(t, err)

	blob, err := client.NewRoaringBitmapBlob([]int64{0, 1, 2, 3, 4, 500, 501, 502, 503, 504})
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField), blob)
	require.NotEmpty(t, got)
	sawSealed, sawGrowing := false, false
	for _, id := range got {
		if id < roaringTotalRows {
			sawSealed = true
		} else {
			sawGrowing = true
		}
	}
	require.True(t, sawSealed, "roaring_match missed sealed-segment members")
	require.True(t, sawGrowing, "roaring_match missed growing-segment members")
}

// TestRoaringMatchEmptyBitmap verifies an empty bitmap matches nothing.
func TestRoaringMatchEmptyBitmap(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_empty", 6)

	createRoaringCollection(t, ctx, mc, collectionName, false)
	insertRoaringRows(t, ctx, mc, collectionName, false)
	flushLoadRoaring(t, ctx, mc, collectionName)

	blob, err := client.NewRoaringBitmapBlob([]int64{})
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField), blob)
	require.Empty(t, got, "empty bitmap must match nothing")
}

// TestRoaringMatchInvalidInputRejected verifies a literal array, malformed blob
// and truncated blob are rejected rather than silently unfiltered.
func TestRoaringMatchInvalidInputRejected(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_reject", 6)

	createRoaringCollection(t, ctx, mc, collectionName, false)
	insertRoaringRows(t, ctx, mc, collectionName, false)
	flushLoadRoaring(t, ctx, mc, collectionName)

	// literal array argument
	_, err := mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf("roaring_match(%s, [1,2,3])", roaringCreatorField)).WithOutputFields("id").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "must be a {template} placeholder")

	// malformed MRB1 blob
	_, err = mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField)).WithOutputFields("id").
		WithTemplateParam("rb", client.RoaringBitmapBlob([]byte("not-an-mrb1-blob"))).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "roaring_match bitmap blob is invalid")

	// truncated valid blob
	valid, err := client.NewRoaringBitmapBlob([]int64{1, 2, 3})
	require.NoError(t, err)
	_, err = mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField)).WithOutputFields("id").
		WithTemplateParam("rb", valid[:len(valid)-1]).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "roaring_match bitmap blob is invalid")
}

// TestRoaringMatchNullRowsFoldToFalse verifies NULL rows never match
// roaring_match nor its negation.
func TestRoaringMatchNullRowsFoldToFalse(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_null", 6)

	createRoaringCollection(t, ctx, mc, collectionName, true)
	insertRoaringRows(t, ctx, mc, collectionName, true)
	flushLoadRoaring(t, ctx, mc, collectionName)

	members := make([]int64, 0, roaringDomain)
	for v := 0; v < roaringDomain; v++ {
		members = append(members, int64(v))
	}
	blob, err := client.NewRoaringBitmapBlob(members)
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField), blob)
	for _, id := range got {
		require.NotEqualf(t, int64(7), id%8, "roaring_match matched a NULL row id=%d", id)
	}

	notRs := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("not roaring_match(%s, {rb})", roaringCreatorField), blob)
	for _, id := range notRs {
		require.NotEqualf(t, int64(7), id%8, "not roaring_match matched a NULL row id=%d", id)
	}
}

// TestRoaringMatchNegativeValues verifies negative ids round-trip through the
// bitmap's two's-complement encoding. Negative ids map to the top of the uint64
// key space and land in a different Roaring high container than positive ids; a
// build/probe pair that zero-extended instead of sign-extending would still pass
// every all-positive test, so this pins the highest-risk case.
func TestRoaringMatchNegativeValues(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_negative", 6)

	const (
		domain    = 100
		negShift  = 50 // creator = i % domain - negShift, straddling zero
		totalRows = 2000
	)
	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(roaringCreatorField).WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(roaringVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(roaringVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	ids := make([]int64, totalRows)
	creators := make([]int64, totalRows)
	vectors := make([][]float32, totalRows)
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		creators[i] = int64(i%domain) - negShift
		v := make([]float32, roaringVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithInt64Column(roaringCreatorField, creators).
		WithFloatVectorColumn(roaringVectorField, roaringVectorDim, vectors))
	require.NoError(t, err)
	flushLoadRoaring(t, ctx, mc, collectionName)

	// Half negative, half positive, plus values absent from the collection.
	members := []int64{-negShift, -30, -1, 0, 1, 17, 49, 1 << 20, -(1 << 20)}
	blob, err := client.NewRoaringBitmapBlob(members)
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField), blob)

	present := map[int64]bool{}
	for _, c := range creators {
		present[c] = true
	}
	want := map[int64]bool{}
	for _, m := range members {
		if present[m] {
			want[m] = true
		}
	}
	require.NotEmpty(t, want, "test setup must select at least one present id")

	seen := map[int64]bool{}
	for _, id := range got {
		creator := creators[id]
		seen[creator] = true
		require.Truef(t, want[creator], "roaring_match returned non-member creator=%d (id=%d)", creator, id)
	}
	for m := range want {
		require.Truef(t, seen[m], "roaring_match missed member %d", m)
	}
}

// TestRoaringMatchScalarIndexTypeMatrix verifies roaring_match stays exact when
// a scalar index is built on the field before load. STL_SORT drops the raw
// column (has_raw_data=true), forcing roaring_match through the index's
// Reverse_Lookup path; INVERTED keeps the raw column (data path). Both must
// select exactly the requested ids.
func TestRoaringMatchScalarIndexTypeMatrix(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	members := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1000000}
	blob, err := client.NewRoaringBitmapBlob(members)
	require.NoError(t, err)

	for _, indexType := range []string{"STL_SORT", "INVERTED"} {
		t.Run(indexType, func(t *testing.T) {
			collectionName := common.GenRandomString("roaring_idx_"+indexType, 6)
			createRoaringCollection(t, ctx, mc, collectionName, false)
			insertRoaringRows(t, ctx, mc, collectionName, false)

			flushTask, err := mc.Flush(ctx, client.NewFlushOption(collectionName))
			require.NoError(t, err)
			require.NoError(t, flushTask.Await(ctx))

			var scalarIndex index.Index
			switch indexType {
			case "STL_SORT":
				scalarIndex = index.NewSortedIndex()
			case "INVERTED":
				scalarIndex = index.NewInvertedIndex()
			}
			idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, roaringCreatorField,
				scalarIndex))
			require.NoError(t, err)
			require.NoError(t, idxTask.Await(ctx))

			vecTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, roaringVectorField,
				index.NewFlatIndex(entity.L2)))
			require.NoError(t, err)
			require.NoError(t, vecTask.Await(ctx))

			loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
			require.NoError(t, err)
			require.NoError(t, loadTask.Await(ctx))

			got := queryRoaringIDs(t, ctx, mc, collectionName,
				fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField), blob)
			require.NotEmpty(t, got)
			seen := map[int64]bool{}
			for _, id := range got {
				creator := id % roaringDomain
				seen[creator] = true
				require.Truef(t, creator >= 0 && creator <= 9, "roaring_match returned non-member id=%d under %s", id, indexType)
			}
			for _, m := range []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} {
				require.Truef(t, seen[m], "roaring_match missed member %d under %s", m, indexType)
			}
		})
	}
}

// TestRoaringMatchAutoIndex verifies roaring_match stays exact when the field is
// indexed with AUTOINDEX (resolved to HYBRID, which picks BITMAP for
// low-cardinality data and STLSORT for high-cardinality data).
func TestRoaringMatchAutoIndex(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	members := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1000000}
	blob, err := client.NewRoaringBitmapBlob(members)
	require.NoError(t, err)

	for _, domain := range []int{200, 50} {
		t.Run(fmt.Sprintf("domain_%d", domain), func(t *testing.T) {
			collectionName := common.GenRandomString("roaring_auto", 6)
			schema := entity.NewSchema().WithName(collectionName).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
				WithField(entity.NewField().WithName(roaringCreatorField).WithDataType(entity.FieldTypeInt64)).
				WithField(entity.NewField().WithName(roaringVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(roaringVectorDim))
			require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
				WithConsistencyLevel(entity.ClStrong)))
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
			})

			ids := make([]int64, roaringTotalRows)
			creators := make([]int64, roaringTotalRows)
			vectors := make([][]float32, roaringTotalRows)
			for i := 0; i < roaringTotalRows; i++ {
				ids[i] = int64(i)
				creators[i] = int64(i % domain)
				v := make([]float32, roaringVectorDim)
				v[0] = float32(i)
				vectors[i] = v
			}
			_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
				WithInt64Column("id", ids).
				WithInt64Column(roaringCreatorField, creators).
				WithFloatVectorColumn(roaringVectorField, roaringVectorDim, vectors))
			require.NoError(t, err)

			flushTask, err := mc.Flush(ctx, client.NewFlushOption(collectionName))
			require.NoError(t, err)
			require.NoError(t, flushTask.Await(ctx))

			idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, roaringCreatorField,
				index.NewAutoIndex(entity.L2)))
			require.NoError(t, err)
			require.NoError(t, idxTask.Await(ctx))

			vecTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, roaringVectorField,
				index.NewFlatIndex(entity.L2)))
			require.NoError(t, err)
			require.NoError(t, vecTask.Await(ctx))

			loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
			require.NoError(t, err)
			require.NoError(t, loadTask.Await(ctx))

			got := queryRoaringIDs(t, ctx, mc, collectionName,
				fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField), blob)
			require.NotEmpty(t, got)
			seen := map[int64]bool{}
			for _, id := range got {
				creator := id % int64(domain)
				seen[creator] = true
				require.Truef(t, creator >= 0 && creator <= 9, "roaring_match returned non-member id=%d under AUTOINDEX domain %d", id, domain)
			}
			for _, m := range []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} {
				require.Truef(t, seen[m], "roaring_match missed member %d under AUTOINDEX domain %d", m, domain)
			}
		})
	}
}

// TestRoaringMatchIntTypeMatrix verifies one int64-built bitmap probes every
// integer field width, including the sign-extension of narrow negative values.
func TestRoaringMatchIntTypeMatrix(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_intmatrix", 6)

	const (
		domain = 100
		shift  = 50 // value = i % domain - shift, straddling zero
	)
	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("i8").WithDataType(entity.FieldTypeInt8)).
		WithField(entity.NewField().WithName("i16").WithDataType(entity.FieldTypeInt16)).
		WithField(entity.NewField().WithName("i32").WithDataType(entity.FieldTypeInt32)).
		WithField(entity.NewField().WithName("i64").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(roaringVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(roaringVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	const totalRows = 2000
	ids := make([]int64, totalRows)
	i8 := make([]int8, totalRows)
	i16 := make([]int16, totalRows)
	i32 := make([]int32, totalRows)
	i64 := make([]int64, totalRows)
	vectors := make([][]float32, totalRows)
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		v := int64(i%domain) - shift
		i8[i] = int8(v)
		i16[i] = int16(v)
		i32[i] = int32(v)
		i64[i] = v
		vec := make([]float32, roaringVectorDim)
		vec[0] = float32(i)
		vectors[i] = vec
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithInt8Column("i8", i8).
		WithInt16Column("i16", i16).
		WithInt32Column("i32", i32).
		WithInt64Column("i64", i64).
		WithFloatVectorColumn(roaringVectorField, roaringVectorDim, vectors))
	require.NoError(t, err)
	flushLoadRoaring(t, ctx, mc, collectionName)

	// Straddle zero so a zero-extending narrow build/probe would place a negative
	// value in the wrong Roaring high container and miss.
	members := []int64{-shift, -30, -1, 0, 1, 17, 49}
	blob, err := client.NewRoaringBitmapBlob(members)
	require.NoError(t, err)

	for _, field := range []string{"i8", "i16", "i32", "i64"} {
		got := queryRoaringIDs(t, ctx, mc, collectionName,
			fmt.Sprintf("roaring_match(%s, {rb})", field), blob)
		seen := map[int64]bool{}
		for _, id := range got {
			v := id%domain - shift
			seen[v] = true
			require.Truef(t, isMember(members, v), "roaring_match returned non-member value=%d (id=%d) on %s", v, id, field)
		}
		for _, m := range members {
			if m >= -shift && m <= domain-shift-1 {
				require.Truef(t, seen[m], "roaring_match missed member %d on %s", m, field)
			}
		}
	}
}

func isMember(members []int64, v int64) bool {
	for _, m := range members {
		if m == v {
			return true
		}
	}
	return false
}

// TestRoaringMatchInt64Bounds verifies INT64_MIN/MAX round-trip through the
// two's-complement mapping.
func TestRoaringMatchInt64Bounds(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_bounds", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(roaringCreatorField).WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(roaringVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(roaringVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	const totalRows = 2000
	ids := make([]int64, totalRows)
	creators := make([]int64, totalRows)
	vectors := make([][]float32, totalRows)
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		v := make([]float32, roaringVectorDim)
		v[0] = float32(i)
		vectors[i] = v
		switch i % 4 {
		case 0:
			creators[i] = math.MinInt64
		case 1:
			creators[i] = math.MaxInt64
		case 2:
			creators[i] = -1
		default:
			creators[i] = 42
		}
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithInt64Column(roaringCreatorField, creators).
		WithFloatVectorColumn(roaringVectorField, roaringVectorDim, vectors))
	require.NoError(t, err)
	flushLoadRoaring(t, ctx, mc, collectionName)

	blob, err := client.NewRoaringBitmapBlob([]int64{math.MinInt64, math.MaxInt64, -1, 42})
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField), blob)
	require.Len(t, got, totalRows, "all four values are present, so every row must match")

	// A single absent extreme must match nothing.
	absent, err := client.NewRoaringBitmapBlob([]int64{math.MaxInt64 - 1})
	require.NoError(t, err)
	gotAbsent := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField), absent)
	require.Empty(t, gotAbsent)
}

// TestRoaringMatchNotEmptyBitmap verifies `not roaring_match(field, {empty})`
// selects every non-NULL row (the exact complement of an empty set).
func TestRoaringMatchNotEmptyBitmap(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_notempty", 6)

	createRoaringCollection(t, ctx, mc, collectionName, false)
	insertRoaringRows(t, ctx, mc, collectionName, false)
	flushLoadRoaring(t, ctx, mc, collectionName)

	blob, err := client.NewRoaringBitmapBlob([]int64{})
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("not roaring_match(%s, {rb})", roaringCreatorField), blob)
	require.Len(t, got, roaringTotalRows, "negating an empty bitmap must select every non-NULL row")
}

// TestRoaringMatchDeleteThreeStates verifies delete with a positive set, a
// negated set, and an empty set (positive deletes nothing, negated deletes all).
func TestRoaringMatchDeleteThreeStates(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Positive empty: delete nothing.
	t.Run("positive_empty", func(t *testing.T) {
		collectionName := common.GenRandomString("roaring_del_pos_empty", 6)
		createRoaringCollection(t, ctx, mc, collectionName, false)
		insertRoaringRows(t, ctx, mc, collectionName, false)
		flushLoadRoaring(t, ctx, mc, collectionName)

		blob, err := client.NewRoaringBitmapBlob([]int64{})
		require.NoError(t, err)
		dr, err := mc.Delete(ctx, client.NewDeleteOption(collectionName).
			WithExpr(fmt.Sprintf("roaring_match(%s, {rb})", roaringCreatorField)).
			WithTemplateParam("rb", blob))
		require.NoError(t, err)
		require.EqualValues(t, 0, dr.DeleteCount)
	})

	// Negated set: delete everything outside the set.
	t.Run("negated", func(t *testing.T) {
		collectionName := common.GenRandomString("roaring_del_neg", 6)
		createRoaringCollection(t, ctx, mc, collectionName, false)
		insertRoaringRows(t, ctx, mc, collectionName, false)
		flushLoadRoaring(t, ctx, mc, collectionName)

		keep := []int64{0, 1, 2}
		blob, err := client.NewRoaringBitmapBlob(keep)
		require.NoError(t, err)
		dr, err := mc.Delete(ctx, client.NewDeleteOption(collectionName).
			WithExpr(fmt.Sprintf("not roaring_match(%s, {rb})", roaringCreatorField)).
			WithTemplateParam("rb", blob))
		require.NoError(t, err)
		require.EqualValues(t, int64((roaringDomain-len(keep))*roaringTotalRows/roaringDomain), dr.DeleteCount)
	})
}
