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

// Roaring-kind membership_match coverage through the public Go SDK.
package testcases

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func queryRoaringIDs(t *testing.T, ctx CtxT, mc MC, collectionName, expr string, blob any) []int64 {
	t.Helper()
	return queryMembershipIDs(t, ctx, mc, collectionName, expr, "rb", blob)
}

// TestRoaringMatchExactMembership verifies roaring_match selects exactly the
// requested ids that exist — no false positives and no false negatives.
func TestRoaringMatchExactMembership(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_exact", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	members := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1000000} // last one absent
	blob, err := client.NewRoaringBitmapBlob(members)
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField), blob)
	expected := expectedMembershipIDs(membershipTotalRows, membershipDomain, map[int64]struct{}{
		0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {},
	})
	require.ElementsMatch(t, expected, got, "roaring_match must return the exact matching row PK set")
	searchGot := searchMembershipIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField), "rb", blob)
	require.ElementsMatch(t, expected, searchGot, "roaring_match Search must return the exact row PK set")
}

// TestRoaringMatchNotExactComplement verifies `not roaring_match` returns the
// exact complement over non-NULL rows.
func TestRoaringMatchNotExactComplement(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_not", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	blob, err := client.NewRoaringBitmapBlob([]int64{0, 1, 2, 3, 4})
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("not membership_match(%s, {rb}, type=roaring)", membershipCreatorField), blob)
	complement := make(map[int64]struct{}, membershipDomain-5)
	for value := int64(5); value < membershipDomain; value++ {
		complement[value] = struct{}{}
	}
	expected := expectedMembershipIDs(membershipTotalRows, membershipDomain, complement)
	require.ElementsMatch(t, expected, got, "not roaring_match must return the exact complement row PK set")
}

// TestRoaringMatchDelete verifies roaring_match is accepted in a delete
// expression (bloom_match is not, since it has false positives) and removes
// exactly the requested ids.
func TestRoaringMatchDelete(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_delete", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	victims := []int64{0, 1, 2}
	blob, err := client.NewRoaringBitmapBlob(victims)
	require.NoError(t, err)

	dr, err := mc.Delete(ctx, client.NewDeleteOption(collectionName).
		WithExpr(fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField)).
		WithTemplateParam("rb", blob))
	require.NoError(t, err)
	// Fixture couples pk id == row index i and creator == i % domain, so each
	// creator value 0..domain-1 appears exactly totalRows/domain times; deleting
	// len(victims) distinct values removes len(victims) * totalRows/domain rows.
	require.EqualValues(t, int64(len(victims)*membershipTotalRows/membershipDomain), dr.DeleteCount)

	remaining := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField), blob)
	require.Empty(t, remaining, "victim rows must be deleted")
}

// TestRoaringMatchGrowingAndSealedMixed verifies roaring_match evaluates both
// sealed and growing segments in one query, exactly.
func TestRoaringMatchGrowingAndSealedMixed(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_growing", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	const growingN = 200
	gids := make([]int64, growingN)
	gcreators := make([]int64, growingN)
	gvectors := make([][]float32, growingN)
	for i := 0; i < growingN; i++ {
		gids[i] = int64(membershipTotalRows + i)
		gcreators[i] = int64(500 + i%10)
		v := make([]float32, membershipVectorDim)
		v[0] = float32(membershipTotalRows + i)
		gvectors[i] = v
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", gids).
		WithInt64Column(membershipCreatorField, gcreators).
		WithFloatVectorColumn(membershipVectorField, membershipVectorDim, gvectors))
	require.NoError(t, err)

	blob, err := client.NewRoaringBitmapBlob([]int64{0, 1, 2, 3, 4, 500, 501, 502, 503, 504})
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField), blob)
	expected := expectedMembershipIDs(membershipTotalRows, membershipDomain, map[int64]struct{}{
		0: {}, 1: {}, 2: {}, 3: {}, 4: {},
	})
	for i := 0; i < growingN; i++ {
		if i%10 < 5 {
			expected = append(expected, int64(membershipTotalRows+i))
		}
	}
	require.ElementsMatch(t, expected, got,
		"roaring_match must return every exact member across sealed and growing segments")
}

// TestRoaringMatchEmptyBitmap verifies an empty bitmap matches nothing.
func TestRoaringMatchEmptyBitmap(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_empty", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	blob, err := client.NewRoaringBitmapBlob([]int64{})
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField), blob)
	require.Empty(t, got, "empty bitmap must match nothing")
}

// TestRoaringMatchInvalidInputRejected verifies a literal array, malformed blob
// and truncated blob are rejected rather than silently unfiltered.
func TestRoaringMatchInvalidInputRejected(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_reject", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	// literal array argument
	_, err := mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf("membership_match(%s, [1,2,3], type=roaring)", membershipCreatorField)).WithOutputFields("id").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "must be a {template} placeholder")

	// malformed MRB1 blob
	_, err = mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField)).WithOutputFields("id").
		WithTemplateParam("rb", client.RoaringBitmapBlob([]byte("not-an-mrb1-blob"))).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "unknown format magic")

	// truncated valid blob
	valid, err := client.NewRoaringBitmapBlob([]int64{1, 2, 3})
	require.NoError(t, err)
	_, err = mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField)).WithOutputFields("id").
		WithTemplateParam("rb", valid[:len(valid)-1]).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "membership_match roaring bitmap blob is invalid")
}

// TestRoaringMatchNullRowsFoldToFalse verifies NULL rows never match
// roaring_match nor its negation.
func TestRoaringMatchNullRowsFoldToFalse(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_null", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, true)
	insertIntMembershipRows(t, ctx, mc, collectionName, true)
	flushLoadMembership(t, ctx, mc, collectionName)

	members := make([]int64, 0, membershipDomain)
	for v := 0; v < membershipDomain; v++ {
		members = append(members, int64(v))
	}
	blob, err := client.NewRoaringBitmapBlob(members)
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField), blob)
	expected := make([]int64, 0, membershipTotalRows-membershipTotalRows/8)
	for id := 0; id < membershipTotalRows; id++ {
		if id%8 != 7 {
			expected = append(expected, int64(id))
		}
	}
	require.ElementsMatch(t, expected, got, "all and only non-NULL rows must match when every value is a member")

	notRs := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("not membership_match(%s, {rb}, type=roaring)", membershipCreatorField), blob)
	require.Empty(t, notRs, "NULL folds to false and every non-NULL value matches, so negation must match nothing")
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
		WithField(entity.NewField().WithName(membershipCreatorField).WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(membershipVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(membershipVectorDim))
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
		v := make([]float32, membershipVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithInt64Column(membershipCreatorField, creators).
		WithFloatVectorColumn(membershipVectorField, membershipVectorDim, vectors))
	require.NoError(t, err)
	flushLoadMembership(t, ctx, mc, collectionName)

	// Half negative, half positive, plus values absent from the collection.
	members := []int64{-negShift, -30, -1, 0, 1, 17, 49, 1 << 20, -(1 << 20)}
	blob, err := client.NewRoaringBitmapBlob(members)
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField), blob)

	expected := make([]int64, 0, totalRows)
	for id, creator := range creators {
		if isMember(members, creator) {
			expected = append(expected, int64(id))
		}
	}
	require.NotEmpty(t, expected, "test setup must select at least one present id")
	require.ElementsMatch(t, expected, got,
		"negative and positive members must return every matching row, not merely one row per value")
}

// TestRoaringMatchScalarIndexTypeMatrix verifies roaring_match stays exact when
// a scalar index is built on the field before load. STL_SORT drops the raw
// column (has_raw_data=true), forcing roaring_match through the index's
// Reverse_Lookup path; INVERTED and BITMAP cover the other supported scalar
// index entry points. All must select exactly the requested ids.
func TestRoaringMatchScalarIndexTypeMatrix(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	members := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1000000}
	blob, err := client.NewRoaringBitmapBlob(members)
	require.NoError(t, err)

	for _, indexType := range []string{"STL_SORT", "INVERTED", "BITMAP"} {
		t.Run(indexType, func(t *testing.T) {
			collectionName := common.GenRandomString("roaring_idx_"+indexType, 6)
			createIntMembershipCollection(t, ctx, mc, collectionName, false)
			insertIntMembershipRows(t, ctx, mc, collectionName, false)

			flushTask, err := mc.Flush(ctx, client.NewFlushOption(collectionName))
			require.NoError(t, err)
			require.NoError(t, flushTask.Await(ctx))

			var scalarIndex index.Index
			switch indexType {
			case "STL_SORT":
				scalarIndex = index.NewSortedIndex()
			case "INVERTED":
				scalarIndex = index.NewInvertedIndex()
			case "BITMAP":
				scalarIndex = index.NewBitmapIndex()
			}
			idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, membershipCreatorField,
				scalarIndex))
			require.NoError(t, err)
			require.NoError(t, idxTask.Await(ctx))

			vecTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, membershipVectorField,
				index.NewFlatIndex(entity.L2)))
			require.NoError(t, err)
			require.NoError(t, vecTask.Await(ctx))

			loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
			require.NoError(t, err)
			require.NoError(t, loadTask.Await(ctx))

			got := queryRoaringIDs(t, ctx, mc, collectionName,
				fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField), blob)
			expected := expectedMembershipIDs(membershipTotalRows, membershipDomain, map[int64]struct{}{
				0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {},
			})
			require.ElementsMatch(t, expected, got, "roaring_match row set mismatch under %s", indexType)
		})
	}
}

// TestRoaringMatchAutoIndex verifies roaring_match stays exact when the field is
// indexed with AUTOINDEX for both low- and high-cardinality datasets, without
// coupling the assertion to AUTOINDEX's internal implementation choice.
func TestRoaringMatchAutoIndex(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	members := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1000000}
	blob, err := client.NewRoaringBitmapBlob(members)
	require.NoError(t, err)

	// Exercise AUTOINDEX with both high- and low-cardinality inputs. Explicit
	// index-path coverage lives in TestRoaringMatchScalarIndexTypeMatrix.
	for _, domain := range []int{200, 50} {
		t.Run(fmt.Sprintf("domain_%d", domain), func(t *testing.T) {
			collectionName := common.GenRandomString("roaring_auto", 6)
			schema := entity.NewSchema().WithName(collectionName).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
				WithField(entity.NewField().WithName(membershipCreatorField).WithDataType(entity.FieldTypeInt64)).
				WithField(entity.NewField().WithName(membershipVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(membershipVectorDim))
			require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
				WithConsistencyLevel(entity.ClStrong)))
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
			})

			ids := make([]int64, membershipTotalRows)
			creators := make([]int64, membershipTotalRows)
			vectors := make([][]float32, membershipTotalRows)
			for i := 0; i < membershipTotalRows; i++ {
				ids[i] = int64(i)
				creators[i] = int64(i % domain)
				v := make([]float32, membershipVectorDim)
				v[0] = float32(i)
				vectors[i] = v
			}
			_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
				WithInt64Column("id", ids).
				WithInt64Column(membershipCreatorField, creators).
				WithFloatVectorColumn(membershipVectorField, membershipVectorDim, vectors))
			require.NoError(t, err)

			flushTask, err := mc.Flush(ctx, client.NewFlushOption(collectionName))
			require.NoError(t, err)
			require.NoError(t, flushTask.Await(ctx))

			idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, membershipCreatorField,
				index.NewAutoIndex(entity.L2)))
			require.NoError(t, err)
			require.NoError(t, idxTask.Await(ctx))

			vecTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, membershipVectorField,
				index.NewFlatIndex(entity.L2)))
			require.NoError(t, err)
			require.NoError(t, vecTask.Await(ctx))

			loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
			require.NoError(t, err)
			require.NoError(t, loadTask.Await(ctx))

			got := queryRoaringIDs(t, ctx, mc, collectionName,
				fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField), blob)
			expected := expectedMembershipIDs(membershipTotalRows, domain, map[int64]struct{}{
				0: {}, 1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {},
			})
			require.ElementsMatch(t, expected, got, "roaring_match row set mismatch under AUTOINDEX domain %d", domain)
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
		WithField(entity.NewField().WithName(membershipVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(membershipVectorDim))
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
		vec := make([]float32, membershipVectorDim)
		vec[0] = float32(i)
		vectors[i] = vec
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithInt8Column("i8", i8).
		WithInt16Column("i16", i16).
		WithInt32Column("i32", i32).
		WithInt64Column("i64", i64).
		WithFloatVectorColumn(membershipVectorField, membershipVectorDim, vectors))
	require.NoError(t, err)
	flushLoadMembership(t, ctx, mc, collectionName)

	// Straddle zero so a zero-extending narrow build/probe would place a negative
	// value in the wrong Roaring high container and miss.
	members := []int64{-shift, -30, -1, 0, 1, 17, 49}
	blob, err := client.NewRoaringBitmapBlob(members)
	require.NoError(t, err)

	for _, field := range []string{"i8", "i16", "i32", "i64"} {
		got := queryRoaringIDs(t, ctx, mc, collectionName,
			fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", field), blob)
		expected := make([]int64, 0, totalRows)
		for id := 0; id < totalRows; id++ {
			if isMember(members, int64(id%domain-shift)) {
				expected = append(expected, int64(id))
			}
		}
		require.ElementsMatchf(t, expected, got,
			"roaring_match must return every matching PK across integer widths on %s", field)
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
		WithField(entity.NewField().WithName(membershipCreatorField).WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(membershipVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(membershipVectorDim))
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
		v := make([]float32, membershipVectorDim)
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
		WithInt64Column(membershipCreatorField, creators).
		WithFloatVectorColumn(membershipVectorField, membershipVectorDim, vectors))
	require.NoError(t, err)
	flushLoadMembership(t, ctx, mc, collectionName)

	blob, err := client.NewRoaringBitmapBlob([]int64{math.MinInt64, math.MaxInt64, -1, 42})
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField), blob)
	require.Len(t, got, totalRows, "all four values are present, so every row must match")

	// A single absent extreme must match nothing.
	absent, err := client.NewRoaringBitmapBlob([]int64{math.MaxInt64 - 1})
	require.NoError(t, err)
	gotAbsent := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField), absent)
	require.Empty(t, gotAbsent)
}

// TestRoaringMatchNotEmptyBitmap verifies `not membership_match(field, {empty})`
// selects every non-NULL row (the exact complement of an empty set).
func TestRoaringMatchNotEmptyBitmap(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("roaring_notempty", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	blob, err := client.NewRoaringBitmapBlob([]int64{})
	require.NoError(t, err)

	got := queryRoaringIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("not membership_match(%s, {rb}, type=roaring)", membershipCreatorField), blob)
	require.Len(t, got, membershipTotalRows, "negating an empty bitmap must select every non-NULL row")
}

// TestRoaringMatchDeleteThreeStates verifies positive-empty, negated-nonempty,
// and negated-empty deletion (delete nothing, delete the complement, delete all).
func TestRoaringMatchDeleteThreeStates(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Positive empty: delete nothing.
	t.Run("positive_empty", func(t *testing.T) {
		collectionName := common.GenRandomString("roaring_del_pos_empty", 6)
		createIntMembershipCollection(t, ctx, mc, collectionName, false)
		insertIntMembershipRows(t, ctx, mc, collectionName, false)
		flushLoadMembership(t, ctx, mc, collectionName)

		blob, err := client.NewRoaringBitmapBlob([]int64{})
		require.NoError(t, err)
		dr, err := mc.Delete(ctx, client.NewDeleteOption(collectionName).
			WithExpr(fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", membershipCreatorField)).
			WithTemplateParam("rb", blob))
		require.NoError(t, err)
		require.EqualValues(t, 0, dr.DeleteCount)
		remaining := queryRoaringIDs(t, ctx, mc, collectionName, "id >= 0", nil)
		require.Len(t, remaining, membershipTotalRows, "empty positive delete must preserve every row")
	})

	// Negated set: delete everything outside the set.
	t.Run("negated", func(t *testing.T) {
		collectionName := common.GenRandomString("roaring_del_neg", 6)
		createIntMembershipCollection(t, ctx, mc, collectionName, false)
		insertIntMembershipRows(t, ctx, mc, collectionName, false)
		flushLoadMembership(t, ctx, mc, collectionName)

		keep := []int64{0, 1, 2}
		blob, err := client.NewRoaringBitmapBlob(keep)
		require.NoError(t, err)
		dr, err := mc.Delete(ctx, client.NewDeleteOption(collectionName).
			WithExpr(fmt.Sprintf("not membership_match(%s, {rb}, type=roaring)", membershipCreatorField)).
			WithTemplateParam("rb", blob))
		require.NoError(t, err)
		// Negating the set deletes every creator value except len(keep), i.e.
		// (domain - len(keep)) values; each value spans totalRows/domain rows.
		require.EqualValues(t, int64((membershipDomain-len(keep))*membershipTotalRows/membershipDomain), dr.DeleteCount)

		remaining := queryRoaringIDs(t, ctx, mc, collectionName, "id >= 0", nil)
		expected := expectedMembershipIDs(membershipTotalRows, membershipDomain, map[int64]struct{}{
			0: {}, 1: {}, 2: {},
		})
		require.ElementsMatch(t, expected, remaining,
			"negated delete must preserve exactly the rows whose creator is in the keep set")
	})

	// Negated empty: every non-NULL row is outside the empty set, so delete all.
	t.Run("negated_empty", func(t *testing.T) {
		collectionName := common.GenRandomString("roaring_del_neg_empty", 6)
		createIntMembershipCollection(t, ctx, mc, collectionName, false)
		insertIntMembershipRows(t, ctx, mc, collectionName, false)
		flushLoadMembership(t, ctx, mc, collectionName)

		blob, err := client.NewRoaringBitmapBlob([]int64{})
		require.NoError(t, err)
		dr, err := mc.Delete(ctx, client.NewDeleteOption(collectionName).
			WithExpr(fmt.Sprintf("not membership_match(%s, {rb}, type=roaring)", membershipCreatorField)).
			WithTemplateParam("rb", blob))
		require.NoError(t, err)
		require.EqualValues(t, membershipTotalRows, dr.DeleteCount)

		remaining := queryRoaringIDs(t, ctx, mc, collectionName, "id >= 0", nil)
		require.Empty(t, remaining, "negated empty delete must remove every row")
	})
}
