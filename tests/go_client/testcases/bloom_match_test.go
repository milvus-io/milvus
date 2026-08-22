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

// L0 coverage for the bloom_match membership filter through the public Go SDK.
package testcases

import (
	"context"
	"fmt"
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

func queryBloomIDs(t *testing.T, ctx CtxT, mc MC, collectionName, expr string, blob any) []int64 {
	t.Helper()
	return queryMembershipIDs(t, ctx, mc, collectionName, expr, "bf", blob)
}

func queryBloomIDSet(t *testing.T, ctx CtxT, mc MC, collectionName, expr string, blob any) map[int64]struct{} {
	t.Helper()
	return queryMembershipIDSet(t, ctx, mc, collectionName, expr, "bf", blob)
}

// TestBloomMatchZeroFalseNegatives verifies bloom_match has no row-level false
// negatives: exact `in` result is a subset of bloom_match result, and
// `not bloom_match` never leaks a true member.
func TestBloomMatchZeroFalseNegatives(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("bloom_zero_fn", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	members := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	blob, err := client.NewBloomFilterBlob(members, 0.001)
	require.NoError(t, err)

	exact := queryBloomIDSet(t, ctx, mc, collectionName,
		fmt.Sprintf("%s in [0,1,2,3,4,5,6,7,8,9]", membershipCreatorField), nil)
	got := queryBloomIDSet(t, ctx, mc, collectionName,
		fmt.Sprintf("bloom_match(%s, {bf})", membershipCreatorField), blob)
	require.NotEmpty(t, exact)
	for pk := range exact {
		_, ok := got[pk]
		require.Truef(t, ok, "bloom_match dropped true member id=%d (false negative)", pk)
	}

	notRs := queryBloomIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("not bloom_match(%s, {bf})", membershipCreatorField), blob)
	require.NotEmpty(t, notRs)
	for _, id := range notRs {
		creator := id % membershipDomain
		require.GreaterOrEqualf(t, creator, int64(10), "not bloom_match leaked a true member id=%d", id)
	}
}

// TestBloomMatchVarcharAndDomainMismatch verifies varchar membership has no
// false negatives and that a blob built from the wrong value domain is rejected.
func TestBloomMatchVarcharAndDomainMismatch(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("bloom_varchar", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("tag").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
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
	tags := make([]string, membershipTotalRows)
	creators := make([]int64, membershipTotalRows)
	vectors := make([][]float32, membershipTotalRows)
	for i := 0; i < membershipTotalRows; i++ {
		ids[i] = int64(i)
		tags[i] = fmt.Sprintf("tag%d", i%membershipDomain)
		creators[i] = int64(i % membershipDomain)
		v := make([]float32, membershipVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithVarcharColumn("tag", tags).
		WithInt64Column(membershipCreatorField, creators).
		WithFloatVectorColumn(membershipVectorField, membershipVectorDim, vectors))
	require.NoError(t, err)
	flushLoadMembership(t, ctx, mc, collectionName)

	strMembers := []string{"tag0", "tag1", "tag2", "tag3", "tag4"}
	strBlob, err := client.NewBloomFilterBlob(strMembers, 0.001)
	require.NoError(t, err)

	exact := queryBloomIDSet(t, ctx, mc, collectionName,
		`tag in ["tag0","tag1","tag2","tag3","tag4"]`, nil)
	bloomSet := queryBloomIDSet(t, ctx, mc, collectionName, "bloom_match(tag, {bf})", strBlob)
	for pk := range exact {
		_, ok := bloomSet[pk]
		require.Truef(t, ok, "bloom_match(varchar) dropped true member id=%d", pk)
	}

	// int64-domain blob on a varchar field must be rejected
	intBlob, err := client.NewBloomFilterBlob([]int64{0, 1, 2}, 0.001)
	require.NoError(t, err)
	_, err = mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter("bloom_match(tag, {bf})").WithOutputFields("id").
		WithTemplateParam("bf", intBlob).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "value domain but field")

	// utf8-domain blob on an int64 field must be rejected
	_, err = mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf("bloom_match(%s, {bf})", membershipCreatorField)).WithOutputFields("id").
		WithTemplateParam("bf", strBlob).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "value domain but field")
}

// TestBloomMatchMalformedBlobRejected verifies a malformed blob and a literal
// array argument are rejected rather than silently unfiltered.
func TestBloomMatchMalformedBlobRejected(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("bloom_malformed", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	// malformed MBF1 blob
	_, err := mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf("bloom_match(%s, {bf})", membershipCreatorField)).WithOutputFields("id").
		WithTemplateParam("bf", client.BloomFilterBlob([]byte("not-a-real-blob"))).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "bloom_match filter blob is invalid")

	// literal array argument is not a pre-built blob
	_, err = mc.Query(ctx, client.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf("bloom_match(%s, [1,2,3])", membershipCreatorField)).WithOutputFields("id").
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, false, "must be a {template} placeholder")
}

// TestBloomMatchNullRowsFoldToFalse verifies NULL rows never match bloom_match
// nor its negation (NULL folds to FALSE on both sides).
func TestBloomMatchNullRowsFoldToFalse(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("bloom_null", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(membershipCreatorField).WithDataType(entity.FieldTypeInt64).WithNullable(true)).
		WithField(entity.NewField().WithName(membershipVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(membershipVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	ids := make([]int64, membershipTotalRows)
	values := make([]int64, 0, membershipTotalRows)
	valid := make([]bool, membershipTotalRows)
	vectors := make([][]float32, membershipTotalRows)
	for i := 0; i < membershipTotalRows; i++ {
		ids[i] = int64(i)
		v := make([]float32, membershipVectorDim)
		v[0] = float32(i)
		vectors[i] = v
		if i%8 == 7 {
			valid[i] = false
			continue
		}
		valid[i] = true
		values = append(values, int64(i%membershipDomain))
	}
	col, err := column.NewNullableColumnInt64(membershipCreatorField, values, valid)
	require.NoError(t, err)
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithColumns(col).
		WithFloatVectorColumn(membershipVectorField, membershipVectorDim, vectors))
	require.NoError(t, err)
	flushLoadMembership(t, ctx, mc, collectionName)

	members := make([]int64, 0, membershipDomain)
	for v := 0; v < membershipDomain; v++ {
		members = append(members, int64(v))
	}
	blob, err := client.NewBloomFilterBlob(members, 0.001)
	require.NoError(t, err)

	got := queryBloomIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("bloom_match(%s, {bf})", membershipCreatorField), blob)
	for _, id := range got {
		require.NotEqualf(t, int64(7), id%8, "bloom_match matched a NULL row id=%d", id)
	}

	notRs := queryBloomIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("not bloom_match(%s, {bf})", membershipCreatorField), blob)
	for _, id := range notRs {
		require.NotEqualf(t, int64(7), id%8, "not bloom_match matched a NULL row id=%d", id)
	}
}

// TestBloomMatchScalarIndexTypeMatrix verifies bloom_match stays correct when a
// scalar index is built on the field before load. STL_SORT reports
// has_raw_data=true, so the loader drops the raw column and bloom_match must
// recover each value via the index's per-row Reverse_Lookup; INVERTED keeps the
// raw column and uses the data path. Both must remain zero-false-negative.
func TestBloomMatchScalarIndexTypeMatrix(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	members := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	blob, err := client.NewBloomFilterBlob(members, 0.001)
	require.NoError(t, err)

	for _, indexType := range []string{"STL_SORT", "INVERTED"} {
		t.Run(indexType, func(t *testing.T) {
			collectionName := common.GenRandomString("bloom_idx_"+indexType, 6)
			createIntMembershipCollection(t, ctx, mc, collectionName, false)
			insertIntMembershipRows(t, ctx, mc, collectionName, false)

			flushTask, err := mc.Flush(ctx, client.NewFlushOption(collectionName))
			require.NoError(t, err)
			require.NoError(t, flushTask.Await(ctx))

			// Build the scalar index on creator_id BEFORE the first load so the
			// loader materializes it and (for STL_SORT) drops the raw column.
			var scalarIndex index.Index
			switch indexType {
			case "STL_SORT":
				scalarIndex = index.NewSortedIndex()
			case "INVERTED":
				scalarIndex = index.NewInvertedIndex()
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

			exact := queryBloomIDSet(t, ctx, mc, collectionName,
				fmt.Sprintf("%s in [0,1,2,3,4,5,6,7,8,9]", membershipCreatorField), nil)
			got := queryBloomIDSet(t, ctx, mc, collectionName,
				fmt.Sprintf("bloom_match(%s, {bf})", membershipCreatorField), blob)
			require.NotEmpty(t, exact)
			for pk := range exact {
				_, ok := got[pk]
				require.Truef(t, ok, "bloom_match dropped true member id=%d (false negative) under %s", pk, indexType)
			}
		})
	}
}

// TestBloomMatchAutoIndex verifies bloom_match stays correct when the field is
// indexed with AUTOINDEX (the default entry point when no index type is
// specified). AUTOINDEX resolves to HYBRID, which internally selects BITMAP for
// low-cardinality data and STLSORT for high-cardinality data; either way the
// loader may drop the raw column and bloom_match must recover values through the
// index's per-row Reverse_Lookup.
func TestBloomMatchAutoIndex(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	members := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	blob, err := client.NewBloomFilterBlob(members, 0.001)
	require.NoError(t, err)

	// 200 >= 100 -> STLSORT, 50 < 100 -> BITMAP. The 100-cardinality cutoff is
	// an internal HYBRID-index detail: if it changes, this test still passes but
	// its two-path (STLSORT/BITMAP) coverage silently degrades.
	for _, domain := range []int{200, 50} {
		t.Run(fmt.Sprintf("domain_%d", domain), func(t *testing.T) {
			collectionName := common.GenRandomString("bloom_auto", 6)
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

			exact := queryBloomIDSet(t, ctx, mc, collectionName,
				fmt.Sprintf("%s in [0,1,2,3,4,5,6,7,8,9]", membershipCreatorField), nil)
			got := queryBloomIDSet(t, ctx, mc, collectionName,
				fmt.Sprintf("bloom_match(%s, {bf})", membershipCreatorField), blob)
			require.NotEmpty(t, exact)
			for pk := range exact {
				_, ok := got[pk]
				require.Truef(t, ok, "bloom_match dropped true member id=%d under AUTOINDEX domain %d", pk, domain)
			}
		})
	}
}

// TestBloomMatchRejectedInDelete verifies bloom_match is rejected in a delete
// expression: it has false positives, so a delete driven by it would remove rows
// outside the caller's set (roaring_match, being exact, is allowed instead).
func TestBloomMatchRejectedInDelete(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("bloom_delete", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	blob, err := client.NewBloomFilterBlob([]int64{0, 1, 2}, 0.001)
	require.NoError(t, err)

	_, err = mc.Delete(ctx, client.NewDeleteOption(collectionName).
		WithExpr(fmt.Sprintf("bloom_match(%s, {bf})", membershipCreatorField)).
		WithTemplateParam("bf", blob))
	common.CheckErr(t, err, false, "bloom_match is approximate and cannot be used in delete expressions")
}

// TestBloomMatchIntTypeMatrix verifies one int64-built blob probes every integer
// field width (INT8/16/32/64 all widen to int64 before hashing).
func TestBloomMatchIntTypeMatrix(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("bloom_intmatrix", 6)

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

	ids := make([]int64, membershipTotalRows)
	i8 := make([]int8, membershipTotalRows)
	i16 := make([]int16, membershipTotalRows)
	i32 := make([]int32, membershipTotalRows)
	i64 := make([]int64, membershipTotalRows)
	vectors := make([][]float32, membershipTotalRows)
	for i := 0; i < membershipTotalRows; i++ {
		ids[i] = int64(i)
		i8[i] = int8(i % membershipDomain)
		i16[i] = int16(i % membershipDomain)
		i32[i] = int32(i % membershipDomain)
		i64[i] = int64(i % membershipDomain)
		v := make([]float32, membershipVectorDim)
		v[0] = float32(i)
		vectors[i] = v
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

	blob, err := client.NewBloomFilterBlob([]int64{0, 1, 2, 3, 4}, 0.001)
	require.NoError(t, err)

	for _, field := range []string{"i8", "i16", "i32", "i64"} {
		exact := queryBloomIDSet(t, ctx, mc, collectionName,
			fmt.Sprintf("%s in [0,1,2,3,4]", field), nil)
		got := queryBloomIDSet(t, ctx, mc, collectionName,
			fmt.Sprintf("bloom_match(%s, {bf})", field), blob)
		require.NotEmpty(t, exact)
		for pk := range exact {
			_, ok := got[pk]
			require.Truef(t, ok, "bloom_match dropped true member id=%d on %s", pk, field)
		}
	}
}

// TestBloomMatchJsonPathStrictTyping verifies JSON-path membership is strictly
// typed: int-encoded members match, float-encoded values (e.g. 5.0) never match
// an int64 member 5, and rows missing the key match under neither polarity.
func TestBloomMatchJsonPathStrictTyping(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("bloom_json", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("meta").WithDataType(entity.FieldTypeJSON)).
		WithField(entity.NewField().WithName(membershipVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(membershipVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	const totalRows = 1000
	ids := make([]int64, totalRows)
	jsonValues := make([][]byte, totalRows)
	vectors := make([][]float32, totalRows)
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		v := make([]float32, membershipVectorDim)
		v[0] = float32(i)
		vectors[i] = v
		switch {
		case i%11 == 0:
			jsonValues[i] = []byte(`{"other": 1}`) // missing key
		case i%3 == 0:
			jsonValues[i] = []byte(fmt.Sprintf(`{"uid": %d.0}`, i%10)) // float-encoded
		default:
			jsonValues[i] = []byte(fmt.Sprintf(`{"uid": %d}`, i%10)) // int-encoded
		}
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithColumns(column.NewColumnJSONBytes("meta", jsonValues)).
		WithFloatVectorColumn(membershipVectorField, membershipVectorDim, vectors))
	require.NoError(t, err)
	flushLoadMembership(t, ctx, mc, collectionName)

	blob, err := client.NewBloomFilterBlob([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0.001)
	require.NoError(t, err)

	bloomRes := queryBloomIDs(t, ctx, mc, collectionName, `bloom_match(meta["uid"], {bf})`, blob)
	require.NotEmpty(t, bloomRes)
	for _, id := range bloomRes {
		// int-encoded member rows only; float-encoded (i%3==0) and missing-key
		// (i%11==0) rows must never match.
		require.Falsef(t, id%11 == 0, "bloom_match(json) matched a missing-key row id=%d", id)
		require.Falsef(t, id%3 == 0, "bloom_match(json) matched a float-encoded row id=%d", id)
	}

	// The divergence from exact `in`: `in` unifies 5.0 == 5, so it returns the
	// float-encoded rows too; bloom_match, strictly typed, does not.
	exactRes := queryBloomIDs(t, ctx, mc, collectionName, `meta["uid"] in [0,1,2,3,4,5,6,7,8,9]`, nil)
	exactSet := make(map[int64]struct{}, len(exactRes))
	for _, id := range exactRes {
		exactSet[id] = struct{}{}
	}
	bloomSet := make(map[int64]struct{}, len(bloomRes))
	for _, id := range bloomRes {
		bloomSet[id] = struct{}{}
	}
	floatEncodedPresent := 0
	for id := range exactSet {
		if id%3 == 0 && id%11 != 0 {
			floatEncodedPresent++
			_, inBloom := bloomSet[id]
			require.Falsef(t, inBloom, "float-encoded row id=%d must be in exact `in` but NOT in bloom_match", id)
		}
	}
	require.Positive(t, floatEncodedPresent, "fixture must contain float-encoded member rows")
}

// TestBloomMatchJsonWholeDocNestedAndDynamicPath verifies the JSON-path forms
// beyond a single key: the whole JSON document, a nested path, and a dynamic
// field (an unknown identifier resolving to a $meta path). Each is strictly
// typed, so a float-encoded value never matches an int64 member.
func TestBloomMatchJsonWholeDocNestedAndDynamicPath(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("bloom_json_forms", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("meta").WithDataType(entity.FieldTypeJSON)).
		WithField(entity.NewField().WithName(membershipVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(membershipVectorDim))
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	const totalRows = 1000
	ids := make([]int64, totalRows)
	jsonValues := make([][]byte, totalRows)
	vectors := make([][]float32, totalRows)
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		v := make([]float32, membershipVectorDim)
		v[0] = float32(i)
		vectors[i] = v
		switch i % 3 {
		case 0:
			// nested object: {"a": {"b": <int>}} — int-encoded
			jsonValues[i] = []byte(fmt.Sprintf(`{"a": {"b": %d}}`, i%10))
		case 1:
			// nested object with float-encoded value
			jsonValues[i] = []byte(fmt.Sprintf(`{"a": {"b": %d.0}}`, i%10))
		default:
			// bare scalar whole-document value (int)
			jsonValues[i] = []byte(fmt.Sprintf(`%d`, i%10))
		}
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithColumns(column.NewColumnJSONBytes("meta", jsonValues)).
		WithFloatVectorColumn(membershipVectorField, membershipVectorDim, vectors))
	require.NoError(t, err)
	flushLoadMembership(t, ctx, mc, collectionName)

	blob, err := client.NewBloomFilterBlob([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0.001)
	require.NoError(t, err)

	// nested path: meta["a"]["b"] — int-encoded rows (i%3==0) match, float-encoded
	// rows (i%3==1) never match.
	nested := queryBloomIDs(t, ctx, mc, collectionName, `bloom_match(meta["a"]["b"], {bf})`, blob)
	require.NotEmpty(t, nested)
	for _, id := range nested {
		require.EqualValuesf(t, 0, id%3, "bloom_match(nested) matched a non-int row id=%d", id)
	}

	// whole document: meta — bare-scalar int rows (i%3==2) match.
	whole := queryBloomIDs(t, ctx, mc, collectionName, `bloom_match(meta, {bf})`, blob)
	require.NotEmpty(t, whole)
	for _, id := range whole {
		require.EqualValuesf(t, 2, id%3, "bloom_match(whole doc) matched a non-bare-scalar row id=%d", id)
	}
}

// TestBloomMatchDynamicFieldPath verifies bloom_match over a dynamic field: an
// unknown identifier resolves to a $meta path, strictly typed per row value.
func TestBloomMatchDynamicFieldPath(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("bloom_dynfield", 6)

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(membershipVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(membershipVectorDim)).
		WithDynamicFieldEnabled(true)
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema).
		WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})

	const totalRows = 1000
	ids := make([]int64, totalRows)
	dynInts := make([]int64, totalRows)
	vectors := make([][]float32, totalRows)
	for i := 0; i < totalRows; i++ {
		ids[i] = int64(i)
		dynInts[i] = int64(i % 10)
		v := make([]float32, membershipVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithInt64Column("uid", dynInts).
		WithFloatVectorColumn(membershipVectorField, membershipVectorDim, vectors))
	require.NoError(t, err)
	flushLoadMembership(t, ctx, mc, collectionName)

	blob, err := client.NewBloomFilterBlob([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0.001)
	require.NoError(t, err)

	// "uid" is not a schema field -> resolves to a $meta dynamic path.
	res := queryBloomIDs(t, ctx, mc, collectionName, `bloom_match(uid, {bf})`, blob)
	require.NotEmpty(t, res)
	// Every member is present, so all rows match.
	require.Len(t, res, totalRows)
}

// TestBloomMatchGrowingAndSealedMixed verifies bloom_match evaluates both sealed
// and growing segments in one query. A blob spanning a sealed member (0..4) and a
// growing-only member (500..504) must return rows from both.
func TestBloomMatchGrowingAndSealedMixed(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("bloom_growing", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	// A second batch NOT flushed stays in a growing segment; creator ids 500..509
	// are disjoint from the sealed batch's 0..49.
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

	blob, err := client.NewBloomFilterBlob([]int64{0, 1, 2, 3, 4, 500, 501, 502, 503, 504}, 0.001)
	require.NoError(t, err)

	got := queryBloomIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("bloom_match(%s, {bf})", membershipCreatorField), blob)
	require.NotEmpty(t, got)
	sawSealed, sawGrowing := false, false
	for _, id := range got {
		if id < membershipTotalRows {
			sawSealed = true
		} else {
			sawGrowing = true
		}
	}
	require.True(t, sawSealed, "bloom_match missed sealed-segment members")
	require.True(t, sawGrowing, "bloom_match missed growing-segment members")
}

// TestBloomMatchFalsePositiveRateSanity verifies the measured false-positive
// count over the disjoint (non-member) probe rows is bounded and in line with the
// configured fpr. The assertion is a loose upper bound (the exact fpr is
// statistical); it exists to catch a filter that is effectively unfiltered.
func TestBloomMatchFalsePositiveRateSanity(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("bloom_fpr", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	// Members 0..9; creators 10..49 are the disjoint (non-member) probe rows.
	// totalRows/domain rows per value, so the non-member population is
	// (domain-10) * totalRows/domain rows.
	blob, err := client.NewBloomFilterBlob([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0.05)
	require.NoError(t, err)

	got := queryBloomIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("bloom_match(%s, {bf})", membershipCreatorField), blob)
	require.NotEmpty(t, got)
	fp := 0
	for _, id := range got {
		creator := id % membershipDomain
		if creator >= 10 {
			fp++
		}
	}
	nonMemberRows := (membershipDomain - 10) * membershipTotalRows / membershipDomain
	// At fpr=0.05 the expected false-positive count is ~5% of the non-member
	// population; reject only if it is wildly above that (the filter is broken),
	// keeping the bound loose enough to never be flaky.
	require.Lessf(t, fp, nonMemberRows*3/10,
		"bloom_match false-positive count %d far exceeds fpr=0.05 over %d non-member rows", fp, nonMemberRows)
}

// TestBloomMatchEmptyBlob verifies an empty membership blob (0 members) matches
// nothing and is not rejected.
func TestBloomMatchEmptyBlob(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collectionName := common.GenRandomString("bloom_empty", 6)

	createIntMembershipCollection(t, ctx, mc, collectionName, false)
	insertIntMembershipRows(t, ctx, mc, collectionName, false)
	flushLoadMembership(t, ctx, mc, collectionName)

	blob, err := client.NewBloomFilterBlob([]int64{}, 0.001)
	require.NoError(t, err)

	got := queryBloomIDs(t, ctx, mc, collectionName,
		fmt.Sprintf("bloom_match(%s, {bf})", membershipCreatorField), blob)
	require.Empty(t, got, "empty blob must match nothing")
}

// TestBloomMatchFprOutOfRange verifies the client builder rejects an out-of-range
// fpr rather than building a filter the server would later reject.
func TestBloomMatchFprOutOfRange(t *testing.T) {
	_, err := client.NewBloomFilterBlob([]int64{0, 1, 2}, 0.00001)
	require.Error(t, err, "fpr below [0.0001, 0.05] must be rejected")
	_, err = client.NewBloomFilterBlob([]int64{0, 1, 2}, 0.06)
	require.Error(t, err, "fpr above [0.0001, 0.05] must be rejected")
}
