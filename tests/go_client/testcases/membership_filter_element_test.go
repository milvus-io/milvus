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

// L0 coverage for membership-filter rejection inside struct-array element
// predicates through the public Go SDK.
package testcases

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const membershipElementRows = 200

// setupMembershipElementCollection builds a small struct-array collection with a
// doc-level int64 field (doc_int) and an int64 element sub-field, inserts, flushes,
// indexes and loads it. Used to exercise parser-level rejection of bloom_match /
// roaring_match inside element predicates.
func setupMembershipElementCollection(t *testing.T, ctx CtxT, mc MC, namePrefix string) string {
	collName := common.GenRandomString(namePrefix, 6)
	opt := hp.DefaultStructAElementSchemaOption(collName)
	opt.IncludeDocVChar = false
	opt.IncludeCategory = false

	schema, structSchema := hp.CreateStructAElementSchema(opt)
	require.NoError(t, mc.CreateCollection(ctx,
		client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collName)))
	})

	ds := hp.GenerateStructAElementData(membershipElementRows, 0, opt)
	insertElemDataset(t, ctx, mc, collName, structSchema, ds, opt)

	flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
	require.NoError(t, err)
	require.NoError(t, flushTask.Await(ctx))

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "normal_vector",
		index.NewHNSWIndex(entity.COSINE, 16, 200)))
	require.NoError(t, err)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "structA[embedding]",
		index.NewHNSWIndex(entity.MaxSimCosine, 16, 200)))
	require.NoError(t, err)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))
	return collName
}

// TestMembershipFilterRejectedInElementPredicates verifies bloom_match and
// roaring_match are rejected inside element_filter / MATCH_* element predicates,
// while a document-level sibling conjunction stays legal.
func TestMembershipFilterRejectedInElementPredicates(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collName := setupMembershipElementCollection(t, ctx, mc, "memb_elem")

	bloomBlob, err := client.NewBloomFilterBlob([]int64{0, 1, 2}, 0.001)
	require.NoError(t, err)
	roaringBlob, err := client.NewRoaringBitmapBlob([]int64{0, 1, 2})
	require.NoError(t, err)

	query := func(expr string, tmplKey string, tmplVal any) error {
		opt := client.NewQueryOption(collName).
			WithFilter(expr).WithOutputFields("id").
			WithConsistencyLevel(entity.ClStrong)
		if tmplKey != "" {
			opt.WithTemplateParam(tmplKey, tmplVal)
		}
		_, err := mc.Query(ctx, opt)
		return err
	}

	t.Run("bloom membership inside element_filter rejected", func(t *testing.T) {
		err := query(`element_filter(structA, membership_match(doc_int, {bf}, type=bloom) && $[int_val] > 0)`, "bf", bloomBlob)
		common.CheckErr(t, err, false, "membership_match filters are not supported inside element_filter")
	})

	t.Run("roaring membership inside element_filter rejected", func(t *testing.T) {
		err := query(`element_filter(structA, membership_match(doc_int, {rb}, type=roaring) && $[int_val] > 0)`, "rb", roaringBlob)
		common.CheckErr(t, err, false, "membership_match filters are not supported inside element_filter")
	})

	t.Run("bloom_match inside MATCH_ANY rejected", func(t *testing.T) {
		err := query(`MATCH_ANY(structA, membership_match(doc_int, {bf}, type=bloom) && $[int_val] > 0)`, "bf", bloomBlob)
		common.CheckErr(t, err, false, "function calls are not supported inside MATCH predicate")
	})

	t.Run("document-level sibling conjunction stays legal", func(t *testing.T) {
		expr := "membership_match(doc_int, {bf}, type=bloom) && element_filter(structA, $[int_val] > 0)"
		got := queryMembershipIDs(t, ctx, mc, collName, expr, "bf", bloomBlob)
		gotSet := int64IDSet(got)
		require.Len(t, gotSet, len(got), "sibling conjunction returned duplicate PKs")
		expected := map[int64]struct{}{0: {}, 1: {}, 2: {}}
		requireBloomResult(t, gotSet, expected, membershipElementRows, "document-level sibling conjunction")
	})

	t.Run("bloom_match on struct sub-field rejected", func(t *testing.T) {
		// A struct-array sub-field is an ARRAY type, which bloom_match does not
		// support (only INT8/16/32/64/VARCHAR/JSON paths).
		err := query(`membership_match(structA[int_val], {bf}, type=bloom)`, "bf", bloomBlob)
		common.CheckErr(t, err, false, "membership_match only supports INT8/INT16/INT32/INT64/VARCHAR fields and JSON paths")
	})

	t.Run("roaring_match on struct sub-field rejected", func(t *testing.T) {
		err := query(`membership_match(structA[int_val], {rb}, type=roaring)`, "rb", roaringBlob)
		common.CheckErr(t, err, false, "membership_match only supports INT8/INT16/INT32/INT64 fields")
	})

	t.Run("bloom_match on struct varchar sub-field rejected", func(t *testing.T) {
		// A VARCHAR-typed struct sub-field is still an ARRAY to the parser.
		strBlob, err := client.NewBloomFilterBlob([]string{"a", "b"}, 0.001)
		require.NoError(t, err)
		err = query(`membership_match(structA[str_val], {bf}, type=bloom)`, "bf", strBlob)
		common.CheckErr(t, err, false, "membership_match only supports INT8/INT16/INT32/INT64/VARCHAR fields and JSON paths")
	})

	t.Run("bloom_match on struct float sub-field rejected", func(t *testing.T) {
		err := query(`membership_match(structA[float_val], {bf}, type=bloom)`, "bf", bloomBlob)
		common.CheckErr(t, err, false, "membership_match only supports INT8/INT16/INT32/INT64/VARCHAR fields and JSON paths")
	})

	t.Run("roaring_match on struct varchar sub-field rejected", func(t *testing.T) {
		err := query(`membership_match(structA[str_val], {rb}, type=roaring)`, "rb", roaringBlob)
		common.CheckErr(t, err, false, "membership_match only supports INT8/INT16/INT32/INT64 fields")
	})
}
