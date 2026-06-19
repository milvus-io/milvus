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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// End-to-end tests for ARRAY_APPEND / ARRAY_REMOVE partial-update
// operators on Array fields. Unlike the in-process integration test,
// these run against a live Milvus deployment through the Go SDK.

const (
	arrayPartialOpDim      = 4
	arrayPartialOpCapacity = 16
	arrayPartialOpTagsFld  = "tags"
)

// setupArrayPartialOpCollection creates a minimal collection
// (pk int64, vec float-vec dim=4, tags Array<Int64> max_capacity=16),
// inserts seed rows (one per element in seeds), then indexes + loads
// the collection so it is queryable.
func setupArrayPartialOpCollection(
	ctx context.Context, t *testing.T, mc *base.MilvusClient,
	collName string, seeds [][]int64,
) *entity.Schema {
	schema := entity.NewSchema().WithName(collName).
		WithField(entity.NewField().
			WithName(common.DefaultInt64FieldName).
			WithDataType(entity.FieldTypeInt64).
			WithIsPrimaryKey(true)).
		WithField(entity.NewField().
			WithName(common.DefaultFloatVecFieldName).
			WithDataType(entity.FieldTypeFloatVector).
			WithDim(arrayPartialOpDim)).
		WithField(entity.NewField().
			WithName(arrayPartialOpTagsFld).
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeInt64).
			WithMaxCapacity(arrayPartialOpCapacity))

	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema))
	common.CheckErr(t, err, true)

	pks := make([]int64, len(seeds))
	vecs := make([][]float32, len(seeds))
	for i := range seeds {
		pks[i] = int64(i)
		row := make([]float32, arrayPartialOpDim)
		for j := 0; j < arrayPartialOpDim; j++ {
			row[j] = float32((i*arrayPartialOpDim+j)%7) / 10.0
		}
		vecs[i] = row
	}
	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(
			column.NewColumnInt64(common.DefaultInt64FieldName, pks),
			column.NewColumnFloatVector(common.DefaultFloatVecFieldName, arrayPartialOpDim, vecs),
			column.NewColumnInt64Array(arrayPartialOpTagsFld, seeds),
		))
	common.CheckErr(t, err, true)

	_, err = mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)

	indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(
		collName, common.DefaultFloatVecFieldName, index.NewAutoIndex(entity.COSINE)))
	common.CheckErr(t, err, true)
	require.NoError(t, indexTask.Await(ctx))

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	require.NoError(t, loadTask.Await(ctx))

	return schema
}

// queryTagsByPK returns pk → tags map for all rows with pk >= 0.
func queryTagsByPK(
	ctx context.Context, t *testing.T, mc *base.MilvusClient, collName string,
) map[int64][]int64 {
	resSet, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter(fmt.Sprintf("%s >= 0", common.DefaultInt64FieldName)).
		WithOutputFields(common.DefaultInt64FieldName, arrayPartialOpTagsFld).
		WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)

	var pks []int64
	var tags [][]int64
	for _, col := range resSet.Fields {
		switch col.Name() {
		case common.DefaultInt64FieldName:
			pks = col.(*column.ColumnInt64).Data()
		case arrayPartialOpTagsFld:
			tags = col.(*column.ColumnInt64Array).Data()
		}
	}
	require.Len(t, tags, len(pks))
	out := make(map[int64][]int64, len(pks))
	for i, pk := range pks {
		out[pk] = tags[i]
	}
	return out
}

// equalAsMultiset returns true when got and want contain the same
// elements with the same multiplicity, ignoring order.
func equalAsMultiset(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	a := append([]int64(nil), got...)
	b := append([]int64(nil), want...)
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestArrayPartialOpAppend(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("array_partial_op_append_", 6)
	_ = setupArrayPartialOpCollection(ctx, t, mc,
		collName, [][]int64{{1, 2}, {10, 20}})

	// Append: row 0 payload = [3,4]; row 1 payload = [30].
	// Expected: row 0 → [1,2,3,4], row 1 → [10,20,30].
	_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(
			column.NewColumnInt64(common.DefaultInt64FieldName, []int64{0, 1}),
			column.NewColumnInt64Array(arrayPartialOpTagsFld,
				[][]int64{{3, 4}, {30}}),
		).
		WithArrayAppend(arrayPartialOpTagsFld))
	common.CheckErr(t, err, true)

	got := queryTagsByPK(ctx, t, mc, collName)
	require.True(t, equalAsMultiset(got[0], []int64{1, 2, 3, 4}), "row 0 got=%v", got[0])
	require.True(t, equalAsMultiset(got[1], []int64{10, 20, 30}), "row 1 got=%v", got[1])
}

func TestArrayPartialOpRemove(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("array_partial_op_remove_", 6)
	_ = setupArrayPartialOpCollection(ctx, t, mc,
		collName, [][]int64{{1, 2, 3, 2, 1}, {10, 20, 30}})

	// Remove: row 0 payload = [2] → [1,3,1]; row 1 payload = [40] → no-op.
	_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(
			column.NewColumnInt64(common.DefaultInt64FieldName, []int64{0, 1}),
			column.NewColumnInt64Array(arrayPartialOpTagsFld,
				[][]int64{{2}, {40}}),
		).
		WithArrayRemove(arrayPartialOpTagsFld))
	common.CheckErr(t, err, true)

	got := queryTagsByPK(ctx, t, mc, collName)
	require.True(t, equalAsMultiset(got[0], []int64{1, 3, 1}), "row 0 got=%v", got[0])
	require.True(t, equalAsMultiset(got[1], []int64{10, 20, 30}), "row 1 got=%v", got[1])
}

func TestArrayPartialOpAppendExceedsCapacity(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("array_partial_op_overflow_", 6)
	// Seed with length-15 base; appending 5 more exceeds capacity 16.
	base := make([]int64, 15)
	for i := range base {
		base[i] = int64(i)
	}
	payload := make([]int64, 5)
	for i := range payload {
		payload[i] = int64(100 + i)
	}
	_ = setupArrayPartialOpCollection(ctx, t, mc, collName, [][]int64{base})

	_, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithColumns(
			column.NewColumnInt64(common.DefaultInt64FieldName, []int64{0}),
			column.NewColumnInt64Array(arrayPartialOpTagsFld, [][]int64{payload}),
		).
		WithArrayAppend(arrayPartialOpTagsFld))
	common.CheckErr(t, err, false, "max_capacity")
}

// TestArrayPartialOpAutoPromotesPartialUpdate asserts that a non-REPLACE
// op alone is enough to trigger partial-update semantics: the caller
// does not need to set partial_update=true explicitly.
func TestArrayPartialOpAutoPromotesPartialUpdate(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("array_partial_op_auto_", 6)
	_ = setupArrayPartialOpCollection(ctx, t, mc, collName, [][]int64{{1, 2}})

	// Payload intentionally omits the vector field. Without partial-update
	// semantics the server would reject the request for missing fields;
	// here the op must auto-promote partial_update=true so only the tags
	// field is merged.
	opt := client.NewColumnBasedInsertOption(collName).
		WithColumns(
			column.NewColumnInt64(common.DefaultInt64FieldName, []int64{0}),
			column.NewColumnInt64Array(arrayPartialOpTagsFld, [][]int64{{3}}),
		).
		WithFieldPartialOp(arrayPartialOpTagsFld, schemapb.FieldPartialUpdateOp_ARRAY_APPEND)
	_, err := mc.Upsert(ctx, opt)
	common.CheckErr(t, err, true)

	got := queryTagsByPK(ctx, t, mc, collName)
	require.True(t, equalAsMultiset(got[0], []int64{1, 2, 3}), "row 0 got=%v", got[0])
}
