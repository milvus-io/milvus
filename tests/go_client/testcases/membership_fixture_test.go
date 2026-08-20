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

// Shared int64 membership fixture for the bloom_match / roaring_match tests: a
// primary-key id, an int64 creator_id field and a small float vector. The row
// count is kept above indexCoord.segment.minSegmentNumRowsToEnableIndex (1024)
// so scalar indexes are really built, not fake-finished.
package testcases

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
)

const (
	membershipCreatorField = "creator_id"
	membershipVectorField  = "vector"
	membershipVectorDim    = 8
	membershipTotalRows    = 2000
	membershipDomain       = 50
)

func intMembershipSchema(collectionName string, nullable bool) *entity.Schema {
	creatorField := entity.NewField().WithName(membershipCreatorField).WithDataType(entity.FieldTypeInt64)
	if nullable {
		creatorField.WithNullable(true)
	}
	return entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(creatorField).
		WithField(entity.NewField().WithName(membershipVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(membershipVectorDim))
}

func createIntMembershipCollection(t *testing.T, ctx CtxT, mc MC, collectionName string, nullable bool) {
	t.Helper()

	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, intMembershipSchema(collectionName, nullable)).
		WithConsistencyLevel(entity.ClStrong)))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collectionName)))
	})
}

func insertIntMembershipRows(t *testing.T, ctx CtxT, mc MC, collectionName string, nullable bool) {
	t.Helper()

	ids := make([]int64, membershipTotalRows)
	vectors := make([][]float32, membershipTotalRows)
	for i := 0; i < membershipTotalRows; i++ {
		ids[i] = int64(i)
		v := make([]float32, membershipVectorDim)
		v[0] = float32(i)
		vectors[i] = v
	}

	opt := client.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", ids).
		WithFloatVectorColumn(membershipVectorField, membershipVectorDim, vectors)

	if nullable {
		values := make([]int64, 0, membershipTotalRows)
		valid := make([]bool, membershipTotalRows)
		for i := 0; i < membershipTotalRows; i++ {
			if i%8 == 7 {
				valid[i] = false
				continue
			}
			valid[i] = true
			values = append(values, int64(i%membershipDomain))
		}
		col, err := column.NewNullableColumnInt64(membershipCreatorField, values, valid)
		require.NoError(t, err)
		opt.WithColumns(col)
	} else {
		creators := make([]int64, membershipTotalRows)
		for i := 0; i < membershipTotalRows; i++ {
			creators[i] = int64(i % membershipDomain)
		}
		opt.WithInt64Column(membershipCreatorField, creators)
	}

	_, err := mc.Insert(ctx, opt)
	require.NoError(t, err)
}

func flushLoadMembership(t *testing.T, ctx CtxT, mc MC, collectionName string) {
	t.Helper()

	flushTask, err := mc.Flush(ctx, client.NewFlushOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, flushTask.Await(ctx))

	vecTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collectionName, membershipVectorField,
		index.NewFlatIndex(entity.L2)))
	require.NoError(t, err)
	require.NoError(t, vecTask.Await(ctx))

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))
}

// queryMembershipIDs runs a filter query and returns the sorted primary keys.
// The template key differs per filter ("bf" for bloom_match, "rb" for
// roaring_match), so it is passed explicitly.
func queryMembershipIDs(t *testing.T, ctx CtxT, mc MC, collectionName, expr, templateKey string, blob any) []int64 {
	t.Helper()

	opt := client.NewQueryOption(collectionName).
		WithFilter(expr).WithOutputFields("id").
		WithConsistencyLevel(entity.ClStrong)
	if blob != nil {
		opt.WithTemplateParam(templateKey, blob)
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

func queryMembershipIDSet(t *testing.T, ctx CtxT, mc MC, collectionName, expr, templateKey string, blob any) map[int64]struct{} {
	t.Helper()
	m := make(map[int64]struct{})
	for _, id := range queryMembershipIDs(t, ctx, mc, collectionName, expr, templateKey, blob) {
		m[id] = struct{}{}
	}
	return m
}
