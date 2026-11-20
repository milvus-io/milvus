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

func TestStructArrayColumnInsertPreservesNestedNulls(t *testing.T) {
	for _, sparse := range []bool{false, true} {
		name := "compact"
		if sparse {
			name = "sparse"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			mc := hp.CreateDefaultMilvusClient(ctx, t)
			collName := common.GenRandomString("struct_direct_", 6)
			children := entity.NewStructSchema().
				WithField(entity.NewField().WithName("score").WithDataType(entity.FieldTypeInt64).WithNullable(true)).
				WithField(entity.NewField().WithName("tags").WithDataType(entity.FieldTypeArray).
					WithElementType(entity.FieldTypeInt64).WithMaxCapacity(4).WithNullable(true).WithElementNullable(true))
			schema := entity.NewSchema().WithName(collName).
				WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
				WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(elemNullDim)).
				WithField(entity.NewField().WithName("clips").WithDataType(entity.FieldTypeArray).
					WithElementType(entity.FieldTypeStruct).WithMaxCapacity(4).WithNullable(true).WithStructSchema(children))
			require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong)))
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				require.NoError(t, mc.DropCollection(cleanup, client.NewDropCollectionOption(collName)))
			})
			scores := [][]int64{{1, 3}, {}, {}}
			scoreValid := [][]bool{{true, false, true}, {}, {false, false}}
			tags := [][][]int64{{{1}, {}}, {}, {}}
			tagValid := [][]bool{{true, false, true}, {}, {false, false}}
			leafValid := [][][]bool{{{true, false}, {}}, {}, {}}
			if sparse {
				scores = [][]int64{{1, 0, 3}, nil, {}, {0, 0}}
				scoreValid = [][]bool{{true, false, true}, nil, {}, {false, false}}
				tags = [][][]int64{{{1, 0}, nil, {}}, nil, {}, {nil, nil}}
				tagValid = [][]bool{{true, false, true}, nil, {}, {false, false}}
				leafValid = [][][]bool{{{true, false}, nil, {}}, nil, {}, {nil, nil}}
			}
			opts := []column.ElementNullableOption{column.WithRowValidData([]bool{true, false, true, true}), column.WithElementSparseMode(sparse)}
			scoreCol, err := column.NewElementNullableColumnInt64Array("score", scores, scoreValid, opts...)
			require.NoError(t, err)
			tagCol, err := column.NewElementNullableColumnInt64ArrayArray("tags", tags, tagValid, leafValid, opts...)
			require.NoError(t, err)
			clips := column.NewColumnStructArray("clips", []column.Column{scoreCol, tagCol})
			_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
				WithInt64Column("id", []int64{0, 1, 2, 3}).
				WithFloatVectorColumn("vector", elemNullDim, [][]float32{elemNullVec(0), elemNullVec(1), elemNullVec(2), elemNullVec(3)}).
				WithColumns(clips))
			require.NoError(t, err)
			build, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, "vector", index.NewFlatIndex(entity.L2)))
			require.NoError(t, err)
			require.NoError(t, build.Await(ctx))
			load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
			require.NoError(t, err)
			require.NoError(t, load.Await(ctx))
			rows, rs := elemNullQueryClips(t, ctx, mc, collName, "id >= 0")
			require.Len(t, rows, 4)
			require.Equal(t, []*int64{new(int64(1)), nil, new(int64(3))}, rows[0]["score"])
			require.Equal(t, [][]*int64{{new(int64(1)), nil}, nil, {}}, rows[0]["tags"])
			require.Nil(t, rows[1])
			require.Empty(t, rows[2]["score"])
			require.Empty(t, rows[2]["tags"])
			require.Equal(t, []*int64{nil, nil}, rows[3]["score"])
			require.Equal(t, [][]*int64{nil, nil}, rows[3]["tags"])
			for i := range rs.ResultCount {
				id, err := rs.GetColumn("id").GetAsInt64(i)
				require.NoError(t, err)
				if id != 0 {
					continue
				}
				for _, sub := range rs.GetColumn("clips").(column.StructArrayColumn).Fields() {
					valid, err := sub.(column.ElementNullableColumn).ElementValidData(i)
					require.NoError(t, err)
					require.Equal(t, []bool{true, false, true}, valid)
					if sub.Name() == "tags" {
						leaf, err := sub.(column.NestedArrayColumn).LeafValidData(i, 0)
						require.NoError(t, err)
						require.Equal(t, []bool{true, false}, leaf)
					}
				}
			}
			_, filtered := elemNullQueryClips(t, ctx, mc, collName, "element_filter(clips, $[score] IS NULL)")
			require.Equal(t, 2, filtered.ResultCount)
			for i := range filtered.ResultCount {
				id, err := filtered.GetColumn("id").GetAsInt64(i)
				require.NoError(t, err)
				require.Equal(t, map[int64][]int64{0: {1}, 3: {0, 1}}[id], filtered.QueryElementOffsets[i])
			}
		})
	}
}

func TestStructArrayAddNullableParentOptionRoundTrip(t *testing.T) {
	ctx := t.Context()
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	name := elemNullCreate(t, ctx, mc, "whole")
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanup, client.NewDropCollectionOption(name)))
	})
	elemNullInsert(t, ctx, mc, name, 0, elemNullRows)
	elemNullIndexAndLoad(t, ctx, mc, name)
	children := entity.NewStructSchema().WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeInt64).WithNullable(true))
	field := entity.NewField().WithName("extra").WithDataType(entity.FieldTypeArray).
		WithElementType(entity.FieldTypeStruct).WithMaxCapacity(4).WithStructSchema(children)
	err := mc.AddCollectionStructField(ctx, client.NewAddCollectionStructFieldOption(name, field))
	require.ErrorContains(t, err, "must be nullable")
	field.WithNullable(true)
	require.NoError(t, mc.AddCollectionStructField(ctx, client.NewAddCollectionStructFieldOption(name, field)))
	flush, err := mc.Flush(ctx, client.NewFlushOption(name))
	require.NoError(t, err)
	require.NoError(t, flush.Await(ctx))
	require.NoError(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(name)))
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, load.Await(ctx))
	rs, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0").WithOutputFields("id", "extra").WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)
	require.Equal(t, elemNullRows, rs.ResultCount)
	for i := range rs.ResultCount {
		value, err := rs.GetColumn("extra").Get(i)
		require.NoError(t, err)
		require.Nil(t, value)
	}
}
