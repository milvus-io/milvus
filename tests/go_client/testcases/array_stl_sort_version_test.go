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

// This test deliberately does not call t.Parallel: it changes cluster-wide
// build settings before parallel SDK tests start, then restores both settings.
// Expectations are explicit for each target, never inferred from RPC success.
func TestArraySTLSORTVersionCompatibility(t *testing.T) {
	const forceKey = "dataCoord.forceRebuildScalarSegmentIndex"
	const targetKey = "dataCoord.targetScalarIndexVersion"
	previous := make(map[string]string)
	for _, key := range []string{forceKey, targetKey} {
		value, err := hp.GetServerConfig(key)
		require.NoError(t, err, "ARRAY version compatibility requires the management API")
		previous[key] = value
	}
	setArraySTLSORTConfig(t, forceKey, "true")
	t.Cleanup(func() {
		setArraySTLSORTConfig(t, targetKey, previous[targetKey])
		setArraySTLSORTConfig(t, forceKey, previous[forceKey])
	})
	for _, version := range []int{5, 6} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			setArraySTLSORTConfig(t, targetKey, fmt.Sprint(version))
			for _, family := range []string{"STL_SORT", "HYBRID"} {
				t.Run(family, func(t *testing.T) {
					checkArraySTLSORTVersion(t, version, family)
				})
			}
		})
	}
}

func setArraySTLSORTConfig(t *testing.T, key, value string) {
	t.Helper()
	_, err := hp.AlterServerConfig(key, value)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		actual, err := hp.GetServerConfig(key)
		return err == nil && actual == value
	}, 30*time.Second, 100*time.Millisecond, "%s must become %s", key, value)
}

func checkArraySTLSORTVersion(t *testing.T, version int, family string) {
	t.Helper()
	ctx := hp.CreateContext(t, 3*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	name := common.GenRandomString(fmt.Sprintf("array_sort_v%d", version), 6)
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(2))
	for _, field := range []string{"low", "high"} {
		schema.WithField(entity.NewField().WithName(field).WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeInt64).WithMaxCapacity(8))
	}
	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithShardNum(1)))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, mc.DropCollection(ctx, client.NewDropCollectionOption(name)))
	})

	if version == 5 && family == "STL_SORT" {
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "high", index.NewSortedIndex()))
		require.ErrorIs(t, err, client.ErrServiceNotReady)
		require.ErrorContains(t, err, "requires scalar index engine version >= 6, current resolved version: 5")
		indexes, err := mc.ListIndexes(ctx, client.NewListIndexOption(name))
		require.ErrorIs(t, err, client.ErrIndexNotFound)
		require.Empty(t, indexes, "rejected ARRAY SORT must not leave index metadata")
		return
	}

	// Exceed the default indexing row threshold so Await cannot pass merely
	// because a small segment was left unindexed. With the HYBRID limit of 2,
	// low selects BITMAP, high selects INVERTED at v5 and SORT at v6. Native
	// selector tests independently inspect the persisted physical family.
	const rows = 1200
	ids := make([]int64, rows)
	vectors := make([][]float32, rows)
	low, high := make([][]int64, rows), make([][]int64, rows)
	pattern := [][]int64{{1, 1, 100}, {}, {2, 3}, {1, 4}}
	for i := range ids {
		ids[i] = int64(i)
		vectors[i] = []float32{1, 0}
		low[i], high[i] = []int64{1}, pattern[i%len(pattern)]
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name).WithColumns(
		column.NewColumnInt64("id", ids), column.NewColumnFloatVector("vec", 2, vectors),
		column.NewColumnInt64Array("low", low), column.NewColumnInt64Array("high", high)))
	require.NoError(t, err)
	flush, err := mc.Flush(ctx, client.NewFlushOption(name))
	require.NoError(t, err)
	require.NoError(t, flush.Await(ctx))
	vectorIndex, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vec", index.NewFlatIndex(entity.L2)))
	require.NoError(t, err)
	require.NoError(t, vectorIndex.Await(ctx))
	for _, field := range []string{"low", "high"} {
		idx := index.NewSortedIndex()
		if family == "HYBRID" {
			// HYBRID is internal; exercise it through the public AUTOINDEX entry.
			idx = index.NewGenericIndex("", map[string]string{"index_type": "AUTOINDEX", "bitmap_cardinality_limit": "2"})
		}
		task, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, field, idx))
		require.NoError(t, err)
		require.NoError(t, task.Await(ctx))
		desc, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(name, field))
		require.NoError(t, err)
		require.EqualValues(t, rows, desc.IndexedRows)
		require.Equal(t, idx.Params()["index_type"], desc.Params()["index_type"])
	}
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, load.Await(ctx))
	for _, query := range []struct {
		filter string
		want   []int64
	}{
		{"array_contains(low, 1)", []int64{0, 1, 2, 3}},
		{"array_contains(high, 1)", []int64{0, 3}},
		{"array_contains_any(high, [1,4,1])", []int64{0, 3}},
		{"array_contains_all(high, [1,4])", []int64{3}},
		{"not array_contains(high, 1)", []int64{1, 2}},
		{"array_contains_all(high, [])", []int64{0, 1, 2, 3}},
	} {
		result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id < 4 && ("+query.filter+")").
			WithOutputFields("id").WithConsistencyLevel(entity.ClStrong))
		require.NoError(t, err, query.filter)
		require.ElementsMatch(t, query.want, result.GetColumn("id").(*column.ColumnInt64).Data(), query.filter)
	}
}
