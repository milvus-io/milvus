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
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const conditionalUpsertDim = 4

type conditionalUpsertRow struct {
	version int64
	status  string
	vec     []float32
}

func setupConditionalUpsertCollection(ctx context.Context, t *testing.T, mc *base.MilvusClient, withTags bool) string {
	t.Helper()
	name := fmt.Sprintf("cond_upsert_%d", time.Now().UnixNano())
	schema := entity.NewSchema().WithName(name).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("version").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("status").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(conditionalUpsertDim))
	if withTags {
		schema.WithField(entity.NewField().
			WithName("tags").
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeInt64).
			WithMaxCapacity(16))
	}

	require.NoError(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema)))
	t.Cleanup(func() { _ = mc.DropCollection(context.Background(), client.NewDropCollectionOption(name)) })

	cols := []column.Column{
		column.NewColumnInt64("id", []int64{1, 2, 3}),
		column.NewColumnInt64("version", []int64{10, 10, 10}),
		column.NewColumnVarChar("status", []string{"init_1", "init_2", "init_3"}),
		column.NewColumnFloatVector("vec", conditionalUpsertDim, [][]float32{
			{1, 0, 0, 0},
			{0, 1, 0, 0},
			{0, 0, 1, 0},
		}),
	}
	if withTags {
		cols = append(cols, column.NewColumnInt64Array("tags", [][]int64{{1}, {2}, {3}}))
	}
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, cols...))
	require.NoError(t, err)

	flushTask, err := mc.Flush(ctx, client.NewFlushOption(name))
	require.NoError(t, err)
	require.NoError(t, flushTask.Await(ctx))

	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vec", index.NewAutoIndex(entity.L2)))
	require.NoError(t, err)
	require.NoError(t, idxTask.Await(ctx))

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	require.NoError(t, err)
	require.NoError(t, loadTask.Await(ctx))

	return name
}

func queryConditionalUpsertRows(ctx context.Context, t *testing.T, mc *base.MilvusClient, collName string) map[int64]conditionalUpsertRow {
	t.Helper()
	res, err := mc.Query(ctx, client.NewQueryOption(collName).
		WithFilter("id >= 0").
		WithOutputFields("id", "version", "status", "vec").
		WithConsistencyLevel(entity.ClStrong))
	require.NoError(t, err)

	idCol := res.GetColumn("id")
	verCol := res.GetColumn("version")
	statusCol := res.GetColumn("status")
	vecCol := res.GetColumn("vec")

	rows := make(map[int64]conditionalUpsertRow, res.ResultCount)
	for i := 0; i < res.ResultCount; i++ {
		id, err := idCol.GetAsInt64(i)
		require.NoError(t, err)
		ver, err := verCol.GetAsInt64(i)
		require.NoError(t, err)
		status, err := statusCol.GetAsString(i)
		require.NoError(t, err)
		vecVal, err := vecCol.Get(i)
		require.NoError(t, err)
		rows[id] = conditionalUpsertRow{
			version: ver,
			status:  status,
			vec:     vecVal.(entity.FloatVector),
		}
	}
	return rows
}

func TestConditionalUpsertGTAndGTE(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collName := setupConditionalUpsertCollection(ctx, t, mc, false)

	// Column-based GT: id=1 newer (written), id=2 equal (skipped), id=3 stale (skipped), id=4 missing (inserted by default).
	res, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", []int64{1, 2, 3, 4}).
		WithInt64Column("version", []int64{12, 10, 8, 1}).
		WithVarcharColumn("status", []string{"gt_1", "gt_2", "gt_3", "new_4"}).
		WithFloatVectorColumn("vec", conditionalUpsertDim, [][]float32{
			{1, 1, 0, 0},
			{0, 2, 0, 0},
			{0, 0, 2, 0},
			{0, 0, 0, 1},
		}).
		WithWriteCondition(&client.WriteCondition{
			Field: "version",
			Op:    client.WriteConditionOpGT,
		}))
	require.NoError(t, err)
	require.EqualValues(t, 2, res.UpsertCount)
	require.Equal(t, []uint32{0, 3}, res.SuccIndex)
	require.Equal(t, 2, res.IDs.Len())
	id0, err := res.IDs.GetAsInt64(0)
	require.NoError(t, err)
	id1, err := res.IDs.GetAsInt64(1)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 4}, []int64{id0, id1})

	rows := queryConditionalUpsertRows(ctx, t, mc, collName)
	require.Len(t, rows, 4)
	require.Equal(t, int64(12), rows[1].version)
	require.Equal(t, "gt_1", rows[1].status)
	require.Equal(t, int64(10), rows[2].version)
	require.Equal(t, "init_2", rows[2].status)
	require.Equal(t, int64(10), rows[3].version)
	require.Equal(t, "init_3", rows[3].status)
	require.Equal(t, int64(1), rows[4].version)
	require.Equal(t, "new_4", rows[4].status)

	// Row-based GTE partial update (omitting vec): id=1 equal (written), id=2 stale (skipped), id=3 newer (written).
	res, err = mc.Upsert(ctx, client.NewRowBasedInsertOption(collName,
		map[string]any{"id": int64(1), "version": int64(12), "status": "gte_1"},
		map[string]any{"id": int64(2), "version": int64(9), "status": "gte_2"},
		map[string]any{"id": int64(3), "version": int64(15), "status": "gte_3"},
	).WithWriteCondition(&client.WriteCondition{
		Field: "version",
		Op:    client.WriteConditionOpGTE,
	}))
	require.NoError(t, err)
	require.EqualValues(t, 2, res.UpsertCount)
	require.Equal(t, []uint32{0, 2}, res.SuccIndex)

	rows = queryConditionalUpsertRows(ctx, t, mc, collName)
	require.Equal(t, "gte_1", rows[1].status)
	require.Equal(t, []float32{1, 1, 0, 0}, rows[1].vec)
	require.Equal(t, "init_2", rows[2].status)
	require.Equal(t, int64(15), rows[3].version)
	require.Equal(t, "gte_3", rows[3].status)
	require.Equal(t, []float32{0, 0, 1, 0}, rows[3].vec)

	// All-skipped batch succeeds with zero count and empty IDs / SuccIndex.
	res, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", []int64{1, 3}).
		WithInt64Column("version", []int64{11, 14}).
		WithVarcharColumn("status", []string{"stale_1", "stale_3"}).
		WithWriteCondition(&client.WriteCondition{
			Field: "version",
			Op:    client.WriteConditionOpGT,
		}))
	require.NoError(t, err)
	require.EqualValues(t, 0, res.UpsertCount)
	require.Empty(t, res.SuccIndex)
	require.Equal(t, 0, res.IDs.Len())
}

func TestConditionalUpsertMissingRowPolicies(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collName := setupConditionalUpsertCollection(ctx, t, mc, false)

	// SkipIfMissing: existing newer row is written, missing row is skipped without error.
	res, err := mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", []int64{1, 99}).
		WithInt64Column("version", []int64{20, 5}).
		WithVarcharColumn("status", []string{"updated_1", "missing_99"}).
		WithWriteCondition(&client.WriteCondition{
			Field:     "version",
			Op:        client.WriteConditionOpGT,
			OnMissing: client.SkipIfMissing,
		}))
	require.NoError(t, err)
	require.EqualValues(t, 1, res.UpsertCount)
	require.Equal(t, []uint32{0}, res.SuccIndex)

	rows := queryConditionalUpsertRows(ctx, t, mc, collName)
	require.Equal(t, int64(20), rows[1].version)
	require.Equal(t, "updated_1", rows[1].status)
	_, exists := rows[99]
	require.False(t, exists)

	// ErrorIfMissing: fails before submission and leaves existing rows untouched.
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", []int64{2, 100}).
		WithInt64Column("version", []int64{25, 1}).
		WithVarcharColumn("status", []string{"should_not_commit", "missing_100"}).
		WithWriteCondition(&client.WriteCondition{
			Field:     "version",
			Op:        client.WriteConditionOpGT,
			OnMissing: client.ErrorIfMissing,
		}))
	require.Error(t, err)

	rows = queryConditionalUpsertRows(ctx, t, mc, collName)
	require.Equal(t, int64(10), rows[2].version)
	require.Equal(t, "init_2", rows[2].status)
	_, exists = rows[100]
	require.False(t, exists)
}

func TestConditionalUpsertValidationErrors(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	collName := setupConditionalUpsertCollection(ctx, t, mc, true)

	// Insert rejects WriteCondition.
	_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", []int64{10}).
		WithInt64Column("version", []int64{1}).
		WithVarcharColumn("status", []string{"s"}).
		WithFloatVectorColumn("vec", conditionalUpsertDim, [][]float32{{1, 0, 0, 0}}).
		WithColumns(column.NewColumnInt64Array("tags", [][]int64{{1}})).
		WithWriteCondition(&client.WriteCondition{
			Field: "version",
			Op:    client.WriteConditionOpGT,
		}))
	require.Error(t, err)

	// Missing op is rejected by server.
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", []int64{1}).
		WithInt64Column("version", []int64{20}).
		WithWriteCondition(&client.WriteCondition{
			Field: "version",
		}))
	require.Error(t, err)

	// PK field cannot be used as version condition field.
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", []int64{1}).
		WithInt64Column("version", []int64{20}).
		WithWriteCondition(&client.WriteCondition{
			Field: "id",
			Op:    client.WriteConditionOpGT,
		}))
	require.Error(t, err)

	// GTE is incompatible with relative field operations (ARRAY_APPEND).
	_, err = mc.Upsert(ctx, client.NewColumnBasedInsertOption(collName).
		WithInt64Column("id", []int64{1}).
		WithInt64Column("version", []int64{20}).
		WithColumns(column.NewColumnInt64Array("tags", [][]int64{{9}})).
		WithArrayAppend("tags").
		WithWriteCondition(&client.WriteCondition{
			Field: "version",
			Op:    client.WriteConditionOpGTE,
		}))
	require.Error(t, err)
}
