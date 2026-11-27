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

package milvusclient

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

func buildConditionalUpsertTestCollection() *entity.Collection {
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("version").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("tags").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt64).WithMaxCapacity(32))
	schema.CollectionName = "conditional_upsert_test"
	return &entity.Collection{Name: "conditional_upsert_test", Schema: schema}
}

func TestColumnBasedWithWriteCondition(t *testing.T) {
	coll := buildConditionalUpsertTestCollection()

	t.Run("sets_condition_and_auto_enables_partial_update", func(t *testing.T) {
		cond := &WriteCondition{
			Field:     "version",
			Op:        WriteConditionOpGT,
			OnMissing: SkipIfMissing,
		}
		opt := NewColumnBasedInsertOption(coll.Name,
			column.NewColumnInt64("id", []int64{1, 2}),
			column.NewColumnInt64("version", []int64{10, 20}),
		).WithWriteCondition(cond)

		// Mutating caller's struct after WithWriteCondition must not affect the built request.
		cond.Field = "mutated"
		cond.Op = WriteConditionOpGTE
		cond.OnMissing = ErrorIfMissing

		req, err := opt.UpsertRequest(coll)
		require.NoError(t, err)
		assert.True(t, req.GetPartialUpdate(), "WriteCondition should auto-enable partial_update")
		require.NotNil(t, req.GetWriteCondition())
		assert.Equal(t, "version", req.GetWriteCondition().GetField())
		assert.Equal(t, milvuspb.WriteCondition_GT, req.GetWriteCondition().GetOp())
		assert.Equal(t, milvuspb.WriteCondition_SKIP_IF_MISSING, req.GetWriteCondition().GetOnMissing())
	})

	t.Run("default_on_missing_is_insert", func(t *testing.T) {
		opt := NewColumnBasedInsertOption(coll.Name,
			column.NewColumnInt64("id", []int64{1}),
			column.NewColumnInt64("version", []int64{10}),
		).WithWriteCondition(&WriteCondition{
			Field: "version",
			Op:    WriteConditionOpGTE,
		})

		req, err := opt.UpsertRequest(coll)
		require.NoError(t, err)
		assert.True(t, req.GetPartialUpdate())
		require.NotNil(t, req.GetWriteCondition())
		assert.Equal(t, milvuspb.WriteCondition_GTE, req.GetWriteCondition().GetOp())
		assert.Equal(t, milvuspb.WriteCondition_INSERT_IF_MISSING, req.GetWriteCondition().GetOnMissing())
	})

	t.Run("nil_clears_condition", func(t *testing.T) {
		opt := NewColumnBasedInsertOption(coll.Name,
			column.NewColumnInt64("id", []int64{1}),
			column.NewColumnInt64("version", []int64{10}),
		).WithWriteCondition(&WriteCondition{
			Field: "version",
			Op:    WriteConditionOpGT,
		}).WithWriteCondition(nil)

		req, err := opt.UpsertRequest(coll)
		require.NoError(t, err)
		assert.False(t, req.GetPartialUpdate())
		assert.Nil(t, req.GetWriteCondition())
	})

	t.Run("combines_with_field_ops", func(t *testing.T) {
		opt := NewColumnBasedInsertOption(coll.Name,
			column.NewColumnInt64("id", []int64{1}),
			column.NewColumnInt64("version", []int64{10}),
			column.NewColumnInt64Array("tags", [][]int64{{1, 2}}),
		).WithArrayAppend("tags").WithWriteCondition(&WriteCondition{
			Field:     "version",
			Op:        WriteConditionOpGT,
			OnMissing: ErrorIfMissing,
		})

		req, err := opt.UpsertRequest(coll)
		require.NoError(t, err)
		assert.True(t, req.GetPartialUpdate())
		require.NotNil(t, req.GetWriteCondition())
		assert.Equal(t, milvuspb.WriteCondition_ERROR_IF_MISSING, req.GetWriteCondition().GetOnMissing())
		tagsOp := findOp(req.GetFieldOps(), "tags")
		require.NotNil(t, tagsOp)
		assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_APPEND, tagsOp.GetOp())
	})

	t.Run("insert_request_rejects_write_condition", func(t *testing.T) {
		opt := NewColumnBasedInsertOption(coll.Name,
			column.NewColumnInt64("id", []int64{1}),
			column.NewColumnInt64("version", []int64{10}),
		).WithWriteCondition(&WriteCondition{
			Field: "version",
			Op:    WriteConditionOpGT,
		})

		_, err := opt.InsertRequest(coll)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.True(t, errors.Is(err, errWriteConditionUnsupportedForDML))
	})
}

func TestRowBasedWithWriteCondition(t *testing.T) {
	coll := buildConditionalUpsertTestCollection()
	rows := []any{
		map[string]any{"id": int64(1), "version": int64(10)},
		map[string]any{"id": int64(2), "version": int64(20)},
	}

	t.Run("sets_condition_and_auto_enables_partial_update", func(t *testing.T) {
		opt := NewRowBasedInsertOption(coll.Name, rows...).WithWriteCondition(&WriteCondition{
			Field:     "version",
			Op:        WriteConditionOpGT,
			OnMissing: SkipIfMissing,
		})

		req, err := opt.UpsertRequest(coll)
		require.NoError(t, err)
		assert.True(t, req.GetPartialUpdate())
		require.NotNil(t, req.GetWriteCondition())
		assert.Equal(t, "version", req.GetWriteCondition().GetField())
		assert.Equal(t, milvuspb.WriteCondition_GT, req.GetWriteCondition().GetOp())
		assert.Equal(t, milvuspb.WriteCondition_SKIP_IF_MISSING, req.GetWriteCondition().GetOnMissing())
	})

	t.Run("insert_request_rejects_write_condition", func(t *testing.T) {
		opt := NewRowBasedInsertOption(coll.Name, rows...).WithWriteCondition(&WriteCondition{
			Field: "version",
			Op:    WriteConditionOpGT,
		})

		_, err := opt.InsertRequest(coll)
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.True(t, errors.Is(err, errWriteConditionUnsupportedForDML))
	})
}

func TestConditionalWriteClient(t *testing.T) {
	type service struct{ milvuspb.MilvusServiceClient }
	coll := buildConditionalUpsertTestCollection()
	c := &Client{
		service:   &service{},
		collCache: NewCollectionCache(func(context.Context, string) (*entity.Collection, error) { return coll, nil }),
	}
	ctx := context.Background()
	opt := NewColumnBasedInsertOption(coll.Name).
		WithInt64Column("id", []int64{1, 2, 3}).
		WithInt64Column("version", []int64{10, 5, 20}).
		WithWriteCondition(&WriteCondition{Field: "version", Op: WriteConditionOpGT, OnMissing: SkipIfMissing})
	for _, tc := range []struct {
		name    string
		ids     []int64
		indexes []uint32
		target  error
	}{
		{name: "some written", ids: []int64{1, 3}, indexes: []uint32{0, 2}},
		{name: "all skipped"},
		{name: "partial update conflict", target: ErrCollectionPartialUpdateConflict},
		{name: "execution error", target: ErrCollectionConditionalUpsertExecution},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch := mockey.Mock((*service).Upsert).To(func(_ *service, _ context.Context, req *milvuspb.UpsertRequest, _ ...grpc.CallOption) (*milvuspb.MutationResult, error) {
				assert.Equal(t, coll.Name, req.GetCollectionName())
				assert.True(t, req.GetPartialUpdate())
				condition := req.GetWriteCondition()
				require.NotNil(t, condition)
				assert.Equal(t, "version", condition.GetField())
				assert.Equal(t, WriteConditionOpGT, condition.GetOp())
				assert.Equal(t, SkipIfMissing, condition.GetOnMissing())
				return &milvuspb.MutationResult{
					Status: merr.Status(tc.target), UpsertCnt: int64(len(tc.ids)), SuccIndex: tc.indexes,
					IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: tc.ids}}},
				}, nil
			}).Build()
			defer patch.UnPatch()
			result, err := c.Upsert(ctx, opt)
			require.EqualValues(t, 1, patch.Times())
			if tc.target != nil {
				require.ErrorIs(t, err, tc.target)
				assert.False(t, IsRetryableError(err))
				return
			}
			require.NoError(t, err)
			assert.EqualValues(t, len(tc.ids), result.UpsertCount)
			assert.Equal(t, tc.indexes, result.SuccIndex)
			assert.Equal(t, len(tc.ids), result.IDs.Len())
		})
	}
	t.Run("partial update without a condition", func(t *testing.T) {
		// Ordinary partial updates can return existing PKs before new PKs.
		patch := mockey.Mock((*service).Upsert).Return(&milvuspb.MutationResult{
			Status: merr.Success(), UpsertCnt: 2, SuccIndex: []uint32{0, 1},
			IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 99}}}},
		}, nil).Build()
		defer patch.UnPatch()

		result, err := c.Upsert(ctx, NewColumnBasedInsertOption(coll.Name).
			WithInt64Column("id", []int64{99, 1}).
			WithInt64Column("version", []int64{1, 10}).
			WithPartialUpdate(true))
		require.NoError(t, err)
		assert.EqualValues(t, 2, result.UpsertCount)
		assert.Equal(t, column.NewColumnInt64("id", []int64{1, 99}), result.IDs)
		assert.Nil(t, result.SuccIndex)
	})
	_, err := c.Insert(ctx, opt)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.ErrorContains(t, err, "only supported for Upsert")
}
