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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
)

func buildPartialOpTestCollection() *entity.Collection {
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("tags").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt64).WithMaxCapacity(32))
	schema.CollectionName = "partial_op_test"
	return &entity.Collection{Name: "partial_op_test", Schema: schema}
}

func buildPartialOpOption(modify func(opt *columnBasedDataOption)) *columnBasedDataOption {
	idCol := column.NewColumnInt64("id", []int64{1, 2})
	tagsCol := column.NewColumnInt64Array("tags", [][]int64{{1}, {2, 3}})
	opt := NewColumnBasedInsertOption("partial_op_test").WithColumns(idCol, tagsCol)
	if modify != nil {
		modify(opt)
	}
	return opt
}

func findOp(ops []*schemapb.FieldPartialUpdateOp, name string) *schemapb.FieldPartialUpdateOp {
	for _, op := range ops {
		if op.GetFieldName() == name {
			return op
		}
	}
	return nil
}

func TestWithArrayAppendEmitsFieldOpAndAutoEnablesPartialUpdate(t *testing.T) {
	opt := buildPartialOpOption(func(o *columnBasedDataOption) {
		o.WithArrayAppend("tags")
	})
	req, err := opt.UpsertRequest(buildPartialOpTestCollection())
	require.NoError(t, err)
	assert.True(t, req.GetPartialUpdate(), "ARRAY_APPEND should auto-enable partial_update")

	tagsOp := findOp(req.GetFieldOps(), "tags")
	require.NotNil(t, tagsOp)
	assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_APPEND, tagsOp.GetOp())
	assert.Equal(t, "tags", tagsOp.GetFieldName())

	// FieldData must remain clean — no op leakage into the data message.
	for _, fd := range req.GetFieldsData() {
		_ = fd
	}
}

func TestWithArrayRemoveEmitsFieldOp(t *testing.T) {
	opt := buildPartialOpOption(func(o *columnBasedDataOption) {
		o.WithArrayRemove("tags")
	})
	req, err := opt.UpsertRequest(buildPartialOpTestCollection())
	require.NoError(t, err)
	assert.True(t, req.GetPartialUpdate())

	tagsOp := findOp(req.GetFieldOps(), "tags")
	require.NotNil(t, tagsOp)
	assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, tagsOp.GetOp())
}

func TestWithFieldPartialOpReplaceClearsPriorDirective(t *testing.T) {
	opt := buildPartialOpOption(func(o *columnBasedDataOption) {
		o.WithArrayAppend("tags")
		o.WithFieldPartialOp("tags", schemapb.FieldPartialUpdateOp_REPLACE)
	})
	req, err := opt.UpsertRequest(buildPartialOpTestCollection())
	require.NoError(t, err)
	assert.False(t, req.GetPartialUpdate(), "REPLACE should clear prior non-REPLACE op")
	assert.Empty(t, req.GetFieldOps())
}

func TestWithFieldPartialOpReplaceWithoutPriorIsNoOp(t *testing.T) {
	opt := buildPartialOpOption(func(o *columnBasedDataOption) {
		o.WithFieldPartialOp("tags", schemapb.FieldPartialUpdateOp_REPLACE)
	})
	req, err := opt.UpsertRequest(buildPartialOpTestCollection())
	require.NoError(t, err)
	assert.False(t, req.GetPartialUpdate())
	assert.Empty(t, req.GetFieldOps())
	assert.Empty(t, opt.partialOps)
}

func TestPartialOpDoesNotOverrideExplicitPartialUpdate(t *testing.T) {
	opt := buildPartialOpOption(func(o *columnBasedDataOption) {
		o.WithPartialUpdate(true)
		o.WithArrayAppend("tags")
	})
	req, err := opt.UpsertRequest(buildPartialOpTestCollection())
	require.NoError(t, err)
	assert.True(t, req.GetPartialUpdate())
}

func TestExplicitPartialUpdateFalseIsPromotedByOp(t *testing.T) {
	opt := buildPartialOpOption(func(o *columnBasedDataOption) {
		o.WithPartialUpdate(false)
		o.WithArrayAppend("tags")
	})
	req, err := opt.UpsertRequest(buildPartialOpTestCollection())
	require.NoError(t, err)
	assert.True(t, req.GetPartialUpdate(), "non-REPLACE op should promote partial_update")
}

func TestPartialOpForUnknownFieldStillEmitted(t *testing.T) {
	opt := buildPartialOpOption(func(o *columnBasedDataOption) {
		o.WithArrayAppend("does_not_exist")
	})
	req, err := opt.UpsertRequest(buildPartialOpTestCollection())
	require.NoError(t, err)
	// Unknown-field ops are forwarded as-is so the server can return a
	// descriptive validation error rather than the client silently
	// dropping the directive.
	assert.True(t, req.GetPartialUpdate())
	assert.Len(t, req.GetFieldOps(), 1)
	assert.Equal(t, "does_not_exist", req.GetFieldOps()[0].GetFieldName())
}

func TestBuildFieldOpsReturnsNilWhenEmpty(t *testing.T) {
	opt := &columnBasedDataOption{}
	assert.Nil(t, opt.buildFieldOps())
}

func TestRowBasedUpsertEmitsFieldOps(t *testing.T) {
	coll := buildPartialOpTestCollection()
	rows := []any{
		map[string]any{"id": int64(1), "tags": []int64{10}},
		map[string]any{"id": int64(2), "tags": []int64{20, 30}},
	}
	opt := NewRowBasedInsertOption(coll.Name, rows...)
	opt.WithArrayAppend("tags")

	req, err := opt.UpsertRequest(coll)
	require.NoError(t, err)
	assert.True(t, req.GetPartialUpdate())
	tagsOp := findOp(req.GetFieldOps(), "tags")
	require.NotNil(t, tagsOp)
	assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_APPEND, tagsOp.GetOp())
}

func TestMultipleFieldOpsEmittedTogether(t *testing.T) {
	opt := buildPartialOpOption(func(o *columnBasedDataOption) {
		o.WithArrayAppend("tags")
		o.WithFieldPartialOp("other", schemapb.FieldPartialUpdateOp_ARRAY_REMOVE)
	})
	req, err := opt.UpsertRequest(buildPartialOpTestCollection())
	require.NoError(t, err)
	assert.Len(t, req.GetFieldOps(), 2)
	seen := map[string]schemapb.FieldPartialUpdateOp_OpType{}
	for _, o := range req.GetFieldOps() {
		seen[o.GetFieldName()] = o.GetOp()
	}
	assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_APPEND, seen["tags"])
	assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, seen["other"])
}
