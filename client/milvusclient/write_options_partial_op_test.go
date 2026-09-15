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

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
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

func TestWithPathReplaceEmitsPathAndAutoEnablesPartialUpdate(t *testing.T) {
	opt := buildPartialOpOption(func(o *columnBasedDataOption) {
		o.WithPathReplace("tags", "[1]")
	})
	req, err := opt.UpsertRequest(buildPartialOpTestCollection())
	require.NoError(t, err)
	assert.True(t, req.GetPartialUpdate())

	tagsOp := findOp(req.GetFieldOps(), "tags")
	require.NotNil(t, tagsOp)
	assert.Equal(t, schemapb.FieldPartialUpdateOp_PATH_REPLACE, tagsOp.GetOp())
	assert.Equal(t, "[1]", tagsOp.GetPath())
}

func TestWithFieldPartialOpReplaceClearsPriorDirective(t *testing.T) {
	opt := buildPartialOpOption(func(o *columnBasedDataOption) {
		o.WithArrayAppend("tags")
		o.WithFieldPartialOp("tags", schemapb.FieldPartialUpdateOp_REPLACE)
	})
	req, err := opt.UpsertRequest(buildPartialOpTestCollection())
	require.NoError(t, err)
	assert.False(t, req.GetPartialUpdate())
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

func TestPartialOpBuilderUsesLastDirectiveForSameField(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*columnBasedDataOption)
		wantOp    schemapb.FieldPartialUpdateOp_OpType
		wantPath  string
		wantNoOp  bool
	}{
		{
			name: "path then path",
			configure: func(opt *columnBasedDataOption) {
				opt.WithPathReplace("tags", "[0]").WithPathReplace("tags", "[1]")
			},
			wantOp:   schemapb.FieldPartialUpdateOp_PATH_REPLACE,
			wantPath: "[1]",
		},
		{
			name: "append then path",
			configure: func(opt *columnBasedDataOption) {
				opt.WithArrayAppend("tags").WithPathReplace("tags", "[1]")
			},
			wantOp:   schemapb.FieldPartialUpdateOp_PATH_REPLACE,
			wantPath: "[1]",
		},
		{
			name: "path then replace",
			configure: func(opt *columnBasedDataOption) {
				opt.WithPathReplace("tags", "[1]").
					WithFieldPartialOp("tags", schemapb.FieldPartialUpdateOp_REPLACE)
			},
			wantNoOp: true,
		},
		{
			name: "append then remove",
			configure: func(opt *columnBasedDataOption) {
				opt.WithArrayAppend("tags").WithArrayRemove("tags")
			},
			wantOp: schemapb.FieldPartialUpdateOp_ARRAY_REMOVE,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opt := buildPartialOpOption(test.configure)
			req, err := opt.UpsertRequest(buildPartialOpTestCollection())
			require.NoError(t, err)
			if test.wantNoOp {
				assert.False(t, req.GetPartialUpdate())
				assert.Empty(t, req.GetFieldOps())
				return
			}
			require.Len(t, req.GetFieldOps(), 1)
			assert.Equal(t, test.wantOp, req.GetFieldOps()[0].GetOp())
			assert.Equal(t, test.wantPath, req.GetFieldOps()[0].GetPath())
		})
	}
}

func TestRowBasedPartialOpBuilderUsesLastDirectiveForSameField(t *testing.T) {
	coll := buildPartialOpTestCollection()
	rows := []any{map[string]any{"id": int64(1), "tags": []int64{10}}}
	req, err := NewRowBasedInsertOption(coll.Name, rows...).
		WithPathReplace("tags", "[0]").
		WithArrayAppend("tags").
		UpsertRequest(coll)
	require.NoError(t, err)
	require.Len(t, req.GetFieldOps(), 1)
	assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_APPEND, req.GetFieldOps()[0].GetOp())
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
	opt := NewRowBasedInsertOption(coll.Name, rows...).
		WithArrayAppend("tags")

	req, err := opt.UpsertRequest(coll)
	require.NoError(t, err)
	assert.EqualValues(t, len(rows), req.GetNumRows())
	assert.True(t, req.GetPartialUpdate())
	tagsOp := findOp(req.GetFieldOps(), "tags")
	require.NotNil(t, tagsOp)
	assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_APPEND, tagsOp.GetOp())
}

func TestRowBasedPathReplaceEmitsPath(t *testing.T) {
	coll := buildPartialOpTestCollection()
	rows := []any{
		map[string]any{"id": int64(1), "tags": []int64{10}},
		map[string]any{"id": int64(2), "tags": []int64{20}},
	}
	opt := NewRowBasedInsertOption(coll.Name, rows...).WithPathReplace("tags", "[2]")

	req, err := opt.UpsertRequest(coll)
	require.NoError(t, err)
	op := findOp(req.GetFieldOps(), "tags")
	require.NotNil(t, op)
	assert.Equal(t, schemapb.FieldPartialUpdateOp_PATH_REPLACE, op.GetOp())
	assert.Equal(t, "[2]", op.GetPath())
}

func buildStructPathReplaceCollection() (*entity.Collection, *entity.StructSchema) {
	profileSchema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("age").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("city").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName("score").WithDataType(entity.FieldTypeFloat))
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("profile").WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).WithMaxCapacity(32).WithStructSchema(profileSchema))
	schema.CollectionName = "struct_path_replace_test"
	return &entity.Collection{Name: schema.CollectionName, Schema: schema}, profileSchema
}

func TestColumnBasedStructPathReplaceSerializesSelectedChildrenOnly(t *testing.T) {
	coll, _ := buildStructPathReplaceCollection()
	operandSchema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("age").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("city").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64))
	opt := NewColumnBasedInsertOption(coll.Name).
		WithInt64Column("id", []int64{1, 2}).
		WithStructArrayColumn("profile", operandSchema, []map[string]any{
			{"age": []int64{18}, "city": []string{"Hangzhou"}},
			{"age": []int64{21}, "city": []string{"Ningbo"}},
		}).
		WithPathReplace("profile", "[1]")

	req, err := opt.UpsertRequest(coll)
	require.NoError(t, err)
	profile := lo.FindOrElse(req.GetFieldsData(), nil, func(field *schemapb.FieldData) bool {
		return field.GetFieldName() == "profile"
	})
	require.NotNil(t, profile)
	children := profile.GetStructArrays().GetFields()
	require.Len(t, children, 2)
	assert.ElementsMatch(t, []string{"age", "city"}, []string{children[0].GetFieldName(), children[1].GetFieldName()})
}

func TestRowBasedStructPathReplaceUsesUniformChildMask(t *testing.T) {
	coll, _ := buildStructPathReplaceCollection()
	rows := []any{
		map[string]any{"id": int64(1), "profile": map[string]any{"age": []int64{18}, "city": []string{"Hangzhou"}}},
		map[string]any{"id": int64(2), "profile": map[string]any{"age": []int64{21}, "city": []string{"Ningbo"}}},
	}

	req, err := NewRowBasedInsertOption(coll.Name, rows...).WithPathReplace("profile", "[1]").UpsertRequest(coll)
	require.NoError(t, err)
	profile := lo.FindOrElse(req.GetFieldsData(), nil, func(field *schemapb.FieldData) bool {
		return field.GetFieldName() == "profile"
	})
	require.NotNil(t, profile)
	children := profile.GetStructArrays().GetFields()
	require.Len(t, children, 2)
	assert.ElementsMatch(t, []string{"age", "city"}, []string{children[0].GetFieldName(), children[1].GetFieldName()})
	assert.Len(t, coll.Schema.Fields[1].StructSchema.Fields, 3)
}

func TestRowBasedStructPathReplaceUsesMilvusFieldTags(t *testing.T) {
	type taggedRow struct {
		ID      int64          `milvus:"name:id"`
		Profile map[string]any `milvus:"name:profile"`
	}

	coll, _ := buildStructPathReplaceCollection()
	rows := []any{
		taggedRow{ID: 1, Profile: map[string]any{"age": []int64{18}}},
		taggedRow{ID: 2, Profile: map[string]any{"age": []int64{21}}},
	}

	req, err := NewRowBasedInsertOption(coll.Name, rows...).WithPathReplace("profile", "[1]").UpsertRequest(coll)
	require.NoError(t, err)
	profile := lo.FindOrElse(req.GetFieldsData(), nil, func(field *schemapb.FieldData) bool {
		return field.GetFieldName() == "profile"
	})
	require.NotNil(t, profile)
	children := profile.GetStructArrays().GetFields()
	require.Len(t, children, 1)
	assert.Equal(t, "age", children[0].GetFieldName())
}

func TestRowBasedStructPathReplaceRejectsDifferentChildMasks(t *testing.T) {
	coll, _ := buildStructPathReplaceCollection()
	rows := []any{
		map[string]any{"id": int64(1), "profile": map[string]any{"age": []int64{18}}},
		map[string]any{"id": int64(2), "profile": map[string]any{"city": []string{"Ningbo"}}},
	}

	_, err := NewRowBasedInsertOption(coll.Name, rows...).WithPathReplace("profile", "[1]").UpsertRequest(coll)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match request mask")
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
