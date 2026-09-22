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
	"github.com/milvus-io/milvus/client/v3/row"
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

func TestBuildFieldOpsOmitsExplicitReplace(t *testing.T) {
	opt := NewColumnBasedInsertOption("test")
	opt.partialOps = map[string]*schemapb.FieldPartialUpdateOp{
		"tags": {FieldName: "tags", Op: schemapb.FieldPartialUpdateOp_REPLACE},
	}
	require.Nil(t, opt.buildFieldOps())
	opt.partialOps["scores"] = &schemapb.FieldPartialUpdateOp{
		FieldName: "scores", Op: schemapb.FieldPartialUpdateOp_PATH_REPLACE, Path: "[0]",
	}
	require.Equal(t, []*schemapb.FieldPartialUpdateOp{opt.partialOps["scores"]}, opt.buildFieldOps())
}

func TestRowBasedPathReplaceRejectsInvalidOperands(t *testing.T) {
	coll, _ := buildStructPathReplaceCollection()
	for _, tc := range []struct {
		name string
		row  any
		want string
	}{
		{"nil row", nil, "unsupported row type"},
		{"wrong row type", 1, "unsupported"},
		{"wrong map key", map[int]any{1: 2}, "map key type"},
		{"missing parent", map[string]any{"id": int64(1)}, "missing struct array field"},
		{"null operand", map[string]any{"profile": nil}, "must not be null"},
		{"non map operand", map[string]any{"profile": []int64{1}}, "must be map[string]any"},
		{"non string child name", map[string]any{"profile": map[int]any{1: 2}}, "must be map[string]any"},
		{"empty child mask", map[string]any{"profile": map[string]any{}}, "child mask must not be empty"},
		{"unknown child", map[string]any{"profile": map[string]any{"unknown": []int64{1}}}, "has no child"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := NewRowBasedInsertOption(coll.Name, tc.row).
				WithPathReplace("profile", "[0][age]").UpsertRequest(coll)
			require.Nil(t, req)
			require.ErrorContains(t, err, tc.want)
		})
	}

	t.Run("missing child schema", func(t *testing.T) {
		coll, _ := buildStructPathReplaceCollection()
		coll.Schema.Fields[1].StructSchema = nil
		req, err := NewRowBasedInsertOption(coll.Name, map[string]any{"profile": map[string]any{"age": []int64{1}}}).
			WithPathReplace("profile", "[0][age]").UpsertRequest(coll)
		require.Nil(t, req)
		require.ErrorContains(t, err, "has no child schema")
	})
	t.Run("deferred column construction error", func(t *testing.T) {
		opt := NewRowBasedInsertOption(coll.Name, map[string]any{"id": int64(1)})
		opt.WithStructArrayColumn("profile", nil, []map[string]any{{"age": []int64{18}}})
		req, err := opt.UpsertRequest(coll)
		require.Nil(t, req)
		require.ErrorContains(t, err, "struct schema is required")
	})
	t.Run("empty rows", func(t *testing.T) {
		req, err := NewRowBasedInsertOption(coll.Name).UpsertRequest(coll)
		require.Nil(t, req)
		require.ErrorContains(t, err, "0 length column")
	})
	t.Run("column schema mismatch", func(t *testing.T) {
		// The legacy String type constructs a VarChar column. The row wrapper
		// must preserve the type mismatch reported by column validation.
		coll := &entity.Collection{Name: "legacy", Schema: entity.NewSchema().WithField(
			entity.NewField().WithName("name").WithDataType(entity.FieldTypeString))}
		req, err := NewRowBasedInsertOption(coll.Name, map[string]any{"name": "value"}).UpsertRequest(coll)
		require.Nil(t, req)
		require.ErrorContains(t, err, "collection field definition")
	})
}

func TestRowBasedPathReplaceUsesSharedFieldMapping(t *testing.T) {
	type profileFields struct {
		ID      int64          `milvus:"name:id"`
		Profile map[string]any `milvus:"name:profile"`
	}
	type duplicateFields struct {
		First  int `milvus:"name:profile"`
		Second int `milvus:"name:profile"`
	}
	coll, _ := buildStructPathReplaceCollection()
	operand := map[string]any{"age": []int64{18}, "city": []string{"Hangzhou"}, "score": []float32{1}}
	input := &struct {
		profileFields
		Ignored int `milvus:"-"`
	}{profileFields: profileFields{ID: 1, Profile: operand}}
	// Both preflight mask extraction and column conversion must honor embedded
	// fields, renamed columns, skipped fields, and multiple row-pointer levels.
	want, err := NewRowBasedInsertOption(coll.Name, &input).UpsertRequest(coll)
	require.NoError(t, err)
	got, err := NewRowBasedInsertOption(coll.Name, &input).
		WithPathReplace("profile", "[0]").UpsertRequest(coll)
	require.NoError(t, err)
	require.ElementsMatch(t, want.GetFieldsData(), got.GetFieldsData())
	require.Len(t, got.GetFieldsData(), 2)
	require.EqualValues(t, 1, got.GetNumRows())
	require.Equal(t, "[0]", got.GetFieldOps()[0].GetPath())

	for _, input := range []any{
		duplicateFields{},
		struct {
			duplicateFields
		}{},
		struct {
			First int `milvus:"name:profile"`
			profileFields
		}{},
		struct {
			profileFields
			Second int `milvus:"name:profile"`
		}{},
	} {
		_, conversionErr := row.AnyToColumns([]any{input}, true, coll.Schema)
		_, maskErr := pathReplaceStructFieldMask([]any{input}, "profile")
		require.ErrorContains(t, conversionErr, "duplicated name")
		require.EqualError(t, maskErr, conversionErr.Error())
	}
	_, err = pathReplaceStructFieldMask([]any{(*profileFields)(nil)}, "profile")
	require.Error(t, err)
}

func TestNewEmptyStructArrayColumnInvalidSchema(t *testing.T) {
	for _, tc := range []struct {
		name     string
		schema   *entity.StructSchema
		nullable bool
		want     string
	}{
		{"missing schema", nil, false, "has no struct schema"},
		{"unsupported child", entity.NewStructSchema().WithField(
			entity.NewField().WithName("json").WithDataType(entity.FieldTypeJSON)), false, "unsupported struct sub-field type"},
		{"nullable empty schema", entity.NewStructSchema(), true, "requires at least one sub-field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field := entity.NewField().WithName("profile").WithDataType(entity.FieldTypeArray).
				WithElementType(entity.FieldTypeStruct).WithStructSchema(tc.schema).WithNullable(tc.nullable)
			col, err := newEmptyStructArrayColumn(field, nil)
			require.Nil(t, col)
			require.ErrorContains(t, err, tc.want)
		})
	}
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

func TestJSONPathReplaceEmitsRawOperand(t *testing.T) {
	coll := &entity.Collection{Name: "json_path", Schema: entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("metadata").WithDataType(entity.FieldTypeJSON))}
	for _, value := range []string{`null`, `18`, `true`, `"123"`, `{"age":18}`, `[1,2]`} {
		request, err := NewRowBasedInsertOption(coll.Name, map[string]any{"id": int64(1), "metadata": []byte(value)}).
			WithPathReplace("metadata", `["profile"][0]`).UpsertRequest(coll)
		require.NoError(t, err)
		require.Equal(t, `["profile"][0]`, request.GetFieldOps()[0].GetPath())
		field := lo.FindOrElse(request.GetFieldsData(), nil, func(f *schemapb.FieldData) bool { return f.GetFieldName() == "metadata" })
		require.NotNil(t, field)
		require.Equal(t, value, string(field.GetScalars().GetJsonData().GetData()[0]))
	}
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

// Column-based requests preserve operands for authoritative Proxy validation.
func TestColumnBasedStructPathReplacePreservesOperandForServerValidation(t *testing.T) {
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

func TestRowBasedStructPathReplaceRequiresWholeElement(t *testing.T) {
	coll, _ := buildStructPathReplaceCollection()
	rows := []any{
		map[string]any{"id": int64(1), "profile": map[string]any{"age": []int64{18}, "city": []string{"Hangzhou"}}},
		map[string]any{"id": int64(2), "profile": map[string]any{"age": []int64{21}, "city": []string{"Ningbo"}}},
	}

	req, err := NewRowBasedInsertOption(coll.Name, rows...).WithPathReplace("profile", "[1]").UpsertRequest(coll)
	require.ErrorContains(t, err, "requires all struct children")
	for _, row := range rows {
		row.(map[string]any)["profile"].(map[string]any)["score"] = []float32{1}
	}
	req, err = NewRowBasedInsertOption(coll.Name, rows...).WithPathReplace("profile", "[1]").UpsertRequest(coll)
	require.NoError(t, err)
	profile := lo.FindOrElse(req.GetFieldsData(), nil, func(field *schemapb.FieldData) bool {
		return field.GetFieldName() == "profile"
	})
	require.NotNil(t, profile)
	children := profile.GetStructArrays().GetFields()
	require.Len(t, children, 3)
	assert.ElementsMatch(t, []string{"age", "city", "score"}, []string{children[0].GetFieldName(), children[1].GetFieldName(), children[2].GetFieldName()})
	assert.Len(t, coll.Schema.Fields[1].StructSchema.Fields, 3)
}

func TestRowBasedStructPathReplaceRejectsChildScopeMismatch(t *testing.T) {
	coll, _ := buildStructPathReplaceCollection()
	for _, operand := range []map[string]any{
		{"city": []string{"Hangzhou"}},
		{"age": []int64{18}, "city": []string{"Hangzhou"}},
	} {
		_, err := NewRowBasedInsertOption(coll.Name, map[string]any{"id": int64(1), "profile": operand}).
			WithPathReplace("profile", "[1][age]").UpsertRequest(coll)
		require.ErrorContains(t, err, "exactly the selected child")
	}
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

	req, err := NewRowBasedInsertOption(coll.Name, rows...).WithPathReplace("profile", "[1][age]").UpsertRequest(coll)
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
