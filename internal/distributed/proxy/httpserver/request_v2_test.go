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

package httpserver

import (
	"context"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/requestutil"
)

func TestRequestV2_GetCollectionName(t *testing.T) {
	tests := []struct {
		name string
		req  requestutil.CollectionNameGetter
		want string
	}{
		{"RenameCollectionReq", &RenameCollectionReq{CollectionName: "col1"}, "col1"},
		{"QueryReqV2", &QueryReqV2{CollectionName: "col2"}, "col2"},
		{"CollectionIDReq", &CollectionIDReq{CollectionName: "col3"}, "col3"},
		{"CollectionFilterReq", &CollectionFilterReq{CollectionName: "col4"}, "col4"},
		{"CollectionDataReq", &CollectionDataReq{CollectionName: "col5"}, "col5"},
		{"SearchReqV2", &SearchReqV2{CollectionName: "col6"}, "col6"},
		{"HybridSearchReq", &HybridSearchReq{CollectionName: "col7"}, "col7"},
		{"PartitionsReq", &PartitionsReq{CollectionName: "col8"}, "col8"},
		{"GrantV2Req", &GrantV2Req{CollectionName: "col9"}, "col9"},
		{"IndexParamReq", &IndexParamReq{CollectionName: "col10"}, "col10"},
		{"CollectionReq", &CollectionReq{CollectionName: "col11"}, "col11"},
		{"RunAnalyzerReq", &RunAnalyzerReq{CollectionName: "col12"}, "col12"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.req.GetCollectionName())
		})
	}
}

func TestBuildFieldPartialUpdateOps(t *testing.T) {
	ops, err := buildFieldPartialUpdateOpsV2([]FieldPartialUpdateOpReq{
		{FieldName: "tags", Op: "ARRAY_APPEND"},
		{FieldName: "scores", Op: "ARRAY_REMOVE"},
		{FieldName: "profile", Op: "PATH_REPLACE", Path: "[1][age]"},
	})
	assert.NoError(t, err)
	assert.Len(t, ops, 3)
	assert.Equal(t, "tags", ops[0].GetFieldName())
	assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_APPEND, ops[0].GetOp())
	assert.Equal(t, "scores", ops[1].GetFieldName())
	assert.Equal(t, schemapb.FieldPartialUpdateOp_ARRAY_REMOVE, ops[1].GetOp())
	assert.Equal(t, "profile", ops[2].GetFieldName())
	assert.Equal(t, schemapb.FieldPartialUpdateOp_PATH_REPLACE, ops[2].GetOp())
	assert.Equal(t, "[1][age]", ops[2].GetPath())
}

func TestBuildFieldPartialUpdateOps_RejectsUnknownOp(t *testing.T) {
	_, err := buildFieldPartialUpdateOps([]FieldPartialUpdateOpReq{
		{FieldName: "tags", Op: "ARRAY_EXTEND"},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported partial update op")
}

func TestBuildFieldPartialUpdateOps_PathReplaceIsV2Only(t *testing.T) {
	fieldOps := []FieldPartialUpdateOpReq{{FieldName: "profile", Op: "PATH_REPLACE", Path: "[1][age]"}}
	_, err := buildFieldPartialUpdateOps(fieldOps)
	assert.Error(t, err)

	ops, err := buildFieldPartialUpdateOpsV2(fieldOps)
	assert.NoError(t, err)
	assert.Len(t, ops, 1)
	assert.Equal(t, schemapb.FieldPartialUpdateOp_PATH_REPLACE, ops[0].GetOp())
	assert.Equal(t, "[1][age]", ops[0].GetPath())
}

func TestBuildFieldPartialUpdateOps_LegacyBuilderDoesNotForwardPath(t *testing.T) {
	ops, err := buildFieldPartialUpdateOps([]FieldPartialUpdateOpReq{{
		FieldName: "tags",
		Op:        "ARRAY_APPEND",
		Path:      "[1]",
	}})
	assert.NoError(t, err)
	assert.Len(t, ops, 1)
	assert.Empty(t, ops[0].GetPath())
}

func TestHasNonReplaceFieldPartialUpdateOp(t *testing.T) {
	assert.False(t, hasNonReplaceFieldPartialUpdateOp(nil))
	assert.False(t, hasNonReplaceFieldPartialUpdateOp([]*schemapb.FieldPartialUpdateOp{{Op: schemapb.FieldPartialUpdateOp_REPLACE}}))
	assert.True(t, hasNonReplaceFieldPartialUpdateOp([]*schemapb.FieldPartialUpdateOp{{Op: schemapb.FieldPartialUpdateOp_PATH_REPLACE}}))
}

func TestTypeSchemaReqGetProto(t *testing.T) {
	nullable := true
	ts := &TypeSchemaReq{
		TypeParams: map[string]interface{}{"max_capacity": 10},
		ArrayElement: &TypeSchemaReq{
			TypeParams: map[string]interface{}{"max_capacity": "4"},
			Nullable:   &nullable,
			ArrayElement: &TypeSchemaReq{
				LeafType:   "VarChar",
				TypeParams: map[string]interface{}{"max_length": 8},
			},
		},
	}
	root, err := ts.GetProto("tags")
	require.NoError(t, err)
	assert.False(t, root.GetNullable())
	assert.Equal(t, "10", kvPairsToMap(root.GetTypeParams())[common.MaxCapacityKey])
	inner := root.GetArrayElement()
	require.NotNil(t, inner)
	assert.True(t, inner.GetNullable())
	assert.Equal(t, "4", kvPairsToMap(inner.GetTypeParams())[common.MaxCapacityKey])
	leaf := inner.GetArrayElement()
	require.NotNil(t, leaf)
	assert.Equal(t, schemapb.DataType_VarChar, leaf.GetLeafType())
	assert.False(t, leaf.GetNullable())
	assert.Equal(t, "8", kvPairsToMap(leaf.GetTypeParams())[common.MaxLengthKey])

	for _, bad := range []*TypeSchemaReq{
		{},
		{LeafType: "Int64", ArrayElement: &TypeSchemaReq{LeafType: "Int64"}},
		{ArrayElement: &TypeSchemaReq{LeafType: "int64"}},
		{ArrayElement: &TypeSchemaReq{ArrayElement: &TypeSchemaReq{}}},
	} {
		_, err := bad.GetProto("tags")
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	}
}

func TestFieldSchemaGetProtoElementNullable(t *testing.T) {
	field := &FieldSchema{
		FieldName:       "tags",
		DataType:        "Array",
		ElementDataType: "Array",
		ElementNullable: true,
		TypeSchema: &TypeSchemaReq{ArrayElement: &TypeSchemaReq{
			ArrayElement: &TypeSchemaReq{LeafType: "Int64"},
		}},
	}
	fieldProto, err := field.GetProto(context.Background())
	require.NoError(t, err)
	assert.True(t, fieldProto.GetElementNullable())
	assert.Equal(t, schemapb.DataType_Array, fieldProto.GetElementType())
	assert.Equal(t, schemapb.DataType_Int64, fieldProto.GetTypeSchema().GetArrayElement().GetArrayElement().GetLeafType())

	field.TypeSchema = &TypeSchemaReq{}
	_, err = field.GetProto(context.Background())
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestStructArrayFieldSchemaGetProtoElementNullable(t *testing.T) {
	ctx := context.Background()
	newStruct := func(nullable bool, sub FieldSchema) *StructArrayFieldSchema {
		return &StructArrayFieldSchema{
			FieldName:  "clips",
			Nullable:   nullable,
			TypeParams: map[string]interface{}{"max_capacity": 16},
			Fields:     []FieldSchema{sub},
		}
	}

	t.Run("scalar and vector", func(t *testing.T) {
		sf := newStruct(false, FieldSchema{FieldName: "lang", DataType: "Array", ElementDataType: "VarChar", ElementNullable: true})
		sf.Fields = append(sf.Fields, FieldSchema{
			FieldName: "emb", DataType: "ArrayOfVector", ElementDataType: "FloatVector", ElementNullable: true,
			ElementTypeParams: map[string]interface{}{"dim": 4},
		})
		structProto, err := sf.GetProto(ctx)
		require.NoError(t, err)
		require.Len(t, structProto.GetFields(), 2)
		for _, sub := range structProto.GetFields() {
			assert.True(t, sub.GetElementNullable(), sub.GetName())
			assert.Nil(t, sub.GetTypeSchema(), sub.GetName())
			assert.Equal(t, "16", kvPairsToMap(sub.GetTypeParams())[common.MaxCapacityKey], sub.GetName())
		}
	})

	t.Run("nested fills root capacity and inner nullable", func(t *testing.T) {
		sf := newStruct(true, FieldSchema{
			FieldName: "tags", DataType: "Array", ElementDataType: "Array", ElementNullable: true,
			TypeSchema: &TypeSchemaReq{ArrayElement: &TypeSchemaReq{
				TypeParams:   map[string]interface{}{"max_capacity": 4},
				ArrayElement: &TypeSchemaReq{LeafType: "Int64", Nullable: new(bool)},
			}},
		})
		structProto, err := sf.GetProto(ctx)
		require.NoError(t, err)
		sub := structProto.GetFields()[0]
		root := sub.GetTypeSchema()
		assert.Equal(t, "16", kvPairsToMap(root.GetTypeParams())[common.MaxCapacityKey])
		// the struct capacity goes to the root only; the sub-field's own
		// type params would be compared with the root as its mirror
		assert.NotContains(t, kvPairsToMap(sub.GetTypeParams()), common.MaxCapacityKey)
		assert.True(t, root.GetArrayElement().GetNullable())
		assert.Equal(t, "4", kvPairsToMap(root.GetArrayElement().GetTypeParams())[common.MaxCapacityKey])
		assert.False(t, root.GetArrayElement().GetArrayElement().GetNullable())
	})

	t.Run("nested keeps an explicit root capacity", func(t *testing.T) {
		sf := newStruct(false, FieldSchema{
			FieldName: "tags", DataType: "Array", ElementDataType: "Array",
			TypeSchema: &TypeSchemaReq{
				TypeParams:   map[string]interface{}{"max_capacity": 16},
				ArrayElement: &TypeSchemaReq{ArrayElement: &TypeSchemaReq{LeafType: "Int64"}},
			},
		})
		structProto, err := sf.GetProto(ctx)
		require.NoError(t, err)
		root := structProto.GetFields()[0].GetTypeSchema()
		assert.Len(t, root.GetTypeParams(), 1)
		assert.False(t, root.GetArrayElement().GetNullable())
	})

	t.Run("nested inner nullable conflicts with elementNullable", func(t *testing.T) {
		inner := false
		sf := newStruct(false, FieldSchema{
			FieldName: "tags", DataType: "Array", ElementDataType: "Array", ElementNullable: true,
			TypeSchema: &TypeSchemaReq{ArrayElement: &TypeSchemaReq{
				Nullable:     &inner,
				ArrayElement: &TypeSchemaReq{LeafType: "Int64"},
			}},
		})
		_, err := sf.GetProto(ctx)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Contains(t, err.Error(), "must match elementNullable")
	})

	t.Run("sub-field nullable", func(t *testing.T) {
		lang := func(nullable *bool) FieldSchema {
			return FieldSchema{FieldName: "lang", DataType: "Array", ElementDataType: "Int64", Nullable: nullable}
		}
		// equal to the struct's: accepted, which lets a described schema be
		// sent back as it is
		for _, structNullable := range []bool{true, false} {
			structProto, err := newStruct(structNullable, lang(new(structNullable))).GetProto(ctx)
			require.NoError(t, err)
			assert.Equal(t, structNullable, structProto.GetFields()[0].GetNullable())
		}
		// omitted: takes the struct's
		for _, structNullable := range []bool{true, false} {
			structProto, err := newStruct(structNullable, lang(nil)).GetProto(ctx)
			require.NoError(t, err)
			assert.Equal(t, structNullable, structProto.GetFields()[0].GetNullable())
		}
		// different from the struct's: rejected either way
		for _, structNullable := range []bool{true, false} {
			_, err := newStruct(structNullable, lang(new(!structNullable))).GetProto(ctx)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		}
	})

	t.Run("sub-field nullable presence in JSON", func(t *testing.T) {
		decode := func(subNullable string) *StructArrayFieldSchema {
			sub := `{"fieldName": "lang", "dataType": "Array", "elementDataType": "VarChar", "elementTypeParams": {"max_length": 16}` + subNullable + `}`
			var sf StructArrayFieldSchema
			require.NoError(t, json.Unmarshal([]byte(`{"fieldName": "clips", "nullable": true, "typeParams": {"max_capacity": 4}, "fields": [`+sub+`]}`), &sf))
			return &sf
		}
		_, err := decode(`, "nullable": false`).GetProto(ctx)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		structProto, err := decode(`, "nullable": true`).GetProto(ctx)
		require.NoError(t, err)
		assert.True(t, structProto.GetFields()[0].GetNullable())
		structProto, err = decode(``).GetProto(ctx)
		require.NoError(t, err)
		assert.True(t, structProto.GetFields()[0].GetNullable())
	})
}

// TestDescribeStructSubFieldRoundTrip: describe prints elementNullable and
// typeSchema in the form a create request takes, and the described struct
// converts back to the same sub-field schema.
func TestDescribeStructSubFieldRoundTrip(t *testing.T) {
	ctx := context.Background()
	sf := &StructArrayFieldSchema{
		FieldName:  "clips",
		Nullable:   true,
		TypeParams: map[string]interface{}{"max_capacity": 16},
		Fields: []FieldSchema{
			{FieldName: "lang", DataType: "Array", ElementDataType: "VarChar", ElementNullable: true, ElementTypeParams: map[string]interface{}{"max_length": 8}},
			{
				FieldName: "tags", DataType: "Array", ElementDataType: "Array", ElementNullable: true,
				TypeSchema: &TypeSchemaReq{ArrayElement: &TypeSchemaReq{
					TypeParams:   map[string]interface{}{"max_capacity": 4},
					ArrayElement: &TypeSchemaReq{LeafType: "Int64", Nullable: &[]bool{true}[0]},
				}},
			},
			{FieldName: "plain", DataType: "Array", ElementDataType: "Int64"},
		},
	}
	created, err := sf.GetProto(ctx)
	require.NoError(t, err)
	// what the server stores: sub-fields take the struct's nullable
	for _, sub := range created.GetFields() {
		sub.Nullable = true
		if sub.GetTypeSchema() != nil {
			sub.TypeSchema.Nullable = true
		}
	}

	described := printStructArrayFieldsV2([]*schemapb.StructArrayFieldSchema{created})
	require.Len(t, described, 1)
	subs := described[0]["fields"].([]gin.H)
	assert.Equal(t, true, subs[0][HTTPReturnFieldElementNullable])
	assert.NotContains(t, subs[0], HTTPReturnFieldTypeSchema)
	assert.Equal(t, true, subs[1][HTTPReturnFieldElementNullable])
	assert.NotContains(t, subs[2], HTTPReturnFieldElementNullable)
	assert.NotContains(t, subs[2], HTTPReturnFieldTypeSchema)

	raw, err := json.Marshal(subs[1][HTTPReturnFieldTypeSchema])
	require.NoError(t, err)
	assert.JSONEq(t, `{"typeParams": {"max_capacity": "16"}, "nullable": true,
		"arrayElement": {"typeParams": {"max_capacity": "4"}, "nullable": true,
			"arrayElement": {"leafType": "Int64", "nullable": true}}}`, string(raw))

	// feed the described sub-field back into a create request
	var again TypeSchemaReq
	require.NoError(t, json.Unmarshal(raw, &again))
	roundTrip := &StructArrayFieldSchema{
		FieldName: "clips", Nullable: true, TypeParams: map[string]interface{}{"max_capacity": 16},
		Fields: []FieldSchema{{
			FieldName: "tags", DataType: "Array", ElementDataType: "Array",
			ElementNullable: true, Nullable: new(true), TypeSchema: &again,
		}},
	}
	recreated, err := roundTrip.GetProto(ctx)
	require.NoError(t, err)
	assert.True(t, proto.Equal(created.GetFields()[1].GetTypeSchema(), recreated.GetFields()[0].GetTypeSchema()))
}
