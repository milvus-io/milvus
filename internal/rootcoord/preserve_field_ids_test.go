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

package rootcoord

import (
	"context"
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// preservedSchema is a restored schema with gaps left by dropped fields: a
// struct with an original and an added sub-field, an embedding function, and the
// dynamic field. It mirrors what DescribeCollection returns after the proxy
// added the stored sub-field names and $meta.
func preservedSchema(name string) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:               name,
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 102, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "64"},
			}},
			{FieldID: 103, Name: "dense", DataType: schemapb.DataType_FloatVector, IsFunctionOutput: true, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "4"},
			}},
			{FieldID: 104, Name: MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true},
			{FieldID: 109, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "4"},
			}},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 105, Name: "s",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID: 106, Name: "s[id]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
					TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "4"}},
				},
				{
					FieldID: 112, Name: "s[added]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
					ElementNullable: true,
					TypeParams:      []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "4"}},
				},
			},
		}},
		Functions: []*schemapb.FunctionSchema{{
			Id: 101, Name: "embed", Type: schemapb.FunctionType_TextEmbedding,
			InputFieldNames: []string{"text"}, InputFieldIds: []int64{102},
			OutputFieldNames: []string{"dense"}, OutputFieldIds: []int64{103},
		}},
	}
}

func preservedTask(schema *schemapb.CollectionSchema, properties ...*commonpb.KeyValuePair) *createCollectionTask {
	return &createCollectionTask{
		Req: &milvuspb.CreateCollectionRequest{
			Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			CollectionName: schema.GetName(),
			Properties:     properties,
		},
		body: &message.CreateCollectionRequest{
			CollectionName:   schema.GetName(),
			CollectionSchema: schema,
		},
		preserveFieldID: true,
	}
}

func TestPrepareSchemaPreservesFieldIDs(t *testing.T) {
	schema := preservedSchema("restored")
	task := preservedTask(schema, &commonpb.KeyValuePair{Key: common.MaxFieldIDKey, Value: "120"})
	require.NoError(t, task.prepareSchema(context.TODO()))

	got := task.body.CollectionSchema
	ids := make(map[string]int64)
	for _, field := range typeutil.GetAllFieldSchemas(got) {
		ids[field.GetName()] = field.GetFieldID()
	}
	require.Equal(t, map[string]int64{
		"pk": 100, "text": 102, "dense": 103, MetaFieldName: 104, "vec": 109,
		"s[id]": 106, "s[added]": 112,
		RowIDFieldName: RowIDField, TimeStampFieldName: TimeStampField,
	}, ids)
	require.Equal(t, int64(105), got.GetStructArrayFields()[0].GetFieldID())
	for _, field := range got.GetFields() {
		if field.GetName() == MetaFieldName {
			require.True(t, field.GetIsDynamic())
		}
	}
	require.Len(t, got.GetFunctions(), 1)
	function := got.GetFunctions()[0]
	require.Equal(t, int64(101), function.GetId())
	require.Equal(t, []int64{102}, function.GetInputFieldIds())
	require.Equal(t, []int64{103}, function.GetOutputFieldIds())

	// The source max_field_id keeps the IDs of dropped fields reserved.
	maxFieldID, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(common.MaxFieldIDKey, got.GetProperties())
	require.True(t, ok)
	require.Equal(t, "120", maxFieldID)
}

func TestPrepareSchemaPreservedMaxFieldIDFromSchema(t *testing.T) {
	task := preservedTask(preservedSchema("restored"))
	require.NoError(t, task.prepareSchema(context.TODO()))
	maxFieldID, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(common.MaxFieldIDKey, task.body.CollectionSchema.GetProperties())
	require.True(t, ok)
	require.Equal(t, strconv.Itoa(112), maxFieldID, "without the property the largest preserved ID is used")
}

func TestPrepareSchemaPreservedFunctionIDsResolvedFromNames(t *testing.T) {
	schema := preservedSchema("restored")
	schema.Functions[0].InputFieldIds = nil
	schema.Functions[0].OutputFieldIds = nil
	task := preservedTask(schema)
	require.NoError(t, task.prepareSchema(context.TODO()))
	function := task.body.CollectionSchema.GetFunctions()[0]
	require.Equal(t, int64(101), function.GetId())
	require.Equal(t, []int64{102}, function.GetInputFieldIds())
	require.Equal(t, []int64{103}, function.GetOutputFieldIds())
}

func TestPrepareSchemaRejectsInvalidPreservedIDs(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*schemapb.CollectionSchema)
		props   []*commonpb.KeyValuePair
		errText string
	}{
		{"missing field id", func(s *schemapb.CollectionSchema) { s.Fields[0].FieldID = 0 }, nil, "field pk has field ID 0"},
		{"field id below user range", func(s *schemapb.CollectionSchema) { s.Fields[1].FieldID = 99 }, nil, "field text has field ID 99"},
		{"missing struct id", func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].FieldID = 0 }, nil, "field s has field ID 0"},
		{"missing sub-field id", func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields[1].FieldID = 0 }, nil, "field s[added] has field ID 0"},
		{"duplicate field ids", func(s *schemapb.CollectionSchema) { s.Fields[4].FieldID = 100 }, nil, "fields pk and vec have the same field ID 100"},
		{"sub-field duplicates field", func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields[1].FieldID = 109 }, nil, "fields vec and s[added] have the same field ID 109"},
		{"sub-field below struct", func(s *schemapb.CollectionSchema) {
			s.StructArrayFields[0].FieldID = 110
		}, nil, "sub-field s[id] has field ID 106, which is not greater than the field ID 110 of its struct s"},
		{
			"dynamic field missing", func(s *schemapb.CollectionSchema) { s.Fields = append(s.Fields[:3], s.Fields[4:]...) }, nil,
			"must include the dynamic field: name $meta, is_dynamic=true, data type JSON, and the source field ID. Only its field ID is used",
		},
		{
			"dynamic field not enabled", func(s *schemapb.CollectionSchema) { s.EnableDynamicField = false }, nil,
			"does not enable the dynamic field",
		},
		{"namespace field not created", func(s *schemapb.CollectionSchema) {
			s.Fields = append(s.Fields, &schemapb.FieldSchema{FieldID: 111, Name: NamespaceFieldName, DataType: schemapb.DataType_VarChar})
		}, nil, "the collection does not get a namespace field"},
		{"function id missing", func(s *schemapb.CollectionSchema) { s.Functions[0].Id = 0 }, nil, "function embed has ID 0"},
		{
			"function input id mismatch", func(s *schemapb.CollectionSchema) { s.Functions[0].InputFieldIds = []int64{100} }, nil,
			"function embed refers to input field text by field ID 100, but the field has field ID 102",
		},
		{
			"function output id count", func(s *schemapb.CollectionSchema) { s.Functions[0].OutputFieldIds = []int64{103, 104} }, nil,
			"function embed has 1 output field names but 2 output field IDs",
		},
		{
			"function input not found", func(s *schemapb.CollectionSchema) { s.Functions[0].InputFieldNames = []string{"missing"} }, nil,
			"input field missing of function embed not found",
		},
		{
			"malformed max_field_id", nil,
			[]*commonpb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "abc"}},
			"property max_field_id must be an integer",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := preservedSchema("restored")
			if tc.mutate != nil {
				tc.mutate(schema)
			}
			err := preservedTask(schema, tc.props...).prepareSchema(context.TODO())
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.ErrorContains(t, err, tc.errText)
		})
	}
}

func TestPrepareSchemaPreservedNamespaceField(t *testing.T) {
	withNamespace := func() *schemapb.CollectionSchema {
		schema := preservedSchema("restored")
		schema.EnableNamespace = true
		return schema
	}
	t.Run("kept", func(t *testing.T) {
		schema := withNamespace()
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 115, Name: NamespaceFieldName, DataType: schemapb.DataType_VarChar})
		task := preservedTask(schema)
		require.NoError(t, task.prepareSchema(context.TODO()))
		found := false
		for _, field := range task.body.CollectionSchema.GetFields() {
			if field.GetName() == NamespaceFieldName {
				found = true
				require.Equal(t, int64(115), field.GetFieldID())
			}
		}
		require.True(t, found)
	})
	t.Run("missing", func(t *testing.T) {
		err := preservedTask(withNamespace()).prepareSchema(context.TODO())
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorContains(t, err, "must include it: name $namespace_id, data type VarChar, and the source field ID. Only its field ID is used")
	})
}

// The origin marker of an added sub-field is internal: a CreateCollection
// request is rejected when it carries the marker, with or without preserved
// field IDs. Restore from a snapshot keeps it.
func TestPrepareSchemaStructSubFieldOriginMarker(t *testing.T) {
	marked := func() *schemapb.CollectionSchema {
		schema := preservedSchema("restored")
		schemautil.MarkAddedStructSubField(schema.StructArrayFields[0].Fields[1])
		return schema
	}
	for _, preserve := range []bool{false, true} {
		t.Run("create collection preserve="+strconv.FormatBool(preserve), func(t *testing.T) {
			task := preservedTask(marked())
			task.preserveFieldID = preserve
			err := task.prepareSchema(context.TODO())
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.ErrorContains(t, err, "type param "+common.StructSubFieldAddedKey+" of field \"s[added]\" is reserved")
		})
	}
	t.Run("restore from snapshot", func(t *testing.T) {
		task := preservedTask(marked())
		task.allowStructSubFieldOriginMarker = true
		require.NoError(t, task.prepareSchema(context.TODO()))
		subFields := task.body.CollectionSchema.GetStructArrayFields()[0].GetFields()
		require.False(t, schemautil.IsAddedStructSubField(subFields[0]))
		require.True(t, schemautil.IsAddedStructSubField(subFields[1]), "the marker must survive the restore")
	})
}

// PrepareRestoreCollection is the only caller that accepts the marker.
func TestPrepareRestoreCollectionAllowsStructSubFieldOriginMarker(t *testing.T) {
	schema, err := proto.Marshal(preservedSchema("restored"))
	require.NoError(t, err)
	var captured *createCollectionTask
	patch := mockey.Mock((*createCollectionTask).Prepare).To(func(task *createCollectionTask, _ context.Context) error {
		captured = task
		return merr.WrapErrServiceInternalMsg("stop after capture")
	}).Build()
	defer patch.UnPatch()

	core := &Core{}
	_, _, err = core.PrepareRestoreCollection(context.TODO(), &milvuspb.CreateCollectionRequest{
		DbName: "default", CollectionName: "restored", Schema: schema,
	}, nil)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.NotNil(t, captured)
	require.True(t, captured.preserveFieldID)
	require.True(t, captured.allowStructSubFieldOriginMarker)
}
