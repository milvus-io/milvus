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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// preservedSchema is a restored schema with gaps left by dropped fields: a
// struct with an original and an added sub-field, an embedding function, and
// the dynamic field. It mirrors what DescribeCollection returns after the proxy
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

func TestPrepareSchemaPreservesFieldIDsAndStripsCreateDirective(t *testing.T) {
	schema := preservedSchema("restored")
	task := preservedTask(
		schema,
		&commonpb.KeyValuePair{Key: util.PreserveFieldIdsKey, Value: "true"},
		&commonpb.KeyValuePair{Key: common.CollectionTTLConfigKey, Value: "3600"},
		&commonpb.KeyValuePair{Key: common.MaxFieldIDKey, Value: "120"},
	)
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
			require.Equal(t, schemapb.DataType_JSON, field.GetDataType())
		}
	}

	fn := got.GetFunctions()[0]
	require.Equal(t, int64(101), fn.GetId())
	require.Equal(t, []int64{102}, fn.GetInputFieldIds())
	require.Equal(t, []int64{103}, fn.GetOutputFieldIds())

	maxID, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(common.MaxFieldIDKey, got.GetProperties())
	require.True(t, ok)
	require.Equal(t, "120", maxID)
	ttl, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(common.CollectionTTLConfigKey, got.GetProperties())
	require.True(t, ok)
	require.Equal(t, "3600", ttl)

	// preserve_field_ids is a request-only directive and must not enter WAL or etcd metadata.
	_, leakedInWAL := funcutil.TryGetAttrByKeyFromRepeatedKV(util.PreserveFieldIdsKey, got.GetProperties())
	require.False(t, leakedInWAL, "preserve_field_ids must be stripped before building the WAL schema")
	collModel := newCollectionModel(&message.CreateCollectionMessageHeader{CollectionId: 1, DbId: 1}, task.body, 0)
	_, leakedInModel := funcutil.TryGetAttrByKeyFromRepeatedKV(util.PreserveFieldIdsKey, collModel.Properties)
	require.False(t, leakedInModel, "preserve_field_ids must not be persisted on model.Collection")
}

func TestPrepareSchemaPreservesFunctionFieldIDsWhenOmitted(t *testing.T) {
	schema := preservedSchema("restored")
	schema.Functions[0].InputFieldIds = nil
	schema.Functions[0].OutputFieldIds = nil
	task := preservedTask(schema, &commonpb.KeyValuePair{Key: common.MaxFieldIDKey, Value: "105"})
	require.NoError(t, task.prepareSchema(context.TODO()))

	got := task.body.CollectionSchema
	require.Equal(t, []int64{102}, got.GetFunctions()[0].GetInputFieldIds())
	require.Equal(t, []int64{103}, got.GetFunctions()[0].GetOutputFieldIds())
	// Schema max (112) beats smaller request max_field_id (105).
	maxID, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(common.MaxFieldIDKey, got.GetProperties())
	require.True(t, ok)
	require.Equal(t, "112", maxID)
}

func TestPrepareSchemaRejectsInvalidPreservedSchema(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*schemapb.CollectionSchema)
		props   []*commonpb.KeyValuePair
		errText string
	}{
		{"top-level field id missing", func(s *schemapb.CollectionSchema) { s.Fields[0].FieldID = 0 }, nil, "field pk has field ID 0"},
		{"top-level field id below 100", func(s *schemapb.CollectionSchema) { s.Fields[0].FieldID = 99 }, nil, "field pk has field ID 99"},
		{"struct field id missing", func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].FieldID = 0 }, nil, "field s has field ID 0"},
		{"sub-field id missing", func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields[0].FieldID = 0 }, nil, "field s[id] has field ID 0"},
		{"sub-field id not above struct", func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields[0].FieldID = 105 }, nil, "fields s and s[id] have the same field ID 105"},
		{"sub-field id below struct", func(s *schemapb.CollectionSchema) {
			s.StructArrayFields[0].FieldID = 108
			s.StructArrayFields[0].Fields[0].FieldID = 106
		}, nil, "sub-field s[id] has field ID 106, which is not greater than the field ID 108 of its struct s"},
		{"duplicate top-level and sub-field id", func(s *schemapb.CollectionSchema) { s.StructArrayFields[0].Fields[0].FieldID = 109 }, nil, "fields vec and s[id] have the same field ID 109"},
		{"dynamic field missing", func(s *schemapb.CollectionSchema) {
			s.Fields = append(s.Fields[:3], s.Fields[4:]...)
		}, nil, "must include the dynamic field: name $meta"},
		{"dynamic field unexpected", func(s *schemapb.CollectionSchema) { s.EnableDynamicField = false }, nil, "does not enable the dynamic field"},
		{"duplicate dynamic field", func(s *schemapb.CollectionSchema) {
			s.Fields = append(s.Fields, &schemapb.FieldSchema{FieldID: 120, Name: MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true})
		}, nil, "more than one $meta field"},
		{"duplicate namespace field", func(s *schemapb.CollectionSchema) {
			s.EnableNamespace = true
			s.Fields = append(s.Fields,
				&schemapb.FieldSchema{FieldID: 120, Name: NamespaceFieldName, DataType: schemapb.DataType_VarChar},
				&schemapb.FieldSchema{FieldID: 121, Name: NamespaceFieldName, DataType: schemapb.DataType_VarChar},
			)
		}, nil, "more than one $namespace_id field"},
		{"namespace field unexpected when disabled", func(s *schemapb.CollectionSchema) {
			s.Fields = append(s.Fields, &schemapb.FieldSchema{FieldID: 111, Name: NamespaceFieldName, DataType: schemapb.DataType_VarChar})
		}, nil, "the collection does not get a namespace field"},
		{"namespace field unexpected in partition mode", func(s *schemapb.CollectionSchema) {
			s.EnableNamespace = true
			s.Fields = append(s.Fields, &schemapb.FieldSchema{FieldID: 111, Name: NamespaceFieldName, DataType: schemapb.DataType_VarChar})
		}, []*commonpb.KeyValuePair{{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition}}, "the collection does not get a namespace field"},
		{"function id missing", func(s *schemapb.CollectionSchema) { s.Functions[0].Id = 0 }, nil, "function embed has ID 0"},
		{"duplicate function id", func(s *schemapb.CollectionSchema) {
			s.Functions = append(s.Functions, &schemapb.FunctionSchema{
				Id: 101, Name: "embed2", Type: schemapb.FunctionType_TextEmbedding,
				InputFieldNames: []string{"text"}, OutputFieldNames: []string{"dense"},
			})
		}, nil, "functions embed and embed2 have the same ID 101"},
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
			"property max_field_id must be a non-negative integer",
		},
		{
			"negative max_field_id", nil,
			[]*commonpb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "-1"}},
			"property max_field_id must be a non-negative integer",
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
	t.Run("kept without duplicating isolation property", func(t *testing.T) {
		schema := withNamespace()
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 115, Name: NamespaceFieldName, DataType: schemapb.DataType_VarChar})
		task := preservedTask(
			schema,
			&commonpb.KeyValuePair{Key: util.PreserveFieldIdsKey, Value: "true"},
			&commonpb.KeyValuePair{Key: common.PartitionKeyIsolationKey, Value: "true"},
		)
		require.NoError(t, task.prepareSchema(context.TODO()))
		found := false
		for _, field := range task.body.CollectionSchema.GetFields() {
			if field.GetName() == NamespaceFieldName {
				found = true
				require.Equal(t, int64(115), field.GetFieldID())
				require.True(t, field.GetIsPartitionKey())
			}
		}
		require.True(t, found)

		isolationCount := 0
		for _, kv := range task.body.CollectionSchema.GetProperties() {
			if kv.GetKey() == common.PartitionKeyIsolationKey {
				isolationCount++
			}
			require.NotEqual(t, util.PreserveFieldIdsKey, kv.GetKey())
		}
		require.Equal(t, 1, isolationCount, "partitionkey.isolation must not be duplicated")
	})
	t.Run("missing", func(t *testing.T) {
		err := preservedTask(withNamespace()).prepareSchema(context.TODO())
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorContains(t, err, "must include it: name $namespace_id, data type VarChar, and the source field ID. Only its field ID is used")
	})
}

func TestPrepareSchemaRejectsStructSubFieldOriginMarkerOnCreateCollection(t *testing.T) {
	marked := func() *schemapb.CollectionSchema {
		schema := preservedSchema("restored")
		schemautil.MarkAddedStructSubField(schema.StructArrayFields[0].Fields[1])
		return schema
	}
	for _, preserve := range []bool{false, true} {
		t.Run("preserve="+strconv.FormatBool(preserve), func(t *testing.T) {
			task := preservedTask(marked())
			task.preserveFieldID = preserve
			err := task.prepareSchema(context.TODO())
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.ErrorContains(t, err, "type param "+common.StructSubFieldAddedKey+" of field \"s[added]\" is reserved")
		})
	}
}

func TestValidateIfCollectionExistsPreservedFieldIDsIdempotency(t *testing.T) {
	ctx := context.Background()
	header := &message.CreateCollectionMessageHeader{
		CollectionId: 1,
		DbId:         1,
		PartitionIds: []int64{10},
	}
	buildCandidate := func() (*createCollectionTask, *message.CreateCollectionRequest) {
		schema := preservedSchema("restored")
		task := preservedTask(
			schema,
			&commonpb.KeyValuePair{Key: util.PreserveFieldIdsKey, Value: "true"},
			&commonpb.KeyValuePair{Key: common.CollectionTTLConfigKey, Value: "600"},
			&commonpb.KeyValuePair{Key: common.MaxFieldIDKey, Value: "120"},
		)
		task.Core = &Core{meta: &MetaTable{}}
		task.header = header
		task.body.PartitionIDs = []int64{10}
		task.body.PartitionNames = []string{"_default"}
		require.NoError(t, task.prepareSchema(ctx))
		return task, task.body
	}

	t.Run("post-upgrade retry matches stripped collection properties", func(t *testing.T) {
		_, firstBody := buildCandidate()
		existing := newCollectionModel(header, firstBody, 0)

		aliasPatch := mockey.Mock((*MetaTable).DescribeAlias).Return("", merr.ErrCollectionNotFound).Build()
		defer aliasPatch.UnPatch()
		getPatch := mockey.Mock((*MetaTable).GetCollectionByName).Return(existing, nil).Build()
		defer getPatch.UnPatch()

		retryTask, _ := buildCandidate()
		require.ErrorIs(t, retryTask.validateIfCollectionExists(ctx), errIgnoredCreateCollection)
	})

	t.Run("rolling-upgrade retry matches pre-upgrade collection with persisted preserve_field_ids", func(t *testing.T) {
		_, firstBody := buildCandidate()
		existing := newCollectionModel(header, firstBody, 0)
		existing.Properties = append(existing.Properties, &commonpb.KeyValuePair{
			Key: util.PreserveFieldIdsKey, Value: "true",
		})

		aliasPatch := mockey.Mock((*MetaTable).DescribeAlias).Return("", merr.ErrCollectionNotFound).Build()
		defer aliasPatch.UnPatch()
		getPatch := mockey.Mock((*MetaTable).GetCollectionByName).Return(existing, nil).Build()
		defer getPatch.UnPatch()

		retryTask, _ := buildCandidate()
		require.ErrorIs(t, retryTask.validateIfCollectionExists(ctx), errIgnoredCreateCollection)

		// Different user property still fails the duplicate check.
		diffTask := preservedTask(
			preservedSchema("restored"),
			&commonpb.KeyValuePair{Key: util.PreserveFieldIdsKey, Value: "true"},
			&commonpb.KeyValuePair{Key: common.CollectionTTLConfigKey, Value: "999"},
			&commonpb.KeyValuePair{Key: common.MaxFieldIDKey, Value: "120"},
		)
		diffTask.Core = &Core{meta: &MetaTable{}}
		diffTask.header = header
		diffTask.body.PartitionIDs = []int64{10}
		diffTask.body.PartitionNames = []string{"_default"}
		require.NoError(t, diffTask.prepareSchema(ctx))
		require.ErrorIs(t, diffTask.validateIfCollectionExists(ctx), merr.ErrParameterInvalid)
	})

	t.Run("rolling-upgrade retry matches ordinary collection with persisted preserve_field_ids=false", func(t *testing.T) {
		buildOrdinary := func(preserveProp string, ttl string) *createCollectionTask {
			schema := &schemapb.CollectionSchema{
				Name:               "ordinary",
				EnableDynamicField: true,
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "4"},
					}},
				},
			}
			var props []*commonpb.KeyValuePair
			if preserveProp != "" {
				props = append(props, &commonpb.KeyValuePair{Key: util.PreserveFieldIdsKey, Value: preserveProp})
			}
			props = append(props, &commonpb.KeyValuePair{Key: common.CollectionTTLConfigKey, Value: ttl})
			task := &createCollectionTask{
				Core: &Core{meta: &MetaTable{}},
				Req: &milvuspb.CreateCollectionRequest{
					Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
					CollectionName: schema.GetName(),
					Properties:     props,
				},
				header: header,
				body: &message.CreateCollectionRequest{
					CollectionName:   schema.GetName(),
					CollectionSchema: schema,
					PartitionIDs:     []int64{10},
					PartitionNames:   []string{"_default"},
				},
				preserveFieldID: preserveProp == "true",
			}
			return task
		}

		firstTask := buildOrdinary("false", "600")
		require.NoError(t, firstTask.prepareSchema(ctx))
		existing := newCollectionModel(header, firstTask.body, 0)
		// Pre-upgrade RootCoord persisted preserve_field_ids=false on ordinary collections.
		existing.Properties = append(existing.Properties, &commonpb.KeyValuePair{
			Key: util.PreserveFieldIdsKey, Value: "false",
		})

		aliasPatch := mockey.Mock((*MetaTable).DescribeAlias).Return("", merr.ErrCollectionNotFound).Build()
		defer aliasPatch.UnPatch()
		getPatch := mockey.Mock((*MetaTable).GetCollectionByName).Return(existing, nil).Build()
		defer getPatch.UnPatch()

		retryTask := buildOrdinary("false", "600")
		require.NoError(t, retryTask.prepareSchema(ctx))
		require.ErrorIs(t, retryTask.validateIfCollectionExists(ctx), errIgnoredCreateCollection)

		// Omitting preserve_field_ids on an identical ordinary retry also succeeds.
		omittedTask := buildOrdinary("", "600")
		require.NoError(t, omittedTask.prepareSchema(ctx))
		require.ErrorIs(t, omittedTask.validateIfCollectionExists(ctx), errIgnoredCreateCollection)

		// Different user property still fails on ordinary collection with legacy preserve_field_ids=false.
		diffOrdinary := buildOrdinary("false", "999")
		require.NoError(t, diffOrdinary.prepareSchema(ctx))
		require.ErrorIs(t, diffOrdinary.validateIfCollectionExists(ctx), merr.ErrParameterInvalid)
	})

	t.Run("cross-mode retry between preserve_field_ids true and false does not match", func(t *testing.T) {
		seqSchema := func() *schemapb.CollectionSchema {
			return &schemapb.CollectionSchema{
				Name: "seq",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "4"},
					}},
				},
			}
		}
		presTask := preservedTask(seqSchema(), &commonpb.KeyValuePair{Key: util.PreserveFieldIdsKey, Value: "true"})
		presTask.Core = &Core{meta: &MetaTable{}}
		presTask.header = header
		presTask.body.PartitionIDs = []int64{10}
		presTask.body.PartitionNames = []string{"_default"}
		require.NoError(t, presTask.prepareSchema(ctx))

		existing := newCollectionModel(header, presTask.body, 0)
		existing.Properties = append(existing.Properties, &commonpb.KeyValuePair{
			Key: util.PreserveFieldIdsKey, Value: "true",
		})

		aliasPatch := mockey.Mock((*MetaTable).DescribeAlias).Return("", merr.ErrCollectionNotFound).Build()
		defer aliasPatch.UnPatch()
		getPatch := mockey.Mock((*MetaTable).GetCollectionByName).Return(existing, nil).Build()
		defer getPatch.UnPatch()

		ordinaryRetry := preservedTask(seqSchema(), &commonpb.KeyValuePair{Key: util.PreserveFieldIdsKey, Value: "false"})
		ordinaryRetry.preserveFieldID = false
		ordinaryRetry.Core = &Core{meta: &MetaTable{}}
		ordinaryRetry.header = header
		ordinaryRetry.body.PartitionIDs = []int64{10}
		ordinaryRetry.body.PartitionNames = []string{"_default"}
		require.NoError(t, ordinaryRetry.prepareSchema(ctx))
		require.ErrorIs(t, ordinaryRetry.validateIfCollectionExists(ctx), merr.ErrParameterInvalid)
	})

	t.Run("pre-upgrade milvus-table with preserve_field_ids=true requires matching field IDs", func(t *testing.T) {
		properties := []*commonpb.KeyValuePair{{Key: common.ConsistencyLevel, Value: "Strong"}}
		existingProps := append([]*commonpb.KeyValuePair(nil), properties...)
		existingProps = append(existingProps, &commonpb.KeyValuePair{Key: util.PreserveFieldIdsKey, Value: "true"})
		existingBody := &message.CreateCollectionRequest{
			CollectionName: "ext_target",
			CollectionSchema: &schemapb.CollectionSchema{
				Name:           "ext_target",
				ExternalSource: "s3://bucket/snapshot.json",
				ExternalSpec:   `{"format":"milvus-table"}`,
				Fields: []*schemapb.FieldSchema{
					{FieldID: 105, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, ExternalField: "pk"},
					{FieldID: 108, Name: "doc", DataType: schemapb.DataType_VarChar, ExternalField: "text"},
				},
				Properties: updateMaxFieldIDProperty(existingProps, 108),
			},
			PartitionIDs:   []int64{10},
			PartitionNames: []string{"_default"},
		}
		existing := newCollectionModel(header, existingBody, 0)

		aliasPatch := mockey.Mock((*MetaTable).DescribeAlias).Return("", merr.ErrCollectionNotFound).Build()
		defer aliasPatch.UnPatch()
		getPatch := mockey.Mock((*MetaTable).GetCollectionByName).Return(existing, nil).Build()
		defer getPatch.UnPatch()

		buildMilvusTableRetry := func(id1, id2 int64) *createCollectionTask {
			reqProps := append([]*commonpb.KeyValuePair(nil), properties...)
			reqProps = append(reqProps, &commonpb.KeyValuePair{Key: util.PreserveFieldIdsKey, Value: "true"})
			schema := &schemapb.CollectionSchema{
				Name:           "ext_target",
				ExternalSource: "s3://bucket/snapshot.json",
				ExternalSpec:   `{"format":"milvus-table"}`,
				Fields: []*schemapb.FieldSchema{
					{FieldID: id1, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, ExternalField: "pk"},
					{FieldID: id2, Name: "doc", DataType: schemapb.DataType_VarChar, ExternalField: "text"},
				},
				Properties: updateMaxFieldIDProperty(stripCreateOnlyProperties(reqProps), max(id1, id2)),
			}
			return &createCollectionTask{
				Core: &Core{meta: &MetaTable{}},
				Req: &milvuspb.CreateCollectionRequest{
					CollectionName: "ext_target",
					Properties:     reqProps,
				},
				header: header,
				body: &message.CreateCollectionRequest{
					CollectionName:   "ext_target",
					CollectionSchema: schema,
					PartitionIDs:     []int64{10},
					PartitionNames:   []string{"_default"},
				},
				preserveFieldID: true,
			}
		}

		// Same preserved field IDs match the pre-upgrade milvus-table collection.
		sameIDsTask := buildMilvusTableRetry(105, 108)
		require.ErrorIs(t, sameIDsTask.validateIfCollectionExists(ctx), errIgnoredCreateCollection)

		// Mismatched preserved field IDs must not be silently overwritten to match existing.
		diffIDsTask := buildMilvusTableRetry(205, 208)
		require.ErrorIs(t, diffIDsTask.validateIfCollectionExists(ctx), merr.ErrParameterInvalid)
	})
}

func TestBroadcastCreateCollectionV1PreservedNamespacePartitions(t *testing.T) {
	for _, tc := range []struct {
		name               string
		enableNamespace    bool
		namespaceFlagOnFld bool
		metaFlagOnFld      bool
		reqPartitions      int64
		wantPartitions     int64
	}{
		{name: "minimal namespace stub keeps explicit partition count", enableNamespace: true, namespaceFlagOnFld: false, reqPartitions: 32, wantPartitions: 32},
		{name: "canonical namespace stub keeps explicit partition count", enableNamespace: true, namespaceFlagOnFld: true, reqPartitions: 32, wantPartitions: 32},
		{name: "minimal namespace stub defaults partition count", enableNamespace: true, namespaceFlagOnFld: false, reqPartitions: 0, wantPartitions: common.DefaultPartitionsWithPartitionKey},
		{name: "bogus partition-key flag on $meta is ignored", enableNamespace: false, metaFlagOnFld: true, reqPartitions: 32, wantPartitions: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey(tc.name, t, func() {
				schema := preservedSchema("ns_restored")
				schema.EnableNamespace = tc.enableNamespace
				if tc.metaFlagOnFld {
					for _, f := range schema.Fields {
						if f.GetName() == MetaFieldName {
							f.IsPartitionKey = true
						}
					}
				}
				if tc.enableNamespace {
					schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
						FieldID:        115,
						Name:           NamespaceFieldName,
						DataType:       schemapb.DataType_VarChar,
						IsPartitionKey: tc.namespaceFlagOnFld,
					})
				}
				schemaBytes, err := proto.Marshal(schema)
				require.NoError(t, err)

				meta := mockrootcoord.NewIMetaTable(t)
				core := newTestCore(withMeta(meta))

				broadcastAPI := mock_broadcaster.NewMockBroadcastAPI(t)
				var broadcastedMsg message.BroadcastMutableMessage
				broadcastAPI.EXPECT().Broadcast(mock.Anything, mock.Anything).
					Run(func(_ context.Context, msg message.BroadcastMutableMessage) {
						broadcastedMsg = msg
					}).
					Return(nil, nil).Once()
				broadcastAPI.EXPECT().Close().Return().Once()

				mockey.Mock((*Core).startBroadcastWithCollectionLock).Return(broadcastAPI, nil).Build()
				var preparedPartitions int64
				mockey.Mock((*createCollectionTask).Prepare).To(func(task *createCollectionTask, ctx context.Context) error {
					preparedPartitions = task.Req.GetNumPartitions()
					require.NoError(t, task.prepareSchema(ctx))
					task.header.CollectionId = 1
					task.body.CollectionID = 1
					task.body.VirtualChannelNames = []string{"by-dev-rootcoord-dml_0_1v0"}
					task.body.PhysicalChannelNames = []string{"by-dev-rootcoord-dml_0"}
					task.body.PartitionIDs = make([]int64, task.Req.GetNumPartitions())
					task.body.PartitionNames = make([]string, task.Req.GetNumPartitions())
					return nil
				}).Build()

				props := []*commonpb.KeyValuePair{
					{Key: util.PreserveFieldIdsKey, Value: "true"},
				}
				if tc.enableNamespace {
					props = append(props, &commonpb.KeyValuePair{Key: common.PartitionKeyIsolationKey, Value: "true"})
				}
				err = core.broadcastCreateCollectionV1(context.Background(), &milvuspb.CreateCollectionRequest{
					DbName:         "default",
					CollectionName: schema.GetName(),
					ShardsNum:      1,
					NumPartitions:  tc.reqPartitions,
					Schema:         schemaBytes,
					Properties:     props,
				})
				require.NoError(t, err)
				require.Equal(t, tc.wantPartitions, preparedPartitions)
				require.NotNil(t, broadcastedMsg)
			})
		})
	}
}

func TestConvertModelToDescStripsLegacyPreserveFieldIDs(t *testing.T) {
	header := &message.CreateCollectionMessageHeader{
		CollectionId: 10,
		DbId:         1,
		PartitionIds: []int64{100},
	}
	task := preservedTask(
		preservedSchema("legacy_restored"),
		&commonpb.KeyValuePair{Key: util.PreserveFieldIdsKey, Value: "true"},
		&commonpb.KeyValuePair{Key: common.CollectionTTLConfigKey, Value: "1800"},
	)
	task.body.PartitionIDs = []int64{100}
	task.body.PartitionNames = []string{"_default"}
	require.NoError(t, task.prepareSchema(context.Background()))

	t.Run("strips legacy persisted preserve_field_ids without mutating model", func(t *testing.T) {
		coll := newCollectionModel(header, task.body, 0)
		// Simulate a v3.0.0–v3.0.6 collection that persisted preserve_field_ids in etcd.
		coll.Properties = append(coll.Properties, &commonpb.KeyValuePair{
			Key: util.PreserveFieldIdsKey, Value: "true",
		})
		origLen := len(coll.Properties)

		desc := convertModelToDesc(coll, nil, "default")
		_, inRespProps := funcutil.TryGetAttrByKeyFromRepeatedKV(util.PreserveFieldIdsKey, desc.GetProperties())
		require.False(t, inRespProps, "DescribeCollection.Properties must not expose legacy preserve_field_ids")
		_, inSchemaProps := funcutil.TryGetAttrByKeyFromRepeatedKV(util.PreserveFieldIdsKey, desc.GetSchema().GetProperties())
		require.False(t, inSchemaProps, "DescribeCollection.Schema.Properties must not expose legacy preserve_field_ids")

		ttl, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(common.CollectionTTLConfigKey, desc.GetProperties())
		require.True(t, ok)
		require.Equal(t, "1800", ttl)

		// Underlying model in MetaTable stays untouched for rolling-upgrade idempotency checks.
		require.Len(t, coll.Properties, origLen)
		_, stillInModel := funcutil.TryGetAttrByKeyFromRepeatedKV(util.PreserveFieldIdsKey, coll.Properties)
		require.True(t, stillInModel)
	})

	t.Run("reuses slice without allocation when directive is absent", func(t *testing.T) {
		coll := newCollectionModel(header, task.body, 0)
		require.NotEmpty(t, coll.Properties)

		desc := convertModelToDesc(coll, nil, "default")
		require.Same(t, coll.Properties[0], desc.GetProperties()[0])
		require.Len(t, desc.GetProperties(), len(coll.Properties))
	})
}
