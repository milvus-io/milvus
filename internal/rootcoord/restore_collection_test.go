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
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/schemautil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestRestoreCollectionVisibilityAfterDurableCompletion(t *testing.T) {
	collection := &model.Collection{CollectionID: 10, DBID: 1, DBName: "default", Name: "restored", State: etcdpb.CollectionState_CollectionRestoring}
	mt := &MetaTable{catalog: &kvrootcoord.Catalog{}, collID2Meta: map[int64]*model.Collection{10: collection}}
	_, err := mt.GetCollectionByID(t.Context(), "default", 10, typeutil.MaxTimestamp, false)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	internal, err := mt.GetCollectionByID(t.Context(), "default", 10, typeutil.MaxTimestamp, true)
	require.NoError(t, err)
	require.Equal(t, etcdpb.CollectionState_CollectionRestoring, internal.State)
	patch := mockey.Mock((*kvrootcoord.Catalog).AlterCollection).Return(merr.ErrServiceUnavailable).Build()
	require.ErrorIs(t, mt.CompleteRestoreCollection(t.Context(), 10), merr.ErrServiceUnavailable)
	require.False(t, mt.collID2Meta[10].Available())
	patch.UnPatch()
	patch = mockey.Mock((*kvrootcoord.Catalog).AlterCollection).Return(nil).Build()
	defer patch.UnPatch()
	require.NoError(t, mt.CompleteRestoreCollection(t.Context(), 10))
	require.NoError(t, mt.CompleteRestoreCollection(t.Context(), 10))
	require.Equal(t, 1, mt.availableCollectionCount)
	require.Equal(t, etcdpb.CollectionState_CollectionRestoring, collection.State, "published metadata is immutable")
	_, err = mt.GetCollectionByID(t.Context(), "default", 10, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	view, ok := mt.collViews.Get(10)
	require.True(t, ok)
	require.Equal(t, etcdpb.CollectionState_CollectionCreated, view.State, "completion publishes the view")
}

func structSubFieldOriginTestSchema(name string) *schemapb.CollectionSchema {
	added := &schemapb.FieldSchema{
		FieldID: 112, Name: "s[added]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
		ElementNullable: true,
		TypeParams:      []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "4"}},
	}
	schemautil.MarkAddedStructSubField(added)
	return &schemapb.CollectionSchema{
		Name:               name,
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "4"},
			}},
			{FieldID: 102, Name: MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 105, Name: "s",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID: 106, Name: "s[id]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
					TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "4"}},
				},
				added,
			},
		}},
	}
}

// The origin marker of an added sub-field is internal: a CreateCollection
// request is rejected when it carries the marker, with or without preserved
// field IDs. Restore from a snapshot keeps it.
func TestPrepareSchemaStructSubFieldOriginMarker(t *testing.T) {
	paramtable.Init()
	for _, preserve := range []bool{false, true} {
		t.Run("create collection preserve="+strconv.FormatBool(preserve), func(t *testing.T) {
			task := &createCollectionTask{
				Req: &milvuspb.CreateCollectionRequest{
					Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
					CollectionName: "restored",
				},
				body: &message.CreateCollectionRequest{
					CollectionName:   "restored",
					CollectionSchema: structSubFieldOriginTestSchema("restored"),
				},
				preserveFieldID: preserve,
			}
			err := task.prepareSchema(context.TODO())
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.ErrorContains(t, err, "type param "+common.StructSubFieldAddedKey+" of field \"s[added]\" is reserved")
		})
	}
	t.Run("restore from snapshot", func(t *testing.T) {
		task := &createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: "restored",
			},
			body: &message.CreateCollectionRequest{
				CollectionName:   "restored",
				CollectionSchema: structSubFieldOriginTestSchema("restored"),
			},
			preserveFieldID:                 true,
			allowStructSubFieldOriginMarker: true,
		}
		require.NoError(t, task.prepareSchema(context.TODO()))
		subFields := task.body.CollectionSchema.GetStructArrayFields()[0].GetFields()
		require.False(t, schemautil.IsAddedStructSubField(subFields[0]))
		require.True(t, schemautil.IsAddedStructSubField(subFields[1]), "the marker must survive the restore")
	})
	t.Run("restore rejects non-element-nullable added sub-field", func(t *testing.T) {
		schema := structSubFieldOriginTestSchema("restored")
		schema.StructArrayFields[0].Fields[1].ElementNullable = false
		task := &createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: "restored",
			},
			body: &message.CreateCollectionRequest{
				CollectionName:   "restored",
				CollectionSchema: schema,
			},
			preserveFieldID:                 true,
			allowStructSubFieldOriginMarker: true,
		}
		err := task.prepareSchema(context.TODO())
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorContains(t, err, "added sub-field \"s[added]\" of struct field \"s\" must set element_nullable=true")
	})
	t.Run("restore rejects struct with all sub-fields marked added", func(t *testing.T) {
		schema := structSubFieldOriginTestSchema("restored")
		schema.StructArrayFields[0].Fields[0].ElementNullable = true
		schemautil.MarkAddedStructSubField(schema.StructArrayFields[0].Fields[0])
		task := &createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: "restored",
			},
			body: &message.CreateCollectionRequest{
				CollectionName:   "restored",
				CollectionSchema: schema,
			},
			preserveFieldID:                 true,
			allowStructSubFieldOriginMarker: true,
		}
		err := task.prepareSchema(context.TODO())
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorContains(t, err, "struct field \"s\" must keep at least one original sub-field without "+common.StructSubFieldAddedKey)
	})
	t.Run("restore rejects malformed origin marker value", func(t *testing.T) {
		schema := structSubFieldOriginTestSchema("restored")
		for _, kv := range schema.StructArrayFields[0].Fields[1].TypeParams {
			if kv.Key == common.StructSubFieldAddedKey {
				kv.Value = "false"
			}
		}
		task := &createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: "restored",
			},
			body: &message.CreateCollectionRequest{
				CollectionName:   "restored",
				CollectionSchema: schema,
			},
			preserveFieldID:                 true,
			allowStructSubFieldOriginMarker: true,
		}
		err := task.prepareSchema(context.TODO())
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorContains(t, err, "type param "+common.StructSubFieldAddedKey+" of field \"s[added]\" must be \"true\", got \"false\"")
	})
	t.Run("restore rejects added sub-field with field ID below original sibling", func(t *testing.T) {
		schema := structSubFieldOriginTestSchema("restored")
		schema.StructArrayFields[0].Fields[0].FieldID = 112
		schema.StructArrayFields[0].Fields[1].FieldID = 106
		task := &createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: "restored",
			},
			body: &message.CreateCollectionRequest{
				CollectionName:   "restored",
				CollectionSchema: schema,
			},
			preserveFieldID:                 true,
			allowStructSubFieldOriginMarker: true,
		}
		err := task.prepareSchema(context.TODO())
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorContains(t, err, "added sub-field \"s[added]\" of struct field \"s\" has field ID 106, which must be greater than original sub-field ID 112")
	})
}

// PrepareRestoreCollection is the only caller that accepts the marker.
func TestPrepareRestoreCollectionAllowsStructSubFieldOriginMarker(t *testing.T) {
	schema, err := proto.Marshal(structSubFieldOriginTestSchema("restored"))
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
