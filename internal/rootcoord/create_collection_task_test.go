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
	"math"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func Test_createCollectionTask_validate(t *testing.T) {
	paramtable.Init()
	t.Run("empty request", func(t *testing.T) {
		task := createCollectionTask{
			Req: nil,
		}
		err := task.validate(context.TODO())
		assert.Error(t, err)
	})

	t.Run("shard num exceeds max configuration", func(t *testing.T) {
		// TODO: better to have a `Set` method for ParamItem.
		var cfgMaxShardNum int32
		if Params.CommonCfg.PreCreatedTopicEnabled.GetAsBool() {
			cfgMaxShardNum = int32(len(Params.CommonCfg.TopicNames.GetAsStrings()))
		} else {
			cfgMaxShardNum = Params.RootCoordCfg.DmlChannelNum.GetAsInt32()
		}
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:      &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				ShardsNum: cfgMaxShardNum + 1,
			},
		}
		err := task.validate(context.TODO())
		assert.Error(t, err)
	})

	t.Run("shard num exceeds limit", func(t *testing.T) {
		// TODO: better to have a `Set` method for ParamItem.
		cfgShardLimit := paramtable.Get().ProxyCfg.MaxShardNum.GetAsInt32()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:      &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				ShardsNum: cfgShardLimit + 1,
			},
		}
		err := task.validate(context.TODO())
		assert.Error(t, err)
	})

	t.Run("total collection num exceeds limit", func(t *testing.T) {
		paramtable.Get().Save(Params.QuotaConfig.MaxCollectionNum.Key, strconv.Itoa(2))
		defer paramtable.Get().Reset(Params.QuotaConfig.MaxCollectionNum.Key)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().ListAllAvailCollections(
			mock.Anything,
		).Return(map[int64][]int64{1: {1, 2}})

		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Database{Name: "db1"}, nil)

		core := newTestCore(withMeta(meta))
		task := createCollectionTask{
			Core: core,
			Req: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			},
			header: &message.CreateCollectionMessageHeader{
				DbId: util.DefaultDBID,
			},
		}
		err := task.validate(context.TODO())
		assert.Error(t, err)

		task = createCollectionTask{
			Core: core,
			Req: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			},
			header: &message.CreateCollectionMessageHeader{
				DbId: util.DefaultDBID,
			},
		}
		err = task.validate(context.TODO())
		assert.Error(t, err)
	})

	t.Run("collection num per db exceeds limit with db properties", func(t *testing.T) {
		paramtable.Get().Save(Params.QuotaConfig.MaxCollectionNumPerDB.Key, strconv.Itoa(2))
		defer paramtable.Get().Reset(Params.QuotaConfig.MaxCollectionNumPerDB.Key)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().ListAllAvailCollections(mock.Anything).Return(map[int64][]int64{util.DefaultDBID: {1, 2}})

		// test reach limit
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Database{
				Name: "db1",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.DatabaseMaxCollectionsKey,
						Value: "2",
					},
				},
			}, nil).Once()

		core := newTestCore(withMeta(meta))
		task := createCollectionTask{
			Core: core,
			Req: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			},
			header: &message.CreateCollectionMessageHeader{
				DbId: util.DefaultDBID,
			},
		}
		err := task.validate(context.TODO())
		assert.Error(t, err)

		// invalid properties
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Database{
				Name: "db1",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.DatabaseMaxCollectionsKey,
						Value: "invalid-value",
					},
				},
			}, nil).Once()
		core = newTestCore(withMeta(meta))
		task = createCollectionTask{
			Core: core,
			Req: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			},
			header: &message.CreateCollectionMessageHeader{
				DbId: util.DefaultDBID,
			},
		}

		err = task.validate(context.TODO())
		assert.Error(t, err)
	})

	t.Run("collection num per db exceeds limit with global configuration", func(t *testing.T) {
		paramtable.Get().Save(Params.QuotaConfig.MaxCollectionNumPerDB.Key, strconv.Itoa(2))
		defer paramtable.Get().Reset(Params.QuotaConfig.MaxCollectionNumPerDB.Key)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().ListAllAvailCollections(mock.Anything).Return(map[int64][]int64{1: {1, 2}})
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Database{Name: "db1"}, nil)

		core := newTestCore(withMeta(meta))
		task := createCollectionTask{
			Core: core,
			Req: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			},
			header: &message.CreateCollectionMessageHeader{
				DbId: util.DefaultDBID,
			},
		}
		err := task.validate(context.TODO())
		assert.Error(t, err)

		task = createCollectionTask{
			Core: core,
			Req: &milvuspb.CreateCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			},
			header: &message.CreateCollectionMessageHeader{
				DbId: util.DefaultDBID,
			},
		}
		err = task.validate(context.TODO())
		assert.Error(t, err)
	})

	t.Run("collection general number exceeds limit", func(t *testing.T) {
		paramtable.Get().Save(Params.RootCoordCfg.MaxGeneralCapacity.Key, strconv.Itoa(1))
		defer paramtable.Get().Reset(Params.RootCoordCfg.MaxGeneralCapacity.Key)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().ListAllAvailCollections(mock.Anything).Return(map[int64][]int64{1: {1, 2}})
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Database{Name: "db1"}, nil).Once()
		meta.EXPECT().GetGeneralCount(mock.Anything).Return(1)

		core := newTestCore(withMeta(meta))

		task := createCollectionTask{
			Core: core,
			Req: &milvuspb.CreateCollectionRequest{
				Base:          &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				NumPartitions: 256,
				ShardsNum:     2,
			},
			header: &message.CreateCollectionMessageHeader{
				DbId: util.DefaultDBID,
			},
		}
		err := task.validate(context.TODO())
		assert.ErrorIs(t, err, merr.ErrGeneralCapacityExceeded)
	})

	t.Run("ok", func(t *testing.T) {
		paramtable.Get().Save(Params.QuotaConfig.MaxCollectionNumPerDB.Key, "1")
		defer paramtable.Get().Reset(Params.QuotaConfig.MaxCollectionNumPerDB.Key)

		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().ListAllAvailCollections(mock.Anything).Return(map[int64][]int64{1: {1, 2}})
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Database{
				Name: "db1",
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.DatabaseMaxCollectionsKey,
						Value: "3",
					},
				},
			}, nil).Once()
		meta.EXPECT().GetGeneralCount(mock.Anything).Return(0)

		core := newTestCore(withMeta(meta))
		task := createCollectionTask{
			Core: core,
			Req: &milvuspb.CreateCollectionRequest{
				Base:          &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				NumPartitions: 2,
				ShardsNum:     2,
			},
			header: &message.CreateCollectionMessageHeader{
				DbId: util.DefaultDBID,
			},
		}

		paramtable.Get().Save(Params.QuotaConfig.MaxCollectionNum.Key, strconv.Itoa(math.MaxInt64))
		defer paramtable.Get().Reset(Params.QuotaConfig.MaxCollectionNum.Key)

		err := task.validate(context.TODO())
		assert.NoError(t, err)
	})
}

func Test_createCollectionTask_validateSchema(t *testing.T) {
	t.Run("name mismatch", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		otherName := collectionName + "_other"
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: otherName,
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.Error(t, err)
	})

	t.Run("primary field set nullable", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					Name:         "pk",
					IsPrimaryKey: true,
					Nullable:     true,
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.Error(t, err)
	})

	t.Run("primary field set default_value", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					Name:         "pk",
					IsPrimaryKey: true,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_LongData{
							LongData: 1,
						},
					},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.Error(t, err)
	})

	t.Run("has system fields", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{Name: RowIDFieldName},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.Error(t, err)
	})

	t.Run("default value type mismatch", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_BinaryVector,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BoolData{
							BoolData: false,
						},
					},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)

		schema1 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_Int16,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_BoolData{
							BoolData: false,
						},
					},
				},
			},
		}
		err1 := task.validateSchema(context.TODO(), schema1)
		assert.ErrorIs(t, err1, merr.ErrParameterInvalid)

		schema2 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_Bool,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: 1,
						},
					},
				},
			},
		}
		err2 := task.validateSchema(context.TODO(), schema2)
		assert.ErrorIs(t, err2, merr.ErrParameterInvalid)

		schema3 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_Bool,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_LongData{
							LongData: 1,
						},
					},
				},
			},
		}
		err3 := task.validateSchema(context.TODO(), schema3)
		assert.ErrorIs(t, err3, merr.ErrParameterInvalid)

		schema4 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_Bool,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_FloatData{
							FloatData: 1,
						},
					},
				},
			},
		}
		err4 := task.validateSchema(context.TODO(), schema4)
		assert.ErrorIs(t, err4, merr.ErrParameterInvalid)

		schema5 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_Bool,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_DoubleData{
							DoubleData: 1,
						},
					},
				},
			},
		}
		err5 := task.validateSchema(context.TODO(), schema5)
		assert.ErrorIs(t, err5, merr.ErrParameterInvalid)

		schema6 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_Bool,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "a",
						},
					},
				},
			},
		}
		err6 := task.validateSchema(context.TODO(), schema6)
		assert.ErrorIs(t, err6, merr.ErrParameterInvalid)

		schema7 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_Int16,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: math.MaxInt32,
						},
					},
				},
			},
		}
		err7 := task.validateSchema(context.TODO(), schema7)
		assert.ErrorIs(t, err7, merr.ErrParameterInvalid)

		schema8 := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_Int8,
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_IntData{
							IntData: math.MaxInt32,
						},
					},
				},
			},
		}
		err8 := task.validateSchema(context.TODO(), schema8)
		assert.ErrorIs(t, err8, merr.ErrParameterInvalid)
	})

	t.Run("default value length exceeds", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "2",
						},
					},
					DefaultValue: &schemapb.ValueField{
						Data: &schemapb.ValueField_StringData{
							StringData: "abc",
						},
					},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("duplicate_type_params", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "256",
						},
						{
							Key:   common.MmapEnabledKey,
							Value: "true",
						},
						{
							Key:   common.MmapEnabledKey,
							Value: "True",
						},
					},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("duplicate_index_params", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "256",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{Key: common.MetricTypeKey, Value: "L2"},
						{Key: common.MetricTypeKey, Value: "IP"},
					},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("normal case", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name:   collectionName,
			Fields: []*schemapb.FieldSchema{},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.NoError(t, err)
	})

	t.Run("struct array field - empty fields", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					Name:   "struct_field",
					Fields: []*schemapb.FieldSchema{},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty fields in StructArrayField")
	})

	t.Run("struct array field - vector type with nullable", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					Name: "struct_field",
					Fields: []*schemapb.FieldSchema{
						{
							Name:        "vector_array_field",
							DataType:    schemapb.DataType_ArrayOfVector,
							ElementType: schemapb.DataType_FloatVector,
							Nullable:    true,
						},
					},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "vector type not support null")
	})

	t.Run("struct array field - field with default value", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					Name: "struct_field",
					Fields: []*schemapb.FieldSchema{
						{
							Name:        "array_field",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
							DefaultValue: &schemapb.ValueField{
								Data: &schemapb.ValueField_IntData{
									IntData: 1,
								},
							},
						},
					},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "fields in struct array field not support default_value")
	})

	t.Run("struct array field - duplicate type params", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					Name: "struct_field",
					Fields: []*schemapb.FieldSchema{
						{
							Name:        "array_field",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_VarChar,
							TypeParams: []*commonpb.KeyValuePair{
								{Key: common.MaxLengthKey, Value: "100"},
								{Key: common.MaxLengthKey, Value: "200"},
							},
						},
					},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicated type param key")
	})

	t.Run("struct array field - duplicate index params", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					Name: "struct_field",
					Fields: []*schemapb.FieldSchema{
						{
							Name:        "vector_array_field",
							DataType:    schemapb.DataType_ArrayOfVector,
							ElementType: schemapb.DataType_FloatVector,
							IndexParams: []*commonpb.KeyValuePair{
								{Key: common.MetricTypeKey, Value: "L2"},
								{Key: common.MetricTypeKey, Value: "IP"},
							},
						},
					},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicated index param key")
	})

	t.Run("struct array field - invalid data type", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					Name: "struct_field",
					Fields: []*schemapb.FieldSchema{
						{
							Name:        "invalid_field",
							DataType:    schemapb.DataType_Int64,
							ElementType: schemapb.DataType_Int64,
						},
					},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Fields in StructArrayField can only be array or array of vector")
	})

	t.Run("struct array field - nested array", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					Name: "struct_field",
					Fields: []*schemapb.FieldSchema{
						{
							Name:        "nested_array",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Array,
						},
					},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Nested array is not supported")
	})

	t.Run("struct array field - invalid element type", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					Name: "struct_field",
					Fields: []*schemapb.FieldSchema{
						{
							Name:        "array_field",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_None,
						},
					},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "field data type: None is not supported")
	})

	t.Run("struct array field - valid case", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
		}
		schema := &schemapb.CollectionSchema{
			Name: collectionName,
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					Name: "struct_field",
					Fields: []*schemapb.FieldSchema{
						{
							Name:        "text_array",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_VarChar,
							TypeParams: []*commonpb.KeyValuePair{
								{Key: common.MaxLengthKey, Value: "100"},
							},
						},
						{
							Name:        "int_array",
							DataType:    schemapb.DataType_Array,
							ElementType: schemapb.DataType_Int32,
						},
						{
							Name:        "vector_array",
							DataType:    schemapb.DataType_ArrayOfVector,
							ElementType: schemapb.DataType_FloatVector,
							TypeParams: []*commonpb.KeyValuePair{
								{Key: common.DimKey, Value: "128"},
							},
						},
					},
				},
			},
		}
		err := task.validateSchema(context.TODO(), schema)
		assert.NoError(t, err)
	})
}

func Test_createCollectionTask_prepareSchema(t *testing.T) {
	t.Run("contain system fields", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		schema := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{Name: TimeStampFieldName},
			},
		}
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
			body: &message.CreateCollectionRequest{
				CollectionSchema: schema,
			},
		}
		err := task.prepareSchema(context.TODO())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()
		schema := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:     field1,
					DataType: schemapb.DataType_Int64,
				},
			},
		}
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
			body: &message.CreateCollectionRequest{
				CollectionSchema: schema,
			},
		}
		err := task.prepareSchema(context.TODO())
		assert.NoError(t, err)
	})

	t.Run("invalid data type", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()
		schema := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:     field1,
					DataType: 300,
				},
			},
		}
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
			body: &message.CreateCollectionRequest{
				CollectionSchema: schema,
			},
		}
		err := task.prepareSchema(context.TODO())
		assert.Error(t, err)
	})

	t.Run("vector type not support null", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()
		schema := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:     field1,
					DataType: 101,
					Nullable: true,
				},
			},
		}
		task := createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
			body: &message.CreateCollectionRequest{
				CollectionSchema: schema,
			},
		}
		err := task.prepareSchema(context.TODO())
		assert.Error(t, err)
	})
}

func Test_createCollectionTask_Prepare(t *testing.T) {
	initStreamingSystemAndCore(t)

	paramtable.Init()
	meta := mockrootcoord.NewIMetaTable(t)
	meta.On("GetDatabaseByName",
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(model.NewDefaultDatabase(nil), nil)
	meta.On("ListAllAvailCollections",
		mock.Anything,
	).Return(map[int64][]int64{
		util.DefaultDBID: {1, 2},
	}, nil)
	meta.EXPECT().GetGeneralCount(mock.Anything).Return(0)
	meta.EXPECT().DescribeAlias(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", errors.New("not found"))
	meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("not found"))

	paramtable.Get().Save(Params.QuotaConfig.MaxCollectionNum.Key, strconv.Itoa(math.MaxInt64))
	defer paramtable.Get().Reset(Params.QuotaConfig.MaxCollectionNum.Key)

	paramtable.Get().Save(Params.QuotaConfig.MaxCollectionNumPerDB.Key, strconv.Itoa(math.MaxInt64))
	defer paramtable.Get().Reset(Params.QuotaConfig.MaxCollectionNumPerDB.Key)

	t.Run("failed to assign id", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()
		schema := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{Name: field1},
			},
		}
		core := newTestCore(withInvalidIDAllocator(), withMeta(meta))

		task := createCollectionTask{
			Core: core,
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
			header: &message.CreateCollectionMessageHeader{},
			body: &message.CreateCollectionRequest{
				CollectionSchema: schema,
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		defer cleanTestEnv()

		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()

		ticker := newRocksMqTtSynchronizer()

		core := newTestCore(withValidIDAllocator(), withTtSynchronizer(ticker), withMeta(meta))

		schema := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:     field1,
					DataType: schemapb.DataType_Int64,
				},
			},
		}

		task := createCollectionTask{
			Core: core,
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
			header: &message.CreateCollectionMessageHeader{},
			body: &message.CreateCollectionRequest{
				CollectionSchema: schema,
			},
		}
		task.Req.ShardsNum = int32(Params.RootCoordCfg.DmlChannelNum.GetAsInt() + 1) // no enough channels.
		err := task.Prepare(context.Background())
		assert.Error(t, err)
		task.Req.ShardsNum = common.DefaultShardsNum
		err = task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func TestCreateCollectionTask_Prepare_WithProperty(t *testing.T) {
	initStreamingSystemAndCore(t)

	paramtable.Init()
	meta := mockrootcoord.NewIMetaTable(t)
	t.Run("with db properties", func(t *testing.T) {
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, mock.Anything).Return(&model.Database{
			Name: "foo",
			ID:   1,
			Properties: []*commonpb.KeyValuePair{
				{
					Key:   common.ReplicateIDKey,
					Value: "local-test",
				},
			},
		}, nil).Twice()
		meta.EXPECT().ListAllAvailCollections(mock.Anything).Return(map[int64][]int64{
			util.DefaultDBID: {1, 2},
		}).Once()
		meta.EXPECT().GetGeneralCount(mock.Anything).Return(0).Once()
		meta.EXPECT().DescribeAlias(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", errors.New("not found"))
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("not found"))
		defer cleanTestEnv()

		collectionName := funcutil.GenRandomStr()
		field1 := funcutil.GenRandomStr()

		ticker := newRocksMqTtSynchronizer()
		core := newTestCore(withValidIDAllocator(), withTtSynchronizer(ticker), withMeta(meta))

		schema := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					Name:     field1,
					DataType: schemapb.DataType_Int64,
				},
			},
		}

		task := createCollectionTask{
			Core: core,
			Req: &milvuspb.CreateCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
				CollectionName: collectionName,
			},
			header: &message.CreateCollectionMessageHeader{},
			body: &message.CreateCollectionRequest{
				CollectionSchema: schema,
			},
		}
		task.Req.ShardsNum = common.DefaultShardsNum
		err := task.Prepare(context.Background())
		assert.Len(t, task.body.CollectionSchema.Properties, 2)
		assert.Len(t, task.Req.Properties, 2)
		assert.NoError(t, err)
	})
}

func Test_createCollectionTask_PartitionKey(t *testing.T) {
	initStreamingSystemAndCore(t)

	paramtable.Init()
	defer cleanTestEnv()

	collectionName := funcutil.GenRandomStr()
	field1 := funcutil.GenRandomStr()
	ticker := newRocksMqTtSynchronizer()

	meta := mockrootcoord.NewIMetaTable(t)
	meta.On("GetDatabaseByName",
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(model.NewDefaultDatabase(nil), nil)
	meta.On("ListAllAvailCollections",
		mock.Anything,
	).Return(map[int64][]int64{
		util.DefaultDBID: {1, 2},
	}, nil)
	meta.EXPECT().GetGeneralCount(mock.Anything).Return(0)
	meta.EXPECT().DescribeAlias(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", errors.New("not found"))
	meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("not found"))

	paramtable.Get().Save(Params.QuotaConfig.MaxCollectionNum.Key, strconv.Itoa(math.MaxInt64))
	defer paramtable.Get().Reset(Params.QuotaConfig.MaxCollectionNum.Key)

	paramtable.Get().Save(Params.QuotaConfig.MaxCollectionNumPerDB.Key, strconv.Itoa(math.MaxInt64))
	defer paramtable.Get().Reset(Params.QuotaConfig.MaxCollectionNumPerDB.Key)

	core := newTestCore(withValidIDAllocator(), withTtSynchronizer(ticker), withMeta(meta))

	partitionKeyField := &schemapb.FieldSchema{
		Name:           field1,
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}

	schema := &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields:      []*schemapb.FieldSchema{partitionKeyField},
	}
	task := createCollectionTask{
		Core: core,
		Req: &milvuspb.CreateCollectionRequest{
			Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			CollectionName: collectionName,
			ShardsNum:      common.DefaultShardsNum,
		},
		header: &message.CreateCollectionMessageHeader{},
		body: &message.CreateCollectionRequest{
			CollectionSchema: schema,
		},
	}

	t.Run("without num partition", func(t *testing.T) {
		task.Req.NumPartitions = 0
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("num partition too large", func(t *testing.T) {
		task.Req.NumPartitions = Params.RootCoordCfg.MaxPartitionNum.GetAsInt64() + 1
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	task.Req.NumPartitions = common.DefaultPartitionsWithPartitionKey
	task.body.CollectionSchema = &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields:      []*schemapb.FieldSchema{partitionKeyField},
	}

	t.Run("normal case", func(t *testing.T) {
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func TestNamespaceProperty(t *testing.T) {
	paramtable.Init()
	paramtable.Get().CommonCfg.EnableNamespace.SwapTempValue("true")
	defer paramtable.Get().CommonCfg.EnableNamespace.SwapTempValue("false")
	ctx := context.Background()
	prefix := "TestNamespaceProperty"
	collectionName := prefix + funcutil.GenRandomStr()

	initSchema := func() *schemapb.CollectionSchema {
		return &schemapb.CollectionSchema{
			Name: collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "field1",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
				{
					FieldID:  101,
					Name:     "vector",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: strconv.Itoa(1024),
						},
					},
				},
			},
		}
	}
	hasNamespaceField := func(schema *schemapb.CollectionSchema) bool {
		for _, f := range schema.Fields {
			if f.Name == common.NamespaceFieldName {
				return true
			}
		}
		return false
	}

	t.Run("test namespace enabled", func(t *testing.T) {
		schema := initSchema()
		task := &createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				CollectionName: collectionName,
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.NamespaceEnabledKey,
						Value: "true",
					},
				},
			},
			header: &message.CreateCollectionMessageHeader{},
			body: &message.CreateCollectionRequest{
				CollectionSchema: schema,
			},
		}

		err := task.handleNamespaceField(ctx, schema)
		assert.NoError(t, err)
		assert.True(t, hasNamespaceField(schema))
	})

	t.Run("test namespace disabled with isolation and partition key", func(t *testing.T) {
		schema := initSchema()
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:        102,
			Name:           "field2",
			DataType:       schemapb.DataType_Int64,
			IsPartitionKey: true,
		})

		task := &createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				CollectionName: collectionName,
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.PartitionKeyIsolationKey,
						Value: "true",
					},
				},
			},
			header: &message.CreateCollectionMessageHeader{},
			body: &message.CreateCollectionRequest{
				CollectionSchema: schema,
			},
		}

		err := task.handleNamespaceField(ctx, schema)
		assert.NoError(t, err)
		assert.False(t, hasNamespaceField(schema))
	})

	t.Run("test namespace enabled with isolation", func(t *testing.T) {
		schema := initSchema()

		task := &createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				CollectionName: collectionName,
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.NamespaceEnabledKey,
						Value: "true",
					},
					{
						Key:   common.PartitionKeyIsolationKey,
						Value: "true",
					},
				},
			},
			header: &message.CreateCollectionMessageHeader{},
			body: &message.CreateCollectionRequest{
				CollectionSchema: schema,
			},
		}

		err := task.handleNamespaceField(ctx, schema)
		assert.NoError(t, err)
		assert.True(t, hasNamespaceField(schema))
	})

	t.Run("test namespace enabled with partition key", func(t *testing.T) {
		schema := initSchema()
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:        102,
			Name:           "field2",
			DataType:       schemapb.DataType_Int64,
			IsPartitionKey: true,
		})

		task := &createCollectionTask{
			Req: &milvuspb.CreateCollectionRequest{
				CollectionName: collectionName,
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.NamespaceEnabledKey,
						Value: "true",
					},
				},
			},
			header: &message.CreateCollectionMessageHeader{},
			body: &message.CreateCollectionRequest{
				CollectionSchema: schema,
			},
		}

		err := task.handleNamespaceField(ctx, schema)
		assert.Error(t, err)
	})
}

func Test_validateMultiAnalyzerParams(t *testing.T) {
	createTestCollectionSchema := func(fields []*schemapb.FieldSchema) *schemapb.CollectionSchema {
		return &schemapb.CollectionSchema{
			Name:   "test_collection",
			Fields: fields,
		}
	}

	createTestFieldSchema := func(name string, dataType schemapb.DataType) *schemapb.FieldSchema {
		return &schemapb.FieldSchema{
			Name:     name,
			DataType: dataType,
		}
	}

	t.Run("invalid json params", func(t *testing.T) {
		coll := createTestCollectionSchema([]*schemapb.FieldSchema{})
		fieldSchema := createTestFieldSchema("test_field", schemapb.DataType_VarChar)
		infos := make([]*querypb.AnalyzerInfo, 0)

		err := validateMultiAnalyzerParams("invalid json", coll, fieldSchema, &infos)
		assert.Error(t, err)
	})

	t.Run("missing by_field", func(t *testing.T) {
		coll := createTestCollectionSchema([]*schemapb.FieldSchema{})
		fieldSchema := createTestFieldSchema("test_field", schemapb.DataType_VarChar)
		infos := make([]*querypb.AnalyzerInfo, 0)

		params := `{"analyzers": {"default": {}}}`
		err := validateMultiAnalyzerParams(params, coll, fieldSchema, &infos)
		assert.Error(t, err)
	})

	t.Run("by_field not string", func(t *testing.T) {
		coll := createTestCollectionSchema([]*schemapb.FieldSchema{})
		fieldSchema := createTestFieldSchema("test_field", schemapb.DataType_VarChar)
		infos := make([]*querypb.AnalyzerInfo, 0)

		params := `{"by_field": 123, "analyzers": {"default": {}}}`
		err := validateMultiAnalyzerParams(params, coll, fieldSchema, &infos)
		assert.Error(t, err)
	})

	t.Run("by_field references non-existent field", func(t *testing.T) {
		coll := createTestCollectionSchema([]*schemapb.FieldSchema{
			createTestFieldSchema("existing_field", schemapb.DataType_VarChar),
		})
		fieldSchema := createTestFieldSchema("test_field", schemapb.DataType_VarChar)
		infos := make([]*querypb.AnalyzerInfo, 0)

		params := `{"by_field": "non_existent_field", "analyzers": {"default": {}}}`
		err := validateMultiAnalyzerParams(params, coll, fieldSchema, &infos)
		assert.Error(t, err)
	})

	t.Run("by_field references non-string field", func(t *testing.T) {
		coll := createTestCollectionSchema([]*schemapb.FieldSchema{
			createTestFieldSchema("int_field", schemapb.DataType_Int64),
		})
		fieldSchema := createTestFieldSchema("test_field", schemapb.DataType_VarChar)
		infos := make([]*querypb.AnalyzerInfo, 0)

		params := `{"by_field": "int_field", "analyzers": {"default": {}}}`
		err := validateMultiAnalyzerParams(params, coll, fieldSchema, &infos)
		assert.Error(t, err)
	})

	t.Run("invalid alias format", func(t *testing.T) {
		coll := createTestCollectionSchema([]*schemapb.FieldSchema{
			createTestFieldSchema("string_field", schemapb.DataType_VarChar),
		})
		fieldSchema := createTestFieldSchema("test_field", schemapb.DataType_VarChar)
		infos := make([]*querypb.AnalyzerInfo, 0)

		params := `{"by_field": "string_field", "alias": "invalid_alias", "analyzers": {"default": {}}}`
		err := validateMultiAnalyzerParams(params, coll, fieldSchema, &infos)
		assert.Error(t, err)
	})

	t.Run("missing analyzers", func(t *testing.T) {
		coll := createTestCollectionSchema([]*schemapb.FieldSchema{
			createTestFieldSchema("string_field", schemapb.DataType_VarChar),
		})
		fieldSchema := createTestFieldSchema("test_field", schemapb.DataType_VarChar)
		infos := make([]*querypb.AnalyzerInfo, 0)

		params := `{"by_field": "string_field"}`
		err := validateMultiAnalyzerParams(params, coll, fieldSchema, &infos)
		assert.Error(t, err)
	})

	t.Run("invalid analyzers format", func(t *testing.T) {
		coll := createTestCollectionSchema([]*schemapb.FieldSchema{
			createTestFieldSchema("string_field", schemapb.DataType_VarChar),
		})
		fieldSchema := createTestFieldSchema("test_field", schemapb.DataType_VarChar)
		infos := make([]*querypb.AnalyzerInfo, 0)

		params := `{"by_field": "string_field", "analyzers": "invalid_analyzers"}`
		err := validateMultiAnalyzerParams(params, coll, fieldSchema, &infos)
		assert.Error(t, err)
	})

	t.Run("missing default analyzer", func(t *testing.T) {
		coll := createTestCollectionSchema([]*schemapb.FieldSchema{
			createTestFieldSchema("string_field", schemapb.DataType_VarChar),
		})
		fieldSchema := createTestFieldSchema("test_field", schemapb.DataType_VarChar)
		infos := make([]*querypb.AnalyzerInfo, 0)

		params := `{"by_field": "string_field", "analyzers": {"custom": {}}}`
		err := validateMultiAnalyzerParams(params, coll, fieldSchema, &infos)
		assert.Error(t, err)
	})

	t.Run("valid params", func(t *testing.T) {
		coll := createTestCollectionSchema([]*schemapb.FieldSchema{
			createTestFieldSchema("string_field", schemapb.DataType_VarChar),
		})
		fieldSchema := createTestFieldSchema("test_field", schemapb.DataType_VarChar)
		infos := make([]*querypb.AnalyzerInfo, 0)

		params := `{
			"by_field": "string_field",
			"alias": {"en": "english", "zh": "chinese"},
			"analyzers": {
				"default": {"type": "standard"},
				"english": {"type": "english"},
				"chinese": {"type": "chinese"}
			}
		}`
		err := validateMultiAnalyzerParams(params, coll, fieldSchema, &infos)
		assert.NoError(t, err)
		assert.Len(t, infos, 3)

		analyzerNames := make(map[string]bool)
		for _, info := range infos {
			assert.Equal(t, "test_field", info.Field)
			analyzerNames[info.Name] = true
		}
		assert.True(t, analyzerNames["default"])
		assert.True(t, analyzerNames["english"])
		assert.True(t, analyzerNames["chinese"])
	})

	t.Run("valid params with minimal config", func(t *testing.T) {
		coll := createTestCollectionSchema([]*schemapb.FieldSchema{
			createTestFieldSchema("string_field", schemapb.DataType_VarChar),
		})
		fieldSchema := createTestFieldSchema("test_field", schemapb.DataType_VarChar)
		infos := make([]*querypb.AnalyzerInfo, 0)

		params := `{
			"by_field": "string_field",
			"analyzers": {"default": {"type": "standard"}}
		}`
		err := validateMultiAnalyzerParams(params, coll, fieldSchema, &infos)
		assert.NoError(t, err)
		assert.Len(t, infos, 1)
		assert.Equal(t, "default", infos[0].Name)
		assert.Equal(t, "test_field", infos[0].Field)
		assert.Equal(t, `{"type": "standard"}`, infos[0].Params)
	})
}

func Test_validateAnalyzer(t *testing.T) {
	createTestCollectionSchemaWithBM25 := func(fields []*schemapb.FieldSchema, inputFieldName string) *schemapb.CollectionSchema {
		return &schemapb.CollectionSchema{
			Name:   "test_collection",
			Fields: fields,
			Functions: []*schemapb.FunctionSchema{
				{
					Name:             "bm25_func",
					Type:             schemapb.FunctionType_BM25,
					InputFieldNames:  []string{inputFieldName},
					OutputFieldNames: []string{"bm25_output"},
				},
			},
		}
	}

	createTestFieldSchema := func(name string, dataType schemapb.DataType, typeParams []*commonpb.KeyValuePair) *schemapb.FieldSchema {
		return &schemapb.FieldSchema{
			Name:       name,
			DataType:   dataType,
			TypeParams: typeParams,
		}
	}

	t.Run("field without enable_match and not BM25 input", func(t *testing.T) {
		fieldSchema := createTestFieldSchema("text_field", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "100"},
		})
		collSchema := createTestCollectionSchemaWithBM25([]*schemapb.FieldSchema{fieldSchema}, "invalid_field")
		infos := make([]*querypb.AnalyzerInfo, 0)

		err := validateAnalyzer(collSchema, fieldSchema, &infos)
		assert.NoError(t, err)
		assert.Len(t, infos, 0)
	})

	t.Run("field with enable_match but no enable_analyzer", func(t *testing.T) {
		fieldSchema := createTestFieldSchema("text_field", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "100"},
			{Key: "enable_match", Value: "true"},
		})
		collSchema := createTestCollectionSchemaWithBM25([]*schemapb.FieldSchema{fieldSchema}, "text_field")
		infos := make([]*querypb.AnalyzerInfo, 0)

		err := validateAnalyzer(collSchema, fieldSchema, &infos)
		assert.Error(t, err)
	})

	t.Run("field with enable_match and enable_analyzer", func(t *testing.T) {
		fieldSchema := createTestFieldSchema("text_field", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "100"},
			{Key: "enable_match", Value: "true"},
			{Key: "enable_analyzer", Value: "true"},
			{Key: "analyzer_params", Value: "{\"type\": \"standard\"}"},
		})
		collSchema := createTestCollectionSchemaWithBM25([]*schemapb.FieldSchema{fieldSchema}, "text_field")
		infos := make([]*querypb.AnalyzerInfo, 0)

		err := validateAnalyzer(collSchema, fieldSchema, &infos)
		assert.NoError(t, err)
		assert.Len(t, infos, 1)
		assert.Equal(t, "text_field", infos[0].Field)
		assert.Equal(t, "{\"type\": \"standard\"}", infos[0].Params)
	})

	t.Run("field with multi analyzer and enable_match", func(t *testing.T) {
		fieldSchema := createTestFieldSchema("text_field", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "100"},
			{Key: "enable_match", Value: "true"},
			{Key: "enable_analyzer", Value: "true"},
			{Key: "multi_analyzer_params", Value: `{"by_field": "lang", "analyzers": {"default": "{}"}}`},
		})
		collSchema := createTestCollectionSchemaWithBM25([]*schemapb.FieldSchema{
			fieldSchema,
			createTestFieldSchema("lang", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "10"},
			}),
		}, "text_field")
		infos := make([]*querypb.AnalyzerInfo, 0)

		err := validateAnalyzer(collSchema, fieldSchema, &infos)
		assert.Error(t, err)
	})

	t.Run("field with multi analyzer and analyzer_params", func(t *testing.T) {
		fieldSchema := createTestFieldSchema("text_field", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "100"},
			{Key: "enable_analyzer", Value: "true"},
			{Key: "multi_analyzer_params", Value: `{"by_field": "lang", "analyzers": {"default": "{}"}}`},
			{Key: "analyzer_params", Value: `{"type": "standard"}`},
		})
		collSchema := createTestCollectionSchemaWithBM25([]*schemapb.FieldSchema{
			fieldSchema,
			createTestFieldSchema("lang", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "10"},
			}),
		}, "text_field")
		infos := make([]*querypb.AnalyzerInfo, 0)

		err := validateAnalyzer(collSchema, fieldSchema, &infos)
		assert.Error(t, err)
	})

	t.Run("field with valid multi analyzer", func(t *testing.T) {
		// Create a field with valid multi analyzer
		fieldSchema := createTestFieldSchema("text_field", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "100"},
			{Key: "enable_analyzer", Value: "true"},
			{Key: "multi_analyzer_params", Value: `{
				"by_field": "lang",
				"analyzers": {
					"default": {"type": "standard"},
					"english": {"type": "english"}
				}
			}`},
		})
		collSchema := createTestCollectionSchemaWithBM25([]*schemapb.FieldSchema{
			fieldSchema,
			createTestFieldSchema("lang", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
				{Key: common.MaxLengthKey, Value: "10"},
			}),
		}, "text_field")
		infos := make([]*querypb.AnalyzerInfo, 0)

		err := validateAnalyzer(collSchema, fieldSchema, &infos)
		assert.NoError(t, err)
		assert.Len(t, infos, 2)

		// Verify analyzer info content
		analyzerNames := make(map[string]bool)
		for _, info := range infos {
			assert.Equal(t, "text_field", info.Field)
			analyzerNames[info.Name] = true
		}
		assert.True(t, analyzerNames["default"])
		assert.True(t, analyzerNames["english"])
	})

	t.Run("field with invalid multi analyzer params", func(t *testing.T) {
		// Create a field with invalid multi analyzer params
		fieldSchema := createTestFieldSchema("text_field", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "100"},
			{Key: "enable_analyzer", Value: "true"},
			{Key: "multi_analyzer_params", Value: `{"by_field": "non_existent_field", "analyzers": {"default": "{}"}}`},
		})
		collSchema := createTestCollectionSchemaWithBM25([]*schemapb.FieldSchema{fieldSchema}, "text_field")
		infos := make([]*querypb.AnalyzerInfo, 0)

		err := validateAnalyzer(collSchema, fieldSchema, &infos)
		assert.Error(t, err)
	})

	t.Run("field with analyzer_params only", func(t *testing.T) {
		// Create a field with analyzer_params only
		fieldSchema := createTestFieldSchema("text_field", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "100"},
			{Key: "enable_analyzer", Value: "true"},
			{Key: "analyzer_params", Value: `{"type": "standard"}`},
		})
		collSchema := createTestCollectionSchemaWithBM25([]*schemapb.FieldSchema{fieldSchema}, "text_field")
		infos := make([]*querypb.AnalyzerInfo, 0)

		err := validateAnalyzer(collSchema, fieldSchema, &infos)
		require.NoError(t, err)
		require.Len(t, infos, 1)
		assert.Equal(t, "text_field", infos[0].Field)
		assert.Equal(t, "", infos[0].Name) // Regular analyzer has no name
		assert.Equal(t, `{"type": "standard"}`, infos[0].Params)
	})

	t.Run("field with enable_analyzer but no analyzer_params", func(t *testing.T) {
		// Create a field with enable_analyzer but no analyzer_params (uses default analyzer)
		fieldSchema := createTestFieldSchema("text_field", schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "100"},
			{Key: "enable_match", Value: "true"},
			{Key: "enable_analyzer", Value: "true"},
		})
		collSchema := createTestCollectionSchemaWithBM25([]*schemapb.FieldSchema{fieldSchema}, "text_field")
		infos := make([]*querypb.AnalyzerInfo, 0)

		err := validateAnalyzer(collSchema, fieldSchema, &infos)
		assert.NoError(t, err)
		assert.Len(t, infos, 0) // No analyzer_params, uses default analyzer
	})
}

func Test_appendConsistecyLevel(t *testing.T) {
	task := &createCollectionTask{
		Req: &milvuspb.CreateCollectionRequest{
			CollectionName: "test_collection",
			Properties: []*commonpb.KeyValuePair{
				{Key: common.ConsistencyLevel, Value: "Strong"},
			},
		},
	}
	task.appendConsistecyLevel()
	require.Len(t, task.Req.Properties, 1)
	assert.Equal(t, common.ConsistencyLevel, task.Req.Properties[0].Key)
	ok, consistencyLevel := getConsistencyLevel(task.Req.Properties...)
	assert.True(t, ok)
	assert.Equal(t, commonpb.ConsistencyLevel_Strong, consistencyLevel)

	task.Req.ConsistencyLevel = commonpb.ConsistencyLevel_Session
	task.appendConsistecyLevel()
	require.Len(t, task.Req.Properties, 1)
	assert.Equal(t, common.ConsistencyLevel, task.Req.Properties[0].Key)
	ok, consistencyLevel = getConsistencyLevel(task.Req.Properties...)
	assert.True(t, ok)
	assert.Equal(t, commonpb.ConsistencyLevel_Strong, consistencyLevel)

	task.Req.Properties = nil
	task.appendConsistecyLevel()
	require.Len(t, task.Req.Properties, 1)
	assert.Equal(t, common.ConsistencyLevel, task.Req.Properties[0].Key)
	ok, consistencyLevel = getConsistencyLevel(task.Req.Properties...)
	assert.True(t, ok)
	assert.Equal(t, commonpb.ConsistencyLevel_Session, consistencyLevel)

	task.Req.Properties = []*commonpb.KeyValuePair{
		{Key: common.ConsistencyLevel, Value: "1020203"},
	}
	task.appendConsistecyLevel()
	require.Len(t, task.Req.Properties, 1)
	assert.Equal(t, common.ConsistencyLevel, task.Req.Properties[0].Key)
	ok, consistencyLevel = getConsistencyLevel(task.Req.Properties...)
	assert.True(t, ok)
	assert.Equal(t, commonpb.ConsistencyLevel_Session, consistencyLevel)

	consistencyLevel, properties := mustConsumeConsistencyLevel(task.Req.Properties)
	assert.Equal(t, commonpb.ConsistencyLevel_Session, consistencyLevel)
	assert.Len(t, properties, 0)
}
