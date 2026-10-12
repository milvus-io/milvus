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
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/common"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

type CollectionSuite struct {
	MockSuiteBase
}

type malformedAddCollectionFieldOption struct{}

func (malformedAddCollectionFieldOption) Request() *milvuspb.AddCollectionFieldRequest {
	return &milvuspb.AddCollectionFieldRequest{
		CollectionName: "coll",
		Schema:         []byte{0xff},
	}
}

func (malformedAddCollectionFieldOption) Validate() error {
	return nil
}

type staticAddCollectionFieldOption struct {
	req *milvuspb.AddCollectionFieldRequest
}

func (opt staticAddCollectionFieldOption) Request() *milvuspb.AddCollectionFieldRequest {
	return opt.req
}

func (staticAddCollectionFieldOption) Validate() error {
	return nil
}

func (s *CollectionSuite) TestListCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		s.mock.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&milvuspb.ShowCollectionsResponse{
			CollectionNames: []string{"test1", "test2", "test3"},
		}, nil).Once()

		names, err := s.client.ListCollections(ctx, NewListCollectionOption())
		s.NoError(err)
		s.ElementsMatch([]string{"test1", "test2", "test3"}, names)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.ListCollections(ctx, NewListCollectionOption())
		s.Error(err)
	})
}

func (s *CollectionSuite) TestCreateCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		s.mock.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()
		s.mock.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()
		s.mock.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()
		s.mock.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(&milvuspb.DescribeIndexResponse{
			Status: merr.Success(),
			IndexDescriptions: []*milvuspb.IndexDescription{
				{FieldName: "vector", State: commonpb.IndexState_Finished},
			},
		}, nil).Once()
		s.mock.EXPECT().GetLoadingProgress(mock.Anything, mock.Anything).Return(&milvuspb.GetLoadingProgressResponse{
			Status:   merr.Success(),
			Progress: 100,
		}, nil).Once()

		err := s.client.CreateCollection(ctx, SimpleCreateCollectionOptions("test_collection", 128))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.CreateCollection(ctx, SimpleCreateCollectionOptions("test_collection", 128))
		s.Error(err)
	})
}

func (s *CollectionSuite) TestCreateCollectionOptions() {
	collectionName := fmt.Sprintf("test_collection_%s", s.randString(6))
	opt := SimpleCreateCollectionOptions(collectionName, 128)
	req := opt.Request()
	s.Equal(collectionName, req.GetCollectionName())
	s.EqualValues(1, req.GetShardsNum())

	collSchema := &schemapb.CollectionSchema{}
	err := proto.Unmarshal(req.GetSchema(), collSchema)
	s.Require().NoError(err)
	s.True(collSchema.GetEnableDynamicField())

	collectionName = fmt.Sprintf("test_collection_%s", s.randString(6))
	opt = SimpleCreateCollectionOptions(collectionName, 128).WithVarcharPK(true, 64).WithAutoID(false).
		WithPKFieldName("pk").WithVectorFieldName("embedding").WithMetricType(entity.L2).
		WithDynamicSchema(false)
	req = opt.Request()
	s.Equal(collectionName, req.GetCollectionName())
	s.EqualValues(1, req.GetShardsNum())

	collSchema = &schemapb.CollectionSchema{}
	err = proto.Unmarshal(req.GetSchema(), collSchema)
	s.Require().NoError(err)
	s.False(collSchema.GetEnableDynamicField())

	collectionName = fmt.Sprintf("test_collection_%s", s.randString(6))
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("int64").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector").WithDim(128).WithDataType(entity.FieldTypeFloatVector))

	opt = NewCreateCollectionOption(collectionName, schema).
		WithShardNum(2).
		WithConsistencyLevel(entity.ClEventually).
		WithProperty(common.CollectionTTLConfigKey, 86400)

	req = opt.Request()
	s.Equal(collectionName, req.GetCollectionName())
	s.EqualValues(2, req.GetShardsNum())
	s.EqualValues(commonpb.ConsistencyLevel_Eventually, req.GetConsistencyLevel())
	if s.Len(req.GetProperties(), 1) {
		kv := req.GetProperties()[0]
		s.Equal(common.CollectionTTLConfigKey, kv.GetKey())
		s.Equal("86400", kv.GetValue())
	}

	collSchema = &schemapb.CollectionSchema{}
	err = proto.Unmarshal(req.GetSchema(), collSchema)
	s.Require().NoError(err)
}

func (s *CollectionSuite) TestDescribeCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Success(),
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, DataType: schemapb.DataType_Int64, AutoID: true, Name: "ID"},
					{
						FieldID: 101, DataType: schemapb.DataType_FloatVector, Name: "vector",
						TypeParams: []*commonpb.KeyValuePair{
							{Key: "dim", Value: "128"},
						},
					},
				},
			},
			CollectionID:   1000,
			CollectionName: "test_collection",
		}, nil).Once()

		coll, err := s.client.DescribeCollection(ctx, NewDescribeCollectionOption("test_collection"))
		s.NoError(err)

		s.EqualValues(1000, coll.ID)
		s.Equal("test_collection", coll.Name)
		s.Len(coll.Schema.Fields, 2)
		idField, ok := lo.Find(coll.Schema.Fields, func(field *entity.Field) bool {
			return field.ID == 100
		})
		s.Require().True(ok)
		s.Equal("ID", idField.Name)
		s.Equal(entity.FieldTypeInt64, idField.DataType)
		s.True(idField.AutoID)

		vectorField, ok := lo.Find(coll.Schema.Fields, func(field *entity.Field) bool {
			return field.ID == 101
		})
		s.Require().True(ok)
		s.Equal("vector", vectorField.Name)
		s.Equal(entity.FieldTypeFloatVector, vectorField.DataType)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.DescribeCollection(ctx, NewDescribeCollectionOption("test_collection"))
		s.Error(err)
	})
}

func (s *CollectionSuite) TestHasCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Success(),
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, DataType: schemapb.DataType_Int64, AutoID: true, Name: "ID"},
					{
						FieldID: 101, DataType: schemapb.DataType_FloatVector, Name: "vector",
						TypeParams: []*commonpb.KeyValuePair{
							{Key: "dim", Value: "128"},
						},
					},
				},
			},
			CollectionID:   1000,
			CollectionName: "test_collection",
		}, nil).Once()

		has, err := s.client.HasCollection(ctx, NewHasCollectionOption("test_collection"))
		s.NoError(err)

		s.True(has)
	})

	s.Run("collection_not_exist", func() {
		s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Status(merr.WrapErrCollectionNotFound("test_collection")),
		}, nil).Once()

		has, err := s.client.HasCollection(ctx, NewHasCollectionOption("test_collection"))
		s.NoError(err)

		s.False(has)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.HasCollection(ctx, NewHasCollectionOption("test_collection"))
		s.Error(err)
	})
}

func (s *CollectionSuite) TestDropCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		s.mock.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		err := s.client.DropCollection(ctx, NewDropCollectionOption("test_collection"))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.DropCollection(ctx, NewDropCollectionOption("test_collection"))
		s.Error(err)
	})
}

func (s *CollectionSuite) TestTruncateCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		s.mock.EXPECT().TruncateCollection(mock.Anything, mock.Anything).
			Return(&milvuspb.TruncateCollectionResponse{Status: merr.Success()}, nil).Once()

		err := s.client.TruncateCollection(ctx, NewTruncateCollectionOption("test_collection"))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().TruncateCollection(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.TruncateCollection(ctx, NewTruncateCollectionOption("test_collection"))
		s.Error(err)
	})
}

func (s *CollectionSuite) TestRenameCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oldName := fmt.Sprintf("test_collection_%s", s.randString(6))
	newName := fmt.Sprintf("%s_new", oldName)

	s.Run("success", func() {
		s.mock.EXPECT().RenameCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, rcr *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
			s.Equal(oldName, rcr.GetOldName())
			s.Equal(newName, rcr.GetNewName())
			return merr.Success(), nil
		}).Once()

		err := s.client.RenameCollection(ctx, NewRenameCollectionOption(oldName, newName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().RenameCollection(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.RenameCollection(ctx, NewRenameCollectionOption(oldName, newName))
		s.Error(err)
	})
}

func (s *CollectionSuite) TestAlterCollectionProperties() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collName := fmt.Sprintf("test_collection_%s", s.randString(6))
	key := s.randString(6)
	value := s.randString(6)

	s.Run("success", func() {
		s.mock.EXPECT().AlterCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, acr *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
			s.Equal(collName, acr.GetCollectionName())
			if s.Len(acr.GetProperties(), 1) {
				item := acr.GetProperties()[0]
				s.Equal(key, item.GetKey())
				s.Equal(value, item.GetValue())
			}
			return merr.Success(), nil
		}).Once()

		err := s.client.AlterCollectionProperties(ctx, NewAlterCollectionPropertiesOption(collName).WithProperty(key, value))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.AlterCollectionProperties(ctx, NewAlterCollectionPropertiesOption(collName).WithProperty(key, value))
		s.Error(err)
	})
}

func (s *CollectionSuite) TestDropCollectionProperties() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		key := fmt.Sprintf("key_%s", s.randString(4))
		s.mock.EXPECT().AlterCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, adr *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
			s.Equal([]string{key}, adr.GetDeleteKeys())
			return merr.Success(), nil
		}).Once()

		err := s.client.DropCollectionProperties(ctx, NewDropCollectionPropertiesOption(dbName, key))
		s.NoError(err)
	})

	s.Run("failure", func() {
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		s.mock.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.DropCollectionProperties(ctx, NewDropCollectionPropertiesOption(dbName, "key"))
		s.Error(err)
	})
}

func (s *CollectionSuite) TestAlterCollectionFieldProperties() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collName := fmt.Sprintf("test_collection_%s", s.randString(6))
	fieldName := fmt.Sprintf("field_%s", s.randString(4))
	key := s.randString(6)
	value := s.randString(6)

	s.Run("success", func() {
		s.mock.EXPECT().AlterCollectionField(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, acr *milvuspb.AlterCollectionFieldRequest) (*commonpb.Status, error) {
			s.Equal(collName, acr.GetCollectionName())
			s.Equal(fieldName, acr.GetFieldName())
			if s.Len(acr.GetProperties(), 1) {
				item := acr.GetProperties()[0]
				s.Equal(key, item.GetKey())
				s.Equal(value, item.GetValue())
			}
			return merr.Success(), nil
		}).Once()

		err := s.client.AlterCollectionFieldProperty(ctx, NewAlterCollectionFieldPropertiesOption(collName, fieldName).WithProperty(key, value))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().AlterCollectionField(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.AlterCollectionFieldProperty(ctx, NewAlterCollectionFieldPropertiesOption("coll", "field").WithProperty(key, value))
		s.Error(err)
	})
}

func (s *CollectionSuite) TestGetCollectionStats() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.mock.EXPECT().GetCollectionStatistics(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, gcsr *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
			s.Equal(collName, gcsr.GetCollectionName())
			return &milvuspb.GetCollectionStatisticsResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				Stats: []*commonpb.KeyValuePair{
					{Key: "row_count", Value: "1000"},
				},
			}, nil
		}).Once()

		stats, err := s.client.GetCollectionStats(ctx, NewGetCollectionStatsOption(collName))
		s.NoError(err)

		s.Len(stats, 1)
		s.Equal("1000", stats["row_count"])
	})

	s.Run("failure", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		s.mock.EXPECT().GetCollectionStatistics(mock.Anything, mock.Anything).Return(nil, errors.New("mocked")).Once()

		_, err := s.client.GetCollectionStats(ctx, NewGetCollectionStatsOption(collName))
		s.Error(err)
	})
}

func (s *CollectionSuite) TestAddCollectionField() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(6))
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.AlterCollectionSchemaRequest) (*milvuspb.AlterCollectionSchemaResponse, error) {
			s.Equal(collName, req.GetCollectionName())
			addRequest := req.GetAction().GetAddRequest()
			s.Require().Len(addRequest.GetFieldInfos(), 1)
			s.Empty(addRequest.GetFuncSchema())
			fieldProto := addRequest.GetFieldInfos()[0].GetFieldSchema()
			s.Require().NotNil(fieldProto)
			s.Equal(fieldName, fieldProto.GetName())
			s.Equal(schemapb.DataType_Int64, fieldProto.GetDataType())
			s.True(fieldProto.GetNullable())
			return &milvuspb.AlterCollectionSchemaResponse{AlterStatus: merr.Success()}, nil
		}).Once()

		field := entity.NewField().WithName(fieldName).WithDataType(entity.FieldTypeInt64).WithNullable(true)

		err := s.client.AddCollectionField(ctx, NewAddCollectionFieldOption(collName, field))
		s.NoError(err)
	})

	s.Run("failure", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(6))
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).Return(&milvuspb.AlterCollectionSchemaResponse{
			AlterStatus: merr.Status(errors.New("mocked")),
		}, nil).Once()

		field := entity.NewField().WithName(fieldName).WithDataType(entity.FieldTypeInt64).WithNullable(true)

		err := s.client.AddCollectionField(ctx, NewAddCollectionFieldOption(collName, field))
		s.Error(err)
	})

	s.Run("fallback_to_legacy_rpc", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(6))
		field := entity.NewField().WithName(fieldName).WithDataType(entity.FieldTypeInt64).WithNullable(true)
		fieldSchema, err := proto.Marshal(field.ProtoMessage())
		s.Require().NoError(err)
		option := staticAddCollectionFieldOption{req: &milvuspb.AddCollectionFieldRequest{
			DbName:         "fallback_db",
			CollectionName: collName,
			CollectionID:   100,
			Schema:         fieldSchema,
		}}
		s.setupCache(collName, entity.NewSchema().WithName(collName))

		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.MatchedBy(func(req *milvuspb.AlterCollectionSchemaRequest) bool {
			return req.GetDbName() == "fallback_db" &&
				req.GetCollectionName() == collName &&
				req.GetCollectionID() == 100
		})).Return(nil, grpcstatus.Error(codes.Unimplemented, "method not implemented")).Once()
		s.mock.EXPECT().AddCollectionField(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.AddCollectionFieldRequest) (*commonpb.Status, error) {
			s.True(proto.Equal(option.req, req))
			s.Require().NoError(grpc.SetHeader(ctx, metadata.Pairs("fallback-rpc", "legacy")))
			return merr.Success(), nil
		}).Once()

		var header metadata.MD
		s.NoError(s.client.AddCollectionField(ctx, option, grpc.Header(&header)))
		s.Equal([]string{"legacy"}, header.Get("fallback-rpc"))
		_, cached := s.client.collCache.collections.Get(collName)
		s.False(cached)
	})

	s.Run("fallback_to_legacy_rpc_on_business_unimplemented", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(6))
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).Return(&milvuspb.AlterCollectionSchemaResponse{
			AlterStatus: &commonpb.Status{
				Code:      10,
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "service unimplemented",
			},
		}, nil).Once()
		s.mock.EXPECT().AddCollectionField(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		field := entity.NewField().WithName(fieldName).WithDataType(entity.FieldTypeInt64).WithNullable(true)
		s.NoError(s.client.AddCollectionField(ctx, NewAddCollectionFieldOption(collName, field)))
	})

	s.Run("non_unimplemented_error_does_not_fallback", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(6))
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).
			Return(nil, grpcstatus.Error(codes.PermissionDenied, "permission denied")).Once()

		field := entity.NewField().WithName(fieldName).WithDataType(entity.FieldTypeInt64).WithNullable(true)
		err := s.client.AddCollectionField(ctx, NewAddCollectionFieldOption(collName, field))
		s.Equal(codes.PermissionDenied, grpcstatus.Code(err))
	})

	s.Run("legacy_rpc_error", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(6))
		s.setupCache(collName, entity.NewSchema().WithName(collName))
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).
			Return(nil, grpcstatus.Error(codes.Unimplemented, "method not implemented")).Once()
		s.mock.EXPECT().AddCollectionField(mock.Anything, mock.Anything).
			Return(merr.Status(merr.WrapErrParameterInvalidMsg("legacy add field failed")), nil).Once()

		field := entity.NewField().WithName(fieldName).WithDataType(entity.FieldTypeInt64).WithNullable(true)
		err := s.client.AddCollectionField(ctx, NewAddCollectionFieldOption(collName, field))
		s.ErrorIs(err, merr.ErrParameterInvalid)
		_, cached := s.client.collCache.collections.Get(collName)
		s.True(cached)
	})

	s.Run("vector_field_without_nullable", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(6))
		// no mock expected because validation should fail before RPC call

		field := entity.NewField().WithName(fieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(128)

		err := s.client.AddCollectionField(ctx, NewAddCollectionFieldOption(collName, field))
		s.Error(err)
		s.Contains(err.Error(), "adding vector field to existing collection requires nullable=true")
	})

	s.Run("vector_field_with_nullable", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(6))
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.AlterCollectionSchemaRequest) (*milvuspb.AlterCollectionSchemaResponse, error) {
			addRequest := req.GetAction().GetAddRequest()
			s.Require().Len(addRequest.GetFieldInfos(), 1)
			s.Empty(addRequest.GetFuncSchema())
			fieldProto := addRequest.GetFieldInfos()[0].GetFieldSchema()
			s.Require().NotNil(fieldProto)
			s.Equal(fieldName, fieldProto.GetName())
			s.Equal(schemapb.DataType_FloatVector, fieldProto.GetDataType())
			s.True(fieldProto.GetNullable())
			return &milvuspb.AlterCollectionSchemaResponse{AlterStatus: merr.Success()}, nil
		}).Once()

		field := entity.NewField().WithName(fieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(128).WithNullable(true)

		err := s.client.AddCollectionField(ctx, NewAddCollectionFieldOption(collName, field))
		s.NoError(err)
	})

	s.Run("malformed_field_schema", func() {
		err := s.client.AddCollectionField(ctx, malformedAddCollectionFieldOption{})
		s.ErrorIs(err, merr.ErrParameterInvalid)
	})
}

func (s *CollectionSuite) TestAddCollectionStructField() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(6))
		s.mock.EXPECT().AddCollectionStructField(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, acsfr *milvuspb.AddCollectionStructFieldRequest) (*commonpb.Status, error) {
			s.Equal(collName, acsfr.GetCollectionName())
			structProto := acsfr.GetStructArrayFieldSchema()
			s.Require().NotNil(structProto)
			s.Equal(fieldName, structProto.GetName())
			s.True(structProto.GetNullable())
			s.Equal("16", entity.KvPairsMap(structProto.GetTypeParams())[common.MaxCapacityKey])
			s.Require().Len(structProto.GetFields(), 2)

			tagField := structProto.GetFields()[0]
			s.Equal("tag", tagField.GetName())
			s.Equal(schemapb.DataType_Array, tagField.GetDataType())
			s.Equal(schemapb.DataType_VarChar, tagField.GetElementType())
			tagParams := entity.KvPairsMap(tagField.GetTypeParams())
			s.Equal("64", tagParams[common.MaxLengthKey])
			s.Equal("16", tagParams[common.MaxCapacityKey])

			embField := structProto.GetFields()[1]
			s.Equal("emb", embField.GetName())
			s.Equal(schemapb.DataType_ArrayOfVector, embField.GetDataType())
			s.Equal(schemapb.DataType_FloatVector, embField.GetElementType())
			embParams := entity.KvPairsMap(embField.GetTypeParams())
			s.Equal("8", embParams[common.DimKey])
			s.Equal("16", embParams[common.MaxCapacityKey])
			return merr.Success(), nil
		}).Once()

		structSchema := entity.NewStructSchema().
			WithField(entity.NewField().WithName("tag").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
			WithField(entity.NewField().WithName("emb").WithDataType(entity.FieldTypeFloatVector).WithDim(8))
		field := entity.NewField().
			WithName(fieldName).
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).
			WithMaxCapacity(16).
			WithNullable(true).
			WithStructSchema(structSchema)

		err := s.client.AddCollectionStructField(ctx, NewAddCollectionStructFieldOption(collName, field))
		s.NoError(err)
	})

	s.Run("failure", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(6))
		s.mock.EXPECT().AddCollectionStructField(mock.Anything, mock.Anything).Return(merr.Status(errors.New("mocked")), nil).Once()

		field := entity.NewField().
			WithName(fieldName).
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).
			WithMaxCapacity(16).
			WithStructSchema(entity.NewStructSchema().WithField(entity.NewField().WithName("tag").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)))

		err := s.client.AddCollectionStructField(ctx, NewAddCollectionStructFieldOption(collName, field))
		s.Error(err)
	})

	s.Run("non_struct_array_field", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		field := entity.NewField().WithName("field").WithDataType(entity.FieldTypeInt64).WithNullable(true)

		err := s.client.AddCollectionStructField(ctx, NewAddCollectionStructFieldOption(collName, field))
		s.Error(err)
		s.Contains(err.Error(), "requires data type Array and element type Struct")
	})

	s.Run("nil_option", func() {
		var opt *addCollectionStructFieldOption
		err := opt.Validate()
		s.Error(err)
		s.Contains(err.Error(), "option is nil")
	})

	s.Run("nil_field_schema", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		err := NewAddCollectionStructFieldOption(collName, nil).Validate()
		s.Error(err)
		s.Contains(err.Error(), "struct array field schema is required")
	})

	s.Run("nil_struct_schema", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		field := entity.NewField().
			WithName("clips").
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct)

		err := s.client.AddCollectionStructField(ctx, NewAddCollectionStructFieldOption(collName, field))
		s.Error(err)
		s.Contains(err.Error(), "struct schema is required")
	})

	s.Run("invalid_sub_field", func() {
		collName := fmt.Sprintf("coll_%s", s.randString(6))
		field := entity.NewField().
			WithName("clips").
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).
			WithStructSchema(entity.NewStructSchema().WithField(entity.NewField().WithName("tag").WithDataType(entity.FieldTypeVarChar).WithNullable(true)))

		err := s.client.AddCollectionStructField(ctx, NewAddCollectionStructFieldOption(collName, field))
		s.Error(err)
		s.Contains(err.Error(), "must not be nullable")
	})
}

func (s *CollectionSuite) TestAddFunctionField() {
	ctx := context.Background()
	field := entity.NewField().WithName("sparse").WithDataType(entity.FieldTypeSparseVector)
	function := entity.NewFunction().
		WithName("bm25").
		WithType(entity.FunctionTypeBM25).
		WithInputFields("text").
		WithOutputFields("sparse")
	boundIndex := index.NewSparseInvertedIndex(entity.BM25, 0.2)

	s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.MatchedBy(func(req *milvuspb.AlterCollectionSchemaRequest) bool {
		add := req.GetAction().GetAddRequest()
		if req.GetCollectionName() != "coll" || len(add.GetFieldInfos()) != 1 || len(add.GetFuncSchema()) != 1 {
			return false
		}
		fieldInfo := add.GetFieldInfos()[0]
		params := entity.KvPairsMap(fieldInfo.GetExtraParams())
		return fieldInfo.GetFieldSchema().GetName() == "sparse" &&
			add.GetFuncSchema()[0].GetName() == "bm25" &&
			params[index.IndexTypeKey] == string(index.SparseInverted)
	})).Return(&milvuspb.AlterCollectionSchemaResponse{AlterStatus: merr.Success()}, nil).Once()

	err := s.client.AddFunctionField(ctx, NewAddFunctionFieldOption("coll", field, function, boundIndex).WithIndexName("sparse_idx"))
	s.NoError(err)

	err = s.client.AddFunctionField(ctx, NewAddFunctionFieldOption("coll", field, function, nil))
	s.ErrorIs(err, merr.ErrParameterMissing)
}

func (s *CollectionSuite) TestDropCollectionField() {
	ctx := context.Background()
	s.Run("by_name", func() {
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.MatchedBy(func(req *milvuspb.AlterCollectionSchemaRequest) bool {
			return req.GetAction().GetDropRequest().GetFieldName() == "field" && req.GetAction().GetDropRequest().GetFieldId() == 0
		})).Return(&milvuspb.AlterCollectionSchemaResponse{AlterStatus: merr.Success()}, nil).Once()
		s.NoError(s.client.DropCollectionField(ctx, NewDropCollectionFieldOption("coll", "field")))
	})

	s.Run("by_id", func() {
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.MatchedBy(func(req *milvuspb.AlterCollectionSchemaRequest) bool {
			return req.GetAction().GetDropRequest().GetFieldId() == 101 && req.GetAction().GetDropRequest().GetFieldName() == ""
		})).Return(&milvuspb.AlterCollectionSchemaResponse{AlterStatus: merr.Success()}, nil).Once()
		s.NoError(s.client.DropCollectionField(ctx, NewDropCollectionFieldByIDOption("coll", 101)))
	})

	s.ErrorIs(s.client.DropCollectionField(ctx, NewDropCollectionFieldOption("coll", "")), merr.ErrParameterMissing)
}

func (s *CollectionSuite) TestDropFunctionField() {
	ctx := context.Background()
	s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.MatchedBy(func(req *milvuspb.AlterCollectionSchemaRequest) bool {
		drop := req.GetAction().GetDropRequest()
		return drop.GetFunctionName() == "bm25" && drop.GetDropFunctionOutputFields()
	})).Return(&milvuspb.AlterCollectionSchemaResponse{AlterStatus: merr.Success()}, nil).Once()

	s.NoError(s.client.DropFunctionField(ctx, NewDropFunctionFieldOption("coll", "bm25")))
}

func (s *CollectionSuite) TestAlterCollectionSchemaErrors() {
	ctx := context.Background()
	option := NewDropCollectionFieldOption("coll", "field")

	s.Run("nil option", func() {
		var nilOption DropCollectionFieldOption
		err := s.client.DropCollectionField(ctx, nilOption)
		s.ErrorIs(err, merr.ErrParameterMissing)
	})

	s.Run("nil response", func() {
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).Return(nil, nil).Once()
		s.Error(s.client.DropCollectionField(ctx, option))
	})

	s.Run("nil alter status", func() {
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).
			Return(&milvuspb.AlterCollectionSchemaResponse{}, nil).Once()
		s.Error(s.client.DropCollectionField(ctx, option))
	})

	s.Run("transport error", func() {
		transportErr := errors.New("transport")
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).Return(nil, transportErr).Once()
		s.ErrorContains(s.client.DropCollectionField(ctx, option), transportErr.Error())
	})

	s.Run("business error", func() {
		s.mock.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).Return(&milvuspb.AlterCollectionSchemaResponse{
			AlterStatus: merr.Status(merr.WrapErrParameterInvalidMsg("invalid field")),
		}, nil).Once()
		s.ErrorIs(s.client.DropCollectionField(ctx, option), merr.ErrParameterInvalid)
	})
}

func TestCollection(t *testing.T) {
	suite.Run(t, new(CollectionSuite))
}
