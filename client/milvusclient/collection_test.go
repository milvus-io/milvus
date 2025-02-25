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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type CollectionSuite struct {
	MockSuiteBase
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
	opt = SimpleCreateCollectionOptions(collectionName, 128).WithVarcharPK(true, 64).WithAutoID(false).WithDynamicSchema(false)
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

func TestCollection(t *testing.T) {
	suite.Run(t, new(CollectionSuite))
}
