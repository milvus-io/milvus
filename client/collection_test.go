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

package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/util/merr"
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

	opt = NewCreateCollectionOption(collectionName, schema).WithShardNum(2)

	req = opt.Request()
	s.Equal(collectionName, req.GetCollectionName())
	s.EqualValues(2, req.GetShardsNum())

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

func TestCollection(t *testing.T) {
	suite.Run(t, new(CollectionSuite))
}
