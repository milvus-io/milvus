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
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type IndexSuite struct {
	MockSuiteBase
}

func (s *IndexSuite) TestCreateIndex() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(4))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))

		done := atomic.NewBool(false)

		mockCreateIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).CreateIndex).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, cir *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, cir.GetCollectionName())
			s.Equal(fieldName, cir.GetFieldName())
			s.Equal(indexName, cir.GetIndexName())
			return merr.Success(), nil
		}).Build()
		defer mockCreateIndex.UnPatch()
		mockDescribeIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DescribeIndex).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, dir *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
			state := commonpb.IndexState_InProgress
			if done.Load() {
				state = commonpb.IndexState_Finished
			}
			return &milvuspb.DescribeIndexResponse{
				Status: merr.Success(),
				IndexDescriptions: []*milvuspb.IndexDescription{
					{
						FieldName: fieldName,
						IndexName: indexName,
						State:     state,
					},
				},
			}, nil
		}).Build()
		defer mockDescribeIndex.UnPatch()

		task, err := s.client.CreateIndex(ctx, NewCreateIndexOption(collectionName, fieldName, index.NewHNSWIndex(entity.L2, 32, 128)).WithIndexName(indexName))
		s.NoError(err)

		ch := make(chan struct{})
		go func() {
			defer close(ch)
			err := task.Await(ctx)
			s.NoError(err)
		}()

		select {
		case <-ch:
			s.FailNow("task done before index state set to finish")
		case <-time.After(time.Second):
		}

		done.Store(true)

		select {
		case <-ch:
		case <-time.After(time.Second):
			s.FailNow("task not done after index set finished")
		}
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(4))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))

		mockCreateIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).CreateIndex).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockCreateIndex.UnPatch()

		_, err := s.client.CreateIndex(ctx, NewCreateIndexOption(collectionName, fieldName, index.NewHNSWIndex(entity.L2, 32, 128)).WithIndexName(indexName))
		s.Error(err)
	})
}

func (s *IndexSuite) TestListIndexes() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		mockDescribeIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DescribeIndex).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, dir *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
			s.Equal(collectionName, dir.GetCollectionName())
			return &milvuspb.DescribeIndexResponse{
				Status: merr.Success(),
				IndexDescriptions: []*milvuspb.IndexDescription{
					{IndexName: "test_idx"},
				},
			}, nil
		}).Build()
		defer mockDescribeIndex.UnPatch()

		names, err := s.client.ListIndexes(ctx, NewListIndexOption(collectionName))
		s.NoError(err)
		s.ElementsMatch([]string{"test_idx"}, names)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		mockDescribeIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DescribeIndex).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockDescribeIndex.UnPatch()

		_, err := s.client.ListIndexes(ctx, NewListIndexOption(collectionName))
		s.Error(err)
	})
}

func (s *IndexSuite) TestDescribeIndex() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))
		mockDescribeIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DescribeIndex).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, dir *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
			s.Equal(collectionName, dir.GetCollectionName())
			s.Equal(indexName, dir.GetIndexName())
			return &milvuspb.DescribeIndexResponse{
				Status: merr.Success(),
				IndexDescriptions: []*milvuspb.IndexDescription{
					{IndexName: indexName, Params: []*commonpb.KeyValuePair{
						{Key: index.IndexTypeKey, Value: string(index.HNSW)},
					}},
				},
			}, nil
		}).Build()
		defer mockDescribeIndex.UnPatch()

		index, err := s.client.DescribeIndex(ctx, NewDescribeIndexOption(collectionName, indexName))
		s.NoError(err)
		s.Equal(indexName, index.Name())
	})

	s.Run("no_index_found", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))
		mockDescribeIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DescribeIndex).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, dir *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
			s.Equal(collectionName, dir.GetCollectionName())
			s.Equal(indexName, dir.GetIndexName())
			return &milvuspb.DescribeIndexResponse{
				Status:            merr.Success(),
				IndexDescriptions: []*milvuspb.IndexDescription{},
			}, nil
		}).Build()
		defer mockDescribeIndex.UnPatch()

		_, err := s.client.DescribeIndex(ctx, NewDescribeIndexOption(collectionName, indexName))
		s.Error(err)
		s.ErrorIs(err, merr.ErrIndexNotFound)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))
		mockDescribeIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DescribeIndex).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockDescribeIndex.UnPatch()

		_, err := s.client.DescribeIndex(ctx, NewDescribeIndexOption(collectionName, indexName))
		s.Error(err)
	})
}

func (s *IndexSuite) TestDropIndexOption() {
	collectionName := fmt.Sprintf("coll_%s", s.randString(6))
	indexName := fmt.Sprintf("idx_%s", s.randString(6))
	opt := NewDropIndexOption(collectionName, indexName)
	req := opt.Request()

	s.Equal(collectionName, req.GetCollectionName())
	s.Equal(indexName, req.GetIndexName())
}

func (s *IndexSuite) TestDropIndex() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		defer mockey.UnPatchAll()
		mockDropIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DropIndex).Return(merr.Success(), nil).Build()
		defer mockDropIndex.UnPatch()

		err := s.client.DropIndex(ctx, NewDropIndexOption("testCollection", "testIndex"))
		s.NoError(err)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		mockDropIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DropIndex).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockDropIndex.UnPatch()

		err := s.client.DropIndex(ctx, NewDropIndexOption("testCollection", "testIndex"))
		s.Error(err)
	})
}

func (s *IndexSuite) TestAlterIndexProperties() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))

		key := fmt.Sprintf("key_%s", s.randString(6))
		val := fmt.Sprintf("val_%s", s.randString(6))

		mockAlterIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).AlterIndex).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, air *milvuspb.AlterIndexRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, air.GetCollectionName())
			s.Equal(indexName, air.GetIndexName())
			if s.Len(air.GetExtraParams(), 1) {
				kv := air.GetExtraParams()[0]
				s.Equal(key, kv.GetKey())
				s.Equal(val, kv.GetValue())
			}

			return merr.Success(), nil
		}).Build()
		defer mockAlterIndex.UnPatch()

		err := s.client.AlterIndexProperties(ctx, NewAlterIndexPropertiesOption(collectionName, indexName).WithProperty(key, val))
		s.NoError(err)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))

		key := fmt.Sprintf("key_%s", s.randString(6))
		val := fmt.Sprintf("val_%s", s.randString(6))
		mockAlterIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).AlterIndex).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockAlterIndex.UnPatch()

		err := s.client.AlterIndexProperties(ctx, NewAlterIndexPropertiesOption(collectionName, indexName).WithProperty(key, val))
		s.Error(err)
	})
}

func (s *IndexSuite) TestDropIndexProperties() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))

		key := fmt.Sprintf("key_%s", s.randString(6))

		mockAlterIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).AlterIndex).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, air *milvuspb.AlterIndexRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, air.GetCollectionName())
			s.Equal(indexName, air.GetIndexName())
			s.ElementsMatch([]string{key}, air.GetDeleteKeys())

			return merr.Success(), nil
		}).Build()
		defer mockAlterIndex.UnPatch()

		err := s.client.DropIndexProperties(ctx, NewDropIndexPropertiesOption(collectionName, indexName, key))
		s.NoError(err)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))

		key := fmt.Sprintf("coll_%s", s.randString(6))
		mockAlterIndex := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).AlterIndex).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockAlterIndex.UnPatch()

		err := s.client.DropIndexProperties(ctx, NewDropIndexPropertiesOption(collectionName, indexName, key))
		s.Error(err)
	})
}

func TestIndex(t *testing.T) {
	suite.Run(t, new(IndexSuite))
}
