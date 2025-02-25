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

	"github.com/stretchr/testify/mock"
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
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(4))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))

		done := atomic.NewBool(false)

		s.mock.EXPECT().CreateIndex(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cir *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, cir.GetCollectionName())
			s.Equal(fieldName, cir.GetFieldName())
			s.Equal(indexName, cir.GetIndexName())
			return merr.Success(), nil
		}).Once()
		s.mock.EXPECT().DescribeIndex(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, dir *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
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
		})
		defer s.mock.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Unset()

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
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldName := fmt.Sprintf("field_%s", s.randString(4))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))

		s.mock.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.CreateIndex(ctx, NewCreateIndexOption(collectionName, fieldName, index.NewHNSWIndex(entity.L2, 32, 128)).WithIndexName(indexName))
		s.Error(err)
	})
}

func (s *IndexSuite) TestListIndexes() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		s.mock.EXPECT().DescribeIndex(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, dir *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
			s.Equal(collectionName, dir.GetCollectionName())
			return &milvuspb.DescribeIndexResponse{
				Status: merr.Success(),
				IndexDescriptions: []*milvuspb.IndexDescription{
					{IndexName: "test_idx"},
				},
			}, nil
		}).Once()

		names, err := s.client.ListIndexes(ctx, NewListIndexOption(collectionName))
		s.NoError(err)
		s.ElementsMatch([]string{"test_idx"}, names)
	})

	s.Run("failure", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		s.mock.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.ListIndexes(ctx, NewListIndexOption(collectionName))
		s.Error(err)
	})
}

func (s *IndexSuite) TestDescribeIndex() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))
		s.mock.EXPECT().DescribeIndex(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, dir *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
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
		}).Once()

		index, err := s.client.DescribeIndex(ctx, NewDescribeIndexOption(collectionName, indexName))
		s.NoError(err)
		s.Equal(indexName, index.Name())
	})

	s.Run("no_index_found", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))
		s.mock.EXPECT().DescribeIndex(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, dir *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
			s.Equal(collectionName, dir.GetCollectionName())
			s.Equal(indexName, dir.GetIndexName())
			return &milvuspb.DescribeIndexResponse{
				Status:            merr.Success(),
				IndexDescriptions: []*milvuspb.IndexDescription{},
			}, nil
		}).Once()

		_, err := s.client.DescribeIndex(ctx, NewDescribeIndexOption(collectionName, indexName))
		s.Error(err)
		s.ErrorIs(err, merr.ErrIndexNotFound)
	})

	s.Run("failure", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		indexName := fmt.Sprintf("idx_%s", s.randString(6))
		s.mock.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

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
		s.mock.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

		err := s.client.DropIndex(ctx, NewDropIndexOption("testCollection", "testIndex"))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.DropIndex(ctx, NewDropIndexOption("testCollection", "testIndex"))
		s.Error(err)
	})
}

func TestIndex(t *testing.T) {
	suite.Run(t, new(IndexSuite))
}
