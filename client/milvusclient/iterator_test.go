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
	"io"
	"math/rand"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type SearchIteratorSuite struct {
	MockSuiteBase

	schema *entity.Schema
}

func (s *SearchIteratorSuite) SetupSuite() {
	s.MockSuiteBase.SetupSuite()
	s.schema = entity.NewSchema().
		WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("Vector").WithDataType(entity.FieldTypeFloatVector).WithDim(128))
}

func (s *SearchIteratorSuite) TestSearchIteratorInit() {
	ctx := context.Background()
	s.Run("success", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			CollectionID: 1,
			Schema:       s.schema.ProtoMessage(),
		}, nil).Once()
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			s.Equal(collectionName, sr.GetCollectionName())
			checkSearchParam := func(kvs []*commonpb.KeyValuePair, key string, value string) bool {
				for _, kv := range kvs {
					if kv.GetKey() == key && kv.GetValue() == value {
						return true
					}
				}
				return false
			}

			s.True(checkSearchParam(sr.GetSearchParams(), IteratorKey, "true"))
			s.True(checkSearchParam(sr.GetSearchParams(), IteratorSearchV2Key, "true"))
			return &milvuspb.SearchResults{
				Status: merr.Success(),
				Results: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					FieldsData: []*schemapb.FieldData{
						s.getInt64FieldData("ID", []int64{1}),
					},
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{1},
							},
						},
					},
					Scores:  make([]float32, 1),
					Topks:   []int64{1},
					Recalls: []float32{1},
					SearchIteratorV2Results: &schemapb.SearchIteratorV2Results{
						Token: s.randString(16),
					},
				},
			}, nil
		}).Once()

		iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption(collectionName, entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
			return rand.Float32()
		}))))

		s.NoError(err)
		_, ok := iter.(*searchIteratorV2)
		s.True(ok)
	})

	s.Run("failure", func() {
		s.Run("option_error", func() {
			collectionName := fmt.Sprintf("coll_%s", s.randString(6))

			_, err := s.client.SearchIterator(ctx, NewSearchIteratorOption(collectionName, entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
				return rand.Float32()
			}))).WithBatchSize(-1).WithIteratorLimit(-2))
			s.Error(err)
		})

		s.Run("describe_fail", func() {
			collectionName := fmt.Sprintf("coll_%s", s.randString(6))

			s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock error")).Once()
			_, err := s.client.SearchIterator(ctx, NewSearchIteratorOption(collectionName, entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
				return rand.Float32()
			}))))
			s.Error(err)
		})

		s.Run("not_v2_result", func() {
			collectionName := fmt.Sprintf("coll_%s", s.randString(6))
			s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionID: 1,
				Schema:       s.schema.ProtoMessage(),
			}, nil).Once()
			s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
				s.Equal(collectionName, sr.GetCollectionName())
				return &milvuspb.SearchResults{
					Status: merr.Success(),
					Results: &schemapb.SearchResultData{
						NumQueries: 1,
						TopK:       1,
						FieldsData: []*schemapb.FieldData{
							s.getInt64FieldData("ID", []int64{1}),
						},
						Ids: &schemapb.IDs{
							IdField: &schemapb.IDs_IntId{
								IntId: &schemapb.LongArray{
									Data: []int64{1},
								},
							},
						},
						Scores:                  make([]float32, 1),
						Topks:                   []int64{1},
						Recalls:                 []float32{1},
						SearchIteratorV2Results: nil, // nil v2 results
					},
				}, nil
			}).Once()

			_, err := s.client.SearchIterator(ctx, NewSearchIteratorOption(collectionName, entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
				return rand.Float32()
			}))))
			s.Error(err)
			s.ErrorIs(err, ErrServerVersionIncompatible)
		})
	})
}

func (s *SearchIteratorSuite) TestNext() {
	ctx := context.Background()
	collectionName := fmt.Sprintf("coll_%s", s.randString(6))

	token := fmt.Sprintf("iter_token_%s", s.randString(8))

	s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1,
		Schema:       s.schema.ProtoMessage(),
	}, nil).Once()
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.Equal(collectionName, sr.GetCollectionName())
		checkSearchParam := func(kvs []*commonpb.KeyValuePair, key string, value string) bool {
			for _, kv := range kvs {
				if kv.GetKey() == key && kv.GetValue() == value {
					return true
				}
			}
			return false
		}

		s.True(checkSearchParam(sr.GetSearchParams(), IteratorKey, "true"))
		s.True(checkSearchParam(sr.GetSearchParams(), IteratorSearchV2Key, "true"))
		return &milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       1,
				FieldsData: []*schemapb.FieldData{
					s.getInt64FieldData("ID", []int64{1}),
				},
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1},
						},
					},
				},
				Scores:  make([]float32, 1),
				Topks:   []int64{1},
				Recalls: []float32{1},
				SearchIteratorV2Results: &schemapb.SearchIteratorV2Results{
					Token: token,
				},
			},
		}, nil
	}).Once()

	iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption(collectionName, entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
		return rand.Float32()
	}))))
	s.Require().NoError(err)
	s.Require().NotNil(iter)

	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.Equal(collectionName, sr.GetCollectionName())
		checkSearchParam := func(kvs []*commonpb.KeyValuePair, key string, value string) bool {
			for _, kv := range kvs {
				if kv.GetKey() == key && kv.GetValue() == value {
					return true
				}
			}
			return false
		}

		s.True(checkSearchParam(sr.GetSearchParams(), IteratorKey, "true"))
		s.True(checkSearchParam(sr.GetSearchParams(), IteratorSearchV2Key, "true"))
		return &milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       1,
				FieldsData: []*schemapb.FieldData{
					s.getInt64FieldData("ID", []int64{1}),
				},
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1},
						},
					},
				},
				Scores:  []float32{0.5},
				Topks:   []int64{1},
				Recalls: []float32{1},
				SearchIteratorV2Results: &schemapb.SearchIteratorV2Results{
					Token:     token,
					LastBound: 0.5,
				},
			},
		}, nil
	}).Once()

	rs, err := iter.Next(ctx)
	s.NoError(err)
	s.EqualValues(1, rs.IDs.Len())

	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.Equal(collectionName, sr.GetCollectionName())
		checkSearchParam := func(kvs []*commonpb.KeyValuePair, key string, value string) bool {
			for _, kv := range kvs {
				if kv.GetKey() == key && kv.GetValue() == value {
					return true
				}
			}
			return false
		}

		s.True(checkSearchParam(sr.GetSearchParams(), IteratorKey, "true"))
		s.True(checkSearchParam(sr.GetSearchParams(), IteratorSearchV2Key, "true"))
		s.True(checkSearchParam(sr.GetSearchParams(), IteratorSearchIDKey, token))
		s.True(checkSearchParam(sr.GetSearchParams(), IteratorSearchLastBoundKey, "0.5"))

		return &milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       1,
				FieldsData: []*schemapb.FieldData{
					s.getInt64FieldData("ID", []int64{}),
				},
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{},
						},
					},
				},
				Scores:  []float32{},
				Topks:   []int64{0},
				Recalls: []float32{1.0},
				SearchIteratorV2Results: &schemapb.SearchIteratorV2Results{
					Token:     token,
					LastBound: 0.5,
				},
			},
		}, nil
	}).Once()

	_, err = iter.Next(ctx)
	s.Error(err)
	s.ErrorIs(err, io.EOF)
}

func (s *SearchIteratorSuite) TestNextWithLimit() {
	ctx := context.Background()
	collectionName := fmt.Sprintf("coll_%s", s.randString(6))

	token := fmt.Sprintf("iter_token_%s", s.randString(8))

	s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1,
		Schema:       s.schema.ProtoMessage(),
	}, nil).Once()
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.Equal(collectionName, sr.GetCollectionName())
		checkSearchParam := func(kvs []*commonpb.KeyValuePair, key string, value string) bool {
			for _, kv := range kvs {
				if kv.GetKey() == key && kv.GetValue() == value {
					return true
				}
			}
			return false
		}

		s.True(checkSearchParam(sr.GetSearchParams(), IteratorKey, "true"))
		s.True(checkSearchParam(sr.GetSearchParams(), IteratorSearchV2Key, "true"))
		return &milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       1,
				FieldsData: []*schemapb.FieldData{
					s.getInt64FieldData("ID", []int64{1}),
				},
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1},
						},
					},
				},
				Scores:  make([]float32, 1),
				Topks:   []int64{5},
				Recalls: []float32{1},
				SearchIteratorV2Results: &schemapb.SearchIteratorV2Results{
					Token: token,
				},
			},
		}, nil
	}).Once()

	iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption(collectionName, entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
		return rand.Float32()
	}))).WithIteratorLimit(6).WithBatchSize(5))
	s.Require().NoError(err)
	s.Require().NotNil(iter)

	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.Equal(collectionName, sr.GetCollectionName())
		checkSearchParam := func(kvs []*commonpb.KeyValuePair, key string, value string) bool {
			for _, kv := range kvs {
				if kv.GetKey() == key && kv.GetValue() == value {
					return true
				}
			}
			return false
		}

		s.True(checkSearchParam(sr.GetSearchParams(), IteratorKey, "true"))
		s.True(checkSearchParam(sr.GetSearchParams(), IteratorSearchV2Key, "true"))
		return &milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: 1,
				TopK:       1,
				FieldsData: []*schemapb.FieldData{
					s.getInt64FieldData("ID", []int64{1, 2, 3, 4, 5}),
				},
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1, 2, 3, 4, 5},
						},
					},
				},
				Scores:  []float32{0.5, 0.4, 0.3, 0.2, 0.1},
				Topks:   []int64{5},
				Recalls: []float32{1},
				SearchIteratorV2Results: &schemapb.SearchIteratorV2Results{
					Token:     token,
					LastBound: 0.5,
				},
			},
		}, nil
	}).Times(2)

	rs, err := iter.Next(ctx)
	s.NoError(err)
	s.EqualValues(5, rs.IDs.Len(), "first batch, return all results")

	rs, err = iter.Next(ctx)
	s.NoError(err)
	s.EqualValues(1, rs.IDs.Len(), "second batch, return sliced results")

	_, err = iter.Next(ctx)
	s.Error(err)
	s.ErrorIs(err, io.EOF, "limit reached, return EOF")
}

func TestSearchIterator(t *testing.T) {
	suite.Run(t, new(SearchIteratorSuite))
}
