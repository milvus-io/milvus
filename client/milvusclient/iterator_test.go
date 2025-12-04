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

	"github.com/bytedance/mockey"
	"github.com/samber/lo"
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
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		mockDescribeCollection := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
			CollectionID: 1,
			Schema:       s.schema.ProtoMessage(),
		}, nil).Build()
		defer mockDescribeCollection.UnPatch()
		mockSearch := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).Search).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
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
		}).Build()
		defer mockSearch.UnPatch()

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
			defer mockey.UnPatchAll()
			collectionName := fmt.Sprintf("coll_%s", s.randString(6))

			mockDescribeCollection := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DescribeCollection).Return(nil, fmt.Errorf("mock error")).Build()
			defer mockDescribeCollection.UnPatch()
			_, err := s.client.SearchIterator(ctx, NewSearchIteratorOption(collectionName, entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
				return rand.Float32()
			}))))
			s.Error(err)
		})

		s.Run("not_v2_result", func() {
			defer mockey.UnPatchAll()
			collectionName := fmt.Sprintf("coll_%s", s.randString(6))
			mockDescribeCollection := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
				CollectionID: 1,
				Schema:       s.schema.ProtoMessage(),
			}, nil).Build()
			defer mockDescribeCollection.UnPatch()
			mockSearch := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).Search).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
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
			}).Build()
			defer mockSearch.UnPatch()

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

	defer mockey.UnPatchAll()
	mockDescribeCollection := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1,
		Schema:       s.schema.ProtoMessage(),
	}, nil).Build()
	defer mockDescribeCollection.UnPatch()

	searchCallCount := 0
	mockSearch := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).Search).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		searchCallCount++
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

		// Third call should return empty results
		if searchCallCount >= 3 {
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
		}

		// Second call returns results with lastBound
		if searchCallCount == 2 {
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
		}

		// First call (init)
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
	}).Build()
	defer mockSearch.UnPatch()

	iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption(collectionName, entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
		return rand.Float32()
	}))))
	s.Require().NoError(err)
	s.Require().NotNil(iter)

	rs, err := iter.Next(ctx)
	s.NoError(err)
	s.EqualValues(1, rs.IDs.Len())

	_, err = iter.Next(ctx)
	s.Error(err)
	s.ErrorIs(err, io.EOF)
}

func (s *SearchIteratorSuite) TestNextWithLimit() {
	ctx := context.Background()
	collectionName := fmt.Sprintf("coll_%s", s.randString(6))

	token := fmt.Sprintf("iter_token_%s", s.randString(8))

	defer mockey.UnPatchAll()
	mockDescribeCollection := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DescribeCollection).Return(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1,
		Schema:       s.schema.ProtoMessage(),
	}, nil).Build()
	defer mockDescribeCollection.UnPatch()

	searchCallCount := 0
	mockSearch := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).Search).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		searchCallCount++
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

		// First call (init)
		if searchCallCount == 1 {
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
		}

		// Subsequent calls return 5 results each
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
	}).Build()
	defer mockSearch.UnPatch()

	iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption(collectionName, entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
		return rand.Float32()
	}))).WithIteratorLimit(6).WithBatchSize(5))
	s.Require().NoError(err)
	s.Require().NotNil(iter)

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
