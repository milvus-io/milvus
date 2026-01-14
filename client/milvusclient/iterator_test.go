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

type QueryIteratorSuite struct {
	MockSuiteBase

	schema *entity.Schema
}

func (s *QueryIteratorSuite) SetupSuite() {
	s.MockSuiteBase.SetupSuite()
	s.schema = entity.NewSchema().
		WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("Vector").WithDataType(entity.FieldTypeFloatVector).WithDim(128)).
		WithField(entity.NewField().WithName("Name").WithDataType(entity.FieldTypeVarChar).WithMaxLength(256))
}

func (s *QueryIteratorSuite) TestQueryIteratorInit() {
	ctx := context.Background()
	s.Run("success", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			CollectionID: 1,
			Schema:       s.schema.ProtoMessage(),
		}, nil).Once()
		s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
			s.Equal(collectionName, qr.GetCollectionName())
			return &milvuspb.QueryResults{
				Status: merr.Success(),
				FieldsData: []*schemapb.FieldData{
					s.getInt64FieldData("ID", []int64{1, 2, 3}),
					s.getVarcharFieldData("Name", []string{"a", "b", "c"}),
				},
			}, nil
		}).Once()

		iter, err := s.client.QueryIterator(ctx, NewQueryIteratorOption(collectionName).
			WithOutputFields("ID", "Name").
			WithBatchSize(10))

		s.NoError(err)
		s.NotNil(iter)
	})

	s.Run("failure", func() {
		s.Run("option_error", func() {
			collectionName := fmt.Sprintf("coll_%s", s.randString(6))

			_, err := s.client.QueryIterator(ctx, NewQueryIteratorOption(collectionName).WithBatchSize(-1))
			s.Error(err)
		})

		s.Run("describe_fail", func() {
			collectionName := fmt.Sprintf("coll_%s", s.randString(6))

			s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock error")).Once()
			_, err := s.client.QueryIterator(ctx, NewQueryIteratorOption(collectionName))
			s.Error(err)
		})

		s.Run("query_fail", func() {
			collectionName := fmt.Sprintf("coll_%s", s.randString(6))

			s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
				CollectionID: 1,
				Schema:       s.schema.ProtoMessage(),
			}, nil).Once()
			s.mock.EXPECT().Query(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock query error")).Once()

			_, err := s.client.QueryIterator(ctx, NewQueryIteratorOption(collectionName))
			s.Error(err)
		})
	})
}

func (s *QueryIteratorSuite) TestQueryIteratorNext() {
	ctx := context.Background()
	collectionName := fmt.Sprintf("coll_%s", s.randString(6))

	s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1,
		Schema:       s.schema.ProtoMessage(),
	}, nil).Once()

	// first query for init
	s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
		s.Equal(collectionName, qr.GetCollectionName())
		return &milvuspb.QueryResults{
			Status: merr.Success(),
			FieldsData: []*schemapb.FieldData{
				s.getInt64FieldData("ID", []int64{1, 2, 3}),
				s.getVarcharFieldData("Name", []string{"a", "b", "c"}),
			},
		}, nil
	}).Once()

	iter, err := s.client.QueryIterator(ctx, NewQueryIteratorOption(collectionName).
		WithOutputFields("ID", "Name").
		WithBatchSize(3))
	s.Require().NoError(err)
	s.Require().NotNil(iter)

	// first Next should return cached data
	rs, err := iter.Next(ctx)
	s.NoError(err)
	s.EqualValues(3, rs.ResultCount)

	// second query
	s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
		s.Equal(collectionName, qr.GetCollectionName())
		// verify pagination expression contains PK filter
		s.Contains(qr.GetExpr(), "ID > 3")
		return &milvuspb.QueryResults{
			Status: merr.Success(),
			FieldsData: []*schemapb.FieldData{
				s.getInt64FieldData("ID", []int64{4, 5}),
				s.getVarcharFieldData("Name", []string{"d", "e"}),
			},
		}, nil
	}).Once()

	rs, err = iter.Next(ctx)
	s.NoError(err)
	s.EqualValues(2, rs.ResultCount)

	// third query - empty result
	s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
		s.Equal(collectionName, qr.GetCollectionName())
		s.Contains(qr.GetExpr(), "ID > 5")
		return &milvuspb.QueryResults{
			Status:     merr.Success(),
			FieldsData: []*schemapb.FieldData{},
		}, nil
	}).Once()

	_, err = iter.Next(ctx)
	s.Error(err)
	s.ErrorIs(err, io.EOF)
}

func (s *QueryIteratorSuite) TestQueryIteratorWithLimit() {
	ctx := context.Background()
	collectionName := fmt.Sprintf("coll_%s", s.randString(6))

	s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1,
		Schema:       s.schema.ProtoMessage(),
	}, nil).Once()

	// first query for init
	s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
		return &milvuspb.QueryResults{
			Status: merr.Success(),
			FieldsData: []*schemapb.FieldData{
				s.getInt64FieldData("ID", []int64{1, 2, 3, 4, 5}),
				s.getVarcharFieldData("Name", []string{"a", "b", "c", "d", "e"}),
			},
		}, nil
	}).Once()

	iter, err := s.client.QueryIterator(ctx, NewQueryIteratorOption(collectionName).
		WithOutputFields("ID", "Name").
		WithBatchSize(5).
		WithIteratorLimit(7))
	s.Require().NoError(err)
	s.Require().NotNil(iter)

	// first Next - returns 5 items
	rs, err := iter.Next(ctx)
	s.NoError(err)
	s.EqualValues(5, rs.ResultCount)

	// second query
	s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
		return &milvuspb.QueryResults{
			Status: merr.Success(),
			FieldsData: []*schemapb.FieldData{
				s.getInt64FieldData("ID", []int64{6, 7, 8, 9, 10}),
				s.getVarcharFieldData("Name", []string{"f", "g", "h", "i", "j"}),
			},
		}, nil
	}).Once()

	// second Next - returns only 2 items due to limit (7 - 5 = 2)
	rs, err = iter.Next(ctx)
	s.NoError(err)
	s.EqualValues(2, rs.ResultCount, "should return sliced result due to limit")

	// third Next - limit reached, should return EOF
	_, err = iter.Next(ctx)
	s.Error(err)
	s.ErrorIs(err, io.EOF, "limit reached, return EOF")
}

func (s *QueryIteratorSuite) TestQueryIteratorWithVarCharPK() {
	ctx := context.Background()
	collectionName := fmt.Sprintf("coll_%s", s.randString(6))

	schemaVarCharPK := entity.NewSchema().
		WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true).WithMaxLength(64)).
		WithField(entity.NewField().WithName("Vector").WithDataType(entity.FieldTypeFloatVector).WithDim(128))

	s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1,
		Schema:       schemaVarCharPK.ProtoMessage(),
	}, nil).Once()

	s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
		return &milvuspb.QueryResults{
			Status: merr.Success(),
			FieldsData: []*schemapb.FieldData{
				s.getVarcharFieldData("ID", []string{"a", "b", "c"}),
			},
		}, nil
	}).Once()

	iter, err := s.client.QueryIterator(ctx, NewQueryIteratorOption(collectionName).
		WithOutputFields("ID").
		WithBatchSize(3))
	s.Require().NoError(err)
	s.Require().NotNil(iter)

	rs, err := iter.Next(ctx)
	s.NoError(err)
	s.EqualValues(3, rs.ResultCount)

	// second query - verify varchar PK filter
	s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
		s.Contains(qr.GetExpr(), `ID > "c"`)
		return &milvuspb.QueryResults{
			Status:     merr.Success(),
			FieldsData: []*schemapb.FieldData{},
		}, nil
	}).Once()

	_, err = iter.Next(ctx)
	s.Error(err)
	s.ErrorIs(err, io.EOF)
}

func (s *QueryIteratorSuite) TestQueryIteratorWithFilter() {
	ctx := context.Background()
	collectionName := fmt.Sprintf("coll_%s", s.randString(6))

	s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1,
		Schema:       s.schema.ProtoMessage(),
	}, nil).Once()

	s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
		s.Equal(`Name == "test"`, qr.GetExpr())
		return &milvuspb.QueryResults{
			Status: merr.Success(),
			FieldsData: []*schemapb.FieldData{
				s.getInt64FieldData("ID", []int64{1, 2}),
				s.getVarcharFieldData("Name", []string{"test", "test"}),
			},
		}, nil
	}).Once()

	iter, err := s.client.QueryIterator(ctx, NewQueryIteratorOption(collectionName).
		WithFilter(`Name == "test"`).
		WithOutputFields("ID", "Name").
		WithBatchSize(10))
	s.Require().NoError(err)
	s.Require().NotNil(iter)

	rs, err := iter.Next(ctx)
	s.NoError(err)
	s.EqualValues(2, rs.ResultCount)

	// second query - filter combined with PK filter
	s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
		s.Contains(qr.GetExpr(), `Name == "test"`)
		s.Contains(qr.GetExpr(), "ID > 2")
		return &milvuspb.QueryResults{
			Status:     merr.Success(),
			FieldsData: []*schemapb.FieldData{},
		}, nil
	}).Once()

	_, err = iter.Next(ctx)
	s.Error(err)
	s.ErrorIs(err, io.EOF)
}

func TestQueryIterator(t *testing.T) {
	suite.Run(t, new(QueryIteratorSuite))
}
