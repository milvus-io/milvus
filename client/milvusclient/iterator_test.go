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
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
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

func (s *SearchIteratorSuite) TestSearchIteratorOptionWithNamespace() {
	namespace := "tenant_a"

	opt := NewSearchIteratorOption("coll", entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
		return rand.Float32()
	}))).WithNamespace(namespace)
	req, err := opt.SearchOption().Request()

	s.Require().NoError(err)
	s.Equal(namespace, req.GetNamespace())
}

func (s *SearchIteratorSuite) TestSearchIteratorOptionWithRLSContext() {
	opt := NewSearchIteratorOption("coll", entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
		return rand.Float32()
	}))).WithRLSPrincipal("alice").WithSkipRLS(true).WithBatchSize(10)
	req, err := opt.SearchOption().Request()

	s.Require().NoError(err)
	s.Equal("alice", req.GetRlsPrincipal())
	s.True(req.GetSkipRls())
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

func iteratorPage(ids []int64, scores []float32, token string, timestamp uint64, version string) *milvuspb.SearchResults {
	bound := float32(0)
	if len(scores) > 0 {
		bound = scores[len(scores)-1]
	}
	status := merr.Success()
	if version != "" {
		status.ExtraInfo = map[string]string{IteratorSearchCursorVersionKey: version, IteratorSearchLastPKTypeKey: "int64"}
		if len(ids) > 0 {
			status.ExtraInfo[IteratorSearchLastPKKey] = strconv.FormatInt(ids[len(ids)-1], 10)
		}
	}
	return &milvuspb.SearchResults{
		Status: status, SessionTs: timestamp,
		Results: &schemapb.SearchResultData{
			NumQueries: 1, TopK: int64(len(ids)), Topks: []int64{int64(len(ids))}, Scores: scores,
			Ids:                     &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}},
			SearchIteratorV2Results: &schemapb.SearchIteratorV2Results{Token: token, LastBound: bound},
		},
	}
}

func (s *SearchIteratorSuite) iteratorSearchCalls() int {
	count := 0
	for _, call := range s.mock.Calls {
		if call.Method == "Search" {
			count++
		}
	}
	return count
}

func (s *SearchIteratorSuite) describeIterator(schema *entity.Schema) {
	s.mock.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionID: 1, Schema: schema.ProtoMessage(),
	}, nil).Once()
}

func (s *SearchIteratorSuite) TestFirstPageSnapshotAndCursor() {
	ctx := context.Background()
	initialCalls := s.iteratorSearchCalls()
	s.describeIterator(s.schema)
	opt := NewSearchIteratorOption("coll", entity.FloatVector{1}).WithSearchParam(IteratorSearchCursorVersionKey, "2").WithBatchSize(2)
	original, err := opt.SearchOption().Request()
	s.Require().NoError(err)
	original = proto.Clone(original).(*milvuspb.SearchRequest)
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.Equal("2", searchIteratorParam(req, "topk"))
		s.Equal("2", searchIteratorParam(req, IteratorSearchBatchSizeKey))
		s.Equal("2", searchIteratorParam(req, IteratorSearchCursorVersionKey))
		s.Zero(req.GetGuaranteeTimestamp())
		return iteratorPage([]int64{math.MinInt64, math.MaxInt64}, []float32{1, 1}, "token", 12345, "2"), nil
	}).Once()
	iter, err := s.client.SearchIterator(ctx, opt)
	s.Require().NoError(err)
	after, err := opt.SearchOption().Request()
	s.Require().NoError(err)
	sort.Slice(original.SearchParams, func(i, j int) bool { return original.SearchParams[i].Key < original.SearchParams[j].Key })
	sort.Slice(after.SearchParams, func(i, j int) bool { return after.SearchParams[i].Key < after.SearchParams[j].Key })
	s.True(proto.Equal(original, after), "iterator must not mutate caller options")
	opt.WithBatchSize(99).WithFilter("ID > 0")
	first, err := iter.Next(ctx)
	s.Require().NoError(err)
	s.Equal(2, first.Len())
	s.Equal([]int64{math.MinInt64, math.MaxInt64}, first.IDs.(*column.ColumnInt64).Data())
	s.Equal(initialCalls+1, s.iteratorSearchCalls())
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.Equal(uint64(12345), req.GetGuaranteeTimestamp())
		s.Equal("2", searchIteratorParam(req, IteratorSearchBatchSizeKey))
		s.Equal("", req.GetDsl())
		s.Equal("token", searchIteratorParam(req, IteratorSearchIDKey))
		s.Equal("1", searchIteratorParam(req, IteratorSearchLastBoundKey))
		s.Equal("int64", searchIteratorParam(req, IteratorSearchLastPKTypeKey))
		s.Equal(strconv.FormatInt(math.MaxInt64, 10), searchIteratorParam(req, IteratorSearchLastPKKey))
		return iteratorPage([]int64{5}, []float32{2}, "token", 0, "2"), nil
	}).Once()
	second, err := iter.Next(ctx)
	s.Require().NoError(err)
	s.Equal(1, second.Len())
	s.Equal(uint64(12345), iter.(*searchIteratorV2).request.GetGuaranteeTimestamp())
}

func (s *SearchIteratorSuite) TestDuplicatePKsAcrossScoresDoNotConsumeLimit() {
	ctx := context.Background()
	s.describeIterator(s.schema)
	for _, page := range []*milvuspb.SearchResults{
		iteratorPage([]int64{1}, []float32{0}, "token", 12345, "2"),
		iteratorPage([]int64{1}, []float32{1}, "token", 12345, "2"),
		iteratorPage([]int64{2}, []float32{2}, "token", 12345, "2"),
	} {
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(page, nil).Once()
	}
	iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption("coll", entity.FloatVector{1}).
		WithSearchParam(IteratorSearchCursorVersionKey, "2").WithBatchSize(1).WithIteratorLimit(2))
	s.Require().NoError(err)
	first, err := iter.Next(ctx)
	s.Require().NoError(err)
	s.Equal([]int64{1}, first.IDs.(*column.ColumnInt64).Data())
	second, err := iter.Next(ctx)
	s.Require().NoError(err)
	s.Equal([]int64{2}, second.IDs.(*column.ColumnInt64).Data())
	s.Equal("2", searchIteratorParam(iter.(*searchIteratorV2).request, IteratorSearchLastBoundKey))
	_, err = iter.Next(ctx)
	s.ErrorIs(err, io.EOF)
}

func (s *SearchIteratorSuite) TestDistinctRowsPreserveNullableAndDynamicFields() {
	values := column.NewColumnInt64("nullable", nil)
	values.SetNullable(true)
	s.Require().NoError(values.AppendValue(int64(11)))
	s.Require().NoError(values.AppendNull())
	s.Require().NoError(values.AppendValue(int64(33)))
	json := column.NewColumnJSONBytes("$meta", [][]byte{[]byte(`{"tag":"a"}`), []byte(`{"tag":"b"}`), []byte(`{"tag":"c"}`)})
	input := ResultSet{ResultCount: 3, IDs: column.NewColumnInt64("ID", []int64{1, 2, 1}),
		Scores: []float32{0, 1, 2}, Fields: DataSet{values, column.NewColumnDynamic(json, "tag")}}
	result, keys, err := distinctIteratorResults(input, map[any]struct{}{int64(1): {}})
	s.Require().NoError(err)
	s.Equal([]int64{2}, result.IDs.(*column.ColumnInt64).Data())
	s.Equal([]float32{1}, result.Scores)
	s.Equal(1, result.Len())
	null, err := result.Fields[0].IsNull(0)
	s.Require().NoError(err)
	s.True(null)
	tag, err := result.Fields[1].Get(0)
	s.Require().NoError(err)
	s.Equal(`"b"`, tag)
	s.Len(keys, 1)
	s.Equal(3, input.Fields[0].Len())
}

func (s *SearchIteratorSuite) TestVarcharCursor() {
	ctx := context.Background()
	initialCalls := s.iteratorSearchCalls()
	schema := entity.NewSchema().WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("Vector").WithDataType(entity.FieldTypeFloatVector).WithDim(1))
	s.describeIterator(schema)
	lastPK := "a\"b\\c\n中文"
	page := iteratorPage(nil, []float32{1}, "token", 999, "2")
	page.Results.Topks = []int64{1}
	page.Results.Ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{lastPK}}}}
	page.Status.ExtraInfo[IteratorSearchLastPKTypeKey] = "varchar"
	page.Status.ExtraInfo[IteratorSearchLastPKKey] = lastPK
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(page, nil).Once()
	iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption("coll", entity.FloatVector{1}).WithSearchParam(IteratorSearchCursorVersionKey, "2").WithBatchSize(1))
	s.Require().NoError(err)
	_, err = iter.Next(ctx)
	s.Require().NoError(err)
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.Equal(lastPK, searchIteratorParam(req, IteratorSearchLastPKKey))
		s.Equal("varchar", searchIteratorParam(req, IteratorSearchLastPKTypeKey))
		empty := iteratorPage(nil, nil, "token", 0, "2")
		empty.Results.Ids = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{}}}
		delete(empty.Status.ExtraInfo, IteratorSearchLastPKTypeKey)
		return empty, nil
	}).Once()
	_, err = iter.Next(ctx)
	s.ErrorIs(err, io.EOF)
	_, err = iter.Next(ctx)
	s.ErrorIs(err, io.EOF)
	s.Equal(initialCalls+2, s.iteratorSearchCalls())
}

func (s *SearchIteratorSuite) TestLegacyV2() {
	ctx := context.Background()
	s.describeIterator(s.schema)
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(iteratorPage([]int64{1, 2}, []float32{1, 2}, "token", 999, ""), nil).Once()
	iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption("coll", entity.FloatVector{1}).WithSearchParam(IteratorSearchCursorVersionKey, "2").WithBatchSize(2))
	s.Require().NoError(err)
	_, err = iter.Next(ctx)
	s.Require().NoError(err)
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.Equal(uint64(999), req.GetGuaranteeTimestamp())
		s.Equal("2", searchIteratorParam(req, IteratorSearchLastBoundKey))
		s.Empty(searchIteratorParam(req, IteratorSearchLastPKKey))
		s.Empty(searchIteratorParam(req, IteratorSearchCursorVersionKey))
		return iteratorPage(nil, nil, "token", 0, ""), nil
	}).Once()
	_, err = iter.Next(ctx)
	s.ErrorIs(err, io.EOF)
}

func (s *SearchIteratorSuite) TestLegacyMissingSnapshot() {
	ctx := context.Background()
	s.describeIterator(s.schema)
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(iteratorPage([]int64{1}, []float32{1}, "token", 0, ""), nil).Once()
	iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption("coll", entity.FloatVector{1}).WithBatchSize(1))
	s.Require().NoError(err)
	_, err = iter.Next(ctx)
	s.Require().NoError(err)
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.Zero(req.GetGuaranteeTimestamp())
		return iteratorPage(nil, nil, "token", 0, ""), nil
	}).Once()
	_, err = iter.Next(ctx)
	s.ErrorIs(err, io.EOF)
}

func (s *SearchIteratorSuite) TestLimitsAndEmpty() {
	ctx := context.Background()
	s.Run("finite_limit", func() {
		s.describeIterator(s.schema)
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(iteratorPage([]int64{1, 2}, []float32{1, 2}, "token", 123, "2"), nil).Once()
		iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption("coll", entity.FloatVector{1}).WithSearchParam(IteratorSearchCursorVersionKey, "2").WithBatchSize(2).WithIteratorLimit(3))
		s.Require().NoError(err)
		first, err := iter.Next(ctx)
		s.Require().NoError(err)
		s.Equal(2, first.Len())
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(iteratorPage([]int64{3, 4}, []float32{3, 4}, "token", 0, "2"), nil).Once()
		last, err := iter.Next(ctx)
		s.Require().NoError(err)
		s.Equal(1, last.Len())
		_, err = iter.Next(ctx)
		s.ErrorIs(err, io.EOF)
	})
	s.Run("initial_empty", func() {
		s.describeIterator(s.schema)
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(iteratorPage(nil, nil, "empty", 123, "2"), nil).Once()
		iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption("coll", entity.FloatVector{1}).WithSearchParam(IteratorSearchCursorVersionKey, "2"))
		s.Require().NoError(err)
		_, err = iter.Next(ctx)
		s.ErrorIs(err, io.EOF)
		_, err = iter.Next(ctx)
		s.ErrorIs(err, io.EOF)
	})
	s.Run("zero_limit", func() {
		iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption("coll", entity.FloatVector{1}).WithSearchParam(IteratorSearchCursorVersionKey, "2").WithIteratorLimit(0))
		s.Require().NoError(err)
		_, err = iter.Next(ctx)
		s.ErrorIs(err, io.EOF)
	})
}

func (s *SearchIteratorSuite) TestFailedPagesDoNotAdvance() {
	ctx := context.Background()
	s.describeIterator(s.schema)
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(iteratorPage([]int64{1}, []float32{1}, "token", 123, "2"), nil).Once()
	iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption("coll", entity.FloatVector{1}).WithSearchParam(IteratorSearchCursorVersionKey, "2").WithBatchSize(1))
	s.Require().NoError(err)
	_, err = iter.Next(ctx)
	s.Require().NoError(err)
	private := iter.(*searchIteratorV2)
	before := proto.Clone(private.request).(*milvuspb.SearchRequest)
	badCursor := iteratorPage([]int64{2}, []float32{2}, "token", 0, "2")
	badCursor.Status.ExtraInfo[IteratorSearchLastPKKey] = "3"
	badType := iteratorPage([]int64{2}, []float32{2}, "token", 0, "2")
	badType.Status.ExtraInfo[IteratorSearchLastPKTypeKey] = "varchar"
	badShape := iteratorPage([]int64{2}, []float32{2}, "token", 0, "2")
	badShape.Results.Scores = nil
	badField := iteratorPage([]int64{2}, []float32{2}, "token", 0, "2")
	badField.Results.FieldsData = []*schemapb.FieldData{{FieldName: "ID", Type: schemapb.DataType_Int64}}
	badScore := iteratorPage([]int64{2}, []float32{float32(math.NaN())}, "token", 0, "2")
	missingPK := iteratorPage([]int64{2}, []float32{2}, "token", 0, "2")
	delete(missingPK.Status.ExtraInfo, IteratorSearchLastPKKey)
	for _, page := range []*milvuspb.SearchResults{
		iteratorPage([]int64{2}, []float32{2}, "token", 0, ""),
		iteratorPage([]int64{2}, []float32{2}, "token", 0, "3"),
		iteratorPage([]int64{2}, []float32{2}, "changed-token", 0, "2"),
		badCursor, badType, badShape, badField, badScore, missingPK,
	} {
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(page, nil).Once()
		_, err = iter.Next(ctx)
		s.Require().Error(err)
		s.True(proto.Equal(before, private.request), "failure must not advance request cursor")
	}
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(nil, status.Error(codes.Canceled, "temporary transport failure")).Once()
	_, err = iter.Next(ctx)
	s.Require().Error(err)
	s.True(proto.Equal(before, private.request))
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.True(proto.Equal(before, req), "retry must request the same page")
		return iteratorPage([]int64{2}, []float32{2}, "token", 0, "2"), nil
	}).Once()
	page, err := iter.Next(ctx)
	s.Require().NoError(err)
	s.Equal(int64(2), page.IDs.(*column.ColumnInt64).Data()[0])
}

func (s *SearchIteratorSuite) TestNegotiationViolations() {
	ctx := context.Background()
	for _, page := range []*milvuspb.SearchResults{
		iteratorPage([]int64{1}, []float32{1}, "token", 0, "2"),
		iteratorPage([]int64{1}, []float32{1}, "token", 123, "3"),
		iteratorPage([]int64{1}, []float32{1}, "", 123, "2"),
	} {
		s.describeIterator(s.schema)
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(page, nil).Once()
		_, err := s.client.SearchIterator(ctx, NewSearchIteratorOption("coll", entity.FloatVector{1}).WithSearchParam(IteratorSearchCursorVersionKey, "2"))
		s.Require().Error(err)
		s.NotErrorIs(err, ErrServerVersionIncompatible, "malformed PK metadata is a protocol error, not legacy fallback")
	}
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

func (s *QueryIteratorSuite) TestQueryIteratorOptionWithNamespace() {
	namespace := "tenant_a"

	req, err := NewQueryIteratorOption("coll").WithNamespace(namespace).Request()

	s.Require().NoError(err)
	s.Equal(namespace, req.GetNamespace())
}

func (s *QueryIteratorSuite) TestQueryIteratorOptionWithRLSContext() {
	req, err := NewQueryIteratorOption("coll").
		WithRLSPrincipal("alice").
		WithSkipRLS(true).
		WithBatchSize(10).
		Request()

	s.Require().NoError(err)
	s.Equal("alice", req.GetRlsPrincipal())
	s.True(req.GetSkipRls())
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

func (s *SearchIteratorSuite) TestManualLegacyContinuation() {
	ctx := context.Background()
	s.describeIterator(s.schema)
	token := "4ea6247d-4b47-4e95-a65c-3bca62bbf7c1"
	opt := NewSearchIteratorOption("coll", entity.FloatVector{1}).WithBatchSize(1).WithIteratorLimit(1).
		WithSearchParam(IteratorSearchIDKey, token).WithSearchParam(IteratorSearchLastBoundKey, "0.5")
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.Equal(token, searchIteratorParam(req, IteratorSearchIDKey))
		s.Equal("0.5", searchIteratorParam(req, IteratorSearchLastBoundKey))
		s.Empty(searchIteratorParam(req, IteratorSearchCursorVersionKey))
		s.Empty(searchIteratorParam(req, IteratorSearchLastPKTypeKey))
		return iteratorPage([]int64{2}, []float32{0.7}, token, 123, ""), nil
	}).Once()
	iter, err := s.client.SearchIterator(ctx, opt)
	s.Require().NoError(err)
	page, err := iter.Next(ctx)
	s.Require().NoError(err)
	s.Equal(int64(2), page.IDs.(*column.ColumnInt64).Data()[0])
	_, err = iter.Next(ctx)
	s.ErrorIs(err, io.EOF)
}

func (s *SearchIteratorSuite) TestDefaultDistanceModeAndUnsupportedRequestVersion() {
	ctx := context.Background()
	s.describeIterator(s.schema)
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		s.Equal("2", searchIteratorParam(req, "topk"))
		s.Empty(searchIteratorParam(req, IteratorSearchCursorVersionKey))
		return iteratorPage([]int64{1, 2}, []float32{0.9, 0.8}, "token", 123, ""), nil
	}).Once()
	iter, err := s.client.SearchIterator(ctx, NewSearchIteratorOption("coll", entity.FloatVector{1}).WithBatchSize(2).WithIteratorLimit(2))
	s.Require().NoError(err)
	page, err := iter.Next(ctx)
	s.Require().NoError(err)
	s.Equal(2, page.Len())
	_, err = iter.Next(ctx)
	s.ErrorIs(err, io.EOF)
	_, err = s.client.SearchIterator(ctx, NewSearchIteratorOption("coll", entity.FloatVector{1}).WithSearchParam(IteratorSearchCursorVersionKey, "3"))
	s.Require().Error(err)
	s.describeIterator(s.schema)
	s.mock.EXPECT().Search(mock.Anything, mock.Anything).Return(iteratorPage([]int64{1}, []float32{0.9}, "token", 123, "2"), nil).Once()
	_, err = s.client.SearchIterator(ctx, NewSearchIteratorOption("coll", entity.FloatVector{1}))
	s.Require().Error(err)
}

func (s *SearchIteratorSuite) TestExplicitPKModeRejectsPartialLegacyContinuation() {
	callsBefore := len(s.mock.Calls)
	opt := NewSearchIteratorOption("coll", entity.FloatVector{1}).
		WithSearchParam(IteratorSearchCursorVersionKey, "2").
		WithSearchParam(IteratorSearchIDKey, "4ea6247d-4b47-4e95-a65c-3bca62bbf7c1").
		WithSearchParam(IteratorSearchLastBoundKey, "0.5")
	_, err := s.client.SearchIterator(context.Background(), opt)
	s.Require().Error(err)
	s.Contains(err.Error(), "complete typed cursor")
	s.Equal(callsBefore, len(s.mock.Calls), "input rejection must not send another RPC")
}
