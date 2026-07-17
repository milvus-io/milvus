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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
)

func makeAggBucketProto() *schemapb.AggBucket {
	return &schemapb.AggBucket{
		Key: []*schemapb.BucketKeyEntry{
			{FieldId: 100, FieldName: "brand", Value: &schemapb.BucketKeyEntry_StringVal{StringVal: "acme"}},
			{FieldId: 101, FieldName: "active", Value: &schemapb.BucketKeyEntry_BoolVal{BoolVal: true}},
			{FieldId: 102, Value: &schemapb.BucketKeyEntry_IntVal{IntVal: 7}},
		},
		Count: 3,
		Metrics: map[string]*schemapb.MetricValue{
			"count": {Value: &schemapb.MetricValue_IntVal{IntVal: 3}},
			"avg":   {Value: &schemapb.MetricValue_DoubleVal{DoubleVal: 12.5}},
			"label": {Value: &schemapb.MetricValue_StringVal{StringVal: "hot"}},
			"flag":  {Value: &schemapb.MetricValue_BoolVal{BoolVal: true}},
		},
		Hits: []*schemapb.AggHit{
			{
				Pk:    &schemapb.AggHit_IntPk{IntPk: 10},
				Score: 0.5,
				Fields: []*schemapb.AggHitField{
					{FieldId: 200, FieldName: "price", Value: &schemapb.AggHitField_IntVal{IntVal: 99}},
					{FieldId: 201, FieldName: "in_stock", Value: &schemapb.AggHitField_BoolVal{BoolVal: true}},
					{FieldId: 202, FieldName: "score_f", Value: &schemapb.AggHitField_FloatVal{FloatVal: 1.5}},
					{FieldId: 203, FieldName: "rating", Value: &schemapb.AggHitField_DoubleVal{DoubleVal: 4.8}},
					{FieldId: 204, FieldName: "name", Value: &schemapb.AggHitField_StringVal{StringVal: "item"}},
					{FieldId: 205, FieldName: "raw", Value: &schemapb.AggHitField_BytesVal{BytesVal: []byte{1, 2}}},
					{FieldId: 206, Value: &schemapb.AggHitField_StringVal{StringVal: "fallback"}},
				},
			},
			{
				Pk:    &schemapb.AggHit_StrPk{StrPk: "pk2"},
				Score: 0.8,
			},
		},
		SubGroups: []*schemapb.AggBucket{
			{
				Key:   []*schemapb.BucketKeyEntry{{FieldId: 110, FieldName: "color", Value: &schemapb.BucketKeyEntry_StringVal{StringVal: "red"}}},
				Count: 1,
			},
		},
	}
}

func TestParseAggregationBuckets(t *testing.T) {
	results := &schemapb.SearchResultData{
		NumQueries: 2,
		AggTopks:   []int64{1, 0},
		AggBuckets: []*schemapb.AggBucket{makeAggBucketProto()},
	}

	buckets, err := parseAggregationBuckets(results)
	require.NoError(t, err)
	require.Len(t, buckets, 2)
	require.Len(t, buckets[0], 1)
	require.Empty(t, buckets[1])

	bucket := buckets[0][0]
	require.EqualValues(t, 3, bucket.Count)
	require.Equal(t, []BucketKeyEntry{
		{FieldName: "brand", FieldID: 100, Value: "acme"},
		{FieldName: "active", FieldID: 101, Value: true},
		{FieldName: "102", FieldID: 102, Value: int64(7)},
	}, bucket.Key)
	require.Equal(t, map[string]any{
		"count": int64(3),
		"avg":   float64(12.5),
		"label": "hot",
		"flag":  true,
	}, bucket.Metrics)
	require.Len(t, bucket.Hits, 2)
	require.EqualValues(t, 10, bucket.Hits[0].PK)
	require.EqualValues(t, 0.5, bucket.Hits[0].Score)
	require.Equal(t, map[string]any{
		"price":    int64(99),
		"in_stock": true,
		"score_f":  float32(1.5),
		"rating":   float64(4.8),
		"name":     "item",
		"raw":      []byte{1, 2},
		"206":      "fallback",
	}, bucket.Hits[0].Fields)
	require.Equal(t, map[string]int64{
		"price":    200,
		"in_stock": 201,
		"score_f":  202,
		"rating":   203,
		"name":     204,
		"raw":      205,
		"206":      206,
	}, bucket.Hits[0].FieldIDs)
	require.Equal(t, "pk2", bucket.Hits[1].PK)
	require.Len(t, bucket.SubGroups, 1)
	require.Equal(t, "red", bucket.SubGroups[0].Key[0].Value)
}

func TestParseAggregationBucketsRejectsMalformedAggTopks(t *testing.T) {
	cases := []*schemapb.SearchResultData{
		{NumQueries: 2, AggTopks: []int64{1}, AggBuckets: []*schemapb.AggBucket{makeAggBucketProto()}},
		{NumQueries: 1, AggBuckets: []*schemapb.AggBucket{makeAggBucketProto()}},
		{NumQueries: 1, AggTopks: []int64{2}, AggBuckets: []*schemapb.AggBucket{makeAggBucketProto()}},
		{NumQueries: 1, AggTopks: []int64{-1}},
	}
	for _, tc := range cases {
		_, err := parseAggregationBuckets(tc)
		require.Error(t, err)
	}
}

func (s *ReadSuite) TestHandleSearchResultAggregationWithOutputFieldsDoesNotParseNormalFields() {
	resp := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			Topks:      []int64{0},
			AggTopks:   []int64{1},
			AggBuckets: []*schemapb.AggBucket{makeAggBucketProto()},
		},
	}

	resultSets, err := s.client.handleSearchResult(s.schema, []string{"brand", "price"}, 1, resp)
	s.Require().NoError(err)
	s.Require().Len(resultSets, 1)
	s.Equal(0, resultSets[0].ResultCount)
	s.Nil(resultSets[0].IDs)
	s.Empty(resultSets[0].Fields)
	s.Empty(resultSets[0].Scores)
	s.Require().Len(resultSets[0].AggregationBuckets, 1)
	s.Equal("acme", resultSets[0].AggregationBuckets[0].Key[0].Value)
}

func (s *ReadSuite) TestHandleSearchResultRejectsMalformedAggTopks() {
	resp := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 2,
			Topks:      []int64{0, 0},
			AggTopks:   []int64{1},
			AggBuckets: []*schemapb.AggBucket{makeAggBucketProto()},
		},
	}

	_, err := s.client.handleSearchResult(s.schema, nil, 2, resp)
	s.Require().Error(err)
}

func (s *ResultSetSuite) TestResultSetSliceKeepsAggregationBuckets() {
	rs := ResultSet{
		AggregationBuckets: []AggregationBucket{{Count: 1}},
	}

	sliced := rs.Slice(0, 0)
	s.Require().Len(sliced.AggregationBuckets, 1)
	s.EqualValues(1, sliced.AggregationBuckets[0].Count)
}

func (s *SearchIteratorSuite) TestSearchIteratorRejectsSearchAggregation() {
	opt := NewSearchIteratorOption("coll", entity.FloatVector([]float32{0.1, 0.2}))
	opt.WithSearchAggregation(NewSearchAggregation([]string{"brand"}, 3))

	_, err := s.client.SearchIterator(context.Background(), opt)
	s.Require().Error(err)
	s.Contains(err.Error(), "search_aggregation is not supported with search iterator")
}
