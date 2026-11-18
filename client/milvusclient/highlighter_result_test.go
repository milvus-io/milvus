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
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
)

func TestParseHighlightsBasic(t *testing.T) {
	results := &schemapb.SearchResultData{
		HighlightResults: []*commonpb.HighlightResult{
			{
				FieldName: "text",
				Datas: []*commonpb.HighlightData{
					{Fragments: []string{"<em>a</em> row 1"}, Scores: []float32{0.7}},
					{Fragments: []string{"row 2 <em>b</em>"}, Scores: []float32{0.6}},
				},
			},
			{
				FieldName: "title",
				Datas: []*commonpb.HighlightData{
					{Fragments: []string{"<em>doc</em>"}, Scores: nil},
					{Fragments: []string{"doc 2"}, Scores: nil},
				},
			},
		},
	}

	m, err := parseHighlights(results, 0, 2)
	require.NoError(t, err)
	require.Len(t, m, 2)
	require.Equal(t, []string{"<em>a</em> row 1"}, m["text"][0].Fragments)
	require.Equal(t, []float32{0.7}, m["text"][0].Scores)
	require.Equal(t, []string{"row 2 <em>b</em>"}, m["text"][1].Fragments)
	require.Equal(t, []float32{0.6}, m["text"][1].Scores)
	require.Equal(t, []string{"<em>doc</em>"}, m["title"][0].Fragments)
	require.Nil(t, m["title"][0].Scores)
}

func TestParseHighlightsSlicesByRow(t *testing.T) {
	results := &schemapb.SearchResultData{
		HighlightResults: []*commonpb.HighlightResult{
			{
				FieldName: "text",
				Datas: []*commonpb.HighlightData{
					{Fragments: []string{"row0"}},
					{Fragments: []string{"row1"}},
					{Fragments: []string{"row2"}},
				},
			},
			{
				FieldName: "title",
				Datas: []*commonpb.HighlightData{
					{Fragments: []string{"title0"}},
					{Fragments: []string{"title1"}},
					{Fragments: []string{"title2"}},
				},
			},
		},
	}

	// Take rows 1..2 → only the second element of each field's datas.
	m, err := parseHighlights(results, 1, 2)
	require.NoError(t, err)
	require.Len(t, m, 2)
	require.Len(t, m["text"], 1)
	require.Equal(t, []string{"row1"}, m["text"][0].Fragments)
	require.Len(t, m["title"], 1)
	require.Equal(t, []string{"title1"}, m["title"][0].Fragments)
}

func TestParseHighlightsNilOnEmpty(t *testing.T) {
	t.Run("nil result", func(t *testing.T) {
		m, err := parseHighlights(&schemapb.SearchResultData{}, 0, 0)
		require.NoError(t, err)
		require.Nil(t, m)
	})

	t.Run("no highlight results", func(t *testing.T) {
		results := &schemapb.SearchResultData{
			HighlightResults: nil,
		}
		m, err := parseHighlights(results, 0, 5)
		require.NoError(t, err)
		require.Nil(t, m)
	})
}

func TestParseHighlightsBoundsCheck(t *testing.T) {
	results := &schemapb.SearchResultData{
		HighlightResults: []*commonpb.HighlightResult{
			{
				FieldName: "text",
				Datas: []*commonpb.HighlightData{
					{Fragments: []string{"row0"}},
					{Fragments: []string{"row1"}},
				},
			},
		},
	}

	t.Run("offset greater than end", func(t *testing.T) {
		_, err := parseHighlights(results, 2, 1)
		require.Error(t, err)
	})
	t.Run("negative offset", func(t *testing.T) {
		_, err := parseHighlights(results, -1, 1)
		require.Error(t, err)
	})
	t.Run("negative end", func(t *testing.T) {
		_, err := parseHighlights(results, 0, -1)
		require.Error(t, err)
	})
	t.Run("end exceeds datas length", func(t *testing.T) {
		_, err := parseHighlights(results, 0, 5)
		require.Error(t, err)
	})
}

func TestParseHighlightsPreservesFragmentsAndScores(t *testing.T) {
	d := &commonpb.HighlightData{Fragments: []string{"x"}, Scores: []float32{0.1}}
	results := &schemapb.SearchResultData{
		HighlightResults: []*commonpb.HighlightResult{
			{FieldName: "text", Datas: []*commonpb.HighlightData{d}},
		},
	}
	m, err := parseHighlights(results, 0, 1)
	require.NoError(t, err)
	// Mutate the source after parsing — must not affect the parsed value.
	d.Fragments[0] = "MUTATED"
	d.Scores[0] = 0.99
	require.Equal(t, []string{"x"}, m["text"][0].Fragments)
	require.Equal(t, []float32{0.1}, m["text"][0].Scores)
}

func TestParseHighlightsSkipsNilHighlightResult(t *testing.T) {
	results := &schemapb.SearchResultData{
		HighlightResults: []*commonpb.HighlightResult{
			nil,
			{FieldName: "text", Datas: []*commonpb.HighlightData{{Fragments: []string{"r0"}}}},
		},
	}
	m, err := parseHighlights(results, 0, 1)
	require.NoError(t, err)
	require.Len(t, m, 1)
	require.Equal(t, []string{"r0"}, m["text"][0].Fragments)
}

// --- Suite-based tests: ResultSet.Slice, handleSearchResult, iterator rejection.

type HighlighterResultSetSuite struct {
	ResultSetSuite
}

type HighlighterReadSuite struct {
	ReadSuite
}

type HighlighterSearchIteratorSuite struct {
	SearchIteratorSuite
}

func (s *HighlighterResultSetSuite) TestResultSetSliceKeepsHighlights() {
	// Build a ResultSet with IDs and Fields set so Slice derives ResultCount
	// from the columns (mirrors real search results), then assert Highlights
	// slices in lockstep.
	ids := column.NewColumnInt64("ID", []int64{1, 2, 3})
	rs := ResultSet{
		IDs:    ids,
		Fields: DataSet{ids},
		Scores: []float32{0.1, 0.2, 0.3},
		Highlights: map[string][]Highlight{
			"text": {
				{Fragments: []string{"a"}},
				{Fragments: []string{"b"}},
				{Fragments: []string{"c"}},
			},
			"title": {
				{Fragments: []string{"ta"}},
				{Fragments: []string{"tb"}},
				{Fragments: []string{"tc"}},
			},
		},
	}
	sliced := rs.Slice(0, 2)
	s.Require().Len(sliced.Highlights["text"], 2)
	s.Equal([]string{"a"}, sliced.Highlights["text"][0].Fragments)
	s.Equal([]string{"b"}, sliced.Highlights["text"][1].Fragments)
	s.Require().Len(sliced.Highlights["title"], 2)
	s.Equal([]string{"ta"}, sliced.Highlights["title"][0].Fragments)
}

func (s *HighlighterResultSetSuite) TestResultSetSliceNilHighlightsSafe() {
	ids := column.NewColumnInt64("ID", []int64{1})
	rs := ResultSet{
		IDs:    ids,
		Fields: DataSet{ids},
		Scores: []float32{0.1},
	}
	sliced := rs.Slice(0, 1)
	s.Nil(sliced.Highlights)
}

func (s *HighlighterResultSetSuite) TestResultSetSliceClampsHighlightsToResultCount() {
	// Server returned highlights for only 1 row but ResultCount=2.
	ids := column.NewColumnInt64("ID", []int64{1, 2})
	rs := ResultSet{
		IDs:    ids,
		Fields: DataSet{ids},
		Scores: []float32{0.1, 0.2},
		Highlights: map[string][]Highlight{
			"text": {{Fragments: []string{"only_one"}}},
		},
	}
	sliced := rs.Slice(0, 2)
	s.Require().Len(sliced.Highlights["text"], 1)
}

func (s *HighlighterReadSuite) TestHandleSearchResultPopulatesHighlights() {
	resp := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			Topks:      []int64{2},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2}},
				},
			},
			Scores: []float32{0.1, 0.2},
			FieldsData: []*schemapb.FieldData{
				s.getInt64FieldData("ID", []int64{1, 2}),
			},
			HighlightResults: []*commonpb.HighlightResult{
				{
					FieldName: "text",
					Datas: []*commonpb.HighlightData{
						{Fragments: []string{"<em>hello</em> r1"}, Scores: []float32{0.7}},
						{Fragments: []string{"r2 <em>hello</em>"}, Scores: []float32{0.6}},
					},
				},
			},
		},
	}

	resultSets, err := s.client.handleSearchResult(s.schema, []string{"text"}, 1, resp)
	s.Require().NoError(err)
	s.Require().Len(resultSets, 1)
	s.Require().Contains(resultSets[0].Highlights, "text")
	s.Require().Len(resultSets[0].Highlights["text"], 2)
	s.Equal([]string{"<em>hello</em> r1"}, resultSets[0].Highlights["text"][0].Fragments)
}

func (s *HighlighterReadSuite) TestHandleSearchResultSkipsHighlightsOnEmptyAggResult() {
	// Aggregation result with rc==0 → no highlights populated.
	resp := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			Topks:      []int64{0},
			AggTopks:   []int64{1},
			AggBuckets: []*schemapb.AggBucket{
				{Key: []*schemapb.BucketKeyEntry{
					{FieldName: "brand", Value: &schemapb.BucketKeyEntry_StringVal{StringVal: "acme"}},
				}, Count: 3},
			},
		},
	}

	resultSets, err := s.client.handleSearchResult(s.schema, []string{"brand"}, 1, resp)
	s.Require().NoError(err)
	s.Require().Len(resultSets, 1)
	s.Equal(0, resultSets[0].ResultCount)
	s.Nil(resultSets[0].Highlights)
}

func (s *HighlighterReadSuite) TestHandleSearchResultMultiNqSlicesHighlightsPerRow() {
	resp := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			NumQueries: 2,
			Topks:      []int64{1, 1},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: []int64{1, 2}},
				},
			},
			Scores: []float32{0.1, 0.2},
			FieldsData: []*schemapb.FieldData{
				s.getInt64FieldData("ID", []int64{1, 2}),
			},
			HighlightResults: []*commonpb.HighlightResult{
				{
					FieldName: "text",
					Datas: []*commonpb.HighlightData{
						{Fragments: []string{"row_for_nq0"}},
						{Fragments: []string{"row_for_nq1"}},
					},
				},
			},
		},
	}

	resultSets, err := s.client.handleSearchResult(s.schema, []string{"text"}, 2, resp)
	s.Require().NoError(err)
	s.Require().Len(resultSets, 2)
	s.Equal([]string{"row_for_nq0"}, resultSets[0].Highlights["text"][0].Fragments)
	s.Equal([]string{"row_for_nq1"}, resultSets[1].Highlights["text"][0].Fragments)
}

func (s *HighlighterSearchIteratorSuite) TestSearchIteratorRejectsHighlighter() {
	opt := NewSearchIteratorOption("coll", entity.FloatVector([]float32{0.1, 0.2}))
	opt.WithHighlighter(NewLexicalHighlighter().WithQuery("text", "hi", "TextMatch"))

	_, err := s.client.SearchIterator(context.Background(), opt)
	s.Require().Error(err)
	s.Contains(err.Error(), "highlighter is not supported with search iterator")
}

func TestHighlighterResults(t *testing.T) {
	suite.Run(t, new(HighlighterResultSetSuite))
	suite.Run(t, new(HighlighterReadSuite))
	suite.Run(t, new(HighlighterSearchIteratorSuite))
}
