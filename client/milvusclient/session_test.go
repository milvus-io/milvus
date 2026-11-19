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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

type SessionSuite struct {
	MockSuiteBase

	schema *entity.Schema
}

func (s *SessionSuite) SetupSuite() {
	s.MockSuiteBase.SetupSuite()
	s.schema = entity.NewSchema().
		WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("Vector").WithDataType(entity.FieldTypeFloatVector).WithDim(128))
}

func (s *SessionSuite) randVector() entity.Vector {
	return entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 { return rand.Float32() }))
}

func (s *SessionSuite) searchResultsResponse(n int, token string, lastBound float32) *milvuspb.SearchResults {
	ids := lo.Map(lo.Range(n), func(_ int, i int) int64 { return int64(i + 1) })
	return &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: 1,
			TopK:       int64(n),
			FieldsData: []*schemapb.FieldData{s.getInt64FieldData("ID", ids)},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: ids},
				},
			},
			Scores:  make([]float32, n),
			Topks:   []int64{int64(n)},
			Recalls: []float32{0.9},
			SearchIteratorV2Results: &schemapb.SearchIteratorV2Results{
				Token:     token,
				LastBound: lastBound,
			},
		},
	}
}

func (s *SessionSuite) TestSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("injects cluster id without mutating caller option", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collectionName, s.schema)

		session := s.client.Session("cluster-a")

		opt := NewSearchOption(collectionName, 5, []entity.Vector{s.randVector()})

		// the caller's option alone carries no cluster id
		baseReq, err := opt.Request()
		s.Require().NoError(err)
		s.NotContains(entity.KvPairsMap(baseReq.GetSearchParams()), ClusterIDKey)

		var got *milvuspb.SearchRequest
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			got = sr
			return s.searchResultsResponse(5, "", 0), nil
		}).Once()

		_, err = session.Search(ctx, opt)
		s.Require().NoError(err)

		s.Require().NotNil(got)
		s.Equal("cluster-a", entity.KvPairsMap(got.GetSearchParams())[ClusterIDKey])

		// caller option still untouched
		baseReq2, err := opt.Request()
		s.Require().NoError(err)
		s.NotContains(entity.KvPairsMap(baseReq2.GetSearchParams()), ClusterIDKey)
	})
}

func (s *SessionSuite) TestQueryAndGet() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("query injects cluster id", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collectionName, s.schema)

		session := s.client.Session("cluster-b")

		var got *milvuspb.QueryRequest
		s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
			got = qr
			return &milvuspb.QueryResults{
				Status:       merr.Success(),
				FieldsData:   []*schemapb.FieldData{s.getInt64FieldData("ID", []int64{1})},
				OutputFields: []string{"ID"},
			}, nil
		}).Once()

		rs, err := session.Query(ctx, NewQueryOption(collectionName).WithFilter("ID > 0"))
		s.Require().NoError(err)
		s.Equal(1, rs.ResultCount)
		s.Require().NotNil(got)
		s.Equal("cluster-b", entity.KvPairsMap(got.GetQueryParams())[ClusterIDKey])
	})

	s.Run("get injects cluster id", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collectionName, s.schema)

		session := s.client.Session("cluster-c")

		var got *milvuspb.QueryRequest
		s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
			got = qr
			return &milvuspb.QueryResults{
				Status:       merr.Success(),
				FieldsData:   []*schemapb.FieldData{s.getInt64FieldData("ID", []int64{1})},
				OutputFields: []string{"ID"},
			}, nil
		}).Once()

		_, err := session.Get(ctx, NewQueryOption(collectionName).WithFilter("ID in [1]"))
		s.Require().NoError(err)
		s.Require().NotNil(got)
		s.Equal("cluster-c", entity.KvPairsMap(got.GetQueryParams())[ClusterIDKey])
	})
}

func (s *SessionSuite) TestHybridSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("hybrid search injects cluster id", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collectionName, s.schema)

		session := s.client.Session("cluster-d")

		var got *milvuspb.HybridSearchRequest
		s.mock.EXPECT().HybridSearch(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, hsr *milvuspb.HybridSearchRequest) (*milvuspb.SearchResults, error) {
			got = hsr
			return s.searchResultsResponse(5, "", 0), nil
		}).Once()

		_, err := session.HybridSearch(ctx, NewHybridSearchOption(collectionName, 5,
			NewAnnRequest("Vector", 5, s.randVector())))
		s.Require().NoError(err)
		s.Require().NotNil(got)
		s.Equal("cluster-d", entity.KvPairsMap(got.GetRankParams())[ClusterIDKey])
	})
}

func (s *SessionSuite) TestQueryIterator() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("query iterator injects cluster id on every page", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collectionName, s.schema)

		session := s.client.Session("cluster-e")

		var pageCount int
		var allPages []*milvuspb.QueryRequest
		s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
			pageCount++
			allPages = append(allPages, qr)
			if pageCount == 1 {
				return &milvuspb.QueryResults{
					Status:       merr.Success(),
					FieldsData:   []*schemapb.FieldData{s.getInt64FieldData("ID", []int64{1, 2})},
					OutputFields: []string{"ID"},
				}, nil
			}
			return &milvuspb.QueryResults{Status: merr.Success()}, nil
		}).Times(2)

		iter, err := session.QueryIterator(ctx, NewQueryIteratorOption(collectionName).WithBatchSize(2).WithFilter("ID >= 0"))
		s.Require().NoError(err)

		batch, err := iter.Next(ctx)
		s.Require().NoError(err)
		s.Equal(2, batch.ResultCount)

		_, err = iter.Next(ctx)
		s.Equal(io.EOF, err)

		s.Require().Len(allPages, 2)
		for _, req := range allPages {
			s.Equal("cluster-e", entity.KvPairsMap(req.GetQueryParams())[ClusterIDKey])
		}
	})
}

func (s *SessionSuite) TestSearchIterator() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("search iterator injects cluster id and does not mutate caller option", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupDescribeCollection(collectionName, s.schema)

		session := s.client.Session("cluster-f")

		opt := NewSearchIteratorOption(collectionName, s.randVector()).
			WithBatchSize(5).
			WithIteratorLimit(10)

		var searchCount int
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			searchCount++
			s.Equal("cluster-f", entity.KvPairsMap(sr.GetSearchParams())[ClusterIDKey])
			if searchCount == 1 { // probe call
				return s.searchResultsResponse(0, "probe-token", 0), nil
			}
			return s.searchResultsResponse(5, "tok-1", 1), nil
		}).Times(3)

		iter, err := session.SearchIterator(ctx, opt)
		s.Require().NoError(err)

		rs, err := iter.Next(ctx)
		s.Require().NoError(err)
		s.Equal(5, rs.ResultCount)

		rs, err = iter.Next(ctx)
		s.Require().NoError(err)
		s.Equal(5, rs.ResultCount)

		// caller's option was not mutated by the session iterator
		callerReq, err := opt.SearchOption().Request()
		s.Require().NoError(err)
		s.NotContains(entity.KvPairsMap(callerReq.GetSearchParams()), ClusterIDKey)
		s.NotContains(entity.KvPairsMap(callerReq.GetSearchParams()), IteratorSearchIDKey)
		s.NotContains(entity.KvPairsMap(callerReq.GetSearchParams()), CollectionIDKey)
	})
}

func (s *SessionSuite) TestSessionClosed() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("closed session rejects DQL calls", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		session := s.client.Session("cluster-z")
		session.Close()

		_, err := session.Search(ctx, NewSearchOption(collectionName, 5, []entity.Vector{s.randVector()}))
		s.ErrorIs(err, ErrSessionClosed)

		_, err = session.HybridSearch(ctx, NewHybridSearchOption(collectionName, 5, NewAnnRequest("Vector", 5, s.randVector())))
		s.ErrorIs(err, ErrSessionClosed)

		_, err = session.Query(ctx, NewQueryOption(collectionName).WithFilter("ID > 0"))
		s.ErrorIs(err, ErrSessionClosed)

		_, err = session.Get(ctx, NewQueryOption(collectionName).WithFilter("ID in [1]"))
		s.ErrorIs(err, ErrSessionClosed)

		_, err = session.QueryIterator(ctx, NewQueryIteratorOption(collectionName))
		s.ErrorIs(err, ErrSessionClosed)

		_, err = session.SearchIterator(ctx, NewSearchIteratorOption(collectionName, s.randVector()))
		s.ErrorIs(err, ErrSessionClosed)
	})

	s.Run("closing the parent makes the session unusable", func() {
		session := s.client.Session("cluster-z")
		s.client.Close(ctx)
		_, err := session.Search(ctx, NewSearchOption("coll", 5, []entity.Vector{s.randVector()}))
		s.Error(err)
	})
}

func (s *SessionSuite) TestSessionWithCachingOption() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("session search does not leak cluster id into cached option", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collectionName, s.schema)

		session := s.client.Session("cluster-a")

		// build a valid request once, then cache it in a custom option that
		// returns the same pointer on every call
		baseOpt := NewSearchOption(collectionName, 5, []entity.Vector{s.randVector()})
		cached, err := baseOpt.Request()
		s.Require().NoError(err)
		opt := &cachingSearchOption{req: cached}

		var wire []*milvuspb.SearchRequest
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			wire = append(wire, sr)
			return s.searchResultsResponse(0, "", 0), nil
		}).Times(2)

		// session search carries the cluster id and leaves the cached request clean
		_, err = session.Search(ctx, opt)
		s.Require().NoError(err)
		s.Require().Len(wire, 1)
		s.Equal("cluster-a", entity.KvPairsMap(wire[0].GetSearchParams())[ClusterIDKey])
		s.NotContains(entity.KvPairsMap(cached.GetSearchParams()), ClusterIDKey)

		// a plain client search with the same option is not routed to the cluster
		_, err = s.client.Search(ctx, opt)
		s.Require().NoError(err)
		s.Require().Len(wire, 2)
		s.NotContains(entity.KvPairsMap(wire[1].GetSearchParams()), ClusterIDKey)
	})
}

func TestSession(t *testing.T) {
	suite.Run(t, new(SessionSuite))
}

func TestReplaceClusterID(t *testing.T) {
	t.Run("appends when absent", func(t *testing.T) {
		got := replaceClusterID([]*commonpb.KeyValuePair{{Key: "topk", Value: "10"}}, "cluster-a")
		require.Len(t, got, 2)
		assert.Equal(t, "topk", got[0].GetKey())
		assert.Equal(t, ClusterIDKey, got[1].GetKey())
		assert.Equal(t, "cluster-a", got[1].GetValue())
	})

	t.Run("replaces existing entries", func(t *testing.T) {
		got := replaceClusterID([]*commonpb.KeyValuePair{
			{Key: ClusterIDKey, Value: "caller-cluster"},
			{Key: "topk", Value: "10"},
			{Key: ClusterIDKey, Value: "caller-cluster-2"},
		}, "session-cluster")
		require.Len(t, got, 2)
		assert.Equal(t, "topk", got[0].GetKey())
		assert.Equal(t, ClusterIDKey, got[1].GetKey())
		assert.Equal(t, "session-cluster", got[1].GetValue())
	})

	t.Run("leaves the original slice untouched", func(t *testing.T) {
		original := []*commonpb.KeyValuePair{{Key: ClusterIDKey, Value: "caller-cluster"}}
		_ = replaceClusterID(original, "session-cluster")
		assert.Len(t, original, 1)
		assert.Equal(t, "caller-cluster", original[0].GetValue())
	})
}

func TestCloneSearchIteratorOption(t *testing.T) {
	opt := NewSearchIteratorOption("coll", entity.FloatVector{1, 2, 3}).WithBatchSize(10)

	clonedIface, err := cloneSearchIteratorOption(opt, "cluster-z")
	require.NoError(t, err)

	cloned, ok := clonedIface.(*searchIteratorOption)
	require.True(t, ok)
	require.NotNil(t, cloned.searchOption)
	require.NotNil(t, cloned.searchOption.annRequest)
	assert.NotSame(t, opt.searchOption, cloned.searchOption)
	assert.NotSame(t, opt.searchOption.annRequest, cloned.searchOption.annRequest)

	req, err := cloned.SearchOption().Request()
	require.NoError(t, err)
	assert.Equal(t, "cluster-z", entity.KvPairsMap(req.GetSearchParams())[ClusterIDKey])

	// caller's option carries no cluster id
	callerReq, err := opt.SearchOption().Request()
	require.NoError(t, err)
	assert.NotContains(t, entity.KvPairsMap(callerReq.GetSearchParams()), ClusterIDKey)
}

func TestCloneSearchIteratorOptionUnsupported(t *testing.T) {
	_, err := cloneSearchIteratorOption(struct{ SearchIteratorOption }{}, "cluster-z")
	assert.Error(t, err)
}

// cachingSearchOption returns the same cached *SearchRequest on every call,
// simulating a custom SearchOption that caches its built request.
type cachingSearchOption struct {
	req *milvuspb.SearchRequest
}

func (o *cachingSearchOption) Request() (*milvuspb.SearchRequest, error) {
	return o.req, nil
}

// cachingQueryOption returns the same cached *QueryRequest on every call.
type cachingQueryOption struct {
	req *milvuspb.QueryRequest
}

func (o *cachingQueryOption) Request() (*milvuspb.QueryRequest, error) {
	return o.req, nil
}

// cachingHybridSearchOption returns the same cached *HybridSearchRequest on
// every call.
type cachingHybridSearchOption struct {
	req *milvuspb.HybridSearchRequest
}

func (o *cachingHybridSearchOption) HybridRequest() (*milvuspb.HybridSearchRequest, error) {
	return o.req, nil
}

// cachingQueryIteratorOption returns the same cached *QueryRequest on every
// call, including on every iterator page.
type cachingQueryIteratorOption struct {
	req *milvuspb.QueryRequest
}

func (o *cachingQueryIteratorOption) Request() (*milvuspb.QueryRequest, error) {
	return o.req, nil
}

func (o *cachingQueryIteratorOption) BatchSize() int {
	return 10
}

func (o *cachingQueryIteratorOption) Limit() int64 {
	return 100
}

func (o *cachingQueryIteratorOption) ValidateParams() error {
	return nil
}

// TestWrapperDoesNotPolluteCachedRequest guards against a custom option that
// caches and returns the same request pointer on every call: the session
// wrapper must clone the request before injecting the cluster id so the
// caller's cached request is never written back to (see review comment on
// PR #55675).
func TestWrapperDoesNotPolluteCachedRequest(t *testing.T) {
	const clusterID = "cluster-a"

	t.Run("search", func(t *testing.T) {
		cached := &milvuspb.SearchRequest{CollectionName: "coll"}
		w := &searchOptionWithClusterID{base: &cachingSearchOption{req: cached}, clusterID: clusterID}

		req, err := w.Request()
		require.NoError(t, err)
		assert.Equal(t, clusterID, entity.KvPairsMap(req.GetSearchParams())[ClusterIDKey])
		// the caller's cached request is untouched
		assert.NotContains(t, entity.KvPairsMap(cached.GetSearchParams()), ClusterIDKey)
	})

	t.Run("query", func(t *testing.T) {
		cached := &milvuspb.QueryRequest{CollectionName: "coll"}
		w := &queryOptionWithClusterID{base: &cachingQueryOption{req: cached}, clusterID: clusterID}

		req, err := w.Request()
		require.NoError(t, err)
		assert.Equal(t, clusterID, entity.KvPairsMap(req.GetQueryParams())[ClusterIDKey])
		assert.NotContains(t, entity.KvPairsMap(cached.GetQueryParams()), ClusterIDKey)
	})

	t.Run("hybrid search", func(t *testing.T) {
		cached := &milvuspb.HybridSearchRequest{CollectionName: "coll"}
		w := &hybridSearchOptionWithClusterID{base: &cachingHybridSearchOption{req: cached}, clusterID: clusterID}

		req, err := w.HybridRequest()
		require.NoError(t, err)
		assert.Equal(t, clusterID, entity.KvPairsMap(req.GetRankParams())[ClusterIDKey])
		assert.NotContains(t, entity.KvPairsMap(cached.GetRankParams()), ClusterIDKey)
	})

	t.Run("query iterator", func(t *testing.T) {
		cached := &milvuspb.QueryRequest{CollectionName: "coll"}
		w := &queryIteratorOptionWithClusterID{base: &cachingQueryIteratorOption{req: cached}, clusterID: clusterID}

		req, err := w.Request()
		require.NoError(t, err)
		assert.Equal(t, clusterID, entity.KvPairsMap(req.GetQueryParams())[ClusterIDKey])
		assert.NotContains(t, entity.KvPairsMap(cached.GetQueryParams()), ClusterIDKey)
	})
}
