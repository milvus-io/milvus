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
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// ClusterIDKey is the search/query/rank param key that pins a DQL request to a
// target cluster in a global-cluster deployment. It mirrors pymilvus's
// CLUSTER_ID constant ("cluster_id").
const ClusterIDKey = "cluster_id"

// ErrSessionClosed is returned when a DQL operation is invoked on a session
// that has already been closed.
var ErrSessionClosed = errors.New("session is closed")

// Session is a lightweight DQL-only view of a MilvusClient pinned to a target
// cluster id. Every DQL request issued through the session carries the cluster
// id in the request params (search_params / query_params / rank_params) so a
// global-cluster gateway can route it to the target cluster.
//
// A Session never owns the underlying connection: it delegates every call to
// the parent *Client, so it naturally follows the parent's reconnect behavior.
// Close marks only the session unusable — it does not close the parent.
// Closing the parent makes the session unusable as well.
type Session struct {
	parent    *Client
	clusterID string

	mu     sync.RWMutex
	closed bool
}

// Session creates a lightweight DQL session pinned to clusterID. All DQL
// operations (Search, HybridSearch, Query, Get, QueryIterator, SearchIterator)
// issued through the returned session carry the cluster id on the wire.
func (c *Client) Session(clusterID string) *Session {
	return &Session{parent: c, clusterID: clusterID}
}

// Close marks the session closed. Closed sessions reject new DQL calls with
// ErrSessionClosed but do not cancel or wait for in-flight RPCs, and do not
// close the parent client.
func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

func (s *Session) ensureOpen() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrSessionClosed
	}
	return nil
}

// Search searches the pinned cluster, delegating to the parent client with the
// cluster id injected into the search params.
func (s *Session) Search(ctx context.Context, option SearchOption, callOptions ...grpc.CallOption) ([]ResultSet, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	if option == nil {
		return nil, errors.New("session search: option must not be nil")
	}
	return s.parent.Search(ctx, &searchOptionWithClusterID{base: option, clusterID: s.clusterID}, callOptions...)
}

// HybridSearch performs a hybrid search on the pinned cluster, delegating to
// the parent client with the cluster id injected into the rank params.
func (s *Session) HybridSearch(ctx context.Context, option HybridSearchOption, callOptions ...grpc.CallOption) ([]ResultSet, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	if option == nil {
		return nil, errors.New("session hybrid search: option must not be nil")
	}
	return s.parent.HybridSearch(ctx, &hybridSearchOptionWithClusterID{base: option, clusterID: s.clusterID}, callOptions...)
}

// Query queries the pinned cluster, delegating to the parent client with the
// cluster id injected into the query params.
func (s *Session) Query(ctx context.Context, option QueryOption, callOptions ...grpc.CallOption) (ResultSet, error) {
	if err := s.ensureOpen(); err != nil {
		return ResultSet{}, err
	}
	if option == nil {
		return ResultSet{}, errors.New("session query: option must not be nil")
	}
	return s.parent.Query(ctx, &queryOptionWithClusterID{base: option, clusterID: s.clusterID}, callOptions...)
}

// Get fetches rows by primary key on the pinned cluster, delegating to the
// parent client with the cluster id injected into the query params.
func (s *Session) Get(ctx context.Context, option QueryOption, callOptions ...grpc.CallOption) (ResultSet, error) {
	if err := s.ensureOpen(); err != nil {
		return ResultSet{}, err
	}
	if option == nil {
		return ResultSet{}, errors.New("session get: option must not be nil")
	}
	return s.parent.Get(ctx, &queryOptionWithClusterID{base: option, clusterID: s.clusterID}, callOptions...)
}

// QueryIterator iterates query results on the pinned cluster. Every page
// request carries the cluster id in the query params.
func (s *Session) QueryIterator(ctx context.Context, option QueryIteratorOption, callOptions ...grpc.CallOption) (QueryIterator, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	if option == nil {
		return nil, errors.New("session query iterator: option must not be nil")
	}
	return s.parent.QueryIterator(ctx, &queryIteratorOptionWithClusterID{base: option, clusterID: s.clusterID}, callOptions...)
}

// SearchIterator iterates search results on the pinned cluster. Every page
// request carries the cluster id in the search params.
func (s *Session) SearchIterator(ctx context.Context, option SearchIteratorOption, callOptions ...grpc.CallOption) (SearchIterator, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, err
	}
	if option == nil {
		return nil, errors.New("session search iterator: option must not be nil")
	}
	opt, err := cloneSearchIteratorOption(option, s.clusterID)
	if err != nil {
		return nil, err
	}
	return s.parent.SearchIterator(ctx, opt, callOptions...)
}

// searchOptionWithClusterID wraps a SearchOption so the built request carries
// the session's cluster id in SearchParams. The base option and the request it
// returns are never mutated: the request is cloned before the cluster id is
// injected, so a custom SearchOption that caches and returns the same
// *SearchRequest on every call never leaks the session's cluster id.
type searchOptionWithClusterID struct {
	base      SearchOption
	clusterID string
}

func (w *searchOptionWithClusterID) Request() (*milvuspb.SearchRequest, error) {
	req, err := w.base.Request()
	if err != nil {
		return nil, err
	}
	req = proto.Clone(req).(*milvuspb.SearchRequest)
	req.SearchParams = replaceClusterID(req.SearchParams, w.clusterID)
	return req, nil
}

// queryOptionWithClusterID wraps a QueryOption so the built request carries
// the session's cluster id in QueryParams. The base option and the request it
// returns are never mutated: the request is cloned before the cluster id is
// injected, so a custom QueryOption that caches and returns the same
// *QueryRequest on every call never leaks the session's cluster id.
type queryOptionWithClusterID struct {
	base      QueryOption
	clusterID string
}

func (w *queryOptionWithClusterID) Request() (*milvuspb.QueryRequest, error) {
	req, err := w.base.Request()
	if err != nil {
		return nil, err
	}
	req = proto.Clone(req).(*milvuspb.QueryRequest)
	req.QueryParams = replaceClusterID(req.QueryParams, w.clusterID)
	return req, nil
}

// hybridSearchOptionWithClusterID wraps a HybridSearchOption so the built
// request carries the session's cluster id in RankParams. The base option and
// the request it returns are never mutated: the request is cloned before the
// cluster id is injected, so a custom HybridSearchOption that caches and
// returns the same *HybridSearchRequest on every call never leaks the
// session's cluster id.
type hybridSearchOptionWithClusterID struct {
	base      HybridSearchOption
	clusterID string
}

func (w *hybridSearchOptionWithClusterID) HybridRequest() (*milvuspb.HybridSearchRequest, error) {
	req, err := w.base.HybridRequest()
	if err != nil {
		return nil, err
	}
	req = proto.Clone(req).(*milvuspb.HybridSearchRequest)
	req.RankParams = replaceClusterID(req.RankParams, w.clusterID)
	return req, nil
}

// queryIteratorOptionWithClusterID wraps a QueryIteratorOption so every page
// request built while iterating carries the session's cluster id in
// QueryParams. The base option and the request it returns are never mutated:
// the request is cloned before the cluster id is injected, so a custom
// QueryIteratorOption that caches and returns the same *QueryRequest on every
// page never leaks the session's cluster id.
type queryIteratorOptionWithClusterID struct {
	base      QueryIteratorOption
	clusterID string
}

func (w *queryIteratorOptionWithClusterID) Request() (*milvuspb.QueryRequest, error) {
	req, err := w.base.Request()
	if err != nil {
		return nil, err
	}
	req = proto.Clone(req).(*milvuspb.QueryRequest)
	req.QueryParams = replaceClusterID(req.QueryParams, w.clusterID)
	return req, nil
}

func (w *queryIteratorOptionWithClusterID) BatchSize() int {
	return w.base.BatchSize()
}

func (w *queryIteratorOptionWithClusterID) Limit() int64 {
	return w.base.Limit()
}

func (w *queryIteratorOptionWithClusterID) ValidateParams() error {
	return w.base.ValidateParams()
}

// cloneSearchIteratorOption returns a deep copy of a *searchIteratorOption with
// the session's cluster id pinned into the search params.
//
// A search iterator mutates its option across pages (collection id, iterator
// token, batch size), so the copy keeps those mutations off the caller's
// option while behaving exactly like the original: SearchOption() re-applies
// the batch size on every page and the iterator token persists between pages.
func cloneSearchIteratorOption(option SearchIteratorOption, clusterID string) (SearchIteratorOption, error) {
	base, ok := option.(*searchIteratorOption)
	if !ok {
		return nil, errors.New("session search iterator: unsupported SearchIteratorOption implementation")
	}
	clone := *base
	if base.searchOption == nil {
		return &clone, nil
	}
	so := *base.searchOption
	clone.searchOption = &so
	if base.searchOption.annRequest != nil {
		ann := *base.searchOption.annRequest
		ann.searchParam = make(map[string]string, len(base.searchOption.annRequest.searchParam)+1)
		for k, v := range base.searchOption.annRequest.searchParam {
			ann.searchParam[k] = v
		}
		ann.searchParam[ClusterIDKey] = clusterID
		so.annRequest = &ann
	}
	return &clone, nil
}

// replaceClusterID replaces any existing cluster_id entry in kvs with the
// session's cluster id, appending it if absent. The session pins the cluster,
// so a caller-supplied cluster_id on the same request is overridden, mirroring
// pymilvus's MilvusClientSession, which hard-sets cluster_id per call.
func replaceClusterID(kvs []*commonpb.KeyValuePair, clusterID string) []*commonpb.KeyValuePair {
	var result []*commonpb.KeyValuePair
	for _, kv := range kvs {
		if kv.GetKey() == ClusterIDKey {
			continue
		}
		result = append(result, kv)
	}
	return append(result, &commonpb.KeyValuePair{Key: ClusterIDKey, Value: clusterID})
}
