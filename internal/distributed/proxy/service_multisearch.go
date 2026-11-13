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

package grpcproxy

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/dql"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type multiSearchReceivedAtKey struct{}

type multiSearchRunner interface {
	RunMultiSearch(context.Context, proxy.MultiSearchOptions, []proxy.MultiSearchPrepare, proxy.MultiSearchExecute) ([]*milvuspb.SearchResults, error)
}

var _ multiSearchRunner = (*proxy.Proxy)(nil)

func (s *Server) MultiSearch(ctx context.Context, req *milvuspb.MultiSearchRequest) (*milvuspb.MultiSearchResponse, error) {
	fail := func(err error) (*milvuspb.MultiSearchResponse, error) {
		return &milvuspb.MultiSearchResponse{Status: merr.Status(err)}, nil
	}
	requests := req.GetRequests()
	if len(requests) == 0 {
		return fail(merr.WrapErrParameterMissingMsg("MultiSearch requires at least one search"))
	}
	receivedAt, ok := ctx.Value(multiSearchReceivedAtKey{}).(time.Time)
	if !ok {
		receivedAt = time.Now()
	}
	deadline, err := proxy.MultiSearchExecutionDeadline(receivedAt, req.TimeoutMs)
	if err != nil {
		return fail(err)
	}
	items := make([]proxy.MultiSearchPrepare, len(requests))
	for i, item := range requests {
		search := item.GetSearch()
		if search == nil {
			return fail(merr.WrapErrParameterInvalidMsg("requests[%d] requires a search", i))
		}
		items[i] = func(ctx context.Context) (*milvuspb.SearchRequest, error) {
			return prepareMultiSearchRequest(ctx, search)
		}
	}
	runner, ok := s.proxy.(multiSearchRunner)
	if !ok || s.multiSearchExecute == nil {
		return fail(merr.Wrap(merr.ErrServiceUnimplemented, "MultiSearch is unavailable"))
	}
	options := proxy.MultiSearchOptions{MaxConcurrency: req.GetMaxConcurrency(), ExecutionDeadline: deadline}
	results, err := runner.RunMultiSearch(ctx, options, items, s.multiSearchExecute)
	if err != nil {
		if parentErr := ctx.Err(); parentErr != nil {
			return nil, status.FromContextError(parentErr).Err()
		}
		return fail(err)
	}
	response := &milvuspb.MultiSearchResponse{
		Status:  merr.Success(),
		Results: make([]*milvuspb.MultiSearchSubResult, len(results)),
	}
	for i, result := range results {
		response.Results[i] = &milvuspb.MultiSearchSubResult{Result: &milvuspb.MultiSearchSubResult_Search{Search: result}}
	}
	return response, nil
}

func prepareMultiSearchRequest(ctx context.Context, search *milvuspb.SearchRequest) (*milvuspb.SearchRequest, error) {
	request := proto.CloneOf(search)
	request.DbName = proxy.GetCurDBNameFromRequestOrContext(ctx, request)
	if err := dql.NormalizeMultiSearchNQ(request); err != nil {
		return nil, err
	}
	return request, nil
}

func newMultiSearchExecutor(component types.ProxyComponent, entry grpc.UnaryServerInterceptor) proxy.MultiSearchExecute {
	info := &grpc.UnaryServerInfo{Server: component, FullMethod: milvuspb.MilvusService_Search_FullMethodName}
	search := func(ctx context.Context, req any) (any, error) {
		return component.Search(ctx, req.(*milvuspb.SearchRequest))
	}
	return func(ctx context.Context, req *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		ctx = contextutil.SetToIncomingContext(ctx, util.HeaderDBName, req.GetDbName())
		response, err := entry(ctx, req, info, search)
		if err != nil {
			return nil, err
		}
		result, ok := response.(*milvuspb.SearchResults)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("Search returned an invalid response")
		}
		return result, nil
	}
}
