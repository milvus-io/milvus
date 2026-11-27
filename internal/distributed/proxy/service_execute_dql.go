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

type executeDQLReceivedAtKey struct{}

type executeDQLRunner interface {
	RunExecuteDQL(context.Context, proxy.ExecuteDQLOptions, []proxy.ExecuteDQLPrepare, proxy.ExecuteDQLExecute) ([]*milvuspb.DQLResult, error)
}

var _ executeDQLRunner = (*proxy.Proxy)(nil)

func (s *Server) ExecuteDQL(ctx context.Context, req *milvuspb.ExecuteDQLRequest) (*milvuspb.ExecuteDQLResponse, error) {
	fail := func(err error) (*milvuspb.ExecuteDQLResponse, error) {
		return &milvuspb.ExecuteDQLResponse{Status: merr.Status(err)}, nil
	}
	requests := req.GetRequests()
	if len(requests) == 0 {
		return fail(merr.WrapErrParameterMissingMsg("ExecuteDQL requires at least one operation"))
	}
	if err := proxy.CheckExecuteDQLBatchSize(len(requests)); err != nil {
		return fail(err)
	}
	receivedAt, ok := ctx.Value(executeDQLReceivedAtKey{}).(time.Time)
	if !ok {
		receivedAt = time.Now()
	}
	deadline, err := proxy.ExecuteDQLExecutionDeadline(receivedAt, req.TimeoutMs)
	if err != nil {
		return fail(err)
	}
	items := make([]proxy.ExecuteDQLPrepare, len(requests))
	for i, item := range requests {
		operation, ok := item.GetRequest().(*milvuspb.DQLRequest_Search)
		if !ok {
			return fail(merr.WrapErrParameterInvalidMsg("requests[%d] must contain a Search operation", i))
		}
		var search *milvuspb.SearchRequest
		if operation != nil {
			search = operation.Search
		}
		items[i] = func(ctx context.Context) (*milvuspb.SearchRequest, error) {
			return prepareExecuteDQLSearch(ctx, search)
		}
	}
	runner, ok := s.proxy.(executeDQLRunner)
	if !ok || s.executeDQLExecute == nil {
		return fail(merr.Wrap(merr.ErrServiceUnimplemented, "ExecuteDQL is unavailable"))
	}
	options := proxy.ExecuteDQLOptions{MaxConcurrency: req.GetMaxConcurrency(), ExecutionDeadline: deadline}
	results, err := runner.RunExecuteDQL(ctx, options, items, s.executeDQLExecute)
	if err != nil {
		if parentErr := ctx.Err(); parentErr != nil {
			return nil, status.FromContextError(parentErr).Err()
		}
		return fail(err)
	}
	return &milvuspb.ExecuteDQLResponse{
		Status:  merr.Success(),
		Results: results,
	}, nil
}

func prepareExecuteDQLSearch(ctx context.Context, search *milvuspb.SearchRequest) (*milvuspb.SearchRequest, error) {
	request := proto.CloneOf(search)
	if request == nil {
		// A selected Search with an empty payload is validated as an item.
		request = &milvuspb.SearchRequest{}
	}
	request.DbName = proxy.GetCurDBNameFromRequestOrContext(ctx, request)
	if err := dql.NormalizeExecuteDQLNQ(request); err != nil {
		return nil, err
	}
	return request, nil
}

func newExecuteDQLExecutor(component types.ProxyComponent, entry grpc.UnaryServerInterceptor) proxy.ExecuteDQLExecute {
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
