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

package httpserver

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
	"github.com/milvus-io/milvus/internal/proxy/dql"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type MultiSearchReqV2 struct {
	Requests       []stdjson.RawMessage `json:"requests"`
	MaxConcurrency uint32               `json:"maxConcurrency"`
	TimeoutMs      *int64               `json:"timeoutMs"`
}

type multiSearchProxy interface {
	CheckMultiSearchAdmission() error
	RunMultiSearch(context.Context, proxy.MultiSearchOptions, []proxy.MultiSearchPrepare, proxy.MultiSearchExecute) ([]*milvuspb.SearchResults, error)
}

var _ multiSearchProxy = (*proxy.Proxy)(nil)

func bindMultiSearchRequest(reader io.Reader, req *MultiSearchReqV2) error {
	body, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return io.EOF
	}
	return stdjson.Unmarshal(body, req)
}

func (h *HandlersV2) multiSearchRoute(next gin.HandlerFunc) gin.HandlerFunc {
	respond := newDQLResponseMiddleware(dqlExplicitTransportTimeout)
	handleWithMetrics := restfulSizeMiddleware(func(c *gin.Context) {
		metrics.GetStats(c.Request.Context()).SetNodeID(paramtable.GetNodeID()).SetInboundLabel("MultiSearch")
		timeout, err := multiSearchTransportTimeout(c)
		handle := next
		if err != nil {
			handle = func(c *gin.Context) {
				HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
			}
		}
		respond(c, handle, timeout)
	}, true)
	return func(c *gin.Context) {
		if p, ok := h.proxy.(multiSearchProxy); ok {
			if err := p.CheckMultiSearchAdmission(); err != nil {
				// Admission still takes precedence over an invalid header.
				timeout, _ := multiSearchTransportTimeout(c)
				respond(c, func(c *gin.Context) { rejectQueueFullBeforeDecode(c, err) }, timeout)
				return
			}
		}
		handleWithMetrics(c)
	}
}

func multiSearchTransportTimeout(c *gin.Context) (*time.Duration, error) {
	header := c.Request.Header.Get(mhttp.HTTPHeaderRequestTimeout)
	if header == "" {
		return nil, nil
	}
	timeout, err := parseRequestTimeout(header)
	if err != nil {
		return nil, err
	}
	return &timeout, nil
}

func (h *HandlersV2) multiSearch(ctx context.Context, c *gin.Context, anyReq any, dbName string) (any, error) {
	fail := func(err error) (any, error) {
		if errors.Is(err, merr.ErrServiceTooManyRequests) {
			rejectQueueFull(c, err)
		} else {
			status := http.StatusOK
			if errors.Is(err, merr.ErrNeedAuthenticate) {
				status = http.StatusUnauthorized
			}
			HTTPAbortReturn(c, status, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
		}
		return nil, err
	}

	username := c.GetString(ContextUsername)
	if h.checkAuth && username == "" {
		return fail(merr.ErrNeedAuthenticate)
	}
	p, ok := h.proxy.(multiSearchProxy)
	if !ok {
		return fail(merr.Wrap(merr.ErrServiceUnimplemented, "MultiSearch is unavailable"))
	}
	req := anyReq.(*MultiSearchReqV2)
	if len(req.Requests) == 0 {
		return fail(merr.WrapErrParameterMissingMsg("MultiSearch requires at least one search"))
	}
	state := ctx.Value(dqlStreamKey{}).(*dqlStreamState)
	deadline, err := proxy.MultiSearchExecutionDeadline(state.receivedAt, req.TimeoutMs)
	if err != nil {
		return fail(err)
	}
	vectorEncoding, err := parseFloatVectorEncoding(c.Request.Header)
	if err != nil {
		return fail(err)
	}
	allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
	token := c.GetString(ContextToken)

	preparers := make([]proxy.MultiSearchPrepare, len(req.Requests))
	schemas := make([]*schemapb.CollectionSchema, len(req.Requests))
	for i, item := range req.Requests {
		var operation map[string]stdjson.RawMessage
		if err := stdjson.Unmarshal(item, &operation); err != nil {
			return fail(merr.WrapErrParameterInvalidMsg("requests[%d] must contain a search object", i))
		}
		search := bytes.TrimSpace(operation["search"])
		if len(operation) != 1 || len(search) == 0 || search[0] != '{' {
			return fail(merr.WrapErrParameterInvalidMsg("requests[%d] must contain a search object", i))
		}
		preparers[i] = func(ctx context.Context) (*milvuspb.SearchRequest, error) {
			request, schema, err := h.prepareMultiSearchItem(ctx, search, dbName)
			schemas[i] = schema
			return request, err
		}
	}

	options := proxy.MultiSearchOptions{MaxConcurrency: req.MaxConcurrency, ExecutionDeadline: deadline}
	results, err := p.RunMultiSearch(ctx, options, preparers, func(ctx context.Context, search *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		return h.executeMultiSearchItem(ctx, search, username, token)
	})
	if err != nil {
		return fail(err)
	}
	data := make([]gin.H, len(results))
	for i, result := range results {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		status := result.GetStatus()
		var search gin.H
		if merr.Ok(status) {
			// Publishing a successful result also publishes its preparer's
			// schema. An unfinished preparer only belongs to a failed slot.
			search, _ = buildRESTSearchResponse(ctx, result, allowJS, schemas[i], vectorEncoding)
		} else {
			search = gin.H{HTTPReturnCode: merr.Code(merr.Error(status)), HTTPReturnMessage: status.GetReason()}
		}
		data[i] = gin.H{"search": search}
	}
	HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: data})
	return merr.Success(), nil
}

func (h *HandlersV2) prepareMultiSearchItem(ctx context.Context, body []byte, fallbackDB string) (*milvuspb.SearchRequest, *schemapb.CollectionSchema, error) {
	httpReq := SearchReqV2{Limit: 100}
	if err := stdjson.Unmarshal(body, &httpReq); err != nil {
		return nil, nil, restSearchError{
			responseError: merr.ErrIncorrectParameterFormat,
			message:       merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
			cause:         err,
		}
	}
	if err := binding.Validator.ValidateStruct(&httpReq); err != nil {
		return nil, nil, restSearchError{
			responseError: merr.ErrMissingRequiredParameters,
			message:       merr.ErrMissingRequiredParameters.Error() + ", error: " + err.Error(),
			cause:         err,
		}
	}
	dbName := httpReq.GetDbName()
	if dbName == "" {
		dbName = fallbackDB
	}
	ctx = contextutil.SetToIncomingContext(ctx, util.HeaderDBName, dbName)
	req, err := newRESTSearchRequest(ctx, &httpReq, dbName)
	if err != nil {
		return nil, nil, err
	}
	schema, err := proxy.GetCachedCollectionSchema(ctx, h.metaCache(), dbName, httpReq.CollectionName)
	if err != nil {
		return nil, nil, err
	}
	if schema == nil || schema.CollectionSchema == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("Search collection metadata has no schema")
	}
	if err := prepareRESTSearch(ctx, req, &httpReq, schema.CollectionSchema, string(body)); err != nil {
		return nil, nil, err
	}
	if err := dql.NormalizeMultiSearchNQ(req); err != nil {
		return nil, nil, err
	}
	return req, schema.CollectionSchema, nil
}

func (h *HandlersV2) executeMultiSearchItem(ctx context.Context, req *milvuspb.SearchRequest, username, token string) (*milvuspb.SearchResults, error) {
	const searchMethod = milvuspb.MilvusService_Search_FullMethodName
	ctx = contextutil.SetToIncomingContext(ctx, util.HeaderDBName, req.GetDbName())
	if h.checkAuth {
		var err error
		ctx, err = proxy.PrivilegeInterceptorWithMetaCache(h.metaCache)(ctx, req)
		if err != nil {
			hookutil.GetExtension().ReportAction(ctx, req, WrapErrorToResponse(err), nil, searchMethod, hookutil.ActionAuthorize)
			return nil, err
		}
	}
	if err := h.checkMultiSearchQuota(ctx, req); err != nil {
		hookutil.GetExtension().ReportAction(ctx, req, WrapErrorToResponse(err), nil, searchMethod, hookutil.ActionAuthorize)
		return nil, err
	}
	// Search records its actual consistency level in the request's access info.
	// Give it its own object so concurrent searches don't update the batch log.
	accessInfo := info.NewGrpcAccessInfo(ctx, &grpc.UnaryServerInfo{FullMethod: searchMethod}, req)
	ctx = context.WithValue(ctx, accesslog.AccessKey{}, accessInfo)
	response, err := proxy.HookInterceptor(ctx, req, username, searchMethod, func(ctx context.Context, req any) (any, error) {
		return forwardRESTProxy(ctx, req, token, searchMethod, func(ctx context.Context, req any) (any, error) {
			return h.proxy.Search(ctx, req.(*milvuspb.SearchRequest))
		})
	})
	if err != nil {
		return nil, err
	}
	result, ok := response.(*milvuspb.SearchResults)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("Search returned an invalid response")
	}
	return result, nil
}

func (h *HandlersV2) checkMultiSearchQuota(ctx context.Context, req *milvuspb.SearchRequest) error {
	if !paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.GetAsBool() {
		return nil
	}
	dbID, parts, rateType, nq, err := proxy.GetMultiSearchQuotaInfo(ctx, h.metaCache(), req)
	if err != nil {
		return err
	}
	_, err = checkQuota(ctx, req, h.proxy, dbID, parts, rateType, nq)
	return err
}
