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
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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

type ExecuteDQLReqV2 struct {
	DbName         string               `json:"dbName"`
	Requests       []stdjson.RawMessage `json:"requests"`
	MaxConcurrency uint32               `json:"maxConcurrency"`
	TimeoutMs      *int64               `json:"timeoutMs"`

	headerTimeout *time.Duration
}

func (req *ExecuteDQLReqV2) GetDbName() string { return req.DbName }

type executeDQLProxy interface {
	CheckExecuteDQLAdmission() error
	RunExecuteDQL(context.Context, proxy.ExecuteDQLOptions, []proxy.ExecuteDQLPrepare, proxy.ExecuteDQLExecute) ([]*milvuspb.DQLResult, error)
}

var _ executeDQLProxy = (*proxy.Proxy)(nil)

func bindExecuteDQLRequest(reader io.Reader, req *ExecuteDQLReqV2) error {
	body, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return io.EOF
	}
	return stdjson.Unmarshal(body, req)
}

func (h *HandlersV2) executeDQLRoute() gin.HandlerFunc {
	respond := newDQLResponseMiddleware(dqlExplicitTransportTimeout)
	handle := wrapperTraceLog(h.executeDQL)
	handleWithMetrics := restfulSizeMiddleware(func(c *gin.Context) {
		metrics.GetStats(c.Request.Context()).SetNodeID(paramtable.GetNodeID()).SetInboundLabel("ExecuteDQL")
		timeout, err := executeDQLHeaderTimeout(c.Request.Header)
		if err != nil {
			respond(c, func(c *gin.Context) {
				HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
			}, time.Time{})
			return
		}
		// Only an explicit body budget makes the header a transport deadline.
		// Read the body once before choosing the role of the header.
		req := &ExecuteDQLReqV2{headerTimeout: timeout}
		bindErr := bindExecuteDQLRequest(c.Request.Body, req)
		var transportDeadline time.Time
		if timeout != nil && req.TimeoutMs != nil {
			state := c.Request.Context().Value(dqlStreamKey{}).(*dqlStreamState)
			transportDeadline = state.receivedAt.Add(*timeout)
		}
		respond(c, func(c *gin.Context) {
			if bindErr != nil {
				abortRESTPostBinding(c, bindErr)
				return
			}
			handleBoundRESTPost(c, req, handle)
		}, transportDeadline)
	}, true)
	return func(c *gin.Context) {
		if p, ok := h.proxy.(executeDQLProxy); ok {
			if err := p.CheckExecuteDQLAdmission(); err != nil {
				// Admission still takes precedence over an invalid header.
				respond(c, func(c *gin.Context) { rejectQueueFullBeforeDecode(c, err) }, time.Time{})
				return
			}
		}
		handleWithMetrics(c)
	}
}

func executeDQLHeaderTimeout(header http.Header) (*time.Duration, error) {
	values := header.Values(mhttp.HTTPHeaderRequestTimeout)
	if len(values) == 0 {
		return nil, nil
	}
	const maxSeconds = math.MaxInt64 / int64(time.Second)
	seconds, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil || seconds < 0 || seconds > maxSeconds {
		return nil, merr.WrapErrParameterInvalidMsg("%s must be an integer between 0 and %d seconds, got %q",
			mhttp.HTTPHeaderRequestTimeout, maxSeconds, values[0])
	}
	timeout := time.Duration(seconds) * time.Second
	return &timeout, nil
}

func (h *HandlersV2) executeDQL(ctx context.Context, c *gin.Context, anyReq any, dbName string) (any, error) {
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
	p, ok := h.proxy.(executeDQLProxy)
	if !ok {
		return fail(merr.Wrap(merr.ErrServiceUnimplemented, "ExecuteDQL is unavailable"))
	}
	req := anyReq.(*ExecuteDQLReqV2)
	if len(req.Requests) == 0 {
		return fail(merr.WrapErrParameterMissingMsg("ExecuteDQL requires at least one operation"))
	}
	if err := proxy.CheckExecuteDQLBatchSize(len(req.Requests)); err != nil {
		return fail(err)
	}
	state := ctx.Value(dqlStreamKey{}).(*dqlStreamState)
	deadline, err := proxy.ExecuteDQLExecutionDeadline(state.receivedAt, req.TimeoutMs)
	if err != nil {
		return fail(err)
	}
	if req.TimeoutMs == nil {
		timeout := paramtable.Get().HTTPCfg.RequestTimeoutMs.GetAsDuration(time.Millisecond)
		if req.headerTimeout != nil {
			timeout = *req.headerTimeout
		}
		deadline = state.receivedAt.Add(timeout)
	}
	vectorEncoding, err := parseFloatVectorEncoding(c.Request.Header)
	if err != nil {
		return fail(err)
	}
	allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
	token := c.GetString(ContextToken)

	preparers := make([]proxy.ExecuteDQLPrepare, len(req.Requests))
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
			request, schema, err := h.prepareExecuteDQLItem(ctx, search, dbName)
			schemas[i] = schema
			return request, err
		}
	}

	options := proxy.ExecuteDQLOptions{MaxConcurrency: req.MaxConcurrency, ExecutionDeadline: deadline}
	results, err := p.RunExecuteDQL(ctx, options, preparers, func(ctx context.Context, search *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
		db, collection := search.GetDbName(), search.GetCollectionName()
		ctx, requestMetrics := proxy.WithRequestMetrics(ctx)
		result, err := h.executeExecuteDQLItem(ctx, search, username, token)
		requestMetrics.Record("Search", db, collection, result, err)
		return result, err
	})
	if err != nil {
		return fail(err)
	}
	data := make([]gin.H, len(results))
	for i, result := range results {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Publishing a successful result also publishes its preparer's
		// schema. Failed slots may still have a preparer unwinding.
		if merr.Ok(result.GetStatus()) && result.GetSearch() != nil {
			data[i] = renderExecuteDQLSearch(ctx, result.GetSearch(), allowJS, schemas[i], vectorEncoding)
		} else {
			data[i] = renderExecuteDQLItemFailure(result.GetStatus())
		}
	}
	HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: data})
	return merr.Success(), nil
}

func renderExecuteDQLSearch(ctx context.Context, result *milvuspb.SearchResults, allowJS bool, schema *schemapb.CollectionSchema, encoding floatVectorEncoding) gin.H {
	search, _ := buildRESTSearchResponse(ctx, result, allowJS, schema, encoding)
	if search[HTTPReturnCode] != merr.Code(nil) {
		return gin.H{HTTPReturnCode: search[HTTPReturnCode], HTTPReturnMessage: search[HTTPReturnMessage]}
	}
	return gin.H{HTTPReturnCode: merr.Code(nil), "search": search}
}

func renderExecuteDQLItemFailure(status *commonpb.Status) gin.H {
	if merr.Ok(status) {
		status = merr.Status(merr.WrapErrServiceInternalMsg("ExecuteDQL succeeded without a Search result"))
	}
	return gin.H{HTTPReturnCode: merr.Code(merr.Error(status)), HTTPReturnMessage: status.GetReason()}
}

func (h *HandlersV2) prepareExecuteDQLItem(ctx context.Context, body []byte, fallbackDB string) (*milvuspb.SearchRequest, *schemapb.CollectionSchema, error) {
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
	if err := dql.NormalizeExecuteDQLNQ(req); err != nil {
		return nil, nil, err
	}
	return req, schema.CollectionSchema, nil
}

func (h *HandlersV2) executeExecuteDQLItem(ctx context.Context, req *milvuspb.SearchRequest, username, token string) (result *milvuspb.SearchResults, err error) {
	const searchMethod = milvuspb.MilvusService_Search_FullMethodName
	ctx = contextutil.SetToIncomingContext(ctx, util.HeaderDBName, req.GetDbName())
	// Each attempt owns the consistency, request, and outcome of its Search log.
	// Start before authorization and quota so their rejections are logged too.
	accessInfo := info.NewGrpcAccessInfo(ctx, &grpc.UnaryServerInfo{FullMethod: searchMethod}, req)
	ctx = context.WithValue(ctx, accesslog.AccessKey{}, accessInfo)
	defer func() {
		logErr := err
		if result == nil && logErr == nil {
			// The item goroutine handles the panic after this defer.
			logErr = merr.ErrServiceInternal
		}
		accessInfo.SetResult(result, logErr)
		accesslog.Write(accessInfo)
	}()
	if h.checkAuth {
		ctx, err = proxy.PrivilegeInterceptorWithMetaCache(h.metaCache)(ctx, req)
		if err != nil {
			hookutil.GetExtension().ReportAction(ctx, req, WrapErrorToResponse(err), nil, searchMethod, hookutil.ActionAuthorize)
			return nil, err
		}
	}
	if err := h.checkExecuteDQLQuota(ctx, req); err != nil {
		hookutil.GetExtension().ReportAction(ctx, req, WrapErrorToResponse(err), nil, searchMethod, hookutil.ActionAuthorize)
		return nil, err
	}
	response, err := proxy.HookInterceptor(ctx, req, username, searchMethod, func(ctx context.Context, req any) (any, error) {
		return forwardRESTProxy(ctx, req, token, searchMethod, func(ctx context.Context, req any) (any, error) {
			return h.proxy.Search(ctx, req.(*milvuspb.SearchRequest))
		})
	})
	if err != nil {
		return nil, err
	}
	result, ok := response.(*milvuspb.SearchResults)
	if !ok || result == nil {
		return nil, merr.WrapErrServiceInternalMsg("Search returned an invalid response")
	}
	return result, nil
}

func (h *HandlersV2) checkExecuteDQLQuota(ctx context.Context, req *milvuspb.SearchRequest) error {
	if !paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.GetAsBool() {
		return nil
	}
	dbID, parts, rateType, nq, err := proxy.GetExecuteDQLQuotaInfo(ctx, h.metaCache(), req)
	if err != nil {
		return err
	}
	_, err = checkQuota(ctx, req, h.proxy, dbID, parts, rateType, nq)
	return err
}
