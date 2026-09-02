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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/iotest"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestErrToHTTPStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"nil", nil, http.StatusOK},
		{"need authenticate", merr.ErrNeedAuthenticate, http.StatusUnauthorized},
		{"not authenticated", merr.ErrPrivilegeNotAuthenticated, http.StatusUnauthorized},
		{"not permitted", merr.ErrPrivilegeNotPermitted, http.StatusForbidden},
		{"queue full", merr.ErrServiceTooManyRequests, http.StatusTooManyRequests},
		{"rate limit", merr.ErrServiceRateLimit, http.StatusTooManyRequests},
		{"quota exceeded", merr.ErrServiceQuotaExceeded, http.StatusTooManyRequests},
		{"http rate limit", merr.ErrHTTPRateLimit, http.StatusTooManyRequests},
		{"input sentinel", merr.ErrParameterInvalid, http.StatusBadRequest},
		{"boundary-marked input", merr.WrapErrAsInputError(merr.ErrCollectionNotFound), http.StatusBadRequest},
		{"collection not loaded: mixed provenance, neutral", merr.ErrCollectionNotLoaded, http.StatusInternalServerError},
		{"schema mismatch: transient race, neutralized", merr.ErrCollectionSchemaMismatch, http.StatusInternalServerError},
		{"retriable system", merr.ErrServiceUnavailable, http.StatusServiceUnavailable},
		{"non-retriable system", merr.ErrServiceInternal, http.StatusInternalServerError},
		{"non-merr error", assert.AnError, http.StatusInternalServerError},
		{"deadline exceeded", context.DeadlineExceeded, http.StatusInternalServerError},
		{"canceled without provenance", context.Canceled, http.StatusInternalServerError},
		{"raw grpc InvalidArgument (hook reject)", status.Error(codes.InvalidArgument, "x"), http.StatusBadRequest},
		{"raw grpc Unavailable (transport)", status.Error(codes.Unavailable, "x"), http.StatusServiceUnavailable},
		{"raw grpc DeadlineExceeded", status.Error(codes.DeadlineExceeded, "x"), http.StatusInternalServerError},
		{"raw grpc Canceled without provenance", status.Error(codes.Canceled, "x"), http.StatusInternalServerError},
		{"raw grpc PermissionDenied", status.Error(codes.PermissionDenied, "x"), http.StatusForbidden},
		{"raw grpc NotFound stays neutral 500", status.Error(codes.NotFound, "x"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, errToHTTPStatus(tc.err))
		})
	}
}

func TestErrorTypeForAccessLog(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want merr.ErrorType
	}{
		{"merr input", merr.ErrParameterInvalid, merr.InputError},
		{"merr system", merr.ErrServiceInternal, merr.SystemError},
		{"raw grpc InvalidArgument", status.Error(codes.InvalidArgument, "x"), merr.InputError},
		{"raw grpc Unauthenticated", status.Error(codes.Unauthenticated, "x"), merr.InputError},
		{"raw grpc PermissionDenied", status.Error(codes.PermissionDenied, "x"), merr.InputError},
		{"raw grpc Unavailable", status.Error(codes.Unavailable, "x"), merr.SystemError},
		{"raw grpc Canceled", status.Error(codes.Canceled, "x"), merr.SystemError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, errorTypeForAccessLog(tc.err))
		})
	}
}

// The classification must survive the status round-trip the REST layer
// actually receives: wrapperProxy rebuilds the error via merr.Error(status).
func TestErrToHTTPStatusSurvivesWire(t *testing.T) {
	roundTrip := func(err error) error { return merr.Error(merr.Status(err)) }

	assert.Equal(t, http.StatusBadRequest, errToHTTPStatus(roundTrip(merr.ErrParameterInvalid)))
	assert.Equal(t, http.StatusBadRequest, errToHTTPStatus(roundTrip(merr.WrapErrAsInputError(merr.ErrCollectionNotFound))))
	assert.Equal(t, http.StatusTooManyRequests, errToHTTPStatus(roundTrip(merr.ErrServiceRateLimit)))
	assert.Equal(t, http.StatusServiceUnavailable, errToHTTPStatus(roundTrip(merr.ErrServiceUnavailable)))

	// A legacy peer fills only the old ErrorCode: no Retriable, no input flag.
	// It must land on the non-retriable 500, never on a retriable class.
	legacy := merr.Error(&commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "legacy"})
	assert.Equal(t, http.StatusInternalServerError, errToHTTPStatus(legacy))
}

func TestProjectedStatusGate(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.StandardErrorStatus.Key

	// default: off, everything keeps the 200 envelope
	assert.Equal(t, http.StatusOK, projectedStatus(merr.ErrServiceRateLimit))
	assert.Equal(t, http.StatusOK, projectedStatus(merr.ErrParameterInvalid))

	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)
	assert.Equal(t, http.StatusTooManyRequests, projectedStatus(merr.ErrServiceRateLimit))
	assert.Equal(t, http.StatusBadRequest, projectedStatus(merr.ErrParameterInvalid))
	assert.Equal(t, http.StatusOK, projectedStatus(nil))
}

// 499 requires proof the caller went away; otherwise canceled stays 500.
func TestProjectedStatusForRequest(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.StandardErrorStatus.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	newGinCtx := func(canceled bool) *gin.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		req := httptest.NewRequest(http.MethodPost, "/x", nil)
		if canceled {
			reqCtx, cancel := context.WithCancel(req.Context())
			cancel()
			req = req.WithContext(reqCtx)
		}
		c.Request = req
		return c
	}
	assert.Equal(t, statusClientClosedRequest, projectedStatusForRequest(newGinCtx(true), context.Canceled, "Search"))
	assert.Equal(t, statusClientClosedRequest, projectedStatusForRequest(newGinCtx(true), status.Error(codes.Canceled, "x"), "Search"))
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForRequest(newGinCtx(false), context.Canceled, "Search"))
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForRequest(newGinCtx(false), status.Error(codes.Canceled, "x"), "Search"))
	// non-canceled statuses pass through untouched
	assert.Equal(t, http.StatusServiceUnavailable, projectedStatusForRequest(newGinCtx(true), merr.ErrServiceUnavailable, "Search"))

	// A deadline is server-side once the request body has arrived, regardless
	// of whether the error is a Go context or a raw gRPC status.
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForRequest(newGinCtx(false), context.DeadlineExceeded, "Search"))
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForRequest(newGinCtx(true), status.Error(codes.DeadlineExceeded, "x"), "Search"))
}

func TestProjectedStatusForMethodTimeout(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.StandardErrorStatus.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	// Every server-side timeout is 500, independently of replay safety.
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForMethod(context.DeadlineExceeded, "Insert"))
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForMethod(context.DeadlineExceeded, "PinSnapshotData"))
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForMethod(context.DeadlineExceeded, "Search"))
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForMethod(status.Error(codes.DeadlineExceeded, "x"), "Search"))
	// 503 unsafe downgrade unchanged
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForMethod(merr.ErrServiceUnavailable, "PinSnapshotData"))
}

// The middleware timer branch separates incomplete request upload from every
// timeout after body receipt.
func TestMiddlewareTimeoutStatus(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.StandardErrorStatus.Key

	// Gate off preserves the pre-existing 408 regardless of body progress.
	assert.Equal(t, http.StatusRequestTimeout, middlewareTimeoutStatus(true))
	assert.Equal(t, http.StatusRequestTimeout, middlewareTimeoutStatus(false))

	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)
	assert.Equal(t, http.StatusRequestTimeout, middlewareTimeoutStatus(false))
	assert.Equal(t, http.StatusInternalServerError, middlewareTimeoutStatus(true))

	assert.False(t, isReplaySafe("Insert"))
	assert.False(t, isReplaySafe("Upsert"))
	assert.False(t, isReplaySafe("Import"))
	// job-creating endpoints that return a server-generated handle
	assert.False(t, isReplaySafe("ManualCompaction"))
	assert.False(t, isReplaySafe("RestoreSnapshot"))
	assert.False(t, isReplaySafe("RestoreExternalSnapshot"))
	assert.False(t, isReplaySafe("ExportSnapshot"))
	assert.False(t, isReplaySafe("RefreshExternalCollection"))
	// count-delta transfer: replay moves another NumReplica batch
	assert.False(t, isReplaySafe("TransferMaster"))
	assert.False(t, isReplaySafe("AlterCollectionFunction"))
	// unknown method is unsafe by default (fail-safe)
	assert.False(t, isReplaySafe("BrandNewMethod"))
	// control-plane DDL is policy-excluded even though idempotent
	assert.False(t, isReplaySafe("CreateCollection"))
	assert.False(t, isReplaySafe("CreateIndex"))
	assert.False(t, isReplaySafe("OperatePrivilege"))
	// reads stay safe; every mutation (incl. load/commit) is unsafe by policy
	assert.True(t, isReplaySafe("Search"))
	assert.True(t, isReplaySafe("Query"))
	assert.False(t, isReplaySafe("Delete"))
	assert.False(t, isReplaySafe("Flush"))
	assert.False(t, isReplaySafe("LoadCollection"))
	assert.False(t, isReplaySafe("CommitImport"))
	assert.False(t, isReplaySafe("AbortImport"))
}

// wrapperPost must flip the recorder's bodyReceived flag once the full body is
// read, even if JSON decoding then fails. Engine.HandleContext resets c.Writer,
// so the handler is invoked directly
// with the recorder installed as the Writer, matching what timeoutMiddleware
// hands the handler goroutine.
func TestWrapperPostSetsBodyReceived(t *testing.T) {
	paramtable.Init()
	handler := wrapperPost(func() any { return &DefaultReq{} }, func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		return nil, nil
	})

	send := func(body io.Reader) *timeoutResponseRecorder {
		recorder := newTimeoutResponseRecorder(&bytes.Buffer{})
		c := gin.CreateTestContextOnly(httptest.NewRecorder(), gin.New())
		c.Request = httptest.NewRequest(http.MethodPost, "/bound", body)
		// wrapperPost asserts the username the auth middleware normally sets
		c.Set(ContextUsername, "root")
		c.Writer = recorder
		handler(c)
		return recorder
	}

	assert.True(t, send(bytes.NewReader([]byte(`{}`))).bodyReceived.Load(), "successful bind must set the flag")
	assert.True(t, send(bytes.NewReader([]byte(`{"bad json`))).bodyReceived.Load(), "decode failure after a complete read must set the flag")
	assert.False(t, send(iotest.ErrReader(assert.AnError)).bodyReceived.Load(), "body read failure must leave the flag unset")
}

// Guard against registry drift: every route the middleware can resolve must be
// explicitly classified as replay-safe (allowlisted) or in the known-unsafe
// set below. A new route that is neither fails here, forcing a deliberate
// replay-safety decision instead of silently inheriting the fail-safe 500.
func TestReplaySafetyCoversAllRoutes(t *testing.T) {
	knownUnsafe := map[string]struct{}{
		// replay duplicates data or allocates a second server-side effect
		"Insert": {}, "Upsert": {}, "Import": {}, "PinSnapshotData": {},
		"ManualCompaction": {}, "RestoreSnapshot": {}, "RestoreExternalSnapshot": {},
		"ExportSnapshot": {}, "RefreshExternalCollection": {},
		// count-delta request (NumReplica), not desired-state: replay moves another batch
		"TransferMaster": {},
		// bumps schema version + rebroadcasts even on an identical request
		"AlterCollectionFunction": {},
		// expr-delete re-evaluates its predicate at a fresh TSO snapshot per
		// attempt; partial-apply + replay deletes rows inserted in between
		"Delete": {},
		// streaming flush seals newly-created growings on every strictly-newer
		// attempt (fresh TSO passes the fence) — not convergent under replay
		"Flush": {},
		// allowlist is reads-only: every remaining mutation is excluded by policy
		"LoadCollection": {}, "LoadPartitions": {}, "ReleaseCollection": {}, "ReleasePartitions": {},
		"CommitImport": {},
		// self-failed import job re-broadcasts rollback on each abort (not a no-op)
		"AbortImport": {},
		// idempotent but control-plane heavy: excluded by policy (see
		// replaySafeMethods) — unattended retry of admin ops is undesirable
		"TruncateCollection": {}, "CreateCollection": {}, "DropCollection": {}, "RenameCollection": {},
		"CreatePartition": {}, "DropPartition": {},
		"CreateIndex": {}, "DropIndex": {}, "AlterIndex": {},
		"CreateAlias": {}, "DropAlias": {}, "AlterAlias": {},
		"CreateDatabase": {}, "DropDatabase": {}, "AlterDatabase": {},
		"AlterCollection": {}, "AlterCollectionField": {}, "AlterCollectionSchema": {},
		"AddCollectionFunction": {}, "DropCollectionFunction": {}, "AddCollectionStructField": {},
		"CreateSnapshot": {}, "DropSnapshot": {}, "UnpinSnapshotData": {},
		"CreateCredential": {}, "UpdateCredential": {}, "DeleteCredential": {},
		"CreateRole": {}, "DropRole": {}, "AlterRole": {},
		"CreatePrivilegeGroup": {}, "DropPrivilegeGroup": {},
		"OperatePrivilege": {}, "OperatePrivilegeGroup": {}, "OperatePrivilegeV2": {}, "OperateUserRole": {},
		"CreateResourceGroup": {}, "DropResourceGroup": {}, "UpdateResourceGroups": {},
		"AddFileResource": {}, "RemoveFileResource": {},
	}
	for route, method := range routeToMethod {
		_, safe := replaySafeMethods[method]
		_, unsafe := knownUnsafe[method]
		assert.Truef(t, safe != unsafe,
			"route %q -> method %q is unclassified (safe=%v unsafe=%v); add it to replaySafeMethods or the known-unsafe set",
			route, method, safe, unsafe)
	}
}

func TestIsRateLimitClass(t *testing.T) {
	assert.True(t, isRateLimitClass(merr.ErrServiceRateLimit))
	assert.True(t, isRateLimitClass(merr.ErrHTTPRateLimit))
	assert.True(t, isRateLimitClass(merr.ErrServiceQuotaExceeded))
	assert.False(t, isRateLimitClass(merr.ErrServiceNotReady))
	assert.False(t, isRateLimitClass(assert.AnError))
	assert.False(t, isRateLimitClass(nil))
}

// 503 must not be advertised on methods whose replay duplicates data.
func TestProjectedStatusForMethod(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.StandardErrorStatus.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	const insert = "Insert"
	const upsert = "Upsert"
	const search = "Search"
	// retriable → 503 downgraded to 500 only on replay-unsafe methods
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForMethod(merr.ErrServiceUnavailable, insert))
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForMethod(merr.ErrServiceUnavailable, upsert))
	assert.Equal(t, http.StatusServiceUnavailable, projectedStatusForMethod(merr.ErrServiceUnavailable, search))
	// RPC-borne limit codes (4/8/9) can arrive mid-execution, so on a
	// replay-unsafe method they must not invite a replay; only the HTTP
	// pre-admission 1807 is provably pre-side-effect and keeps 429 anywhere.
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForMethod(merr.ErrServiceRateLimit, insert))
	assert.Equal(t, http.StatusInternalServerError, projectedStatusForMethod(merr.ErrServiceTooManyRequests, insert))
	assert.Equal(t, http.StatusTooManyRequests, projectedStatusForMethod(merr.ErrHTTPRateLimit, insert))
	assert.Equal(t, http.StatusTooManyRequests, projectedStatusForMethod(merr.ErrServiceRateLimit, search))
	assert.Equal(t, http.StatusBadRequest, projectedStatusForMethod(merr.ErrParameterInvalid, insert))
}

// Gate-on behavior through the real request path: the status line changes,
// the body keeps the code/message envelope.
func TestProjectedStatusThroughHandlers(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().HTTPCfg.StandardErrorStatus.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)

	h := &HandlersV2{metaCache: func() proxy.Cache { return nil }}
	ginHandler := gin.Default()
	// wrapperPost asserts ContextUsername unconditionally once binding
	// succeeds; the auth middleware (auth disabled) is what sets it.
	app := ginHandler.Group("", genAuthMiddleWare(false))
	app.POST("/param", wrapperPost(func() any { return &CollectionNameReq{} }, func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		return nil, nil
	}))
	rpcErrRoute := func(rpcErr error) gin.HandlerFunc {
		return wrapperPost(func() any { return &DefaultReq{} }, func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
			return h.wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/Search", func(reqCtx context.Context, req any) (any, error) {
				return nil, rpcErr
			})
		})
	}
	// real v2 routes so the route-keyed projection resolves routeToMethod
	app.POST("/v2/vectordb/entities/hybrid_search", rpcErrRoute(merr.ErrServiceRateLimit))
	app.POST("/v2/vectordb/entities/search", rpcErrRoute(merr.ErrServiceUnavailable))

	cases := []struct {
		path       string
		body       string
		wantStatus int
		wantCode   int32
	}{
		{"/param", `{}`, http.StatusBadRequest, merr.Code(merr.ErrMissingRequiredParameters)},
		{"/v2/vectordb/entities/hybrid_search", `{}`, http.StatusTooManyRequests, merr.Code(merr.ErrServiceRateLimit)},
		{"/v2/vectordb/entities/search", `{}`, http.StatusServiceUnavailable, merr.Code(merr.ErrServiceUnavailable)},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader([]byte(tc.body)))
			w := httptest.NewRecorder()
			ginHandler.ServeHTTP(w, req)
			assert.Equal(t, tc.wantStatus, w.Code)
			returnBody := &ReturnErrMsg{}
			assert.Nil(t, json.Unmarshal(w.Body.Bytes(), returnBody))
			assert.Equal(t, tc.wantCode, returnBody.Code)
		})
	}
}
