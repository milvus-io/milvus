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
	"context"
	"net/http"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// statusClientClosedRequest is the nginx/Envoy convention for "client went
// away before the response was written"; net/http has no constant for it.
const statusClientClosedRequest = 499

// statusOverrides contains only classifications that this boundary can prove
// from the error itself. Authentication/authorization have dedicated HTTP
// statuses, while ErrHTTPRateLimit is created only after the HTTP-layer limiter
// rejects a request before execution. Mixed-provenance downstream limit,
// retryable, timeout, and resource-state errors deliberately fall through to
// the neutral 500.
var statusOverrides = map[int32]int{
	merr.Code(merr.ErrNeedAuthenticate):          http.StatusUnauthorized,
	merr.Code(merr.ErrPrivilegeNotAuthenticated): http.StatusUnauthorized,
	merr.Code(merr.ErrPrivilegeNotPermitted):     http.StatusForbidden,
	merr.Code(merr.ErrHTTPRateLimit):             http.StatusTooManyRequests,
	// SchemaMismatch (109) is marked InputError upstream, but it is a transient
	// WAL schema-version race (concurrent DDL) that every other consumer
	// retries (requestutil Retry bucket, Go SDK evicts its schema cache and
	// replays). A blind HTTP replay is the wrong recovery — the client must
	// refresh schema and rebuild — and unsafe on the fan-out write path, so it
	// is neutralized to 500 (don't blame the request or explicitly advertise
	// retryability) rather than left at 400 (would drop rows) or exposed as 503.
	merr.Code(merr.ErrCollectionSchemaMismatch): http.StatusInternalServerError,
}

// errToHTTPStatus projects a merr classification onto a standard HTTP status.
// The projection is intentionally conservative: only explicit input and auth
// classifications receive client-facing statuses; everything else is 500.
func errToHTTPStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if status, ok := statusOverrides[merr.Code(err)]; ok {
		return status
	}
	if merr.GetErrorType(err) == merr.InputError {
		return http.StatusBadRequest
	}
	if code, ok := grpcCodeToHTTPStatus(err); ok {
		return code
	}
	return http.StatusInternalServerError
}

// grpcCodeToHTTPStatus recovers explicit authentication and authorization
// statuses from a raw gRPC error. InvalidArgument is deliberately not mapped:
// HookInterceptor uses it for every Mock/Before/After hook failure, including
// server failures after the handler has already run, so the code alone cannot
// prove input blame. An explicitly input-marked error is handled above.
func grpcCodeToHTTPStatus(err error) (int, bool) {
	st, ok := grpcstatus.FromError(err)
	if !ok || st.Code() == codes.OK {
		return 0, false
	}
	switch st.Code() {
	case codes.Unauthenticated:
		return http.StatusUnauthorized, true
	case codes.PermissionDenied:
		return http.StatusForbidden, true
	}
	return 0, false
}

// projectedStatus is the status every error-carrying REST response goes
// through. With proxy.http.standardErrorStatus off (the default) it keeps the
// legacy 200-envelope contract; the body keeps the code/message envelope in
// both modes.
func projectedStatus(err error) int {
	if !paramtable.Get().HTTPCfg.StandardErrorStatus.GetAsBool() {
		return http.StatusOK
	}
	return errToHTTPStatus(err)
}

// recordErrorType stamps the access-log classification for a locally-written
// error response. wrapperProxy sets it at the funnel from the same err; local
// sites that write HTTPReturn directly — especially where the body code is a
// different sentinel than the status-driving err — must set it too, or the
// access log falls back to the numeric body code and reports a class that
// disagrees with the projected status. Key matches accesslog/info.ContextErrorType.
func recordErrorType(c *gin.Context, err error) {
	c.Set("error_type", errorTypeForAccessLog(err).String())
}

// errorTypeForAccessLog keeps access-log attribution aligned with the HTTP
// projection, including the conservative treatment of unclassified gRPC errors.
func errorTypeForAccessLog(err error) merr.ErrorType {
	// Schema mismatch is input-classified by the shared merr sentinel for legacy
	// retry consumers, but this HTTP boundary projects the transient schema race
	// as 500, so its access-log attribution must remain system-owned as well.
	if merr.Code(err) == merr.Code(merr.ErrCollectionSchemaMismatch) {
		return merr.SystemError
	}
	if merr.GetErrorType(err) == merr.InputError {
		return merr.InputError
	}
	switch grpcstatus.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return merr.InputError
	default:
		return merr.SystemError
	}
}

// middlewareTimeoutStatus separates only the boundary that is provable here:
// an incomplete request upload is 408, while every timeout after body receipt
// is a server-side 500. Gate-off keeps the pre-existing 408 for compatibility.
// standardErrorStatus is a per-request snapshot so one timeout decision cannot
// observe two different config values.
func middlewareTimeoutStatus(bodyReceived, standardErrorStatus bool) int {
	if !bodyReceived || !standardErrorStatus {
		return http.StatusRequestTimeout
	}
	return http.StatusInternalServerError
}

// projectedBodyReadStatus classifies failures returned while receiving the
// request body before JSON decoding starts. A canceled request context proves
// that the caller went away; a timeout-capable read error proves a receive-phase
// timeout; all other transport failures remain the neutral 500.
func projectedBodyReadStatus(gCtx *gin.Context, err error) int {
	if !paramtable.Get().HTTPCfg.StandardErrorStatus.GetAsBool() {
		return http.StatusOK
	}
	if gCtx != nil && gCtx.Request != nil &&
		errors.Is(gCtx.Request.Context().Err(), context.Canceled) {
		return statusClientClosedRequest
	}
	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return http.StatusRequestTimeout
	}
	return http.StatusInternalServerError
}

func applyRequestCancellationStatus(gCtx *gin.Context, err error, status int) int {
	if gCtx == nil || gCtx.Request == nil {
		return status
	}
	if status == http.StatusInternalServerError &&
		(merr.Code(err) == merr.CanceledCode || grpcstatus.Code(err) == codes.Canceled) &&
		errors.Is(gCtx.Request.Context().Err(), context.Canceled) {
		return statusClientClosedRequest
	}
	return status
}

// projectedStatusForRequest upgrades a canceled error to 499 (client closed)
// only when the request context itself was canceled — the signal that proves
// the caller went away. Both merr CanceledCode and a raw gRPC Canceled status
// are accepted under that guard; without the provenance, cancellation stays
// the neutral 500.
func projectedStatusForRequest(gCtx *gin.Context, err error) int {
	return applyRequestCancellationStatus(gCtx, err, projectedStatus(err))
}

// projectedAuthorizationStatus preserves the legacy 403 used for authorization
// interceptor failures while the feature is disabled. With standard statuses
// enabled, only explicit auth failures remain 401/403; infrastructure failures
// and proven client cancellation become 500/499.
func projectedAuthorizationStatus(gCtx *gin.Context, err error) int {
	standardErrorStatus := paramtable.Get().HTTPCfg.StandardErrorStatus.GetAsBool()
	if !standardErrorStatus {
		return http.StatusForbidden
	}
	return applyRequestCancellationStatus(gCtx, err, errToHTTPStatus(err))
}

// errorFromStatusForHTTP restores the input classification that old peers could
// carry only as deprecated IllegalArgument. It is intentionally local to the
// REST response boundary so shared retry and gRPC behavior remain unchanged.
func errorFromStatusForHTTP(status *commonpb.Status) error {
	err := merr.Error(status)
	if err != nil && status.GetCode() == 0 && status.GetErrorCode() == commonpb.ErrorCode_IllegalArgument {
		return merr.WrapErrAsInputError(err)
	}
	return err
}
