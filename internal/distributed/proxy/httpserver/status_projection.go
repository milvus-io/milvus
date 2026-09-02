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

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// statusClientClosedRequest is the nginx/Envoy convention for "client went
// away before the response was written"; net/http has no constant for it.
const statusClientClosedRequest = 499

// statusOverrides pins codes whose correct status differs from what the coarse
// buckets (InputError→400, retriable→503, other→500) would assign. The
// discipline: a row may exist ONLY for codes with verified single provenance —
// every construction site in the repo must agree with the projected semantic.
// A code that is a mere state observation with mixed producers (user mistake
// on one path, internal race/normalized system failure on another) must NOT
// get a directional status; it falls to the neutral 500. That is why the
// resource-state codes (collection/partition not loaded 101/201, index not
// found 700, snapshot not found 2600) deliberately have no rows: each has
// system/transient producers (failed-load normalization, rebalance windows,
// meta races) besides the user-mistake path.
//   - the auth trio is single-provenance (credential/permission state) and
//     InputError-marked; without rows they would flatten to 400;
//   - the limit codes 4/8/9 would fall to 503/500; they stay 429 here, with
//     the replay-unsafe refinement applied in projectedStatusForMethod;
//   - 1807 is minted only by the HTTP layer's pre-execution admission;
//   - timeout has no override: every server-side deadline falls to the neutral
//     500, while the timeout middleware reserves 408 for an incomplete request
//     upload. Canceled (10000) also has no row: without provenance it stays a
//     monitored 500, and only projectedStatusForRequest upgrades it to 499 on
//     proven client cancel.
var statusOverrides = map[int32]int{
	merr.Code(merr.ErrNeedAuthenticate):          http.StatusUnauthorized,
	merr.Code(merr.ErrPrivilegeNotAuthenticated): http.StatusUnauthorized,
	merr.Code(merr.ErrPrivilegeNotPermitted):     http.StatusForbidden,
	merr.Code(merr.ErrServiceTooManyRequests):    http.StatusTooManyRequests,
	merr.Code(merr.ErrServiceRateLimit):          http.StatusTooManyRequests,
	merr.Code(merr.ErrServiceQuotaExceeded):      http.StatusTooManyRequests,
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

// replaySafeMethods is the allowlist of v2 RPCs that may advertise the
// retriable statuses (503 and the RPC-borne 429 codes). It
// is READS ONLY, on purpose: a read mutates no server state, so replaying it is
// unconditionally safe and no per-method reasoning can regress. Every mutation
// — even ones that look idempotent (load/release converge, import commit is a
// state-machine no-op) — is excluded, because a mutation's replay-safety is an
// implementation detail that can and did surface edge cases (expr-delete's
// fresh-snapshot re-evaluation, streaming flush re-sealing growings, an
// import abort re-broadcasting a rollback for a self-failed job). The fail-safe
// cost of excluding a genuinely-idempotent mutation is only a lost auto-retry,
// never data duplication; the body still carries the real code for a
// deliberate, method-aware client retry.
//
// A method absent here defaults to replay-unsafe (503→500, and 429→500 for the
// RPC-borne limit codes). Keyed by routeToMethod's short method names: the
// funnel resolves the ROUTE to its method via routeToMethod[FullPath], so a
// composite handler's internal RPC steps are judged by the route the client
// actually called.
var replaySafeMethods = map[string]struct{}{
	// reads (mutate nothing → replay is unconditionally safe)
	"Search": {}, "HybridSearch": {}, "Query": {}, "RunAnalyzer": {},
	"HasCollection": {}, "HasPartition": {},
	"DescribeCollection": {}, "DescribeDatabase": {}, "DescribeIndex": {},
	"DescribeAlias": {}, "DescribeSnapshot": {}, "DescribeResourceGroup": {},
	"ShowCollections": {}, "ShowPartitions": {},
	"ListAliases": {}, "ListCredUsers": {}, "ListDatabases": {}, "ListImports": {},
	"ListPrivilegeGroups": {}, "ListRefreshExternalCollectionJobs": {},
	"ListRestoreSnapshotJobs": {}, "ListSnapshots": {}, "ListResourceGroups": {},
	"ListFileResources":       {},
	"GetCollectionStatistics": {}, "GetCompactionState": {}, "GetExportSnapshotState": {},
	"GetImportProgress": {}, "GetLoadingProgress": {}, "GetLoadState": {},
	"GetPartitionStatistics": {}, "GetQuotaMetrics": {},
	"GetRefreshExternalCollectionProgress": {}, "GetRestoreSnapshotState": {},
	"GetSegmentsInfo": {},
	"SelectGrant":     {}, "SelectRole": {}, "SelectUser": {},
}

// isReplaySafe uses an allowlist so anything not explicitly classified as
// replay-safe remains unsafe by default.
func isReplaySafe(method string) bool {
	_, safe := replaySafeMethods[method]
	return safe
}

// errToHTTPStatus projects a merr classification onto a standard HTTP status.
// The wire round-trip preserves the inputs it relies on: merr.Status writes
// Retriable and the is_input_error flag, merr.Error reads them back; a legacy
// status carrying only the old ErrorCode reconstructs as a non-retriable
// SystemError and lands on 500, never on a retriable class.
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
	if merr.IsRetryableErr(err) {
		return http.StatusServiceUnavailable
	}
	if code, ok := grpcCodeToHTTPStatus(err); ok {
		return code
	}
	return http.StatusInternalServerError
}

// grpcCodeToHTTPStatus recovers the status of a raw gRPC error that reached the
// funnel without a merr wrapping — a hook rejecting with InvalidArgument
// (hook_interceptor.go) or a transport-level Unavailable — which merr.Code
// otherwise collapses to the generic 500. Only codes whose meaning is
// unambiguous and matches this layer's scheme are mapped; everything else
// returns false so the caller keeps the neutral 500. A merr error has no
// GRPCStatus(), so status.FromError reports ok=false and this never fires for it.
func grpcCodeToHTTPStatus(err error) (int, bool) {
	st, ok := grpcstatus.FromError(err)
	if !ok || st.Code() == codes.OK {
		return 0, false
	}
	switch st.Code() {
	case codes.InvalidArgument:
		return http.StatusBadRequest, true
	case codes.Unauthenticated:
		return http.StatusUnauthorized, true
	case codes.PermissionDenied:
		return http.StatusForbidden, true
	case codes.Unavailable:
		return http.StatusServiceUnavailable, true
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

// errorTypeForAccessLog keeps the access-log attribution aligned with the HTTP
// projection for raw gRPC client errors, which carry no merr classification.
func errorTypeForAccessLog(err error) merr.ErrorType {
	if merr.GetErrorType(err) == merr.InputError {
		return merr.InputError
	}
	switch grpcstatus.Code(err) {
	case codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied:
		return merr.InputError
	default:
		return merr.SystemError
	}
}

// isRateLimitClass reports whether err is an actual quota/limit rejection —
// the only errors the 429 + ErrHTTPRateLimit envelope may represent.
func isRateLimitClass(err error) bool {
	return statusOverrides[merr.Code(err)] == http.StatusTooManyRequests
}

// projectedStatusForMethod replaces the explicitly retriable statuses — 503
// and the RPC-borne 429 codes — with a generic 500 for methods whose replay is
// unsafe. A 500 reports the server failure honestly but cannot prevent a
// gateway configured to retry every 5xx; callers must not automatically replay
// mutations without idempotency or proof that the request was not applied.
func projectedStatusForMethod(err error, method string) int {
	status := projectedStatus(err)
	if isReplaySafe(method) {
		return status
	}
	switch status {
	case http.StatusServiceUnavailable:
		return http.StatusInternalServerError
	case http.StatusTooManyRequests:
		// 1807 is minted only by the HTTP layer's pre-execution admission —
		// provably before any side effect — so its retry invitation stays
		// honest on any method. The RPC-borne limit codes (4/8/9) can also
		// surface mid-execution (a later delete batch hitting a full queue
		// after earlier batches committed) or from static ceilings, so on a
		// replay-unsafe method they must not explicitly advertise retryability.
		if merr.Code(err) != merr.Code(merr.ErrHTTPRateLimit) {
			return http.StatusInternalServerError
		}
	}
	return status
}

// middlewareTimeoutStatus separates only the boundary that is provable here:
// an incomplete request upload is 408, while every timeout after body receipt
// is a server-side 500. Gate-off keeps the pre-existing 408 for compatibility.
func middlewareTimeoutStatus(bodyReceived bool) int {
	if !bodyReceived || !paramtable.Get().HTTPCfg.StandardErrorStatus.GetAsBool() {
		return http.StatusRequestTimeout
	}
	return http.StatusInternalServerError
}

// projectedStatusForRequest upgrades a canceled error to 499 (client closed)
// only when the request context itself was canceled — the signal that proves
// the caller went away. Both merr CanceledCode and a raw gRPC Canceled status
// are accepted under that guard; without the provenance, cancellation stays
// the monitored 500 from projectedStatusForMethod.
func projectedStatusForRequest(gCtx *gin.Context, err error, method string) int {
	status := projectedStatusForMethod(err, method)
	if gCtx == nil {
		return status
	}
	if status == http.StatusInternalServerError &&
		(merr.Code(err) == merr.CanceledCode || grpcstatus.Code(err) == codes.Canceled) &&
		errors.Is(gCtx.Request.Context().Err(), context.Canceled) {
		return statusClientClosedRequest
	}
	return status
}
