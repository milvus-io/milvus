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
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

const (
	authorizationHeader = `authorization`

	identifierHeader = `identifier`

	databaseHeader = `dbname`

	// idempotencyKeyHeader carries the client-supplied idempotency key for
	// Insert; must stay in sync with util.HeaderIdempotencyKey on the server.
	idempotencyKeyHeader = `idempotency-key`

	// ClientRequestMsecKey temp const value, TODO use common package def after upgrading milvus/pkg version
	ClientRequestMsecKey string = "client-request-unixmsec"

	// ClientRequestIDKey carries a caller-supplied ID used to correlate a request with the
	// server-side logs it produces. The server parses it as an OpenTelemetry TraceID and
	// adopts it as the trace ID for the request, but only when no W3C `traceparent` was
	// propagated. See pkg/tracer/client_request_id_propagator.go.
	//
	// The value MUST be a 32-character lowercase hex string (a 16-byte OTel TraceID);
	// anything else is ignored by the server.
	ClientRequestIDKey string = "client_request_id"

	// traceIDHexLen is the encoded length of a 16-byte OpenTelemetry TraceID.
	traceIDHexLen = 32
)

// clientRequestIDKeyType is the context key used to carry a caller-supplied request ID.
type clientRequestIDKeyType struct{}

// WithClientRequestID returns a context that makes the SDK send `id` as the
// client_request_id header on every request issued with it. The server adopts `id` as the
// trace ID for those requests, so it shows up in Milvus server logs and can be used to
// find everything a specific client call did -- the same mechanism pymilvus exposes via
// CallContext(client_request_id=...).
//
// `id` must be a 32-character lowercase hex OpenTelemetry TraceID (as produced by
// trace.TraceID.String()); NewClientRequestID generates a conforming one. An invalid value
// is dropped rather than sent, because the server would silently ignore it anyway.
//
// This is opt-in by design, because the SDK cannot assume what the server does with the
// header. Servers that predate the clientRequestIDSampler fix turn it into an unsampled
// remote parent, which their ParentBased sampler maps to NeverSample -- so on those, a
// request carrying this header is excluded from tracing entirely, whatever
// trace.sampleFraction says. Fixed servers sample it as a root span, so the ratio applies
// normally. Sending it unconditionally would therefore silently disable tracing against
// any older cluster.
//
// Either way this is for log correlation, not a substitute for W3C traceparent
// propagation: it carries no sampling decision and no parent span.
func WithClientRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, clientRequestIDKeyType{}, id)
}

// NewClientRequestID returns a random, well-formed ID suitable for WithClientRequestID.
// It returns "" if the system entropy source fails.
func NewClientRequestID() string {
	return newClientRequestID()
}

// isValidTraceIDHex reports whether s is a well-formed, non-zero 32-char hex TraceID.
// It mirrors trace.TraceIDFromHex on the server so an invalid value is never put on the
// wire, where it would be silently dropped anyway.
func isValidTraceIDHex(s string) bool {
	if len(s) != traceIDHexLen {
		return false
	}
	nonZero := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch >= '0' && ch <= '9':
		case ch >= 'a' && ch <= 'f':
		default:
			return false
		}
		if ch != '0' {
			nonZero = true
		}
	}
	return nonZero
}

// newClientRequestID returns a random 32-char hex TraceID, or "" if the system entropy
// source fails (in which case the header is simply omitted).
func newClientRequestID() string {
	var buf [traceIDHexLen / 2]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return ""
	}
	id := hex.EncodeToString(buf[:])
	if !isValidTraceIDHex(id) {
		// All-zero read: not a valid TraceID for the server.
		return ""
	}
	return id
}

// withClientMetadata applies the client's metadata enrichment (static headers,
// connection state, and per-request extras) to an outgoing context. It is shared
// by the unary and stream interceptors so new headers stay in sync across both.
func (c *Client) withClientMetadata(ctx context.Context) context.Context {
	ctx = c.metadata(ctx)
	ctx = c.state(ctx)
	ctx = c.extraInfo(ctx)
	return ctx
}

func (c *Client) MetadataUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = c.withClientMetadata(ctx)

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (c *Client) MetadataStreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = c.withClientMetadata(ctx)

		return streamer(ctx, desc, cc, method, opts...)
	}
}

func (c *Client) metadata(ctx context.Context) context.Context {
	for k, v := range c.metadataHeaders {
		ctx = metadata.AppendToOutgoingContext(ctx, k, v)
	}
	return ctx
}

func (c *Client) state(ctx context.Context) context.Context {
	c.stateMut.RLock()
	defer c.stateMut.RUnlock()

	if c.currentDB != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, databaseHeader, c.currentDB)
	}
	if c.identifier != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, identifierHeader, c.identifier)
	}

	return ctx
}

func (c *Client) extraInfo(ctx context.Context) context.Context {
	ctx = metadata.AppendToOutgoingContext(ctx, ClientRequestMsecKey, strconv.FormatInt(time.Now().UnixMilli(), 10))
	if requestID := clientRequestIDFromContext(ctx); requestID != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, ClientRequestIDKey, requestID)
	}
	return ctx
}

// clientRequestIDFromContext returns the caller-supplied request ID, or "" when none was
// set or it is malformed. Nothing is generated here: sending an ID the caller did not ask
// for would opt every request out of server-side trace sampling (see WithClientRequestID).
func clientRequestIDFromContext(ctx context.Context) string {
	id, ok := ctx.Value(clientRequestIDKeyType{}).(string)
	if !ok || !isValidTraceIDHex(id) {
		return ""
	}
	return id
}

type ctxKey int

const (
	RetryOnRateLimit ctxKey = iota
)

// BackoffFuncContext returns the backoff duration to wait before a retry
// attempt.
type BackoffFuncContext func(ctx context.Context, attempt uint) time.Duration

// retryDisabledOption is a grpc.CallOption that disables the client retry
// interceptor entirely for a single call (e.g. the telemetry heartbeat, which
// must be best-effort).
type retryDisabledOption struct {
	grpc.EmptyCallOption
}

// retryTransportDisabledOption disables only the transport-level retry for a
// single call while keeping the rate-limit retry. The mock-server test client
// uses it so mocked error-returning expectations are consumed exactly once.
type retryTransportDisabledOption struct {
	grpc.EmptyCallOption
}

// retryBackoffKey carries an optional per-call backoff function that overrides
// the client-wide retry backoff (e.g. Flush paces its retries at 1s to match
// the server's default flush rate limit of 1 request/sec per collection).
type retryBackoffKey struct{}

// withRetryBackoff returns a context carrying a per-call backoff function used
// by the retry interceptor instead of the client-wide one.
func withRetryBackoff(ctx context.Context, f func(attempt uint) time.Duration) context.Context {
	return context.WithValue(ctx, retryBackoffKey{}, f)
}

// retryOptions extracts the per-call retry control options from the call
// options.
func retryOptions(opts []grpc.CallOption) (disabled, transportDisabled bool) {
	for _, o := range opts {
		switch o.(type) {
		case retryDisabledOption:
			disabled = true
		case retryTransportDisabledOption:
			transportDisabled = true
		}
	}
	return
}

// isRetriableTransportErr reports whether a transport error carries a gRPC
// status code the client retries. Context errors (Canceled / DeadlineExceeded)
// are never retried.
func isRetriableTransportErr(err error) bool {
	code := status.Code(err)
	if code == codes.Canceled || code == codes.DeadlineExceeded {
		return false
	}
	for _, c := range retryableCodes {
		if c == code {
			return true
		}
	}
	return false
}

// RetryInterceptor returns the SDK's retry interceptor: a single loop that
// retries both retriable transport errors (gRPC status codes in
// retryableCodes) and Milvus application-level rate-limit statuses, sharing one
// attempt budget and one backoff sequence, mirroring the pymilvus / C++ / Java
// SDKs. Retry is disabled per call with a retryDisabledOption call option, the
// transport retry with a retryTransportDisabledOption, and the rate-limit retry
// with a context carrying RetryOnRateLimit=false.
func RetryInterceptor(maxRetry uint, maxBackoff time.Duration, backoffFunc BackoffFuncContext) grpc.UnaryClientInterceptor {
	return func(parentCtx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		disabled, transportDisabled := retryOptions(opts)
		if maxRetry == 0 || disabled {
			return invoker(parentCtx, method, req, reply, cc, opts...)
		}
		// A per-call backoff carried in the context overrides the client-wide
		// one for both transport and rate-limit retries.
		if f, ok := parentCtx.Value(retryBackoffKey{}).(func(attempt uint) time.Duration); ok {
			backoffFunc = func(ctx context.Context, attempt uint) time.Duration {
				return f(attempt)
			}
		}
		var lastErr error
		for attempt := uint(0); attempt < maxRetry; attempt++ {
			_, err := waitRetryBackoff(parentCtx, attempt, maxBackoff, backoffFunc)
			if err != nil {
				return err
			}
			lastErr = invoker(parentCtx, method, req, reply, cc, opts...)
			if lastErr != nil {
				// Transport failure: retry only retriable codes. The reply
				// object is reused across attempts, so only inspect it when the
				// call succeeded (lastErr == nil) to avoid misreading a stale
				// rate-limit status from an earlier attempt.
				if parentCtx.Err() != nil {
					return lastErr
				}
				if !transportDisabled && isRetriableTransportErr(lastErr) {
					continue
				}
				return lastErr
			}
			// The call succeeded at the gRPC level: retry only when the reply
			// carries a rate-limit status.
			if retryOnRateLimit(parentCtx) && isRateLimitStatus(getResultStatus(reply)) {
				continue
			}
			return nil
		}
		if lastErr == nil && isRateLimitStatus(getResultStatus(reply)) {
			// The budget was exhausted while the server kept rejecting with a
			// rate-limit status (gRPC OK). Surface it as a merr error so a
			// persistently rate-limited call is not silently swallowed by paths
			// that do not inspect the reply status, and callers can still
			// classify it via merr code 8. This is the only retry layer, so
			// nothing above will re-retry the error.
			return merr.Error(getResultStatus(reply))
		}
		return lastErr
	}
}

func retryOnRateLimit(ctx context.Context) bool {
	retry, ok := ctx.Value(RetryOnRateLimit).(bool)
	if !ok {
		return true // default true
	}
	return retry
}

// rateLimitModernCode is the modern Milvus error code for rate limit, shared
// with the server-side merr partition table (legacy code 49 /
// ErrorCode_RateLimit maps to modern code 8).
const rateLimitModernCode int32 = 8

// isRateLimitStatus reports whether the returned status is a Milvus rate-limit
// error, matching the other SDKs which check both the legacy code
// (ErrorCode_RateLimit) and the modern code (8).
func isRateLimitStatus(s *commonpb.Status) bool {
	if s == nil {
		return false
	}
	return s.GetErrorCode() == commonpb.ErrorCode_RateLimit || s.GetCode() == rateLimitModernCode
}

// statusProvider matches the generated protobuf response types that embed a
// *commonpb.Status. Nearly every Milvus response carries one, so a generic
// assertion keeps the rate-limit retry working across all RPCs instead of a
// hard-coded subset.
type statusProvider interface {
	GetStatus() *commonpb.Status
}

// getResultStatus returns status of response.
func getResultStatus(reply interface{}) *commonpb.Status {
	if r, ok := reply.(*commonpb.Status); ok {
		return r
	}
	if sp, ok := reply.(statusProvider); ok {
		return sp.GetStatus()
	}
	return nil
}

func contextErrToGrpcErr(err error) error {
	switch err {
	case context.DeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, err.Error())
	case context.Canceled:
		return status.Error(codes.Canceled, err.Error())
	default:
		return status.Error(codes.Unknown, err.Error())
	}
}

func waitRetryBackoff(parentCtx context.Context, attempt uint, maxBackoff time.Duration, backoffFunc BackoffFuncContext) (time.Duration, error) {
	var waitTime time.Duration
	if attempt > 0 {
		waitTime = backoffFunc(parentCtx, attempt)
	}
	if waitTime > 0 {
		if waitTime > maxBackoff {
			waitTime = maxBackoff
		}
		timer := time.NewTimer(waitTime)
		select {
		case <-parentCtx.Done():
			timer.Stop()
			return waitTime, contextErrToGrpcErr(parentCtx.Err())
		case <-timer.C:
		}
	}
	return waitTime, nil
}
