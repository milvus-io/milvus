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

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
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

func (c *Client) MetadataUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = c.metadata(ctx)
		ctx = c.state(ctx)
		ctx = c.extraInfo(ctx)

		return invoker(ctx, method, req, reply, cc, opts...)
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

// ref: https://github.com/grpc-ecosystem/go-grpc-middleware

type ctxKey int

const (
	RetryOnRateLimit ctxKey = iota
)

// RetryOnRateLimitInterceptor returns a new retrying unary client interceptor.
func RetryOnRateLimitInterceptor(maxRetry uint, maxBackoff time.Duration, backoffFunc grpc_retry.BackoffFuncContext) grpc.UnaryClientInterceptor {
	return func(parentCtx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if maxRetry == 0 {
			return invoker(parentCtx, method, req, reply, cc, opts...)
		}
		var lastErr error
		for attempt := uint(0); attempt < maxRetry; attempt++ {
			_, err := waitRetryBackoff(parentCtx, attempt, maxBackoff, backoffFunc)
			if err != nil {
				return err
			}
			lastErr = invoker(parentCtx, method, req, reply, cc, opts...)
			rspStatus := getResultStatus(reply)
			if retryOnRateLimit(parentCtx) && rspStatus.GetErrorCode() == commonpb.ErrorCode_RateLimit {
				continue
			}
			return lastErr
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

// getResultStatus returns status of response.
func getResultStatus(reply interface{}) *commonpb.Status {
	switch r := reply.(type) {
	case *commonpb.Status:
		return r
	case *milvuspb.MutationResult:
		return r.GetStatus()
	case *milvuspb.BoolResponse:
		return r.GetStatus()
	case *milvuspb.SearchResults:
		return r.GetStatus()
	case *milvuspb.QueryResults:
		return r.GetStatus()
	case *milvuspb.FlushResponse:
		return r.GetStatus()
	default:
		return nil
	}
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

func waitRetryBackoff(parentCtx context.Context, attempt uint, maxBackoff time.Duration, backoffFunc grpc_retry.BackoffFuncContext) (time.Duration, error) {
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
