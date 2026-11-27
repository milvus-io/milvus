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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

func TestRetryBackoffProfile(t *testing.T) {
	// The default backoff must match the pymilvus / C++ / Java SDKs:
	// initial 10ms multiplied by 3 per attempt, capped at 3s.
	want := []time.Duration{
		10 * time.Millisecond,
		30 * time.Millisecond,
		90 * time.Millisecond,
		270 * time.Millisecond,
		810 * time.Millisecond,
	}
	for attempt, expected := range want {
		assert.Equal(t, expected, retryBackoff(uint(attempt+1), defaultRetryMaxBackoff), "attempt %d", attempt+1)
	}

	// From attempt 7 onward the backoff is capped at 3s and must never
	// overflow for large attempts.
	assert.Equal(t, 3*time.Second, retryBackoff(7, defaultRetryMaxBackoff))
	assert.Equal(t, 3*time.Second, retryBackoff(uint(defaultRetryTimes), defaultRetryMaxBackoff))
}

func TestRetryBackoffHonorsCustomCap(t *testing.T) {
	// A caller-configured RetryRateLimit.MaxBackoff above or below the parity
	// 3s cap must be honored by the rate-limit retry path.
	assert.Equal(t, 5*time.Second, retryBackoff(7, 5*time.Second))
	assert.Equal(t, 1*time.Second, retryBackoff(7, 1*time.Second))
	assert.Equal(t, 30*time.Millisecond, retryBackoff(2, 1*time.Second))
}

func TestRetryableCodesMatchBaseline(t *testing.T) {
	// The other SDKs retry every gRPC status code except this set.
	ignore := map[codes.Code]bool{
		codes.DeadlineExceeded:  true,
		codes.PermissionDenied:  true,
		codes.Unauthenticated:   true,
		codes.InvalidArgument:   true,
		codes.AlreadyExists:     true,
		codes.ResourceExhausted: true,
		codes.Unimplemented:     true,
	}
	// Canceled (and DeadlineExceeded) are context errors that the retry
	// middleware never retries, so they are excluded from the retryable set
	// even though pymilvus's ignore set does not list Canceled.
	contextErrors := map[codes.Code]bool{
		codes.Canceled:         true,
		codes.DeadlineExceeded: true,
	}
	retryable := make(map[codes.Code]bool, len(retryableCodes))
	for _, code := range retryableCodes {
		retryable[code] = true
	}

	for _, code := range []codes.Code{
		codes.OK,
		codes.Canceled,
		codes.Unknown,
		codes.InvalidArgument,
		codes.DeadlineExceeded,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.ResourceExhausted,
		codes.FailedPrecondition,
		codes.Aborted,
		codes.OutOfRange,
		codes.Unimplemented,
		codes.Internal,
		codes.Unavailable,
		codes.DataLoss,
		codes.Unauthenticated,
	} {
		if ignore[code] || contextErrors[code] {
			assert.False(t, retryable[code], "code %v must not be retried", code)
		} else if code != codes.OK {
			assert.True(t, retryable[code], "code %v must be retried", code)
		}
	}
}

func TestDefaultRetryRateLimitOption(t *testing.T) {
	opt := (&ClientConfig{}).defaultRetryRateLimitOption()
	assert.Equal(t, uint(75), opt.MaxRetry)
	assert.Equal(t, 3*time.Second, opt.MaxBackoff)
}

func TestIsRateLimitStatus(t *testing.T) {
	// legacy code 49 (ErrorCode_RateLimit) and modern code 8 both count.
	assert.True(t, isRateLimitStatus(&commonpb.Status{ErrorCode: commonpb.ErrorCode_RateLimit}))
	assert.True(t, isRateLimitStatus(&commonpb.Status{Code: 8}))
	assert.False(t, isRateLimitStatus(&commonpb.Status{}))
	assert.False(t, isRateLimitStatus(nil))
}

func TestGetResultStatusGeneric(t *testing.T) {
	// The rate-limit interceptor must see the embedded status on response
	// types that are not part of a hard-coded switch, e.g. DDL responses.
	reply := &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_RateLimit},
	}
	assert.True(t, isRateLimitStatus(getResultStatus(reply)))

	// Responses without an embedded status yield no status.
	assert.Nil(t, getResultStatus("not-a-response"))
}

func TestRateLimitInterceptorStopsOnTransportError(t *testing.T) {
	// The rate-limit retry only applies to calls that succeeded at the gRPC
	// level. Once an attempt returns a transport error, the interceptor must
	// return it immediately instead of continuing to loop on a stale
	// rate-limit status left in the reused reply object.
	maxRetry := uint(5)
	inter := RetryOnRateLimitInterceptor(maxRetry, 3*time.Second, func(ctx context.Context, attempt uint) time.Duration {
		return time.Millisecond
	})

	var calls int32
	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		switch atomic.AddInt32(&calls, 1) {
		case 1:
			r := reply.(*milvuspb.DescribeCollectionResponse)
			r.Status = &commonpb.Status{ErrorCode: commonpb.ErrorCode_RateLimit}
			return nil
		default:
			// leave the stale rate-limit status in reply and fail at transport
			return grpcstatus.Error(codes.Unavailable, "mock unavailable")
		}
	}

	reply := &milvuspb.DescribeCollectionResponse{}
	err := inter(context.Background(), "m", nil, reply, nil, invoker)
	assert.Error(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls), "must stop after the first transport error")
}

type RetrySuite struct {
	MockSuiteBase
}

func (s *RetrySuite) TestRateLimitRetryWiredByDefault() {
	s.resetMock()
	// The rate-limit retry interceptor must be wired into the client's dial
	// options by default (RetryRateLimit unset in ClientConfig): a RateLimit
	// status on the first attempt is retried transparently.
	ctx := context.Background()

	var calls int32
	s.mock.EXPECT().DescribeCollection(mock.Anything, mock.AnythingOfType("*milvuspb.DescribeCollectionRequest")).
		Call.Return(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) *milvuspb.DescribeCollectionResponse {
		if atomic.AddInt32(&calls, 1) == 1 {
			return &milvuspb.DescribeCollectionResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_RateLimit},
			}
		}
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}
	}, nil)

	has, err := s.client.HasCollection(ctx, NewHasCollectionOption("test"))
	s.Require().NoError(err)
	s.True(has)
	s.Equal(int32(2), atomic.LoadInt32(&calls))
}

func (s *RetrySuite) TestRateLimitRetryOptOutPerCall() {
	s.resetMock()
	// A context carrying RetryOnRateLimit=false disables the rate-limit retry
	// for a single call, mirroring pymilvus's per-call retry_on_rate_limit.
	ctx := context.WithValue(context.Background(), RetryOnRateLimit, false)

	var calls int32
	s.mock.EXPECT().DescribeCollection(mock.Anything, mock.AnythingOfType("*milvuspb.DescribeCollectionRequest")).
		Call.Return(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) *milvuspb.DescribeCollectionResponse {
		atomic.AddInt32(&calls, 1)
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_RateLimit},
		}
	}, nil)

	_, err := s.client.HasCollection(ctx, NewHasCollectionOption("test"))
	s.Require().Error(err)
	s.Equal(int32(1), atomic.LoadInt32(&calls))
}

// newRetryEnabledClient builds a client with the default transport retry
// enabled, unlike the MockSuiteBase client which disables it so mocked
// error-returning expectations are consumed exactly once.
func (s *RetrySuite) newRetryEnabledClient(ctx context.Context) *Client {
	c, err := New(ctx, &ClientConfig{
		Address: "bufnet",
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(s.mockDialer),
		},
	})
	s.Require().NoError(err)
	return c
}

func (s *RetrySuite) TestTransportRetryWiredByDefault() {
	s.resetMock()
	ctx := context.Background()
	c := s.newRetryEnabledClient(ctx)
	defer c.Close(ctx)

	// A gRPC Unavailable error on the first attempt is retried transparently by
	// the transport-level interceptor.
	var calls int32
	s.mock.EXPECT().DescribeCollection(mock.Anything, mock.AnythingOfType("*milvuspb.DescribeCollectionRequest")).
		Call.Return(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return nil, grpcstatus.Error(codes.Unavailable, "mock unavailable")
		}
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil
	}, nil)

	has, err := c.HasCollection(ctx, NewHasCollectionOption("test"))
	s.Require().NoError(err)
	s.True(has)
	s.Equal(int32(2), atomic.LoadInt32(&calls))
}

func (s *RetrySuite) TestTransportRetryOnWidenedCode() {
	s.resetMock()
	ctx := context.Background()
	c := s.newRetryEnabledClient(ctx)
	defer c.Close(ctx)

	// codes.Unknown is one of the codes the widened retryable set now covers
	// (aligned with the other SDKs); the transport retry must fire on it.
	var calls int32
	s.mock.EXPECT().DescribeCollection(mock.Anything, mock.AnythingOfType("*milvuspb.DescribeCollectionRequest")).
		Call.Return(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return nil, grpcstatus.Error(codes.Unknown, "mock unknown")
		}
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil
	}, nil)

	has, err := c.HasCollection(ctx, NewHasCollectionOption("test"))
	s.Require().NoError(err)
	s.True(has)
	s.Equal(int32(2), atomic.LoadInt32(&calls))
}

func TestRateLimitInterceptorSurfacesErrorAfterExhaustion(t *testing.T) {
	// After the rate-limit retry budget is exhausted (every attempt returns a
	// rate-limit status), the interceptor must surface an error instead of
	// returning nil so a persistently rate-limited call is not silently
	// swallowed by paths that do not inspect the reply status.
	maxRetry := uint(3)
	inter := RetryOnRateLimitInterceptor(maxRetry, 3*time.Second, func(ctx context.Context, attempt uint) time.Duration {
		return time.Millisecond
	})

	var calls int32
	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		atomic.AddInt32(&calls, 1)
		r := reply.(*milvuspb.DescribeCollectionResponse)
		r.Status = &commonpb.Status{ErrorCode: commonpb.ErrorCode_RateLimit, Reason: "rate limit exceeded"}
		return nil
	}

	reply := &milvuspb.DescribeCollectionResponse{}
	err := inter(context.Background(), "m", nil, reply, nil, invoker)
	require.Error(t, err)
	assert.Equal(t, int32(3), atomic.LoadInt32(&calls))
	// the exhausted error must carry a code the transport retry never retries,
	// otherwise grpc_retry would classify it as retriable (Unknown) and restart
	// the whole rate-limit loop
	assert.Equal(t, codes.ResourceExhausted, grpcstatus.Code(err))
}

func TestFlushRetryBackoff(t *testing.T) {
	// pymilvus wraps its flush RPC with @retry_on_rpc_failure(initial_back_off=1):
	// the first retry waits 1s, subsequent retries hit the 3s cap.
	assert.Equal(t, time.Second, flushRetryBackoff(1))
	assert.Equal(t, 3*time.Second, flushRetryBackoff(2))
	assert.Equal(t, 3*time.Second, flushRetryBackoff(uint(defaultRetryTimes)))
}

func TestRateLimitInterceptorUsesPerCallBackoff(t *testing.T) {
	// A per-call backoff carried in the context overrides the client-wide
	// rate-limit backoff (Flush uses this to pace its retries at 1s against the
	// server's 1 request/sec flush rate limit).
	maxRetry := uint(3)
	inter := RetryOnRateLimitInterceptor(maxRetry, 3*time.Second, func(ctx context.Context, attempt uint) time.Duration {
		return 50 * time.Millisecond
	})

	var backoffCalls []uint
	perCallBackoff := func(attempt uint) time.Duration {
		backoffCalls = append(backoffCalls, attempt)
		return time.Millisecond
	}
	ctx := withRateLimitBackoff(context.Background(), perCallBackoff)

	var calls int32
	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		atomic.AddInt32(&calls, 1)
		r := reply.(*milvuspb.DescribeCollectionResponse)
		r.Status = &commonpb.Status{ErrorCode: commonpb.ErrorCode_RateLimit}
		return nil
	}

	reply := &milvuspb.DescribeCollectionResponse{}
	err := inter(ctx, "m", nil, reply, nil, invoker)
	require.Error(t, err)
	assert.Equal(t, int32(3), atomic.LoadInt32(&calls))
	assert.Equal(t, []uint{1, 2}, backoffCalls, "the per-call backoff must drive retry attempts 1 and 2")
}

func TestRetry(t *testing.T) {
	suite.Run(t, new(RetrySuite))
}
