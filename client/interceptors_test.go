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

package client

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

var (
	mockInvokerError error
	mockInvokerReply interface{}
	mockInvokeTimes  = 0
)

var mockInvoker grpc.UnaryInvoker = func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
	mockInvokeTimes++
	return mockInvokerError
}

func resetMockInvokeTimes() {
	mockInvokeTimes = 0
}

func TestRateLimitInterceptor(t *testing.T) {
	maxRetry := uint(3)
	maxBackoff := 3 * time.Second
	inter := RetryOnRateLimitInterceptor(maxRetry, maxBackoff, func(ctx context.Context, attempt uint) time.Duration {
		return 60 * time.Millisecond * time.Duration(math.Pow(2, float64(attempt)))
	})

	ctx := context.Background()

	// with retry
	mockInvokerReply = &commonpb.Status{ErrorCode: commonpb.ErrorCode_RateLimit}
	resetMockInvokeTimes()
	err := inter(ctx, "", nil, mockInvokerReply, nil, mockInvoker)
	assert.NoError(t, err)
	assert.Equal(t, maxRetry, uint(mockInvokeTimes))

	// without retry
	ctx1 := context.WithValue(ctx, RetryOnRateLimit, false)
	resetMockInvokeTimes()
	err = inter(ctx1, "", nil, mockInvokerReply, nil, mockInvoker)
	assert.NoError(t, err)
	assert.Equal(t, uint(1), uint(mockInvokeTimes))
}
