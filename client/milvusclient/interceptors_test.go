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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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

func TestIsValidTraceIDHex(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected bool
	}{
		{"valid", "4bf92f3577b34da6a3ce929d0e0e4736", true},
		{"all zero", "00000000000000000000000000000000", false},
		{"too short", "4bf92f3577b34da6a3ce929d0e0e473", false},
		{"too long", "4bf92f3577b34da6a3ce929d0e0e47366", false},
		{"empty", "", false},
		{"uppercase rejected", "4BF92F3577B34DA6A3CE929D0E0E4736", false},
		{"non hex", "4bf92f3577b34da6a3ce929d0e0e473g", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isValidTraceIDHex(tc.input))
		})
	}
}

func TestNewClientRequestID(t *testing.T) {
	id := newClientRequestID()
	assert.Len(t, id, traceIDHexLen)
	assert.True(t, isValidTraceIDHex(id), "generated id must be parseable by the server")

	// IDs must differ per call, otherwise every request collapses onto one trace.
	assert.NotEqual(t, id, newClientRequestID())
}

func TestClientRequestIDFromContext(t *testing.T) {
	t.Run("caller supplied id wins", func(t *testing.T) {
		want := "4bf92f3577b34da6a3ce929d0e0e4736"
		ctx := WithClientRequestID(context.Background(), want)
		assert.Equal(t, want, clientRequestIDFromContext(ctx))
	})

	t.Run("malformed id is dropped", func(t *testing.T) {
		ctx := WithClientRequestID(context.Background(), "not-a-trace-id")
		assert.Empty(t, clientRequestIDFromContext(ctx))
	})

	t.Run("no id yields empty", func(t *testing.T) {
		assert.Empty(t, clientRequestIDFromContext(context.Background()))
	})
}

func TestExtraInfoInjectsClientRequestID(t *testing.T) {
	c := &Client{}

	// The header is opt-in: sending it unrequested would opt the request out of
	// server-side trace sampling, since the server maps it to an unsampled remote parent.
	t.Run("absent unless requested", func(t *testing.T) {
		md, ok := metadata.FromOutgoingContext(c.extraInfo(context.Background()))
		assert.True(t, ok)

		assert.Empty(t, md.Get(ClientRequestIDKey))
		// The timestamp header is unconditional and must still be present.
		assert.Len(t, md.Get(ClientRequestMsecKey), 1)
	})

	t.Run("propagates caller supplied id", func(t *testing.T) {
		want := "4bf92f3577b34da6a3ce929d0e0e4736"
		ctx := WithClientRequestID(context.Background(), want)

		md, ok := metadata.FromOutgoingContext(c.extraInfo(ctx))
		assert.True(t, ok)
		assert.Equal(t, []string{want}, md.Get(ClientRequestIDKey))
	})

	t.Run("malformed id is not sent", func(t *testing.T) {
		ctx := WithClientRequestID(context.Background(), "bogus")

		md, ok := metadata.FromOutgoingContext(c.extraInfo(ctx))
		assert.True(t, ok)
		assert.Empty(t, md.Get(ClientRequestIDKey))
	})

	t.Run("NewClientRequestID round trips", func(t *testing.T) {
		id := NewClientRequestID()
		ctx := WithClientRequestID(context.Background(), id)

		md, ok := metadata.FromOutgoingContext(c.extraInfo(ctx))
		assert.True(t, ok)
		assert.Equal(t, []string{id}, md.Get(ClientRequestIDKey))
	})
}

func TestMetadataStreamInterceptor(t *testing.T) {
	c := &Client{
		metadataHeaders: map[string]string{
			authorizationHeader: "base64-token",
		},
		currentDB:  "db1",
		identifier: "ident1",
	}

	var capturedCtx context.Context
	mockStreamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		capturedCtx = ctx
		return nil, nil
	}

	inter := c.MetadataStreamInterceptor()
	_, err := inter(context.Background(), &grpc.StreamDesc{}, nil, "method", mockStreamer)
	require.NoError(t, err)

	md, ok := metadata.FromOutgoingContext(capturedCtx)
	require.True(t, ok)
	assert.Equal(t, "base64-token", md.Get(authorizationHeader)[0])
	assert.Equal(t, "db1", md.Get(databaseHeader)[0])
	assert.Equal(t, "ident1", md.Get(identifierHeader)[0])
	assert.NotEmpty(t, md.Get(ClientRequestMsecKey)[0])
}
