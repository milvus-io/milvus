package milvusclient

import (
	"context"
	"testing"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConnectionOptionsIncludesCallerDialOptions(t *testing.T) {
	c := &Client{config: &ClientConfig{
		DialOptions: []grpc.DialOption{grpc.WithAuthority("test")},
	}}
	opts := c.connectionOptions()
	assert.Len(t, opts.DialOptions, 1)
	assert.Len(t, opts.UnaryInterceptors, 2)
	assert.NotNil(t, opts.TransportCredentials)
}

func TestConnectionOptionsWithNilDialOptions(t *testing.T) {
	c := &Client{config: &ClientConfig{}}
	opts := c.connectionOptions()
	assert.Empty(t, opts.DialOptions)
	assert.Len(t, opts.UnaryInterceptors, 2)
	assert.NotNil(t, opts.TransportCredentials)
}

func TestRetryConfiguration(t *testing.T) {
	tests := []struct {
		name          string
		config        *ClientConfig
		wantCalls     int
		wantChainSize int
	}{
		{
			name:          "default retry remains enabled",
			config:        &ClientConfig{},
			wantCalls:     6,
			wantChainSize: 2,
		},
		{
			name:          "retry count is configurable",
			config:        &ClientConfig{RetryTransport: &RetryTransportOption{MaxRetry: 3}},
			wantCalls:     3,
			wantChainSize: 2,
		},
		{
			name:          "zero disables retry",
			config:        &ClientConfig{RetryTransport: &RetryTransportOption{MaxRetry: 0}},
			wantCalls:     1,
			wantChainSize: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &Client{config: test.config}
			options := client.connectionOptions()
			require.Len(t, options.UnaryInterceptors, test.wantChainSize)

			calls := 0
			invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				calls++
				return status.Error(codes.Unavailable, "unavailable")
			}
			err := invokeUnaryInterceptors(
				options.UnaryInterceptors,
				invoker,
				nil,
				grpc_retry.WithBackoff(func(uint) time.Duration { return 0 }),
			)
			assert.Equal(t, codes.Unavailable, status.Code(err))
			assert.Equal(t, test.wantCalls, calls)
		})
	}
}

func TestRetryTransportCodes(t *testing.T) {
	tests := []struct {
		name      string
		code      codes.Code
		wantCalls int
	}{
		{name: "unavailable", code: codes.Unavailable, wantCalls: 2},
		{name: "resource exhausted", code: codes.ResourceExhausted, wantCalls: 2},
		{name: "non-retriable", code: codes.InvalidArgument, wantCalls: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &Client{config: &ClientConfig{
				RetryTransport: &RetryTransportOption{MaxRetry: 2},
			}}
			options := client.connectionOptions()

			calls := 0
			invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				calls++
				return status.Error(test.code, test.code.String())
			}

			err := invokeUnaryInterceptors(
				options.UnaryInterceptors,
				invoker,
				nil,
				grpc_retry.WithBackoff(func(uint) time.Duration { return 0 }),
			)
			assert.Equal(t, test.code, status.Code(err))
			assert.Equal(t, test.wantCalls, calls)
		})
	}
}

func invokeUnaryInterceptors(
	interceptors []grpc.UnaryClientInterceptor,
	invoker grpc.UnaryInvoker,
	reply any,
	callOptions ...grpc.CallOption,
) error {
	chainedInvoker := invoker
	for i := len(interceptors) - 1; i >= 0; i-- {
		interceptor := interceptors[i]
		next := chainedInvoker
		chainedInvoker = func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			return interceptor(ctx, method, req, reply, cc, next, opts...)
		}
	}
	return chainedInvoker(context.Background(), "/test.Service/Method", nil, reply, nil, callOptions...)
}

func TestWithGrpcAuthority(t *testing.T) {
	t.Run("sets authority option only", func(t *testing.T) {
		config := &ClientConfig{}
		result := config.WithGrpcAuthority("proxy.example.com")

		assert.Same(t, config, result)
		// Only grpc.WithAuthority; DefaultGrpcOpts are applied separately.
		assert.Equal(t, 1, len(config.DialOptions))
	})

	t.Run("does not mutate DefaultGrpcOpts", func(t *testing.T) {
		originalLen := len(DefaultGrpcOpts)
		config := &ClientConfig{}
		config.WithGrpcAuthority("proxy.example.com")

		assert.Equal(t, originalLen, len(DefaultGrpcOpts))
	})

	t.Run("successive calls replace previous options", func(t *testing.T) {
		config := &ClientConfig{}
		config.WithGrpcAuthority("first.example.com")
		config.WithGrpcAuthority("second.example.com")

		assert.Equal(t, 1, len(config.DialOptions))
	})
}
