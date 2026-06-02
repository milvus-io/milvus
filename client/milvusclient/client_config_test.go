package milvusclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestDialOptionsAlwaysIncludesDefaults(t *testing.T) {
	c := &Client{config: &ClientConfig{
		DialOptions: []grpc.DialOption{grpc.WithAuthority("test")},
	}}
	opts := c.dialOptions()
	// TLS/insecure (1) + DefaultGrpcOpts (len) + user option (1) + interceptors (2)
	assert.True(t, len(opts) >= len(DefaultGrpcOpts)+1, "dialOptions should include DefaultGrpcOpts plus user options")
}

func TestDialOptionsWithNilDialOptions(t *testing.T) {
	c := &Client{config: &ClientConfig{}}
	opts := c.dialOptions()
	assert.True(t, len(opts) >= len(DefaultGrpcOpts), "dialOptions should include DefaultGrpcOpts even when DialOptions is nil")
}

func TestWithGrpcAuthority(t *testing.T) {
	t.Run("sets authority option only", func(t *testing.T) {
		config := &ClientConfig{}
		result := config.WithGrpcAuthority("proxy.example.com")

		assert.Same(t, config, result)
		// DefaultGrpcOpts + grpc.WithAuthority = len(DefaultGrpcOpts) + 1
		assert.Equal(t, len(DefaultGrpcOpts)+1, len(config.DialOptions))
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

		assert.Equal(t, len(DefaultGrpcOpts)+1, len(config.DialOptions))
	})
}
