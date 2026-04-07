package milvusclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithGrpcAuthority(t *testing.T) {
	t.Run("sets authority and preserves default opts", func(t *testing.T) {
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
