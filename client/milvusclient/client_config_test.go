package milvusclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithGrpcAuthority(t *testing.T) {
	t.Run("sets authority option only", func(t *testing.T) {
		config := &ClientConfig{}
		result := config.WithGrpcAuthority("proxy.example.com")

		assert.Same(t, config, result)
		// Only grpc.WithAuthority; DefaultGrpcOpts are applied by dialOptions()
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
