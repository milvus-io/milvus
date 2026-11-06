package milvusclient

import (
	"sync"
	"testing"
	"time"

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

func TestConnectionPoolSize(t *testing.T) {
	config := &ClientConfig{}
	assert.Equal(t, 1, config.getConnectionPoolSize())
	config.ConnectionPoolSize = 4
	assert.Equal(t, 4, config.getConnectionPoolSize())
	config.ConnectionPoolSize = -1
	assert.Equal(t, 1, config.getConnectionPoolSize())
}

func TestConnectionRotationConfig(t *testing.T) {
	config := &ClientConfig{Address: "localhost:19530"}
	assert.Equal(t, 10*time.Second, config.getConnectionRotationTimeout())
	config.ConnectionRotationTimeout = time.Minute
	assert.Equal(t, time.Minute, config.getConnectionRotationTimeout())
	config.ConnectionDrainTimeout = -time.Second
	assert.Error(t, config.parse())
}

func TestClientConfigConcurrentStateAccess(t *testing.T) {
	config := &ClientConfig{}
	versions := []string{"v3.0.0", "v3.0.1-dev", "v3.0.1", "v3.0.2-release"}
	flags := []uint64{disableDatabase, disableJSON, disableDynamicSchema, disableParitionKey}
	config.setServerInfo(versions[0])

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(flags))
	for index, flag := range flags {
		go func(index int, flag uint64) {
			defer wg.Done()
			<-start
			for i := 0; i < 1000; i++ {
				config.setServerInfo(versions[index])
				assert.Contains(t, versions, config.GetServerVersion())
				// Each worker owns one bit; other workers must preserve it.
				config.addFlags(flag)
				assert.True(t, config.hasFlags(flag))
				config.resetFlags(flag)
				assert.False(t, config.hasFlags(flag))
			}
		}(index, flag)
	}
	close(start)
	wg.Wait()
	assert.False(t, config.hasFlags(disableDatabase|disableJSON|disableDynamicSchema|disableParitionKey))
}
