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

package interceptor

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

func TestParseZapLevel(t *testing.T) {
	tests := []struct {
		in   string
		want zapcore.Level
	}{
		{"debug", zapcore.DebugLevel},
		{"DEBUG", zapcore.DebugLevel},
		{"info", zapcore.InfoLevel},
		{"", zapcore.InfoLevel},
		{"warn", zapcore.WarnLevel},
		{"warning", zapcore.WarnLevel},
		{"error", zapcore.ErrorLevel},
		{"bogus", zapcore.InfoLevel},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, parseZapLevel(tt.in))
		})
	}
}

func TestParseMethodList(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want map[string]struct{}
	}{
		{"empty", "", nil},
		{"whitespace only", "   ", nil},
		{"single", "/svc/M", map[string]struct{}{"/svc/M": {}}},
		{"multi trimmed", " /svc/M1 , /svc/M2 ", map[string]struct{}{"/svc/M1": {}, "/svc/M2": {}}},
		{"empty parts", "/svc/M1,,/svc/M2,", map[string]struct{}{"/svc/M1": {}, "/svc/M2": {}}},
		{"only separators", ", , ,", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseMethodList(tt.in))
		})
	}
}

func TestDynamicLogConfig_ShouldLog(t *testing.T) {
	t.Run("empty allowlist matches nothing", func(t *testing.T) {
		c := &dynamicLogConfig{}
		c.level.Store(int32(zapcore.InfoLevel))
		var empty map[string]struct{}
		c.methods.Store(&empty)
		_, ok := c.shouldLog("/svc/M")
		assert.False(t, ok)
	})

	t.Run("listed method matches and returns current level", func(t *testing.T) {
		c := &dynamicLogConfig{}
		c.level.Store(int32(zapcore.DebugLevel))
		set := map[string]struct{}{"/svc/M": {}}
		c.methods.Store(&set)
		lvl, ok := c.shouldLog("/svc/M")
		assert.True(t, ok)
		assert.Equal(t, zapcore.DebugLevel, lvl)
	})

	t.Run("unlisted method does not match", func(t *testing.T) {
		c := &dynamicLogConfig{}
		c.level.Store(int32(zapcore.InfoLevel))
		set := map[string]struct{}{"/svc/M1": {}}
		c.methods.Store(&set)
		_, ok := c.shouldLog("/svc/M2")
		assert.False(t, ok)
	})

	t.Run("concurrent readers do not race", func(t *testing.T) {
		c := &dynamicLogConfig{}
		c.level.Store(int32(zapcore.InfoLevel))
		set := map[string]struct{}{"/svc/M": {}}
		c.methods.Store(&set)

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 1000; j++ {
					c.shouldLog("/svc/M")
				}
			}()
		}
		wg.Wait()
	})
}

func TestNewDynamicLogConfig_SeedsAndHandlesUpdates(t *testing.T) {
	// The production paths use paramtable-driven config keys; here we just
	// verify the seeding path with existing paramtable keys.
	c := newDynamicLogConfig("grpc.serverLog.level", "grpc.serverLog.methods", "debug", "/svc/M")
	lvl, ok := c.shouldLog("/svc/M")
	assert.True(t, ok)
	assert.Equal(t, zapcore.DebugLevel, lvl)

	_, ok = c.shouldLog("/svc/Other")
	assert.False(t, ok)
}

func TestObservabilityInterceptors_ConstructorsDoNotPanic(t *testing.T) {
	// GRPCServerMetric / GRPCClientMetric must be initialized before the
	// constructors run.
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())

	t.Run("server unary", func(t *testing.T) {
		assert.NotNil(t, NewObservabilityServerUnaryInterceptor())
	})
	t.Run("server stream", func(t *testing.T) {
		assert.NotNil(t, NewObservabilityServerStreamInterceptor())
	})
	t.Run("client unary", func(t *testing.T) {
		assert.NotNil(t, NewObservabilityClientUnaryInterceptor())
	})
	t.Run("client stream", func(t *testing.T) {
		assert.NotNil(t, NewObservabilityClientStreamInterceptor())
	})
}

func TestObservabilityServerUnary_FastPath(t *testing.T) {
	// With no methods allow-listed, the interceptor delegates to grpcprom
	// without wrapping the handler. Verify that handler is called and returns
	// the same response/error.
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())
	intercept := NewObservabilityServerUnaryInterceptor()

	handlerCalled := false
	wantErr := errors.New("boom")
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "response", wantErr
	}
	resp, err := intercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/svc/UnknownMethod"}, handler)
	assert.True(t, handlerCalled)
	assert.Equal(t, "response", resp)
	assert.ErrorIs(t, err, wantErr)
}
