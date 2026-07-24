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
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		in   string
		want mlog.Level
	}{
		{"debug", mlog.DebugLevel},
		{"DEBUG", mlog.DebugLevel},
		{"info", mlog.InfoLevel},
		{"", mlog.InfoLevel},
		{"warn", mlog.WarnLevel},
		{"warning", mlog.WarnLevel},
		{"error", mlog.ErrorLevel},
		{"bogus", mlog.InfoLevel},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, parseLogLevel(tt.in))
		})
	}
}

func TestParseMethodFilter(t *testing.T) {
	tests := []struct {
		name         string
		in           string
		matches      []string
		nonMatches   []string
		invalidRegex []string
	}{
		{name: "empty", in: "", nonMatches: []string{"/svc/M"}},
		{name: "whitespace only", in: "   ", nonMatches: []string{"/svc/M"}},
		{name: "single exact", in: "/svc/M", matches: []string{"/svc/M"}, nonMatches: []string{"/svc/Other"}},
		{name: "multi exact trimmed", in: " /svc/M1 , /svc/M2 ", matches: []string{"/svc/M1", "/svc/M2"}, nonMatches: []string{"/svc/M3"}},
		{name: "empty parts", in: "/svc/M1,,/svc/M2,", matches: []string{"/svc/M1", "/svc/M2"}, nonMatches: []string{"/svc/M3"}},
		{name: "only separators", in: ", , ,", nonMatches: []string{"/svc/M"}},
		{name: "regex", in: "re:^/svc/.+$", matches: []string{"/svc/M"}, nonMatches: []string{"/other/M"}},
		{name: "mixed exact and regex", in: "/svc/Exact,re:^/svc/Regex.+$", matches: []string{"/svc/Exact", "/svc/RegexMatched"}, nonMatches: []string{"/svc/Other"}},
		{name: "invalid regex is reported and skipped", in: "/svc/Exact,re:[", matches: []string{"/svc/Exact"}, nonMatches: []string{"/svc/RegexMatched"}, invalidRegex: []string{"re:["}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, invalidRegexs := parseMethodFilter(tt.in)
			assert.Equal(t, tt.invalidRegex, invalidRegexs)

			c := &dynamicLogConfig{}
			c.level.Store(int32(mlog.InfoLevel))
			c.methods.Store(filter)
			for _, method := range tt.matches {
				lvl, ok := c.shouldLog(method)
				assert.True(t, ok)
				assert.Equal(t, mlog.InfoLevel, lvl)
			}
			for _, method := range tt.nonMatches {
				_, ok := c.shouldLog(method)
				assert.False(t, ok)
			}
		})
	}
}

func TestDynamicLogConfig_ShouldLog(t *testing.T) {
	t.Run("empty allowlist matches nothing", func(t *testing.T) {
		c := &dynamicLogConfig{}
		c.level.Store(int32(mlog.InfoLevel))
		_, ok := c.shouldLog("/svc/M")
		assert.False(t, ok)
	})

	t.Run("listed method matches and returns current level", func(t *testing.T) {
		c := &dynamicLogConfig{}
		c.level.Store(int32(mlog.DebugLevel))
		filter, invalidRegexs := parseMethodFilter("/svc/M")
		assert.Empty(t, invalidRegexs)
		c.methods.Store(filter)
		lvl, ok := c.shouldLog("/svc/M")
		assert.True(t, ok)
		assert.Equal(t, mlog.DebugLevel, lvl)
	})

	t.Run("unlisted method does not match", func(t *testing.T) {
		c := &dynamicLogConfig{}
		c.level.Store(int32(mlog.InfoLevel))
		filter, invalidRegexs := parseMethodFilter("/svc/M1")
		assert.Empty(t, invalidRegexs)
		c.methods.Store(filter)
		_, ok := c.shouldLog("/svc/M2")
		assert.False(t, ok)
	})

	t.Run("concurrent readers do not race", func(t *testing.T) {
		c := &dynamicLogConfig{}
		c.level.Store(int32(mlog.InfoLevel))
		filter, invalidRegexs := parseMethodFilter("/svc/M")
		assert.Empty(t, invalidRegexs)
		c.methods.Store(filter)

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

func TestDynamicLogConfig_UpdateMethodsRejectsInvalidRegex(t *testing.T) {
	c := &dynamicLogConfig{}
	c.level.Store(int32(mlog.InfoLevel))
	filter, invalidRegexs := parseMethodFilter("/svc/Old")
	assert.Empty(t, invalidRegexs)
	c.methods.Store(filter)

	assert.False(t, c.updateMethods("grpc.log.server.methods", "re:["))

	lvl, ok := c.shouldLog("/svc/Old")
	assert.True(t, ok)
	assert.Equal(t, mlog.InfoLevel, lvl)
	_, ok = c.shouldLog("/svc/New")
	assert.False(t, ok)
}

func TestNewDynamicLogConfig_SeedsAndHandlesUpdates(t *testing.T) {
	// The production paths use paramtable-driven config keys; here we just
	// verify the seeding path with existing paramtable keys.
	c := newDynamicLogConfig("grpc.log.server.level", "grpc.log.server.methods", "debug", "/svc/M")
	lvl, ok := c.shouldLog("/svc/M")
	assert.True(t, ok)
	assert.Equal(t, mlog.DebugLevel, lvl)

	_, ok = c.shouldLog("/svc/Other")
	assert.False(t, ok)
}

func TestNewDynamicLogConfig_RegexMethodFilter(t *testing.T) {
	c := newDynamicLogConfig("grpc.log.server.level", "grpc.log.server.methods", "debug", "re:^/svc/.+$")

	lvl, ok := c.shouldLog("/svc/RegexMatched")
	assert.True(t, ok)
	assert.Equal(t, mlog.DebugLevel, lvl)

	_, ok = c.shouldLog("/other/RegexNotMatched")
	assert.False(t, ok)
}

func resetDynamicLogConfigSingletonsForTest() {
	serverDynamicLogConfigOnce = sync.Once{}
	serverDynamicLogConfig = nil
	clientDynamicLogConfigOnce = sync.Once{}
	clientDynamicLogConfig = nil
}

func configureObservabilityLogMethodsForTest(t *testing.T, serverMethods, clientMethods string) {
	t.Helper()
	pt := paramtable.Get()
	pt.Save(pt.LogCfg.GrpcServerLogMethods.Key, serverMethods)
	pt.Save(pt.LogCfg.GrpcClientLogMethods.Key, clientMethods)
	resetDynamicLogConfigSingletonsForTest()
	t.Cleanup(func() {
		pt.Reset(pt.LogCfg.GrpcServerLogMethods.Key)
		pt.Reset(pt.LogCfg.GrpcClientLogMethods.Key)
		resetDynamicLogConfigSingletonsForTest()
	})
}

func TestObservabilityLogConfigSingletons(t *testing.T) {
	resetDynamicLogConfigSingletonsForTest()
	defer resetDynamicLogConfigSingletonsForTest()

	assert.Same(t, getServerDynamicLogConfig(), getServerDynamicLogConfig())
	assert.Same(t, getClientDynamicLogConfig(), getClientDynamicLogConfig())
	assert.NotSame(t, getServerDynamicLogConfig(), getClientDynamicLogConfig())
}

func TestObservabilityInterceptors_ConstructorsDoNotPanic(t *testing.T) {
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

func TestObservabilityServerUnary_AccessLogPath(t *testing.T) {
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())
	configureObservabilityLogMethodsForTest(t, "/svc/Unary", "")
	intercept := NewObservabilityServerUnaryInterceptor()

	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "response", nil
	}
	resp, err := intercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/svc/Unary"}, handler)

	assert.True(t, handlerCalled)
	assert.Equal(t, "response", resp)
	assert.NoError(t, err)
}

func TestObservabilityServerUnary_RecordsMetricsLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics.RegisterGRPCMetrics(registry)
	method := "/testpb.ObservabilityMetricService/UnaryDenied"
	intercept := NewObservabilityServerUnaryInterceptor()

	wantErr := status.Error(codes.PermissionDenied, "denied")
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, wantErr
	}
	resp, err := intercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, wantErr)

	mfs, err := registry.Gather()
	assert.NoError(t, err)
	assert.True(t, hasMetricWithLabels(mfs, "milvus_grpc_server_handled_total", map[string]string{
		"grpc_type":    "unary",
		"grpc_service": "testpb.ObservabilityMetricService",
		"grpc_method":  "UnaryDenied",
		"grpc_code":    codes.PermissionDenied.String(),
		"node_id":      paramtable.GetStringNodeID(),
	}))
}

func TestObservabilityServerStream_AccessLogPath(t *testing.T) {
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())
	configureObservabilityLogMethodsForTest(t, "/svc/Stream", "")
	intercept := NewObservabilityServerStreamInterceptor()

	handlerCalled := false
	handler := func(srv any, ss grpc.ServerStream) error {
		handlerCalled = true
		return nil
	}
	err := intercept(nil, newMockSS(context.Background()), &grpc.StreamServerInfo{FullMethod: "/svc/Stream"}, handler)

	assert.True(t, handlerCalled)
	assert.NoError(t, err)
}

func TestObservabilityClientUnary_AccessLogPath(t *testing.T) {
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())
	configureObservabilityLogMethodsForTest(t, "", "/svc/Unary")
	intercept := NewObservabilityClientUnaryInterceptor()

	invokerCalled := false
	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		invokerCalled = true
		return nil
	}
	err := intercept(context.Background(), "/svc/Unary", nil, nil, nil, invoker)

	assert.True(t, invokerCalled)
	assert.NoError(t, err)
}

func TestObservabilityClientStream_AccessLogPath(t *testing.T) {
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())
	configureObservabilityLogMethodsForTest(t, "", "/svc/Stream")
	intercept := NewObservabilityClientStreamInterceptor()

	streamerCalled := false
	rawStream := &mockClientStream{ctx: context.Background()}
	streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		streamerCalled = true
		return rawStream, nil
	}
	cs, err := intercept(context.Background(), &grpc.StreamDesc{}, nil, "/svc/Stream", streamer)

	assert.True(t, streamerCalled)
	assert.NotNil(t, cs)
	assert.NotEqual(t, rawStream, cs)
	assert.NoError(t, err)
}

type mockClientStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s *mockClientStream) Header() (metadata.MD, error) {
	return metadata.MD{}, nil
}

func (s *mockClientStream) Trailer() metadata.MD {
	return metadata.MD{}
}

func (s *mockClientStream) CloseSend() error {
	return nil
}

func (s *mockClientStream) Context() context.Context {
	return s.ctx
}

func (s *mockClientStream) SendMsg(any) error {
	return nil
}

func (s *mockClientStream) RecvMsg(any) error {
	return nil
}

func hasMetricWithLabels(mfs []*dto.MetricFamily, name string, labels map[string]string) bool {
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if metricHasLabels(metric, labels) {
				return true
			}
		}
	}
	return false
}

func metricHasLabels(metric *dto.Metric, labels map[string]string) bool {
	got := make(map[string]string, len(metric.GetLabel()))
	for _, label := range metric.GetLabel() {
		got[label.GetName()] = label.GetValue()
	}
	for key, want := range labels {
		if got[key] != want {
			return false
		}
	}
	return true
}
