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
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	c := newDynamicLogConfig("grpc.log.server.level", "grpc.log.server.methods", "grpc.log.server.events", "grpc.log.server.fields", "debug", "/svc/M", "finish_call", "grpc.code,method")
	lvl, ok := c.shouldLog("/svc/M")
	assert.True(t, ok)
	assert.Equal(t, mlog.DebugLevel, lvl)
	assert.Equal(t, []grpc_logging.LoggableEvent{grpc_logging.FinishCall}, c.logEvents())
	assert.True(t, c.logFields().has("grpc.code"))
	assert.True(t, c.logFields().has("method"))

	_, ok = c.shouldLog("/svc/Other")
	assert.False(t, ok)
}

func TestNewDynamicLogConfig_RegexMethodFilter(t *testing.T) {
	c := newDynamicLogConfig("grpc.log.server.level", "grpc.log.server.methods", "grpc.log.server.events", "grpc.log.server.fields", "debug", "re:^/svc/.+$", "finish_call", "grpc.code")

	lvl, ok := c.shouldLog("/svc/RegexMatched")
	assert.True(t, ok)
	assert.Equal(t, mlog.DebugLevel, lvl)

	_, ok = c.shouldLog("/other/RegexNotMatched")
	assert.False(t, ok)
}

func TestNewDynamicLogConfig_InvalidInitialValues(t *testing.T) {
	keys := dynamicLogConfigTestKeys("test.grpc.log.invalid")
	c := newDynamicLogConfig(keys.level, keys.methods, keys.events, keys.fields, "info", "re:[", "bogus", ", ,")
	unregisterDynamicLogConfigWatchers(t, keys)

	_, ok := c.shouldLog("/svc/Method")
	assert.False(t, ok)
	assert.Equal(t, []grpc_logging.LoggableEvent{grpc_logging.FinishCall}, c.logEvents())
	assert.Nil(t, c.logFields())
}

func TestDynamicLogConfig_Watchers(t *testing.T) {
	keys := dynamicLogConfigTestKeys("test.grpc.log.watchers")
	c := newDynamicLogConfig(keys.level, keys.methods, keys.events, keys.fields, "info", "/svc/Old", "finish_call", "grpc.code")
	handlers := dynamicLogConfigWatchers(t, keys)
	t.Cleanup(func() {
		unregisterDynamicLogConfigHandlers(keys, handlers)
	})

	for _, handler := range handlers {
		handler.OnEvent(&config.Event{HasUpdated: false, Value: "ignored"})
	}
	lvl, ok := c.shouldLog("/svc/Old")
	assert.True(t, ok)
	assert.Equal(t, mlog.InfoLevel, lvl)
	assert.Equal(t, []grpc_logging.LoggableEvent{grpc_logging.FinishCall}, c.logEvents())
	assert.True(t, c.logFields().has("grpc.code"))

	handlers[0].OnEvent(&config.Event{HasUpdated: true, Value: "debug"})
	handlers[1].OnEvent(&config.Event{HasUpdated: true, Value: "/svc/New"})
	handlers[2].OnEvent(&config.Event{HasUpdated: true, Value: "start_call,payload_sent"})
	handlers[3].OnEvent(&config.Event{HasUpdated: true, Value: "method,milvus.code"})

	lvl, ok = c.shouldLog("/svc/New")
	assert.True(t, ok)
	assert.Equal(t, mlog.DebugLevel, lvl)
	_, ok = c.shouldLog("/svc/Old")
	assert.False(t, ok)
	assert.Equal(t, []grpc_logging.LoggableEvent{grpc_logging.StartCall, grpc_logging.PayloadSent}, c.logEvents())
	assert.True(t, c.logFields().has("method"))
	assert.True(t, c.logFields().has("milvus.code"))
	assert.False(t, c.logFields().has("grpc.code"))

	handlers[1].OnEvent(&config.Event{HasUpdated: true, Value: "re:["})
	handlers[2].OnEvent(&config.Event{HasUpdated: true, Value: "bogus"})

	_, ok = c.shouldLog("/svc/New")
	assert.True(t, ok)
	assert.Equal(t, []grpc_logging.LoggableEvent{grpc_logging.StartCall, grpc_logging.PayloadSent}, c.logEvents())
}

func TestParseLogEvents(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []grpc_logging.LoggableEvent
		invalid []string
	}{
		{name: "default", in: "", want: []grpc_logging.LoggableEvent{grpc_logging.FinishCall}},
		{name: "all events", in: "start_call,finish_call,payload_received,payload_sent", want: []grpc_logging.LoggableEvent{grpc_logging.StartCall, grpc_logging.FinishCall, grpc_logging.PayloadReceived, grpc_logging.PayloadSent}},
		{name: "trim canonical names", in: " start_call , finish_call ", want: []grpc_logging.LoggableEvent{grpc_logging.StartCall, grpc_logging.FinishCall}},
		{name: "empty parts skipped", in: "start_call,, ,finish_call,", want: []grpc_logging.LoggableEvent{grpc_logging.StartCall, grpc_logging.FinishCall}},
		{name: "duplicate events are deduplicated", in: "finish_call,start_call,finish_call,start_call", want: []grpc_logging.LoggableEvent{grpc_logging.FinishCall, grpc_logging.StartCall}},
		{name: "invalid event skipped", in: "finish_call,bogus", want: []grpc_logging.LoggableEvent{grpc_logging.FinishCall}, invalid: []string{"bogus"}},
		{name: "aliases are invalid", in: "start,finish,payload_recv,payload_send", want: []grpc_logging.LoggableEvent{grpc_logging.FinishCall}, invalid: []string{"start", "finish", "payload_recv", "payload_send"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events, invalid := parseLogEvents(tt.in)
			assert.Equal(t, tt.want, events)
			assert.Equal(t, tt.invalid, invalid)
		})
	}
}

func TestDynamicLogConfig_UpdateEventsRejectsInvalidEvent(t *testing.T) {
	c := &dynamicLogConfig{}
	c.updateEvents("grpc.log.server.events", "start_call")

	assert.False(t, c.updateEvents("grpc.log.server.events", "bogus"))
	assert.Equal(t, []grpc_logging.LoggableEvent{grpc_logging.StartCall}, c.logEvents())
}

func TestParseLogFields(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{name: "empty disables filtering", in: "", want: nil},
		{name: "whitespace parts disable filtering", in: ", ,", want: nil},
		{name: "trim and deduplicate", in: " grpc.code , method,grpc.code ", want: []string{"grpc.code", "method"}},
		{name: "empty parts skipped", in: "grpc.code,,grpc.duration,", want: []string{"grpc.code", "grpc.duration"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := parseLogFields(tt.in)
			if tt.want == nil {
				assert.Nil(t, filter)
				return
			}
			for _, field := range tt.want {
				assert.True(t, filter.has(field), field)
			}
			assert.Equal(t, len(tt.want), filter.len())
		})
	}
}

func TestFieldFilter_DefaultBehavior(t *testing.T) {
	var filter *fieldFilter
	assert.True(t, filter.has("any-field"))
	assert.Zero(t, filter.len())
}

func TestDynamicLogConfig_DefaultEvents(t *testing.T) {
	c := &dynamicLogConfig{}
	assert.Equal(t, []grpc_logging.LoggableEvent{grpc_logging.FinishCall}, c.logEvents())
}

func TestLogLevelConversions(t *testing.T) {
	tests := []struct {
		name      string
		mlogLevel mlog.Level
		grpcLevel grpc_logging.Level
	}{
		{name: "debug", mlogLevel: mlog.DebugLevel, grpcLevel: grpc_logging.LevelDebug},
		{name: "info", mlogLevel: mlog.InfoLevel, grpcLevel: grpc_logging.LevelInfo},
		{name: "warn", mlogLevel: mlog.WarnLevel, grpcLevel: grpc_logging.LevelWarn},
		{name: "error", mlogLevel: mlog.ErrorLevel, grpcLevel: grpc_logging.LevelError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.grpcLevel, toGRPCLogLevel(test.mlogLevel))
			assert.Equal(t, test.mlogLevel, toMlogLevel(test.grpcLevel))
		})
	}
	assert.Equal(t, grpc_logging.LevelInfo, toGRPCLogLevel(mlog.Level(100)))
	assert.Equal(t, mlog.InfoLevel, toMlogLevel(grpc_logging.Level(-1)))
}

func TestFilterMiddlewareFields(t *testing.T) {
	fields := []any{"method", "/svc/Method", 42, "non-string-key", "grpc.code", "OK", "dangling"}
	assert.Equal(t, fields, filterMiddlewareFields(fields, nil))

	allowlist := parseLogFields("grpc.code")
	assert.Equal(t, []any{"grpc.code", "OK"}, filterMiddlewareFields(fields, allowlist))
}

func TestAppendDurationField(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "duration", value: time.Second},
		{name: "duration string", value: "250ms"},
		{name: "invalid duration string", value: "not-a-duration"},
		{name: "other type", value: 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fields := appendDurationField(nil, "grpc.duration", test.value)
			assert.Len(t, fields, 1)
		})
	}
}

func TestDynamicLogConfig_UpdateFields(t *testing.T) {
	c := &dynamicLogConfig{}
	c.updateFields("grpc.log.server.fields", "grpc.code,method")

	assert.True(t, c.logFields().has("grpc.code"))
	assert.True(t, c.logFields().has("method"))
	assert.False(t, c.logFields().has("grpc.response.content"))

	c.updateFields("grpc.log.server.fields", "")
	assert.Nil(t, c.logFields())
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

func configureObservabilityLogEventsForTest(t *testing.T, serverEvents, clientEvents string) {
	t.Helper()
	pt := paramtable.Get()
	pt.Save(pt.LogCfg.GrpcServerLogEvents.Key, serverEvents)
	pt.Save(pt.LogCfg.GrpcClientLogEvents.Key, clientEvents)
	resetDynamicLogConfigSingletonsForTest()
	t.Cleanup(func() {
		pt.Reset(pt.LogCfg.GrpcServerLogEvents.Key)
		pt.Reset(pt.LogCfg.GrpcClientLogEvents.Key)
		resetDynamicLogConfigSingletonsForTest()
	})
}

func configureObservabilityLogFieldsForTest(t *testing.T, serverFields, clientFields string) {
	t.Helper()
	pt := paramtable.Get()
	pt.Save(pt.LogCfg.GrpcServerLogFields.Key, serverFields)
	pt.Save(pt.LogCfg.GrpcClientLogFields.Key, clientFields)
	resetDynamicLogConfigSingletonsForTest()
	t.Cleanup(func() {
		pt.Reset(pt.LogCfg.GrpcServerLogFields.Key)
		pt.Reset(pt.LogCfg.GrpcClientLogFields.Key)
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

func TestObservabilityServerUnary_MiddlewareLogPath(t *testing.T) {
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

func TestObservabilityServerUnary_LogsMilvusCode(t *testing.T) {
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())
	configureObservabilityLogMethodsForTest(t, "/svc/Unary", "")
	configureObservabilityLogFieldsForTest(t, "grpc.code,milvus.code", "")
	intercept := NewObservabilityServerUnaryInterceptor()

	var gotFields []any
	oldLogger := grpcMiddlewareLogger
	grpcMiddlewareLogger = grpc_logging.LoggerFunc(func(ctx context.Context, level grpc_logging.Level, msg string, fields ...any) {
		if msg == "finished call" {
			gotFields = append([]any(nil), fields...)
		}
	})
	t.Cleanup(func() {
		grpcMiddlewareLogger = oldLogger
	})

	wantStatus := merr.Status(merr.ErrCollectionNotFound)
	resp, err := intercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/svc/Unary"}, func(ctx context.Context, req any) (any, error) {
		return wantStatus, nil
	})

	assert.Same(t, wantStatus, resp)
	assert.NoError(t, err)
	assert.Equal(t, codes.OK.String(), fieldValue(gotFields, "grpc.code"))
	assert.Equal(t, wantStatus.GetCode(), fieldValue(gotFields, "milvus.code"))
}

func TestObservabilityServerUnary_SkipsMilvusCodeExtractionWhenFieldIsExcluded(t *testing.T) {
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())
	configureObservabilityLogMethodsForTest(t, "/svc/Unary", "")
	configureObservabilityLogFieldsForTest(t, "grpc.code", "")
	intercept := NewObservabilityServerUnaryInterceptor()

	resp := &testMilvusStatusResponse{status: merr.Status(merr.ErrCollectionNotFound)}
	_, err := intercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/svc/Unary"}, func(ctx context.Context, req any) (any, error) {
		return resp, nil
	})

	assert.NoError(t, err)
	assert.Zero(t, resp.statusReads)
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

func TestObservabilityServerStream_MiddlewareLogPath(t *testing.T) {
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

func TestObservabilityServerStream_FastPath(t *testing.T) {
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())
	configureObservabilityLogMethodsForTest(t, "", "")
	intercept := NewObservabilityServerStreamInterceptor()

	wantErr := errors.New("stream failed")
	handlerCalled := false
	err := intercept(nil, newMockSS(context.Background()), &grpc.StreamServerInfo{FullMethod: "/svc/Stream"}, func(srv any, ss grpc.ServerStream) error {
		handlerCalled = true
		return wantErr
	})

	assert.True(t, handlerCalled)
	assert.ErrorIs(t, err, wantErr)
}

func TestObservabilityClientUnary_MiddlewareLogPath(t *testing.T) {
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

func TestObservabilityClientUnary_LogsLegacyMilvusCode(t *testing.T) {
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())
	configureObservabilityLogMethodsForTest(t, "", "/svc/Unary")
	configureObservabilityLogFieldsForTest(t, "", "grpc.code,milvus.code")
	intercept := NewObservabilityClientUnaryInterceptor()

	var gotFields []any
	oldLogger := grpcMiddlewareLogger
	grpcMiddlewareLogger = grpc_logging.LoggerFunc(func(ctx context.Context, level grpc_logging.Level, msg string, fields ...any) {
		if msg == "finished call" {
			gotFields = append([]any(nil), fields...)
		}
	})
	t.Cleanup(func() {
		grpcMiddlewareLogger = oldLogger
	})

	reply := &testMilvusStatusResponse{}
	err := intercept(context.Background(), "/svc/Unary", nil, reply, nil, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		reply.(*testMilvusStatusResponse).status = &commonpb.Status{ErrorCode: commonpb.ErrorCode_CollectionNotExists}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, codes.OK.String(), fieldValue(gotFields, "grpc.code"))
	assert.Equal(t, merr.Code(merr.OldCodeToMerr(commonpb.ErrorCode_CollectionNotExists)), fieldValue(gotFields, "milvus.code"))
}

func TestObservabilityClientStream_MiddlewareLogPath(t *testing.T) {
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())
	configureObservabilityLogMethodsForTest(t, "", "/svc/Stream")
	intercept := NewObservabilityClientStreamInterceptor()

	var logs []string
	oldLogger := grpcMiddlewareLogger
	grpcMiddlewareLogger = grpc_logging.LoggerFunc(func(ctx context.Context, level grpc_logging.Level, msg string, fields ...any) {
		logs = append(logs, msg)
	})
	t.Cleanup(func() {
		grpcMiddlewareLogger = oldLogger
	})

	streamerCalled := false
	rawStream := &mockClientStream{ctx: context.Background(), recvErr: io.EOF}
	streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		streamerCalled = true
		return rawStream, nil
	}
	cs, err := intercept(context.Background(), &grpc.StreamDesc{}, nil, "/svc/Stream", streamer)

	assert.True(t, streamerCalled)
	assert.NotNil(t, cs)
	assert.NotEqual(t, rawStream, cs)
	assert.NoError(t, err)
	assert.Empty(t, logs)

	assert.ErrorIs(t, cs.RecvMsg(nil), io.EOF)
	assert.Equal(t, []string{"finished call"}, logs)
}

func TestObservabilityClientStream_FastPath(t *testing.T) {
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())
	configureObservabilityLogMethodsForTest(t, "", "")
	intercept := NewObservabilityClientStreamInterceptor()

	rawStream := &mockClientStream{ctx: context.Background()}
	streamerCalled := false
	cs, err := intercept(context.Background(), &grpc.StreamDesc{}, nil, "/svc/Stream", func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		streamerCalled = true
		return rawStream, nil
	})

	assert.True(t, streamerCalled)
	assert.NotNil(t, cs)
	assert.NotSame(t, rawStream, cs)
	assert.NoError(t, err)
}

func TestObservabilityClientStream_UsesConfiguredMiddlewareEvents(t *testing.T) {
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())
	configureObservabilityLogMethodsForTest(t, "", "/svc/Stream")
	configureObservabilityLogEventsForTest(t, "", "payload_sent,payload_received,finish_call")
	intercept := NewObservabilityClientStreamInterceptor()

	var logs []string
	oldLogger := grpcMiddlewareLogger
	grpcMiddlewareLogger = grpc_logging.LoggerFunc(func(ctx context.Context, level grpc_logging.Level, msg string, fields ...any) {
		logs = append(logs, msg)
	})
	t.Cleanup(func() {
		grpcMiddlewareLogger = oldLogger
	})

	rawStream := &mockClientStream{ctx: context.Background(), recvErrs: []error{nil, io.EOF}}
	streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return rawStream, nil
	}
	cs, err := intercept(context.Background(), &grpc.StreamDesc{}, nil, "/svc/Stream", streamer)
	assert.NoError(t, err)

	assert.NoError(t, cs.SendMsg(&emptypb.Empty{}))
	assert.NoError(t, cs.RecvMsg(&emptypb.Empty{}))
	assert.ErrorIs(t, cs.RecvMsg(&emptypb.Empty{}), io.EOF)
	assert.True(t, slices.Contains(logs, "request sent"))
	assert.True(t, slices.Contains(logs, "response received"))
	assert.True(t, slices.Contains(logs, "finished call"))
}

func TestObservabilityClientStream_FiltersMiddlewareFields(t *testing.T) {
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())
	configureObservabilityLogMethodsForTest(t, "", "/svc/Stream")
	configureObservabilityLogEventsForTest(t, "", "payload_sent,payload_received,finish_call")
	configureObservabilityLogFieldsForTest(t, "", "grpc.code,method,dstServerID")
	intercept := NewObservabilityClientStreamInterceptor()

	var gotFields []any
	oldLogger := grpcMiddlewareLogger
	grpcMiddlewareLogger = grpc_logging.LoggerFunc(func(ctx context.Context, level grpc_logging.Level, msg string, fields ...any) {
		if msg == "finished call" {
			gotFields = append([]any(nil), fields...)
		}
	})
	t.Cleanup(func() {
		grpcMiddlewareLogger = oldLogger
	})

	ctx := metadata.AppendToOutgoingContext(context.Background(), ServerIDKey, "42")
	rawStream := &mockClientStream{ctx: context.Background(), recvErr: io.EOF}
	streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return rawStream, nil
	}
	cs, err := intercept(ctx, &grpc.StreamDesc{}, nil, "/svc/Stream", streamer)
	assert.NoError(t, err)
	assert.ErrorIs(t, cs.RecvMsg(&emptypb.Empty{}), io.EOF)

	assertFieldSet(t, gotFields, "grpc.code")
	assertFieldSet(t, gotFields, "method")
	assertFieldSet(t, gotFields, "dstServerID")
	assertFieldNotSet(t, gotFields, "grpc.duration")
	assertFieldNotSet(t, gotFields, "grpc.response.content")
}

func TestObservabilityLogFields_DefaultExcludesPayloadContent(t *testing.T) {
	pt := paramtable.Get()

	serverFields := parseLogFields(pt.LogCfg.GrpcServerLogFields.GetValue())
	clientFields := parseLogFields(pt.LogCfg.GrpcClientLogFields.GetValue())

	assert.NotNil(t, serverFields)
	assert.NotNil(t, clientFields)
	assert.False(t, serverFields.has("grpc.request.content"))
	assert.False(t, serverFields.has("grpc.response.content"))
	assert.False(t, clientFields.has("grpc.request.content"))
	assert.False(t, clientFields.has("grpc.response.content"))
	assert.True(t, serverFields.has("grpc.code"))
	assert.True(t, clientFields.has("grpc.code"))
	assert.True(t, serverFields.has("milvus.code"))
	assert.True(t, clientFields.has("milvus.code"))
}

type mockClientStream struct {
	grpc.ClientStream
	ctx      context.Context
	recvErr  error
	recvErrs []error
}

type testMilvusStatusResponse struct {
	status      *commonpb.Status
	statusReads int
}

type dynamicLogConfigKeys struct {
	level   string
	methods string
	events  string
	fields  string
}

func dynamicLogConfigTestKeys(prefix string) dynamicLogConfigKeys {
	return dynamicLogConfigKeys{
		level:   prefix + ".level",
		methods: prefix + ".methods",
		events:  prefix + ".events",
		fields:  prefix + ".fields",
	}
}

func dynamicLogConfigWatchers(t *testing.T, keys dynamicLogConfigKeys) []config.EventHandler {
	t.Helper()
	dispatcher := paramtable.GetBaseTable().Manager().Dispatcher
	handlers := make([]config.EventHandler, 0, 4)
	for _, key := range []string{keys.level, keys.methods, keys.events, keys.fields} {
		registered := dispatcher.Get(key)
		if len(registered) == 0 {
			t.Fatalf("no watcher registered for %s", key)
		}
		handlers = append(handlers, registered[len(registered)-1])
	}
	return handlers
}

func unregisterDynamicLogConfigWatchers(t *testing.T, keys dynamicLogConfigKeys) {
	t.Helper()
	handlers := dynamicLogConfigWatchers(t, keys)
	t.Cleanup(func() {
		unregisterDynamicLogConfigHandlers(keys, handlers)
	})
}

func unregisterDynamicLogConfigHandlers(keys dynamicLogConfigKeys, handlers []config.EventHandler) {
	pt := paramtable.Get()
	for i, key := range []string{keys.level, keys.methods, keys.events, keys.fields} {
		pt.Unwatch(key, handlers[i])
	}
}

func (r *testMilvusStatusResponse) GetStatus() *commonpb.Status {
	r.statusReads++
	return r.status
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
	if len(s.recvErrs) > 0 {
		err := s.recvErrs[0]
		s.recvErrs = s.recvErrs[1:]
		return err
	}
	return s.recvErr
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

func assertFieldSet(t *testing.T, fields []any, key string) {
	t.Helper()
	assert.True(t, fieldSet(fields, key), key)
}

func assertFieldNotSet(t *testing.T, fields []any, key string) {
	t.Helper()
	assert.False(t, fieldSet(fields, key), key)
}

func fieldSet(fields []any, key string) bool {
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] == key {
			return true
		}
	}
	return false
}

func fieldValue(fields []any, key string) any {
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] == key {
			return fields[i+1]
		}
	}
	return nil
}
