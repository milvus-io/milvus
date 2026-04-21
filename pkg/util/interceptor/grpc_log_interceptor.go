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
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// parseLogLevel turns a textual log level into logging.Level.
// Unknown values fall through to info — the interceptor should never panic
// over a misconfigured config string.
func parseLogLevel(level string) logging.Level {
	switch strings.ToLower(level) {
	case "debug":
		return logging.LevelDebug
	case "info":
		return logging.LevelInfo
	case "warn", "warning":
		return logging.LevelWarn
	case "error":
		return logging.LevelError
	default:
		return logging.LevelInfo
	}
}

// parseMethodList parses a comma-separated full-method allowlist into a set.
// Returns nil when the input is empty so callers can short-circuit matcher work.
func parseMethodList(methods string) map[string]struct{} {
	methods = strings.TrimSpace(methods)
	if methods == "" {
		return nil
	}
	result := make(map[string]struct{})
	for _, method := range strings.Split(methods, ",") {
		method = strings.TrimSpace(method)
		if method != "" {
			result[method] = struct{}{}
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// dynamicMethodMatcher is a selector.Matcher backed by an atomic pointer so that
// paramtable.Watch updates never block Match calls on the hot path.
type dynamicMethodMatcher struct {
	methodSet atomic.Pointer[map[string]struct{}]
}

// newDynamicMethodMatcher seeds the matcher from the config's current value and
// registers a watcher that swaps in a new set whenever the value is updated.
func newDynamicMethodMatcher(configKey string, initialValue string) *dynamicMethodMatcher {
	m := &dynamicMethodMatcher{}
	methodSet := parseMethodList(initialValue)
	m.methodSet.Store(&methodSet)

	paramtable.Get().Watch(configKey, config.NewHandler("grpc.log."+configKey, func(evt *config.Event) {
		if !evt.HasUpdated {
			return
		}
		newMethodSet := parseMethodList(evt.Value)
		m.methodSet.Store(&newMethodSet)
		log.Info("gRPC log method filter updated", zap.String("key", configKey), zap.String("value", evt.Value))
	}))
	return m
}

// Match implements selector.Matcher. A nil or empty set means "log nothing".
func (m *dynamicMethodMatcher) Match(_ context.Context, c interceptors.CallMeta) bool {
	methodSet := m.methodSet.Load()
	if methodSet == nil || *methodSet == nil {
		return false
	}
	_, ok := (*methodSet)[c.FullMethod()]
	return ok
}

// dynamicLevelLogger is a logging.Logger with atomically updatable threshold.
// The underlying zap logger is taken fresh from log.L() on each call so that
// global log config changes (e.g. rotating the output sink) are picked up.
type dynamicLevelLogger struct {
	level atomic.Value // logging.Level
}

// newDynamicLevelLogger seeds the level from the current config value and registers
// a watcher for updates.
func newDynamicLevelLogger(configKey string, initialValue string) *dynamicLevelLogger {
	l := &dynamicLevelLogger{}
	l.level.Store(parseLogLevel(initialValue))

	paramtable.Get().Watch(configKey, config.NewHandler("grpc.log."+configKey, func(evt *config.Event) {
		if !evt.HasUpdated {
			return
		}
		l.level.Store(parseLogLevel(evt.Value))
		log.Info("gRPC log level updated", zap.String("key", configKey), zap.String("value", evt.Value))
	}))
	return l
}

// Log implements logging.Logger.
func (l *dynamicLevelLogger) Log(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
	threshold := l.level.Load().(logging.Level)
	if lvl < threshold {
		return
	}

	zf := make([]zap.Field, 0, len(fields)/2)
	for i := 0; i+1 < len(fields); i += 2 {
		key, _ := fields[i].(string)
		switch v := fields[i+1].(type) {
		case string:
			zf = append(zf, zap.String(key, v))
		case int:
			zf = append(zf, zap.Int(key, v))
		case bool:
			zf = append(zf, zap.Bool(key, v))
		default:
			zf = append(zf, zap.Any(key, v))
		}
	}
	logger := log.L().WithOptions(zap.AddCallerSkip(1)).WithLazy(zf...)

	switch lvl {
	case logging.LevelDebug:
		logger.Debug(msg)
	case logging.LevelInfo:
		logger.Info(msg)
	case logging.LevelWarn:
		logger.Warn(msg)
	case logging.LevelError:
		logger.Error(msg)
	default:
		panic(fmt.Sprintf("unknown level %v", lvl))
	}
}

// serverFieldsFromContext extracts logging fields from an incoming request context.
// Server-side emits the local node id as nodeID; the remote (caller) node id is not
// present in the metadata carried by the existing cluster / server-id interceptors.
func serverFieldsFromContext(ctx context.Context) logging.Fields {
	sc := trace.SpanContextFromContext(ctx)
	return logging.Fields{
		"traceID", sc.TraceID(),
		"spanID", sc.SpanID(),
		"nodeID", paramtable.GetStringNodeID(),
	}
}

// clientFieldsFromContext extracts logging fields from an outgoing request context.
// The destination node id, if present, is injected onto outgoing metadata by
// ServerIDInjection*ClientInterceptor as interceptor.ServerIDKey.
func clientFieldsFromContext(ctx context.Context) logging.Fields {
	md, _ := metadata.FromOutgoingContext(ctx)
	var dstNodeID string
	if values := md.Get(ServerIDKey); len(values) > 0 {
		dstNodeID = values[0]
	}
	sc := trace.SpanContextFromContext(ctx)
	return logging.Fields{
		"traceID", sc.TraceID(),
		"spanID", sc.SpanID(),
		"srcNodeID", paramtable.GetStringNodeID(),
		"dstNodeID", dstNodeID,
	}
}

// NewLogUnaryServerInterceptor returns a unary server interceptor that emits a
// structured access log per RPC, filtered by a hot-reloadable method allowlist
// and level. No methods are logged by default.
func NewLogUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	pt := paramtable.Get()
	matcher := newDynamicMethodMatcher(pt.LogCfg.GrpcServerLogMethods.Key, pt.LogCfg.GrpcServerLogMethods.GetValue())
	logger := newDynamicLevelLogger(pt.LogCfg.GrpcServerLogLevel.Key, pt.LogCfg.GrpcServerLogLevel.GetValue())

	logInterceptor := logging.UnaryServerInterceptor(logger,
		logging.WithDisableLoggingFields(logging.SystemTag[0]),
		logging.WithFieldsFromContext(serverFieldsFromContext),
	)
	return selector.UnaryServerInterceptor(logInterceptor, matcher)
}

// NewLogStreamServerInterceptor is the stream counterpart of NewLogUnaryServerInterceptor.
func NewLogStreamServerInterceptor() grpc.StreamServerInterceptor {
	pt := paramtable.Get()
	matcher := newDynamicMethodMatcher(pt.LogCfg.GrpcServerLogMethods.Key, pt.LogCfg.GrpcServerLogMethods.GetValue())
	logger := newDynamicLevelLogger(pt.LogCfg.GrpcServerLogLevel.Key, pt.LogCfg.GrpcServerLogLevel.GetValue())

	logInterceptor := logging.StreamServerInterceptor(logger,
		logging.WithDisableLoggingFields(logging.SystemTag[0]),
		logging.WithFieldsFromContext(serverFieldsFromContext),
	)
	return selector.StreamServerInterceptor(logInterceptor, matcher)
}

// NewLogClientUnaryInterceptor returns a unary client interceptor that emits a
// structured access log per outgoing RPC, filtered by a hot-reloadable allowlist.
func NewLogClientUnaryInterceptor() grpc.UnaryClientInterceptor {
	pt := paramtable.Get()
	matcher := newDynamicMethodMatcher(pt.LogCfg.GrpcClientLogMethods.Key, pt.LogCfg.GrpcClientLogMethods.GetValue())
	logger := newDynamicLevelLogger(pt.LogCfg.GrpcClientLogLevel.Key, pt.LogCfg.GrpcClientLogLevel.GetValue())

	logInterceptor := logging.UnaryClientInterceptor(logger,
		logging.WithDisableLoggingFields(logging.SystemTag[0]),
		logging.WithFieldsFromContext(clientFieldsFromContext),
	)
	return selector.UnaryClientInterceptor(logInterceptor, matcher)
}

// NewLogClientStreamInterceptor is the stream counterpart of NewLogClientUnaryInterceptor.
func NewLogClientStreamInterceptor() grpc.StreamClientInterceptor {
	pt := paramtable.Get()
	matcher := newDynamicMethodMatcher(pt.LogCfg.GrpcClientLogMethods.Key, pt.LogCfg.GrpcClientLogMethods.GetValue())
	logger := newDynamicLevelLogger(pt.LogCfg.GrpcClientLogLevel.Key, pt.LogCfg.GrpcClientLogLevel.GetValue())

	logInterceptor := logging.StreamClientInterceptor(logger,
		logging.WithDisableLoggingFields(logging.SystemTag[0]),
		logging.WithFieldsFromContext(clientFieldsFromContext),
	)
	return selector.StreamClientInterceptor(logInterceptor, matcher)
}
