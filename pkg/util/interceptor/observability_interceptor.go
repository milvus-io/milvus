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
	"strings"
	"sync/atomic"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// nodeIDLabels returns the prometheus label set stamped on every RPC metric.
// Kept small because label cardinality multiplies time-series count.
func nodeIDLabels(context.Context) prometheus.Labels {
	return prometheus.Labels{metrics.NodeIDLabelName: paramtable.GetStringNodeID()}
}

// parseZapLevel maps a textual log level to zapcore.Level. Unknown values
// fall through to info so a misconfigured string cannot silence or amplify logs.
func parseZapLevel(s string) zapcore.Level {
	switch strings.ToLower(s) {
	case "debug":
		return zapcore.DebugLevel
	case "warn", "warning":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	case "info", "":
		fallthrough
	default:
		return zapcore.InfoLevel
	}
}

// parseMethodList parses a comma-separated full-method allowlist into a set.
// Returns nil for empty input so the hot path can exit on a nil check.
func parseMethodList(methods string) map[string]struct{} {
	methods = strings.TrimSpace(methods)
	if methods == "" {
		return nil
	}
	result := make(map[string]struct{})
	for _, m := range strings.Split(methods, ",") {
		m = strings.TrimSpace(m)
		if m != "" {
			result[m] = struct{}{}
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// dynamicLogConfig carries the hot-reloadable log level + method allowlist.
// Both fields are read lock-free via atomics so shouldLog adds at most a pointer
// load and a map lookup per RPC on the hot path.
type dynamicLogConfig struct {
	level   atomic.Int32 // zapcore.Level stored as int32
	methods atomic.Pointer[map[string]struct{}]
}

// newDynamicLogConfig seeds the level and allowlist from paramtable and
// registers watchers for hot updates.
func newDynamicLogConfig(levelKey, methodsKey, initialLevel, initialMethods string) *dynamicLogConfig {
	c := &dynamicLogConfig{}
	c.level.Store(int32(parseZapLevel(initialLevel)))
	set := parseMethodList(initialMethods)
	c.methods.Store(&set)

	pt := paramtable.Get()
	pt.Watch(levelKey, config.NewHandler("grpc.log."+levelKey, func(evt *config.Event) {
		if !evt.HasUpdated {
			return
		}
		c.level.Store(int32(parseZapLevel(evt.Value)))
		log.Info("gRPC log level updated", zap.String("key", levelKey), zap.String("value", evt.Value))
	}))
	pt.Watch(methodsKey, config.NewHandler("grpc.log."+methodsKey, func(evt *config.Event) {
		if !evt.HasUpdated {
			return
		}
		s := parseMethodList(evt.Value)
		c.methods.Store(&s)
		log.Info("gRPC log method filter updated", zap.String("key", methodsKey), zap.String("value", evt.Value))
	}))
	return c
}

// shouldLog is the fast allowlist check. Returns ok=false when no methods are
// allow-listed or the current method is not in the allowlist — the default state.
func (c *dynamicLogConfig) shouldLog(fullMethod string) (zapcore.Level, bool) {
	set := c.methods.Load()
	if set == nil || *set == nil {
		return 0, false
	}
	if _, ok := (*set)[fullMethod]; !ok {
		return 0, false
	}
	return zapcore.Level(c.level.Load()), true
}

// emitServerAccessLog writes one structured line via zap using pre-captured
// metadata. The zap Check gate lets us skip field construction entirely when
// the global log level is above lvl (e.g. debug-level RPC logs in a production
// info-level deployment).
func emitServerAccessLog(ctx context.Context, lvl zapcore.Level, method string, err error, duration time.Duration) {
	ce := log.L().WithOptions(zap.AddCallerSkip(1)).Check(lvl, "grpc.server.call")
	if ce == nil {
		return
	}
	sc := trace.SpanContextFromContext(ctx)
	ce.Write(
		zap.String("method", method),
		zap.String("code", status.Code(err).String()),
		zap.Duration("duration", duration),
		zap.Stringer("traceID", sc.TraceID()),
		zap.Stringer("spanID", sc.SpanID()),
		zap.Error(err),
	)
}

// emitClientAccessLog is the client counterpart. The destination server id, if
// present, is injected onto outgoing metadata by ServerIDInjection*ClientInterceptor.
func emitClientAccessLog(ctx context.Context, lvl zapcore.Level, method string, err error, duration time.Duration) {
	ce := log.L().WithOptions(zap.AddCallerSkip(1)).Check(lvl, "grpc.client.call")
	if ce == nil {
		return
	}
	sc := trace.SpanContextFromContext(ctx)
	md, _ := metadata.FromOutgoingContext(ctx)
	var dstServerID string
	if vals := md.Get(ServerIDKey); len(vals) > 0 {
		dstServerID = vals[0]
	}
	ce.Write(
		zap.String("method", method),
		zap.String("code", status.Code(err).String()),
		zap.Duration("duration", duration),
		zap.Stringer("traceID", sc.TraceID()),
		zap.Stringer("spanID", sc.SpanID()),
		zap.String("dstServerID", dstServerID),
		zap.Error(err),
	)
}

// NewObservabilityServerUnaryInterceptor records Prometheus metrics and — when
// the full method is in the server log allowlist — emits a structured access
// log. When no methods are allow-listed (the default), this interceptor is
// equivalent to the bare metrics interceptor plus a single branch.
func NewObservabilityServerUnaryInterceptor() grpc.UnaryServerInterceptor {
	pt := paramtable.Get()
	logCfg := newDynamicLogConfig(
		pt.LogCfg.GrpcServerLogLevel.Key,
		pt.LogCfg.GrpcServerLogMethods.Key,
		pt.LogCfg.GrpcServerLogLevel.GetValue(),
		pt.LogCfg.GrpcServerLogMethods.GetValue(),
	)
	metricsIntercept := metrics.GRPCServerMetric.UnaryServerInterceptor(
		grpcprom.WithLabelsFromContext(nodeIDLabels),
	)

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		lvl, shouldLog := logCfg.shouldLog(info.FullMethod)
		if !shouldLog {
			return metricsIntercept(ctx, req, info, handler)
		}

		// Log-enabled path: capture handler result so we can format an access log.
		start := time.Now()
		var resp any
		var callErr error
		wrapped := func(ctx context.Context, req any) (any, error) {
			resp, callErr = handler(ctx, req)
			return resp, callErr
		}
		_, _ = metricsIntercept(ctx, req, info, wrapped)
		emitServerAccessLog(ctx, lvl, info.FullMethod, callErr, time.Since(start))
		return resp, callErr
	}
}

// NewObservabilityServerStreamInterceptor is the stream counterpart.
// Duration measures the whole stream lifetime, not per message.
func NewObservabilityServerStreamInterceptor() grpc.StreamServerInterceptor {
	pt := paramtable.Get()
	logCfg := newDynamicLogConfig(
		pt.LogCfg.GrpcServerLogLevel.Key,
		pt.LogCfg.GrpcServerLogMethods.Key,
		pt.LogCfg.GrpcServerLogLevel.GetValue(),
		pt.LogCfg.GrpcServerLogMethods.GetValue(),
	)
	metricsIntercept := metrics.GRPCServerMetric.StreamServerInterceptor(
		grpcprom.WithLabelsFromContext(nodeIDLabels),
	)

	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		lvl, shouldLog := logCfg.shouldLog(info.FullMethod)
		if !shouldLog {
			return metricsIntercept(srv, ss, info, handler)
		}

		start := time.Now()
		var callErr error
		wrapped := func(srv any, ss grpc.ServerStream) error {
			callErr = handler(srv, ss)
			return callErr
		}
		_ = metricsIntercept(srv, ss, info, wrapped)
		emitServerAccessLog(ss.Context(), lvl, info.FullMethod, callErr, time.Since(start))
		return callErr
	}
}

// NewObservabilityClientUnaryInterceptor is the unary client counterpart.
func NewObservabilityClientUnaryInterceptor() grpc.UnaryClientInterceptor {
	pt := paramtable.Get()
	logCfg := newDynamicLogConfig(
		pt.LogCfg.GrpcClientLogLevel.Key,
		pt.LogCfg.GrpcClientLogMethods.Key,
		pt.LogCfg.GrpcClientLogLevel.GetValue(),
		pt.LogCfg.GrpcClientLogMethods.GetValue(),
	)
	metricsIntercept := metrics.GRPCClientMetric.UnaryClientInterceptor(
		grpcprom.WithLabelsFromContext(nodeIDLabels),
	)

	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		lvl, shouldLog := logCfg.shouldLog(method)
		if !shouldLog {
			return metricsIntercept(ctx, method, req, reply, cc, invoker, opts...)
		}

		start := time.Now()
		var callErr error
		wrapped := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			callErr = invoker(ctx, method, req, reply, cc, opts...)
			return callErr
		}
		_ = metricsIntercept(ctx, method, req, reply, cc, wrapped, opts...)
		emitClientAccessLog(ctx, lvl, method, callErr, time.Since(start))
		return callErr
	}
}

// NewObservabilityClientStreamInterceptor is the stream client counterpart.
// Duration measures the time to obtain the stream handle, not the stream's
// full lifetime — stream lifetime is not knowable from the interceptor alone.
func NewObservabilityClientStreamInterceptor() grpc.StreamClientInterceptor {
	pt := paramtable.Get()
	logCfg := newDynamicLogConfig(
		pt.LogCfg.GrpcClientLogLevel.Key,
		pt.LogCfg.GrpcClientLogMethods.Key,
		pt.LogCfg.GrpcClientLogLevel.GetValue(),
		pt.LogCfg.GrpcClientLogMethods.GetValue(),
	)
	metricsIntercept := metrics.GRPCClientMetric.StreamClientInterceptor(
		grpcprom.WithLabelsFromContext(nodeIDLabels),
	)

	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		lvl, shouldLog := logCfg.shouldLog(method)
		if !shouldLog {
			return metricsIntercept(ctx, desc, cc, method, streamer, opts...)
		}

		start := time.Now()
		var cs grpc.ClientStream
		var callErr error
		wrapped := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			cs, callErr = streamer(ctx, desc, cc, method, opts...)
			return cs, callErr
		}
		_, _ = metricsIntercept(ctx, desc, cc, method, wrapped, opts...)
		emitClientAccessLog(ctx, lvl, method, callErr, time.Since(start))
		return cs, callErr
	}
}
