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
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	grpc_interceptors "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// nodeIDLabels returns the prometheus label set stamped on every RPC metric.
// Kept small because label cardinality multiplies time-series count.
func nodeIDLabels(context.Context) prometheus.Labels {
	return prometheus.Labels{metrics.NodeIDLabelName: paramtable.GetStringNodeID()}
}

// parseLogLevel maps a textual log level to mlog.Level. Unknown values
// fall through to info so a misconfigured string cannot silence or amplify logs.
func parseLogLevel(s string) mlog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return mlog.DebugLevel
	case "warn", "warning":
		return mlog.WarnLevel
	case "error":
		return mlog.ErrorLevel
	case "info", "":
		fallthrough
	default:
		return mlog.InfoLevel
	}
}

const regexMethodPrefix = "re:"

type methodFilter struct {
	exact  map[string]struct{}
	regexs []*regexp.Regexp
}

type logEvents []grpc_logging.LoggableEvent

type fieldFilter struct {
	fields map[string]struct{}
}

type milvusStatusResponse interface {
	GetStatus() *commonpb.Status
}

// parseMethodFilter parses a comma-separated full-method allowlist. Plain
// entries are matched exactly; entries prefixed with "re:" are Go regexps.
// Returns nil for empty input so the hot path can exit on a nil check.
func parseMethodFilter(methods string) (*methodFilter, []string) {
	methods = strings.TrimSpace(methods)
	if methods == "" {
		return nil, nil
	}
	filter := &methodFilter{}
	var invalidRegexs []string
	for _, m := range strings.Split(methods, ",") {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		if pattern, ok := strings.CutPrefix(m, regexMethodPrefix); ok {
			re, err := regexp.Compile(pattern)
			if err != nil {
				invalidRegexs = append(invalidRegexs, m)
				continue
			}
			filter.regexs = append(filter.regexs, re)
			continue
		}
		if filter.exact == nil {
			filter.exact = make(map[string]struct{})
		}
		filter.exact[m] = struct{}{}
	}
	if len(filter.exact) == 0 && len(filter.regexs) == 0 {
		return nil, invalidRegexs
	}
	return filter, invalidRegexs
}

func parseLogEvents(events string) ([]grpc_logging.LoggableEvent, []string) {
	events = strings.TrimSpace(events)
	if events == "" {
		return []grpc_logging.LoggableEvent{grpc_logging.FinishCall}, nil
	}

	var logEvents []grpc_logging.LoggableEvent
	var invalidEvents []string
	seen := make(map[grpc_logging.LoggableEvent]struct{}, 4)
	for _, e := range strings.Split(events, ",") {
		e = strings.TrimSpace(strings.ToLower(e))
		if e == "" {
			continue
		}
		var event grpc_logging.LoggableEvent
		valid := true
		switch e {
		case "start_call":
			event = grpc_logging.StartCall
		case "finish_call":
			event = grpc_logging.FinishCall
		case "payload_received":
			event = grpc_logging.PayloadReceived
		case "payload_sent":
			event = grpc_logging.PayloadSent
		default:
			valid = false
			invalidEvents = append(invalidEvents, e)
		}
		if !valid {
			continue
		}
		if _, ok := seen[event]; ok {
			continue
		}
		seen[event] = struct{}{}
		logEvents = append(logEvents, event)
	}
	if len(logEvents) == 0 {
		logEvents = []grpc_logging.LoggableEvent{grpc_logging.FinishCall}
	}
	return logEvents, invalidEvents
}

func parseLogFields(fields string) *fieldFilter {
	fields = strings.TrimSpace(fields)
	if fields == "" {
		return nil
	}
	filter := &fieldFilter{fields: make(map[string]struct{})}
	for _, field := range strings.Split(fields, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		filter.fields[field] = struct{}{}
	}
	if len(filter.fields) == 0 {
		return nil
	}
	return filter
}

func (f *fieldFilter) has(field string) bool {
	if f == nil {
		return true
	}
	_, ok := f.fields[field]
	return ok
}

func (f *fieldFilter) len() int {
	if f == nil {
		return 0
	}
	return len(f.fields)
}

// dynamicLogConfig carries the hot-reloadable log level + method allowlist.
// Both fields are read lock-free via atomics so shouldLog adds at most a pointer
// load and a map lookup per RPC on the hot path.
type dynamicLogConfig struct {
	level   atomic.Int32 // mlog.Level stored as int32
	methods atomic.Pointer[methodFilter]
	events  atomic.Pointer[logEvents]
	fields  atomic.Pointer[fieldFilter]
}

var (
	serverDynamicLogConfigOnce sync.Once
	serverDynamicLogConfig     *dynamicLogConfig
	clientDynamicLogConfigOnce sync.Once
	clientDynamicLogConfig     *dynamicLogConfig
	grpcMiddlewareLogger       grpc_logging.Logger = grpc_logging.LoggerFunc(logGRPCMiddlewareEvent)
)

func (c *dynamicLogConfig) updateMethods(methodsKey string, methods string) bool {
	filter, invalidRegexs := parseMethodFilter(methods)
	if len(invalidRegexs) > 0 {
		mlog.Warn(context.TODO(), "ignore invalid gRPC log method regex",
			mlog.String("key", methodsKey),
			mlog.Strings("methods", invalidRegexs),
		)
		return false
	}
	c.methods.Store(filter)
	return true
}

func (c *dynamicLogConfig) updateEvents(eventsKey string, events string) bool {
	parsed, invalidEvents := parseLogEvents(events)
	if len(invalidEvents) > 0 {
		mlog.Warn(context.TODO(), "ignore invalid gRPC log event",
			mlog.String("key", eventsKey),
			mlog.Strings("events", invalidEvents),
		)
		return false
	}
	c.storeLogEvents(parsed)
	return true
}

func (c *dynamicLogConfig) updateFields(fieldsKey string, fields string) {
	c.fields.Store(parseLogFields(fields))
}

func (c *dynamicLogConfig) storeLogEvents(events []grpc_logging.LoggableEvent) {
	stored := logEvents(events)
	c.events.Store(&stored)
}

func (c *dynamicLogConfig) logEvents() []grpc_logging.LoggableEvent {
	events := c.events.Load()
	if events == nil {
		return []grpc_logging.LoggableEvent{grpc_logging.FinishCall}
	}
	return []grpc_logging.LoggableEvent(*events)
}

func (c *dynamicLogConfig) logFields() *fieldFilter {
	return c.fields.Load()
}

// newDynamicLogConfig seeds the level and allowlist from paramtable and
// registers watchers for hot updates.
func newDynamicLogConfig(levelKey, methodsKey, eventsKey, fieldsKey, initialLevel, initialMethods, initialEvents, initialFields string) *dynamicLogConfig {
	c := &dynamicLogConfig{}
	c.level.Store(int32(parseLogLevel(initialLevel)))
	filter, invalidRegexs := parseMethodFilter(initialMethods)
	c.methods.Store(filter)
	if len(invalidRegexs) > 0 {
		mlog.Warn(context.TODO(), "ignore invalid gRPC log method regex",
			mlog.String("key", methodsKey),
			mlog.Strings("methods", invalidRegexs),
		)
	}
	events, invalidEvents := parseLogEvents(initialEvents)
	c.storeLogEvents(events)
	if len(invalidEvents) > 0 {
		mlog.Warn(context.TODO(), "ignore invalid gRPC log event",
			mlog.String("key", eventsKey),
			mlog.Strings("events", invalidEvents),
		)
	}
	c.updateFields(fieldsKey, initialFields)

	pt := paramtable.Get()
	pt.Watch(levelKey, config.NewHandler("grpc.log."+levelKey, func(evt *config.Event) {
		if !evt.HasUpdated {
			return
		}
		c.level.Store(int32(parseLogLevel(evt.Value)))
		mlog.Info(context.TODO(), "gRPC log level updated", mlog.String("key", levelKey), mlog.String("value", evt.Value))
	}))
	pt.Watch(methodsKey, config.NewHandler("grpc.log."+methodsKey, func(evt *config.Event) {
		if !evt.HasUpdated {
			return
		}
		if !c.updateMethods(methodsKey, evt.Value) {
			return
		}
		mlog.Info(context.TODO(), "gRPC log method filter updated", mlog.String("key", methodsKey), mlog.String("value", evt.Value))
	}))
	pt.Watch(eventsKey, config.NewHandler("grpc.log."+eventsKey, func(evt *config.Event) {
		if !evt.HasUpdated {
			return
		}
		if !c.updateEvents(eventsKey, evt.Value) {
			return
		}
		mlog.Info(context.TODO(), "gRPC log events updated", mlog.String("key", eventsKey), mlog.String("value", evt.Value))
	}))
	pt.Watch(fieldsKey, config.NewHandler("grpc.log."+fieldsKey, func(evt *config.Event) {
		if !evt.HasUpdated {
			return
		}
		c.updateFields(fieldsKey, evt.Value)
		mlog.Info(context.TODO(), "gRPC log fields updated", mlog.String("key", fieldsKey), mlog.String("value", evt.Value))
	}))
	return c
}

func getServerDynamicLogConfig() *dynamicLogConfig {
	serverDynamicLogConfigOnce.Do(func() {
		pt := paramtable.Get()
		serverDynamicLogConfig = newDynamicLogConfig(
			pt.LogCfg.GrpcServerLogLevel.Key,
			pt.LogCfg.GrpcServerLogMethods.Key,
			pt.LogCfg.GrpcServerLogEvents.Key,
			pt.LogCfg.GrpcServerLogFields.Key,
			pt.LogCfg.GrpcServerLogLevel.GetValue(),
			pt.LogCfg.GrpcServerLogMethods.GetValue(),
			pt.LogCfg.GrpcServerLogEvents.GetValue(),
			pt.LogCfg.GrpcServerLogFields.GetValue(),
		)
	})
	return serverDynamicLogConfig
}

func getClientDynamicLogConfig() *dynamicLogConfig {
	clientDynamicLogConfigOnce.Do(func() {
		pt := paramtable.Get()
		clientDynamicLogConfig = newDynamicLogConfig(
			pt.LogCfg.GrpcClientLogLevel.Key,
			pt.LogCfg.GrpcClientLogMethods.Key,
			pt.LogCfg.GrpcClientLogEvents.Key,
			pt.LogCfg.GrpcClientLogFields.Key,
			pt.LogCfg.GrpcClientLogLevel.GetValue(),
			pt.LogCfg.GrpcClientLogMethods.GetValue(),
			pt.LogCfg.GrpcClientLogEvents.GetValue(),
			pt.LogCfg.GrpcClientLogFields.GetValue(),
		)
	})
	return clientDynamicLogConfig
}

// shouldLog is the fast allowlist check. Returns ok=false when no methods are
// allow-listed or the current method is not in the allowlist — the default state.
func (c *dynamicLogConfig) shouldLog(fullMethod string) (mlog.Level, bool) {
	filter := c.methods.Load()
	if filter == nil {
		return 0, false
	}
	if _, ok := filter.exact[fullMethod]; ok {
		return mlog.Level(c.level.Load()), true
	}
	for _, re := range filter.regexs {
		if re.MatchString(fullMethod) {
			return mlog.Level(c.level.Load()), true
		}
	}
	return 0, false
}

func toGRPCLogLevel(level mlog.Level) grpc_logging.Level {
	switch level {
	case mlog.DebugLevel:
		return grpc_logging.LevelDebug
	case mlog.WarnLevel:
		return grpc_logging.LevelWarn
	case mlog.ErrorLevel:
		return grpc_logging.LevelError
	case mlog.InfoLevel:
		fallthrough
	default:
		return grpc_logging.LevelInfo
	}
}

func toMlogLevel(level grpc_logging.Level) mlog.Level {
	switch level {
	case grpc_logging.LevelDebug:
		return mlog.DebugLevel
	case grpc_logging.LevelWarn:
		return mlog.WarnLevel
	case grpc_logging.LevelError:
		return mlog.ErrorLevel
	case grpc_logging.LevelInfo:
		fallthrough
	default:
		return mlog.InfoLevel
	}
}

func middlewareLogOptions(level mlog.Level, events []grpc_logging.LoggableEvent) []grpc_logging.Option {
	return []grpc_logging.Option{
		grpc_logging.WithLogOnEvents(events...),
		grpc_logging.WithLevels(func(codes.Code) grpc_logging.Level {
			return toGRPCLogLevel(level)
		}),
		grpc_logging.WithDurationField(grpc_logging.DurationToDurationField),
		grpc_logging.WithFieldsFromContextAndCallMeta(middlewareLogFieldsFromContext),
	}
}

func middlewareLogger(fields *fieldFilter) grpc_logging.Logger {
	return grpc_logging.LoggerFunc(func(ctx context.Context, level grpc_logging.Level, msg string, logFields ...any) {
		grpcMiddlewareLogger.Log(ctx, level, msg, filterMiddlewareFields(logFields, fields)...)
	})
}

func filterMiddlewareFields(fields []any, allowlist *fieldFilter) []any {
	if allowlist == nil {
		return fields
	}
	filtered := make([]any, 0, len(fields))
	for i := 0; i+1 < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok || !allowlist.has(key) {
			continue
		}
		filtered = append(filtered, fields[i], fields[i+1])
	}
	return filtered
}

func middlewareLogFieldsFromContext(ctx context.Context, call grpc_interceptors.CallMeta) grpc_logging.Fields {
	fields := grpc_logging.Fields{"method", call.FullMethod()}
	if !call.IsClient {
		return fields
	}

	var dstServerID string
	md, _ := metadata.FromOutgoingContext(ctx)
	if vals := md.Get(ServerIDKey); len(vals) > 0 {
		dstServerID = vals[0]
	}
	return append(fields, "dstServerID", dstServerID)
}

func injectMilvusCode(ctx context.Context, resp any) {
	status := extractMilvusStatus(resp)
	if status == nil {
		return
	}

	code := status.GetCode()
	if code == 0 && status.GetErrorCode() != commonpb.ErrorCode_Success {
		code = merr.Code(merr.OldCodeToMerr(status.GetErrorCode()))
	}
	grpc_logging.AddFields(ctx, grpc_logging.Fields{"milvus.code", code})
}

func extractMilvusStatus(resp any) *commonpb.Status {
	switch resp := resp.(type) {
	case *commonpb.Status:
		return resp
	case milvusStatusResponse:
		return resp.GetStatus()
	default:
		return nil
	}
}

func logGRPCMiddlewareEvent(ctx context.Context, level grpc_logging.Level, msg string, fields ...any) {
	lvl := toMlogLevel(level)
	if !mlog.LevelEnabled(lvl) {
		return
	}

	logFields := make([]mlog.Field, 0, len(fields)/2)
	for i := 0; i+1 < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			continue
		}
		val := fields[i+1]
		switch key {
		case "grpc.duration":
			logFields = appendDurationField(logFields, key, val)
		default:
			logFields = append(logFields, mlog.Any(key, val))
		}
	}

	mlog.WithOptions(mlog.AddCallerSkip(1)).Log(
		ctx,
		lvl,
		msg,
		logFields...,
	)
}

func appendDurationField(fields []mlog.Field, key string, value any) []mlog.Field {
	switch v := value.(type) {
	case time.Duration:
		return append(fields, mlog.Duration(key, v))
	case string:
		if d, err := time.ParseDuration(v); err == nil {
			return append(fields, mlog.Duration(key, d))
		}
		return append(fields, mlog.String(key, v))
	default:
		return append(fields, mlog.String(key, fmt.Sprint(v)))
	}
}

// NewObservabilityServerUnaryInterceptor records Prometheus metrics and — when
// the full method is in the server log allowlist — emits gRPC middleware log
// events. When no methods are allow-listed (the default), this interceptor is
// equivalent to the bare metrics interceptor plus a single branch.
func NewObservabilityServerUnaryInterceptor() grpc.UnaryServerInterceptor {
	logCfg := getServerDynamicLogConfig()
	metricsIntercept := metrics.GRPCServerMetric.UnaryServerInterceptor(
		grpcprom.WithLabelsFromContext(nodeIDLabels),
	)

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		lvl, shouldLog := logCfg.shouldLog(info.FullMethod)
		if !shouldLog || !mlog.LevelEnabled(lvl) {
			return metricsIntercept(ctx, req, info, handler)
		}

		logFields := logCfg.logFields()
		logIntercept := grpc_logging.UnaryServerInterceptor(middlewareLogger(logFields), middlewareLogOptions(lvl, logCfg.logEvents())...)
		return logIntercept(ctx, req, info, func(ctx context.Context, req any) (any, error) {
			resp, err := metricsIntercept(ctx, req, info, handler)
			if logFields.has("milvus.code") {
				injectMilvusCode(ctx, resp)
			}
			return resp, err
		})
	}
}

// NewObservabilityServerStreamInterceptor is the stream counterpart.
// Duration measures the whole stream lifetime, not per message.
func NewObservabilityServerStreamInterceptor() grpc.StreamServerInterceptor {
	logCfg := getServerDynamicLogConfig()
	metricsIntercept := metrics.GRPCServerMetric.StreamServerInterceptor(
		grpcprom.WithLabelsFromContext(nodeIDLabels),
	)

	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		lvl, shouldLog := logCfg.shouldLog(info.FullMethod)
		if !shouldLog || !mlog.LevelEnabled(lvl) {
			return metricsIntercept(srv, ss, info, handler)
		}

		logIntercept := grpc_logging.StreamServerInterceptor(middlewareLogger(logCfg.logFields()), middlewareLogOptions(lvl, logCfg.logEvents())...)
		return logIntercept(srv, ss, info, func(srv any, ss grpc.ServerStream) error {
			return metricsIntercept(srv, ss, info, handler)
		})
	}
}

// NewObservabilityClientUnaryInterceptor is the unary client counterpart.
func NewObservabilityClientUnaryInterceptor() grpc.UnaryClientInterceptor {
	logCfg := getClientDynamicLogConfig()
	metricsIntercept := metrics.GRPCClientMetric.UnaryClientInterceptor()

	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		lvl, shouldLog := logCfg.shouldLog(method)
		if !shouldLog || !mlog.LevelEnabled(lvl) {
			return metricsIntercept(ctx, method, req, reply, cc, invoker, opts...)
		}

		logFields := logCfg.logFields()
		logIntercept := grpc_logging.UnaryClientInterceptor(middlewareLogger(logFields), middlewareLogOptions(lvl, logCfg.logEvents())...)
		return logIntercept(ctx, method, req, reply, cc, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			err := metricsIntercept(ctx, method, req, reply, cc, invoker, opts...)
			if logFields.has("milvus.code") {
				injectMilvusCode(ctx, reply)
			}
			return err
		}, opts...)
	}
}

// NewObservabilityClientStreamInterceptor is the stream client counterpart.
// Duration is reported by the logging middleware when stream creation fails or
// when RecvMsg observes terminal stream completion.
func NewObservabilityClientStreamInterceptor() grpc.StreamClientInterceptor {
	logCfg := getClientDynamicLogConfig()
	metricsIntercept := metrics.GRPCClientMetric.StreamClientInterceptor()

	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		lvl, shouldLog := logCfg.shouldLog(method)
		if !shouldLog || !mlog.LevelEnabled(lvl) {
			return metricsIntercept(ctx, desc, cc, method, streamer, opts...)
		}

		logIntercept := grpc_logging.StreamClientInterceptor(middlewareLogger(logCfg.logFields()), middlewareLogOptions(lvl, logCfg.logEvents())...)
		return logIntercept(ctx, desc, cc, method, func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return metricsIntercept(ctx, desc, cc, method, streamer, opts...)
		}, opts...)
	}
}
