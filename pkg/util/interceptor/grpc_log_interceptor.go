package interceptor

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/mcontext"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// parseLogLevel parses the log level string to logging.Level.
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

// parseMethodList parses the comma-separated method list to a set.
// Returns nil if the input is empty (meaning log no methods by default).
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

// dynamicMethodMatcher is a selector.Matcher that supports dynamic configuration updates.
// It uses atomic operations to ensure non-blocking read access to the cached method set.
// Configuration updates are handled in the background via paramtable.Watch.
type dynamicMethodMatcher struct {
	methodSet atomic.Pointer[map[string]struct{}]
}

// newDynamicMethodMatcher creates a new dynamicMethodMatcher that watches the given config key.
// It initializes the method set from the current config value and registers a watcher for updates.
func newDynamicMethodMatcher(configKey string, initialValue string) *dynamicMethodMatcher {
	m := &dynamicMethodMatcher{}

	// Initialize with current config value
	methodSet := parseMethodList(initialValue)
	m.methodSet.Store(&methodSet)

	// Register watcher for config updates
	paramtable.Get().Watch(configKey, config.NewHandler("grpc.log."+configKey, func(evt *config.Event) {
		if evt.HasUpdated {
			newMethodSet := parseMethodList(evt.Value)
			m.methodSet.Store(&newMethodSet)
			log.Info("gRPC log method filter updated", zap.String("key", configKey), zap.String("value", evt.Value))
		}
	}))

	return m
}

// Match implements selector.Matcher interface.
// It reads the cached method set atomically without blocking.
func (m *dynamicMethodMatcher) Match(_ context.Context, c interceptors.CallMeta) bool {
	methodSet := m.methodSet.Load()
	if methodSet == nil || *methodSet == nil {
		return false
	}
	_, ok := (*methodSet)[c.FullMethod()]
	return ok
}

// dynamicLevelLogger is a logging.Logger that supports dynamic log level configuration.
// Log level updates are handled in the background via paramtable.Watch.
type dynamicLevelLogger struct {
	level atomic.Value // logging.Level
}

// newDynamicLevelLogger creates a new dynamicLevelLogger that watches the given config key.
// It initializes the log level from the current config value and registers a watcher for updates.
func newDynamicLevelLogger(configKey string, initialValue string) *dynamicLevelLogger {
	l := &dynamicLevelLogger{}

	// Initialize with current config value
	l.level.Store(parseLogLevel(initialValue))

	// Register watcher for config updates
	paramtable.Get().Watch(configKey, config.NewHandler("grpc.log."+configKey, func(evt *config.Event) {
		if evt.HasUpdated {
			newLevel := parseLogLevel(evt.Value)
			l.level.Store(newLevel)
			log.Info("gRPC log level updated", zap.String("key", configKey), zap.String("value", evt.Value))
		}
	}))

	return l
}

// Log implements logging.Logger interface.
// It reads the cached log level atomically without blocking.
func (l *dynamicLevelLogger) Log(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
	// Read cached log level (non-blocking)
	threshold := l.level.Load().(logging.Level)

	// Skip if the message level is below the configured level
	if lvl < threshold {
		return
	}

	f := make([]zap.Field, 0, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		key := fields[i]
		value := fields[i+1]

		switch v := value.(type) {
		case string:
			f = append(f, zap.String(key.(string), v))
		case int:
			f = append(f, zap.Int(key.(string), v))
		case bool:
			f = append(f, zap.Bool(key.(string), v))
		default:
			f = append(f, zap.Any(key.(string), v))
		}
	}
	logger := log.L().WithOptions(zap.AddCallerSkip(1)).WithLazy(f...)

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

// NewLogUnaryServerInterceptor is a grpc interceptor that adds traceID to the log fields.
// It supports dynamic configuration updates for log level and method filter via paramtable.Watch.
func NewLogUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	pt := paramtable.Get()
	matcher := newDynamicMethodMatcher(
		pt.LogCfg.GrpcServerLogMethods.Key,
		pt.LogCfg.GrpcServerLogMethods.GetValue(),
	)
	logger := newDynamicLevelLogger(
		pt.LogCfg.GrpcServerLogLevel.Key,
		pt.LogCfg.GrpcServerLogLevel.GetValue(),
	)

	logInterceptor := logging.UnaryServerInterceptor(logger,
		logging.WithDisableLoggingFields(logging.SystemTag[0]),
		logging.WithFieldsFromContext(func(ctx context.Context) logging.Fields {
			mctx := mcontext.FromContext(ctx)
			sctx := trace.SpanContextFromContext(ctx)
			return logging.Fields{
				"traceID", sctx.TraceID(),
				"spanID", sctx.SpanID(),
				"srcNodeID", mctx.SourceNodeID,
				"dstNodeID", mctx.DestinationNodeID,
			}
		}),
	)

	return selector.UnaryServerInterceptor(logInterceptor, matcher)
}

// NewLogStreamServerInterceptor is a grpc interceptor that adds traceID to the log fields.
// It supports dynamic configuration updates for log level and method filter via paramtable.Watch.
func NewLogStreamServerInterceptor() grpc.StreamServerInterceptor {
	pt := paramtable.Get()
	matcher := newDynamicMethodMatcher(
		pt.LogCfg.GrpcServerLogMethods.Key,
		pt.LogCfg.GrpcServerLogMethods.GetValue(),
	)
	logger := newDynamicLevelLogger(
		pt.LogCfg.GrpcServerLogLevel.Key,
		pt.LogCfg.GrpcServerLogLevel.GetValue(),
	)

	logInterceptor := logging.StreamServerInterceptor(logger,
		logging.WithDisableLoggingFields(logging.SystemTag[0]),
		logging.WithFieldsFromContext(func(ctx context.Context) logging.Fields {
			mctx := mcontext.FromContext(ctx)
			sctx := trace.SpanContextFromContext(ctx)
			return logging.Fields{
				"traceID", sctx.TraceID(),
				"spanID", sctx.SpanID(),
				"srcNodeID", mctx.SourceNodeID,
				"dstNodeID", mctx.DestinationNodeID,
			}
		}),
	)

	return selector.StreamServerInterceptor(logInterceptor, matcher)
}

// NewLogClientUnaryInterceptor is a grpc interceptor that adds traceID to the log fields.
// It supports dynamic configuration updates for log level and method filter via paramtable.Watch.
func NewLogClientUnaryInterceptor() grpc.UnaryClientInterceptor {
	pt := paramtable.Get()
	matcher := newDynamicMethodMatcher(
		pt.LogCfg.GrpcClientLogMethods.Key,
		pt.LogCfg.GrpcClientLogMethods.GetValue(),
	)
	logger := newDynamicLevelLogger(
		pt.LogCfg.GrpcClientLogLevel.Key,
		pt.LogCfg.GrpcClientLogLevel.GetValue(),
	)

	logInterceptor := logging.UnaryClientInterceptor(logger,
		logging.WithDisableLoggingFields(logging.SystemTag[0]),
		logging.WithFieldsFromContext(func(ctx context.Context) logging.Fields {
			md, _ := metadata.FromOutgoingContext(ctx)
			dstNodeID := md.Get(mcontext.DestinationServerIDKey)
			var dstNodeIDString string
			if len(dstNodeID) > 0 {
				dstNodeIDString = dstNodeID[0]
			}
			sctx := trace.SpanContextFromContext(ctx)
			return logging.Fields{
				"traceID", sctx.TraceID(),
				"spanID", sctx.SpanID(),
				"srcNodeID", paramtable.GetStringNodeID(),
				"dstNodeID", dstNodeIDString,
			}
		}),
	)

	return selector.UnaryClientInterceptor(logInterceptor, matcher)
}

// NewLogClientStreamInterceptor is a grpc interceptor that adds traceID to the log fields.
// It supports dynamic configuration updates for log level and method filter via paramtable.Watch.
func NewLogClientStreamInterceptor() grpc.StreamClientInterceptor {
	pt := paramtable.Get()
	matcher := newDynamicMethodMatcher(
		pt.LogCfg.GrpcClientLogMethods.Key,
		pt.LogCfg.GrpcClientLogMethods.GetValue(),
	)
	logger := newDynamicLevelLogger(
		pt.LogCfg.GrpcClientLogLevel.Key,
		pt.LogCfg.GrpcClientLogLevel.GetValue(),
	)

	logInterceptor := logging.StreamClientInterceptor(logger,
		logging.WithDisableLoggingFields(logging.SystemTag[0]),
		logging.WithFieldsFromContext(func(ctx context.Context) logging.Fields {
			md, _ := metadata.FromOutgoingContext(ctx)
			dstNodeID := md.Get(mcontext.DestinationServerIDKey)
			var dstNodeIDString string
			if len(dstNodeID) > 0 {
				dstNodeIDString = dstNodeID[0]
			}
			sctx := trace.SpanContextFromContext(ctx)
			return logging.Fields{
				"traceID", sctx.TraceID(),
				"spanID", sctx.SpanID(),
				"srcNodeID", paramtable.GetStringNodeID(),
				"dstNodeID", dstNodeIDString,
			}
		}),
	)

	return selector.StreamClientInterceptor(logInterceptor, matcher)
}
