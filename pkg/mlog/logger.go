package mlog

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

var (
	// globalLogger is the package-level logger
	globalLogger atomic.Pointer[zap.Logger]

	// nilContextField is added when nil context is passed
	nilContextField = zap.Bool("_ctx_nil", true)
)

func init() {
	logger, props := newStdLogger()
	ReplaceGlobals(logger, props)
}

// initGlobalLogger replaces the global logger with the provided one.
// The caller is responsible for configuring the logger.
// AddCallerSkip(1) is automatically applied.
func initGlobalLogger(logger *zap.Logger) {
	globalLogger.Store(logger.WithOptions(zap.AddCallerSkip(1)))
}

// getLogger returns the current global logger
func getLogger() *zap.Logger {
	return globalLogger.Load()
}

func appendTraceFields(ctx context.Context, fields []Field) []Field {
	spanCtx := trace.SpanContextFromContext(ctx)
	hasTraceID := spanCtx.HasTraceID()
	hasSpanID := spanCtx.HasSpanID()
	if !hasTraceID && !hasSpanID {
		return fields
	}

	allFields := make([]Field, 0, len(fields)+2)
	allFields = append(allFields, fields...)
	if hasTraceID {
		allFields = append(allFields, FieldTraceID(spanCtx.TraceID().String()))
	}
	if hasSpanID {
		allFields = append(allFields, FieldSpanID(spanCtx.SpanID().String()))
	}
	return allFields
}

// prepareLog resolves the logger and fields from context for package-level functions.
// It returns before the actual log call, so it does not appear in the call stack
// when zap captures the caller.
func prepareLog(ctx context.Context, fields []Field) (*zap.Logger, []Field) {
	if ctx == nil {
		// Safe: fields originates from variadic ...Field, so its cap == len;
		// append always allocates a new backing array here.
		return getLogger(), append(fields, nilContextField)
	}

	lc := getLogContext(ctx)
	if lc.logger != nil {
		return lc.logger, appendTraceFields(ctx, fields)
	}

	logger := getLogger()
	ctxFields := lc.getFields()
	if len(ctxFields) > 0 {
		fields = append(ctxFields, fields...)
	}

	return logger, appendTraceFields(ctx, fields)
}

// Log logs a message at the specified level.
func Log(ctx context.Context, level Level, msg string, fields ...Field) {
	if !currentLevel().Enabled(level) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Log(level, msg, fields...)
}

// Debug logs a message at debug level.
func Debug(ctx context.Context, msg string, fields ...Field) {
	if !currentLevel().Enabled(DebugLevel) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Debug(msg, fields...)
}

// Info logs a message at info level.
func Info(ctx context.Context, msg string, fields ...Field) {
	if !currentLevel().Enabled(InfoLevel) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Info(msg, fields...)
}

// Warn logs a message at warn level.
func Warn(ctx context.Context, msg string, fields ...Field) {
	if !currentLevel().Enabled(WarnLevel) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Warn(msg, fields...)
}

// Error logs a message at error level.
func Error(ctx context.Context, msg string, fields ...Field) {
	if !currentLevel().Enabled(ErrorLevel) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Error(msg, fields...)
}

// DPanic logs a message at dpanic level.
// In development mode, the logger then panics. (See DPanicLevel for details.)
func DPanic(ctx context.Context, msg string, fields ...Field) {
	if !currentLevel().Enabled(DPanicLevel) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.DPanic(msg, fields...)
}

// Panic logs a message at panic level, then panics.
func Panic(ctx context.Context, msg string, fields ...Field) {
	logger, fields := prepareLog(ctx, fields)
	logger.Panic(msg, fields...)
}

// Fatal logs a message at fatal level, then calls os.Exit(1).
func Fatal(ctx context.Context, msg string, fields ...Field) {
	logger, fields := prepareLog(ctx, fields)
	logger.Fatal(msg, fields...)
}

// Logger is a component-level logger with pre-configured fields.
// It optimizes logging by selecting the logger with more pre-encoded fields
// when combining with context fields.
type Logger struct {
	logger *zap.Logger // pre-encoded with component fields
	fields []Field     // copy of component fields for passing to other loggers
}

// With creates a new Logger with the given fields (immediately encoded).
// These fields will be included in all log entries from this logger.
func With(fields ...Field) *Logger {
	if len(fields) == 0 {
		return &Logger{
			logger: getLogger(),
			fields: nil,
		}
	}
	return &Logger{
		logger: getLogger().With(fields...),
		fields: fields,
	}
}

// WithLazy creates a new Logger with the given fields (lazily encoded).
// These fields will be included in all log entries from this logger.
func WithLazy(fields ...Field) *Logger {
	if len(fields) == 0 {
		return &Logger{
			logger: getLogger(),
			fields: nil,
		}
	}
	return &Logger{
		logger: getLogger().WithLazy(fields...),
		fields: fields,
	}
}

// With creates a new Logger with additional fields (immediately encoded).
// The new logger inherits all fields from the parent logger.
func (l *Logger) With(fields ...Field) *Logger {
	if len(fields) == 0 {
		return l
	}
	newFields := make([]Field, len(l.fields)+len(fields))
	copy(newFields, l.fields)
	copy(newFields[len(l.fields):], fields)
	return &Logger{
		logger: l.logger.With(fields...),
		fields: newFields,
	}
}

// WithLazy creates a new Logger with additional fields (lazily encoded).
// The new logger inherits all fields from the parent logger.
func (l *Logger) WithLazy(fields ...Field) *Logger {
	if len(fields) == 0 {
		return l
	}
	newFields := make([]Field, len(l.fields)+len(fields))
	copy(newFields, l.fields)
	copy(newFields[len(l.fields):], fields)
	return &Logger{
		logger: l.logger.WithLazy(fields...),
		fields: newFields,
	}
}

// WithOptions creates a new Logger with options applied.
func (l *Logger) WithOptions(opts ...Option) *Logger {
	if len(opts) == 0 {
		return l
	}
	fields := append([]Field(nil), l.fields...)
	return &Logger{
		logger: l.logger.WithOptions(opts...),
		fields: fields,
	}
}

// Level returns the current global log level.
func (l *Logger) Level() Level {
	return GetLevel()
}

// LevelEnabled reports whether a message at the given level would be logged.
// Use this to guard expensive field construction on hot paths:
//
//	if l.LevelEnabled(mlog.DebugLevel) {
//	    l.Debug(ctx, "details", mlog.String("dump", expensiveDump()))
//	}
func (l *Logger) LevelEnabled(level Level) bool {
	return currentLevel().Enabled(level)
}

// prepareLog resolves the logger and fields for Logger methods.
// It optimizes by selecting the logger with more pre-encoded fields
// to minimize the number of fields that need encoding at log time.
// It returns before the actual log call, so it does not appear in the call stack
// when zap captures the caller.
func (l *Logger) prepareLog(ctx context.Context, fields []Field) (*zap.Logger, []Field) {
	if ctx == nil {
		if len(fields) == 0 {
			return l.logger, []Field{nilContextField}
		}
		allFields := make([]Field, len(fields)+1)
		copy(allFields, fields)
		allFields[len(fields)] = nilContextField
		return l.logger, allFields
	}

	lc := getLogContext(ctx)

	if lc.logger != nil && lc.fieldCount() >= len(l.fields) {
		// ctx has more fields, use ctx logger, pass component fields + extra fields
		switch {
		case len(l.fields) == 0:
			return lc.logger, appendTraceFields(ctx, fields)
		case len(fields) == 0:
			return lc.logger, appendTraceFields(ctx, l.fields)
		default:
			allFields := make([]Field, len(l.fields)+len(fields))
			copy(allFields, l.fields)
			copy(allFields[len(l.fields):], fields)
			return lc.logger, appendTraceFields(ctx, allFields)
		}
	}

	// component has more fields (or ctx has no logger), use component logger
	ctxFields := lc.getFields()
	switch {
	case len(ctxFields) == 0:
		return l.logger, appendTraceFields(ctx, fields)
	case len(fields) == 0:
		return l.logger, appendTraceFields(ctx, ctxFields)
	default:
		allFields := make([]Field, len(ctxFields)+len(fields))
		copy(allFields, ctxFields)
		copy(allFields[len(ctxFields):], fields)
		return l.logger, appendTraceFields(ctx, allFields)
	}
}

// Log logs a message at the specified level.
func (l *Logger) Log(ctx context.Context, level Level, msg string, fields ...Field) {
	if !currentLevel().Enabled(level) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Log(level, msg, fields...)
}

// Debug logs a message at debug level.
func (l *Logger) Debug(ctx context.Context, msg string, fields ...Field) {
	if !currentLevel().Enabled(DebugLevel) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Debug(msg, fields...)
}

// Info logs a message at info level.
func (l *Logger) Info(ctx context.Context, msg string, fields ...Field) {
	if !currentLevel().Enabled(InfoLevel) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Info(msg, fields...)
}

// Warn logs a message at warn level.
func (l *Logger) Warn(ctx context.Context, msg string, fields ...Field) {
	if !currentLevel().Enabled(WarnLevel) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Warn(msg, fields...)
}

// Error logs a message at error level.
func (l *Logger) Error(ctx context.Context, msg string, fields ...Field) {
	if !currentLevel().Enabled(ErrorLevel) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Error(msg, fields...)
}

// DPanic logs a message at dpanic level.
// In development mode, the logger then panics. (See DPanicLevel for details.)
func (l *Logger) DPanic(ctx context.Context, msg string, fields ...Field) {
	if !currentLevel().Enabled(DPanicLevel) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.DPanic(msg, fields...)
}

// Panic logs a message at panic level, then panics.
func (l *Logger) Panic(ctx context.Context, msg string, fields ...Field) {
	logger, fields := l.prepareLog(ctx, fields)
	logger.Panic(msg, fields...)
}

// Fatal logs a message at fatal level, then calls os.Exit(1).
func (l *Logger) Fatal(ctx context.Context, msg string, fields ...Field) {
	logger, fields := l.prepareLog(ctx, fields)
	logger.Fatal(msg, fields...)
}
