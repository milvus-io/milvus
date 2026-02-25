package mlog

import (
	"context"
	"sync/atomic"

	"go.uber.org/zap"
)

var (
	// globalLogger is the package-level logger
	globalLogger atomic.Pointer[zap.Logger]

	// nilContextField is added when nil context is passed
	nilContextField = zap.Bool("_ctx_nil", true)
)

func init() {
	// Default logger: production config writing to stderr
	cfg := zap.NewProductionConfig()
	cfg.Level = globalLevel
	logger, _ := cfg.Build(zap.AddCallerSkip(1))
	globalLogger.Store(logger)
}

// Init replaces the global logger with the provided one.
// The caller is responsible for configuring the logger.
// AddCallerSkip(1) is automatically applied.
func Init(logger *zap.Logger) {
	globalLogger.Store(logger.WithOptions(zap.AddCallerSkip(1)))
}

// InitNode initializes the logger with node-level metadata.
// nodeId is global and included in all log entries.
// Call this once at process startup.
func InitNode(logger *zap.Logger, nodeId int64) {
	// Add nodeId to the logger directly, so all derived loggers include it
	field := Int64(keyNodeID, nodeId)
	globalLogger.Store(logger.WithOptions(zap.AddCallerSkip(1)).With(field))
}

// getLogger returns the current global logger
func getLogger() *zap.Logger {
	return globalLogger.Load()
}

// prepareLog resolves the logger and fields from context for package-level functions.
// It returns before the actual log call, so it does not appear in the call stack
// when zap captures the caller.
func prepareLog(ctx context.Context, fields []Field) (*zap.Logger, []Field) {
	if ctx == nil {
		return getLogger(), append(fields, nilContextField)
	}

	lc := getLogContext(ctx)
	if lc.logger != nil {
		return lc.logger, fields
	}

	logger := getLogger()
	ctxFields := lc.getFields()
	if len(ctxFields) > 0 {
		fields = append(ctxFields, fields...)
	}

	return logger, fields
}

// Log logs a message at the specified level.
func Log(ctx context.Context, level Level, msg string, fields ...Field) {
	if !globalLevel.Enabled(level) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Log(level, msg, fields...)
}

// Debug logs a message at debug level.
func Debug(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(DebugLevel) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Debug(msg, fields...)
}

// Info logs a message at info level.
func Info(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(InfoLevel) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Info(msg, fields...)
}

// Warn logs a message at warn level.
func Warn(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(WarnLevel) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Warn(msg, fields...)
}

// Error logs a message at error level.
func Error(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(ErrorLevel) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Error(msg, fields...)
}

// DPanic logs a message at dpanic level.
// In development mode, the logger then panics. (See DPanicLevel for details.)
func DPanic(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(DPanicLevel) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.DPanic(msg, fields...)
}

// Panic logs a message at panic level, then panics.
func Panic(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(PanicLevel) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Panic(msg, fields...)
}

// Fatal logs a message at fatal level, then calls os.Exit(1).
func Fatal(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(FatalLevel) {
		return
	}
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
	return globalLevel.Enabled(level)
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
			return lc.logger, fields
		case len(fields) == 0:
			return lc.logger, l.fields
		default:
			allFields := make([]Field, len(l.fields)+len(fields))
			copy(allFields, l.fields)
			copy(allFields[len(l.fields):], fields)
			return lc.logger, allFields
		}
	}

	// component has more fields (or ctx has no logger), use component logger
	ctxFields := lc.getFields()
	switch {
	case len(ctxFields) == 0:
		return l.logger, fields
	case len(fields) == 0:
		return l.logger, ctxFields
	default:
		allFields := make([]Field, len(ctxFields)+len(fields))
		copy(allFields, ctxFields)
		copy(allFields[len(ctxFields):], fields)
		return l.logger, allFields
	}
}

// Log logs a message at the specified level.
func (l *Logger) Log(ctx context.Context, level Level, msg string, fields ...Field) {
	if !globalLevel.Enabled(level) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Log(level, msg, fields...)
}

// Debug logs a message at debug level.
func (l *Logger) Debug(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(DebugLevel) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Debug(msg, fields...)
}

// Info logs a message at info level.
func (l *Logger) Info(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(InfoLevel) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Info(msg, fields...)
}

// Warn logs a message at warn level.
func (l *Logger) Warn(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(WarnLevel) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Warn(msg, fields...)
}

// Error logs a message at error level.
func (l *Logger) Error(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(ErrorLevel) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Error(msg, fields...)
}

// DPanic logs a message at dpanic level.
// In development mode, the logger then panics. (See DPanicLevel for details.)
func (l *Logger) DPanic(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(DPanicLevel) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.DPanic(msg, fields...)
}

// Panic logs a message at panic level, then panics.
func (l *Logger) Panic(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(PanicLevel) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Panic(msg, fields...)
}

// Fatal logs a message at fatal level, then calls os.Exit(1).
func (l *Logger) Fatal(ctx context.Context, msg string, fields ...Field) {
	if !globalLevel.Enabled(FatalLevel) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Fatal(msg, fields...)
}
