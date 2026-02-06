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
	field := Int64(KeyNodeID, nodeId)
	globalLogger.Store(logger.WithOptions(zap.AddCallerSkip(1)).With(field))
}

// getLogger returns the current global logger
func getLogger() *zap.Logger {
	return globalLogger.Load()
}

// log is the internal logging function
func log(ctx context.Context, level Level, msg string, fields ...Field) {
	// Early return if level is disabled - avoid field processing overhead
	if !globalLevel.Enabled(level) {
		return
	}

	// Handle nil context
	if ctx == nil {
		fields = append(fields, nilContextField)
		logWithLogger(getLogger(), level, msg, fields...)
		return
	}

	// Use cached logger from context if available
	logger := loggerFromContext(ctx)
	if logger == nil {
		// No cached logger, use global logger with context fields
		logger = getLogger()
		ctxFields := FieldsFromContext(ctx)
		if len(ctxFields) > 0 {
			fields = append(ctxFields, fields...)
		}
	}

	logWithLogger(logger, level, msg, fields...)
}

// logWithLogger logs a message using the provided logger
func logWithLogger(logger *zap.Logger, level Level, msg string, fields ...Field) {
	switch level {
	case DebugLevel:
		logger.Debug(msg, fields...)
	case InfoLevel:
		logger.Info(msg, fields...)
	case WarnLevel:
		logger.Warn(msg, fields...)
	case ErrorLevel:
		logger.Error(msg, fields...)
	}
}

// Log logs a message at the specified level.
func Log(ctx context.Context, level Level, msg string, fields ...Field) {
	log(ctx, level, msg, fields...)
}

// Debug logs a message at debug level.
func Debug(ctx context.Context, msg string, fields ...Field) {
	log(ctx, DebugLevel, msg, fields...)
}

// Info logs a message at info level.
func Info(ctx context.Context, msg string, fields ...Field) {
	log(ctx, InfoLevel, msg, fields...)
}

// Warn logs a message at warn level.
func Warn(ctx context.Context, msg string, fields ...Field) {
	log(ctx, WarnLevel, msg, fields...)
}

// Error logs a message at error level.
func Error(ctx context.Context, msg string, fields ...Field) {
	log(ctx, ErrorLevel, msg, fields...)
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

// log is the internal logging method for Logger.
// It optimizes by selecting the logger with more pre-encoded fields.
func (l *Logger) log(ctx context.Context, level Level, msg string, fields ...Field) {
	// Early return if level is disabled
	if !globalLevel.Enabled(level) {
		return
	}

	// Handle nil context
	if ctx == nil {
		if len(fields) == 0 {
			logWithLogger(l.logger, level, msg, nilContextField)
		} else {
			allFields := make([]Field, len(fields)+1)
			copy(allFields, fields)
			allFields[len(fields)] = nilContextField
			logWithLogger(l.logger, level, msg, allFields...)
		}
		return
	}

	lc := getLogContext(ctx)

	// Optimization: use the logger with more pre-encoded fields as base
	// to minimize the number of fields that need encoding at log time
	if lc.logger != nil && lc.fieldCount() >= len(l.fields) {
		// ctx has more fields, use ctx logger, pass component fields + extra fields
		switch {
		case len(l.fields) == 0 && len(fields) == 0:
			logWithLogger(lc.logger, level, msg)
		case len(l.fields) == 0:
			logWithLogger(lc.logger, level, msg, fields...)
		case len(fields) == 0:
			logWithLogger(lc.logger, level, msg, l.fields...)
		default:
			allFields := make([]Field, len(l.fields)+len(fields))
			copy(allFields, l.fields)
			copy(allFields[len(l.fields):], fields)
			logWithLogger(lc.logger, level, msg, allFields...)
		}
	} else {
		// component has more fields (or ctx has no logger), use component logger
		ctxFields := lc.getFields()
		switch {
		case len(ctxFields) == 0 && len(fields) == 0:
			logWithLogger(l.logger, level, msg)
		case len(ctxFields) == 0:
			logWithLogger(l.logger, level, msg, fields...)
		case len(fields) == 0:
			logWithLogger(l.logger, level, msg, ctxFields...)
		default:
			allFields := make([]Field, len(ctxFields)+len(fields))
			copy(allFields, ctxFields)
			copy(allFields[len(ctxFields):], fields)
			logWithLogger(l.logger, level, msg, allFields...)
		}
	}
}

// Debug logs a message at debug level.
func (l *Logger) Debug(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, DebugLevel, msg, fields...)
}

// Info logs a message at info level.
func (l *Logger) Info(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, InfoLevel, msg, fields...)
}

// Warn logs a message at warn level.
func (l *Logger) Warn(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, WarnLevel, msg, fields...)
}

// Error logs a message at error level.
func (l *Logger) Error(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, ErrorLevel, msg, fields...)
}
