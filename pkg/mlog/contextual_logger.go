package mlog

import "context"

// ContextLogger binds a context to a Logger for compatibility with older
// chain-style log call sites.
type ContextLogger struct {
	ctx    context.Context
	logger *Logger
}

// Ctx returns a context-bound logger.
func Ctx(ctx context.Context) *ContextLogger {
	return &ContextLogger{
		ctx:    ctx,
		logger: With(),
	}
}

// With returns a new context-bound logger with additional fields.
func (l *ContextLogger) With(fields ...Field) *ContextLogger {
	return &ContextLogger{
		ctx:    l.ctx,
		logger: l.logger.With(fields...),
	}
}

// Debug logs a message at debug level.
func (l *ContextLogger) Debug(msg string, fields ...Field) {
	l.logger.Debug(l.ctx, msg, fields...)
}

// Info logs a message at info level.
func (l *ContextLogger) Info(msg string, fields ...Field) {
	l.logger.Info(l.ctx, msg, fields...)
}

// Warn logs a message at warn level.
func (l *ContextLogger) Warn(msg string, fields ...Field) {
	l.logger.Warn(l.ctx, msg, fields...)
}

// Error logs a message at error level.
func (l *ContextLogger) Error(msg string, fields ...Field) {
	l.logger.Error(l.ctx, msg, fields...)
}
