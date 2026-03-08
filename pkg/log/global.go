// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ctxLogKeyType struct{}

var CtxLogKey = ctxLogKeyType{}

// Debug logs a message at DebugLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
// Deprecated: Use Ctx(ctx).Debug instead.
func Debug(msg string, fields ...zap.Field) {
	L().Debug(msg, fields...)
}

// Info logs a message at InfoLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
// Deprecated: Use Ctx(ctx).Info instead.
func Info(msg string, fields ...zap.Field) {
	L().Info(msg, fields...)
}

// Warn logs a message at WarnLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
// Deprecated: Use Ctx(ctx).Warn instead.
func Warn(msg string, fields ...zap.Field) {
	L().Warn(msg, fields...)
}

// Error logs a message at ErrorLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
// Deprecated: Use Ctx(ctx).Error instead.
func Error(msg string, fields ...zap.Field) {
	L().Error(msg, fields...)
}

// Panic logs a message at PanicLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then panics, even if logging at PanicLevel is disabled.
// Deprecated: Use Ctx(ctx).Panic instead.
func Panic(msg string, fields ...zap.Field) {
	L().Panic(msg, fields...)
}

// Fatal logs a message at FatalLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then calls os.Exit(1), even if logging at FatalLevel is
// disabled.
// Deprecated: Use Ctx(ctx).Fatal instead.
func Fatal(msg string, fields ...zap.Field) {
	L().Fatal(msg, fields...)
}

// RatedDebug print logs at debug level
// it limit log print to avoid too many logs
// return true if log successfully
// Deprecated: Use Ctx(ctx).RatedDebug instead.
func RatedDebug(cost float64, msg string, fields ...zap.Field) bool {
	if R().CheckCredit(cost) {
		L().Debug(msg, fields...)
		return true
	}
	return false
}

// RatedInfo print logs at info level
// it limit log print to avoid too many logs
// return true if log successfully
// Deprecated: Use Ctx(ctx).RatedInfo instead.
func RatedInfo(cost float64, msg string, fields ...zap.Field) bool {
	if R().CheckCredit(cost) {
		L().Info(msg, fields...)
		return true
	}
	return false
}

// RatedWarn print logs at warn level
// it limit log print to avoid too many logs
// return true if log successfully
// Deprecated: Use Ctx(ctx).RatedWarn instead.
func RatedWarn(cost float64, msg string, fields ...zap.Field) bool {
	if R().CheckCredit(cost) {
		L().Warn(msg, fields...)
		return true
	}
	return false
}

// With creates a child logger and adds structured context to it.
// Fields added to the child don't affect the parent, and vice versa.
// Deprecated: Use Ctx(ctx).With instead.
func With(fields ...zap.Field) *MLogger {
	return &MLogger{
		Logger: L().WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return NewLazyWith(core, fields)
		})).WithOptions(zap.AddCallerSkip(-1)),
	}
}

// SetLevel alters the logging level.
func SetLevel(l zapcore.Level) {
	_globalP.Load().(*ZapProperties).Level.SetLevel(l)
}

// GetLevel gets the logging level.
func GetLevel() zapcore.Level {
	return _globalP.Load().(*ZapProperties).Level.Level()
}

// WithTraceID returns a context with trace_id attached
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return WithFields(ctx, zap.String("traceID", traceID))
}

// WithReqID adds given reqID field to the logger in ctx
func WithReqID(ctx context.Context, reqID int64) context.Context {
	fields := []zap.Field{zap.Int64("reqID", reqID)}
	return WithFields(ctx, fields...)
}

// WithModule adds given module field to the logger in ctx
func WithModule(ctx context.Context, module string) context.Context {
	fields := []zap.Field{zap.String(FieldNameModule, module)}
	return WithFields(ctx, fields...)
}

// WithFields returns a context with fields attached
func WithFields(ctx context.Context, fields ...zap.Field) context.Context {
	var zlogger *zap.Logger
	if ctxLogger, ok := ctx.Value(CtxLogKey).(*MLogger); ok {
		zlogger = ctxLogger.Logger
	} else {
		zlogger = ctxL()
	}
	mLogger := &MLogger{
		Logger: zlogger.With(fields...),
	}
	return context.WithValue(ctx, CtxLogKey, mLogger)
}

// NewIntentContext creates a new context with intent information and returns it along with a span.
func NewIntentContext(name string, intent string) (context.Context, trace.Span) {
	intentCtx, initSpan := otel.Tracer(name).Start(context.Background(), intent)
	intentCtx = WithFields(intentCtx,
		zap.String("role", name),
		zap.String("intent", intent),
		zap.String("traceID", initSpan.SpanContext().TraceID().String()))
	return intentCtx, initSpan
}

// Ctx returns a logger which will log contextual messages attached in ctx
func Ctx(ctx context.Context) *MLogger {
	if ctx == nil {
		return &MLogger{Logger: ctxL()}
	}
	if ctxLogger, ok := ctx.Value(CtxLogKey).(*MLogger); ok {
		return ctxLogger
	}
	return &MLogger{Logger: ctxL()}
}

// withLogLevel returns ctx with a leveled logger, notes that it will overwrite logger previous attached!
func withLogLevel(ctx context.Context, level zapcore.Level) context.Context {
	var zlogger *zap.Logger
	switch level {
	case zap.DebugLevel:
		zlogger = debugL()
	case zap.InfoLevel:
		zlogger = infoL()
	case zap.WarnLevel:
		zlogger = warnL()
	case zap.ErrorLevel:
		zlogger = errorL()
	case zap.FatalLevel:
		zlogger = fatalL()
	default:
		zlogger = L()
	}
	return context.WithValue(ctx, CtxLogKey, &MLogger{Logger: zlogger})
}

// WithDebugLevel returns context with a debug level enabled logger.
// Notes that it will overwrite previous attached logger within context
func WithDebugLevel(ctx context.Context) context.Context {
	return withLogLevel(ctx, zapcore.DebugLevel)
}

// WithInfoLevel returns context with a info level enabled logger.
// Notes that it will overwrite previous attached logger within context
func WithInfoLevel(ctx context.Context) context.Context {
	return withLogLevel(ctx, zapcore.InfoLevel)
}

// WithWarnLevel returns context with a warning level enabled logger.
// Notes that it will overwrite previous attached logger within context
func WithWarnLevel(ctx context.Context) context.Context {
	return withLogLevel(ctx, zapcore.WarnLevel)
}

// WithErrorLevel returns context with a error level enabled logger.
// Notes that it will overwrite previous attached logger within context
func WithErrorLevel(ctx context.Context) context.Context {
	return withLogLevel(ctx, zapcore.ErrorLevel)
}

// WithFatalLevel returns context with a fatal level enabled logger.
// Notes that it will overwrite previous attached logger within context
func WithFatalLevel(ctx context.Context) context.Context {
	return withLogLevel(ctx, zapcore.FatalLevel)
}
