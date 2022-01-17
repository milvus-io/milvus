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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Debug logs a message at DebugLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Debug(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Debug(msg, fields...)
}

// Info logs a message at InfoLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Info(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Info(msg, fields...)
}

// Warn logs a message at WarnLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Warn(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Warn(msg, fields...)
}

// Error logs a message at ErrorLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Error(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
}

// Panic logs a message at PanicLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then panics, even if logging at PanicLevel is disabled.
func Panic(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Panic(msg, fields...)
}

// Fatal logs a message at FatalLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then calls os.Exit(1), even if logging at FatalLevel is
// disabled.
func Fatal(msg string, fields ...zap.Field) {
	L().WithOptions(zap.AddCallerSkip(1)).Fatal(msg, fields...)
}

// RatedDebug print logs at debug level
// it limit log print to avoid too many logs
// return true if log successfully
func RatedDebug(cost float64, msg string, fields ...zap.Field) bool {
	if R().CheckCredit(cost) {
		L().WithOptions(zap.AddCallerSkip(1)).Debug(msg, fields...)
		return true
	}
	return false
}

// RatedInfo print logs at debug level
// it limit log print to avoid too many logs
// return true if log successfully
func RatedInfo(cost float64, msg string, fields ...zap.Field) bool {
	if R().CheckCredit(cost) {
		L().WithOptions(zap.AddCallerSkip(1)).Info(msg, fields...)
		return true
	}
	return false
}

// RatedWarn print logs at debug level
// it limit log print to avoid too many logs
// return true if log successfully
func RatedWarn(cost float64, msg string, fields ...zap.Field) bool {
	if R().CheckCredit(cost) {
		L().WithOptions(zap.AddCallerSkip(1)).Warn(msg, fields...)
		return true
	}
	return false
}

// With creates a child logger and adds structured context to it.
// Fields added to the child don't affect the parent, and vice versa.
func With(fields ...zap.Field) *zap.Logger {
	return L().WithOptions(zap.AddCallerSkip(1)).With(fields...)
}

// SetLevel alters the logging level.
func SetLevel(l zapcore.Level) {
	_globalP.Load().(*ZapProperties).Level.SetLevel(l)
}

// GetLevel gets the logging level.
func GetLevel() zapcore.Level {
	return _globalP.Load().(*ZapProperties).Level.Level()
}
