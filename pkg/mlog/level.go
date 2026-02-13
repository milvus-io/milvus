package mlog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Level is an alias for zapcore.Level
type Level = zapcore.Level

// Re-export level constants for convenience
const (
	DebugLevel = zapcore.DebugLevel
	InfoLevel  = zapcore.InfoLevel
	WarnLevel  = zapcore.WarnLevel
	ErrorLevel = zapcore.ErrorLevel
)

// globalLevel allows runtime level changes
var globalLevel = zap.NewAtomicLevelAt(InfoLevel)

// SetLevel changes the log level at runtime.
// This affects all loggers created with the default config.
// For custom loggers passed via Init(), the caller should
// manage their own AtomicLevel.
func SetLevel(level Level) {
	globalLevel.SetLevel(level)
}

// GetLevel returns the current log level.
func GetLevel() Level {
	return globalLevel.Level()
}

// GetAtomicLevel returns the AtomicLevel for integration with custom configs.
// Callers can use this when building their own zap.Config:
//
//	cfg.Level = mlog.GetAtomicLevel()
func GetAtomicLevel() zap.AtomicLevel {
	return globalLevel
}
