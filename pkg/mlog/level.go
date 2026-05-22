package mlog

import (
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Level is an alias for zapcore.Level
type Level = zapcore.Level

// Re-export level constants for convenience
const (
	DebugLevel  = zapcore.DebugLevel
	InfoLevel   = zapcore.InfoLevel
	WarnLevel   = zapcore.WarnLevel
	ErrorLevel  = zapcore.ErrorLevel
	DPanicLevel = zapcore.DPanicLevel
	PanicLevel  = zapcore.PanicLevel
	FatalLevel  = zapcore.FatalLevel
)

// globalLevel allows runtime level changes.
var (
	defaultGlobalLevel = zap.NewAtomicLevelAt(InfoLevel)
	globalLevel        atomic.Pointer[zap.AtomicLevel]
)

func currentLevel() *zap.AtomicLevel {
	if level := globalLevel.Load(); level != nil {
		return level
	}
	globalLevel.Store(&defaultGlobalLevel)
	return &defaultGlobalLevel
}

// SetLevel changes the log level at runtime.
// This affects all loggers created with the default config.
func SetLevel(level Level) {
	currentLevel().SetLevel(level)
}

// GetLevel returns the current log level.
func GetLevel() Level {
	return currentLevel().Level()
}

// LevelEnabled reports whether a message at the given level would be logged.
// Use this to guard expensive field construction on hot paths:
//
//	if mlog.LevelEnabled(mlog.DebugLevel) {
//	    mlog.Debug(ctx, "details", mlog.String("dump", expensiveDump()))
//	}
func LevelEnabled(level Level) bool {
	return currentLevel().Enabled(level)
}

// GetAtomicLevel returns the AtomicLevel for integration with custom configs.
// Callers can use this when building their own zap.Config:
//
//	cfg.Level = mlog.GetAtomicLevel()
func GetAtomicLevel() zap.AtomicLevel {
	return *currentLevel()
}
