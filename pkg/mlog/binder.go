package mlog

import "sync/atomic"

var (
	_ WithLogger   = &Binder{}
	_ LoggerBinder = &Binder{}
)

// WithLogger is implemented by types that expose a component logger.
type WithLogger interface {
	Logger() *Logger
}

// LoggerBinder is implemented by types that can bind a component logger.
type LoggerBinder interface {
	SetLogger(logger *Logger)
}

// Binder is an embedding helper for component-level loggers.
type Binder struct {
	logger atomic.Pointer[Logger]
}

// SetLogger binds logger to the receiver. Passing nil resets it to the global logger.
func (b *Binder) SetLogger(logger *Logger) {
	b.logger.Store(logger)
}

// Logger returns the bound logger, or the current global logger if none is bound.
func (b *Binder) Logger() *Logger {
	logger := b.logger.Load()
	if logger == nil {
		return With()
	}
	return logger
}
