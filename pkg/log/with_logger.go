package log

import "go.uber.org/atomic"

var (
	_ WithLogger   = &Binder{}
	_ LoggerBinder = &Binder{}
)

// WithLogger is an interface to help access local logger.
type WithLogger interface {
	Logger() *MLogger
}

// LoggerBinder is an interface to help set logger.
type LoggerBinder interface {
	SetLogger(logger *MLogger)
}

// Binder is a embedding type to help access local logger.
type Binder struct {
	logger atomic.Pointer[MLogger]
}

// SetLogger sets logger to Binder.
func (w *Binder) SetLogger(logger *MLogger) {
	w.logger.Store(logger)
}

// Logger returns the logger of Binder.
func (w *Binder) Logger() *MLogger {
	l := w.logger.Load()
	if l == nil {
		return With()
	}
	return l
}
