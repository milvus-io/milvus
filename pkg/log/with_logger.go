package log

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
	logger *MLogger
}

// SetLogger sets logger to Binder.
func (w *Binder) SetLogger(logger *MLogger) {
	if w.logger != nil {
		panic("logger already set")
	}
	w.logger = logger
}

// Logger returns the logger of Binder.
func (w *Binder) Logger() *MLogger {
	if w.logger == nil {
		return With()
	}
	return w.logger
}
