package mlog

import "go.uber.org/zap"

// Option configures logger initialization and logger instances.
type Option = zap.Option

// AddCallerSkip increases the number of callers skipped by caller annotation.
func AddCallerSkip(skip int) Option {
	return zap.AddCallerSkip(skip)
}

// AddStacktrace configures the level that captures stack traces.
func AddStacktrace(level Level) Option {
	return zap.AddStacktrace(level)
}
