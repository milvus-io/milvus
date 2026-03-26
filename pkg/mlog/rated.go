package mlog

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"golang.org/x/time/rate"
)

// ratedEntry holds a rate limiter and the count of ignored log entries for a specific call site.
type ratedEntry struct {
	limiter     *rate.Limiter
	ignoreCount atomic.Int64
}

// ratedRegistry is a global registry of rate limiters keyed by caller's program counter.
var ratedRegistry sync.Map

// getOrCreateRatedEntry returns an existing or newly created ratedEntry for the given pc.
// The limiter is created lazily on first access with the specified rate limit and burst=1.
func getOrCreateRatedEntry(pc uintptr, limit rate.Limit) *ratedEntry {
	if v, ok := ratedRegistry.Load(pc); ok {
		return v.(*ratedEntry)
	}
	entry := &ratedEntry{
		limiter: rate.NewLimiter(limit, 1),
	}
	actual, _ := ratedRegistry.LoadOrStore(pc, entry)
	return actual.(*ratedEntry)
}

// ratedAllow checks if a log entry should be emitted based on rate limiting.
// If suppressed, increments ignore count and returns false.
// If allowed and previous entries were suppressed, appends a _suppressed field.
func ratedAllow(pc uintptr, limit rate.Limit, fields *[]Field) bool {
	entry := getOrCreateRatedEntry(pc, limit)
	if !entry.limiter.Allow() {
		entry.ignoreCount.Add(1)
		return false
	}
	if ignored := entry.ignoreCount.Swap(0); ignored > 0 {
		*fields = append(*fields, Int64("_suppressed", ignored))
	}
	return true
}

// RatedLog logs a message at the specified level with rate limiting.
func RatedLog(ctx context.Context, level Level, limit rate.Limit, msg string, fields ...Field) {
	if !globalLevel.Enabled(level) {
		return
	}
	pc, _, _, _ := runtime.Caller(1)
	if !ratedAllow(pc, limit, &fields) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Log(level, msg, fields...)
}

// RatedDebug logs a message at debug level with rate limiting.
func RatedDebug(ctx context.Context, limit rate.Limit, msg string, fields ...Field) {
	if !globalLevel.Enabled(DebugLevel) {
		return
	}
	pc, _, _, _ := runtime.Caller(1)
	if !ratedAllow(pc, limit, &fields) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Debug(msg, fields...)
}

// RatedInfo logs a message at info level with rate limiting.
func RatedInfo(ctx context.Context, limit rate.Limit, msg string, fields ...Field) {
	if !globalLevel.Enabled(InfoLevel) {
		return
	}
	pc, _, _, _ := runtime.Caller(1)
	if !ratedAllow(pc, limit, &fields) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Info(msg, fields...)
}

// RatedWarn logs a message at warn level with rate limiting.
func RatedWarn(ctx context.Context, limit rate.Limit, msg string, fields ...Field) {
	if !globalLevel.Enabled(WarnLevel) {
		return
	}
	pc, _, _, _ := runtime.Caller(1)
	if !ratedAllow(pc, limit, &fields) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Warn(msg, fields...)
}

// RatedError logs a message at error level with rate limiting.
func RatedError(ctx context.Context, limit rate.Limit, msg string, fields ...Field) {
	if !globalLevel.Enabled(ErrorLevel) {
		return
	}
	pc, _, _, _ := runtime.Caller(1)
	if !ratedAllow(pc, limit, &fields) {
		return
	}
	logger, fields := prepareLog(ctx, fields)
	logger.Error(msg, fields...)
}

// RatedLog logs a message at the specified level with rate limiting.
func (l *Logger) RatedLog(ctx context.Context, level Level, limit rate.Limit, msg string, fields ...Field) {
	if !globalLevel.Enabled(level) {
		return
	}
	pc, _, _, _ := runtime.Caller(1)
	if !ratedAllow(pc, limit, &fields) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Log(level, msg, fields...)
}

// RatedDebug logs a message at debug level with rate limiting.
func (l *Logger) RatedDebug(ctx context.Context, limit rate.Limit, msg string, fields ...Field) {
	if !globalLevel.Enabled(DebugLevel) {
		return
	}
	pc, _, _, _ := runtime.Caller(1)
	if !ratedAllow(pc, limit, &fields) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Debug(msg, fields...)
}

// RatedInfo logs a message at info level with rate limiting.
func (l *Logger) RatedInfo(ctx context.Context, limit rate.Limit, msg string, fields ...Field) {
	if !globalLevel.Enabled(InfoLevel) {
		return
	}
	pc, _, _, _ := runtime.Caller(1)
	if !ratedAllow(pc, limit, &fields) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Info(msg, fields...)
}

// RatedWarn logs a message at warn level with rate limiting.
func (l *Logger) RatedWarn(ctx context.Context, limit rate.Limit, msg string, fields ...Field) {
	if !globalLevel.Enabled(WarnLevel) {
		return
	}
	pc, _, _, _ := runtime.Caller(1)
	if !ratedAllow(pc, limit, &fields) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Warn(msg, fields...)
}

// RatedError logs a message at error level with rate limiting.
func (l *Logger) RatedError(ctx context.Context, limit rate.Limit, msg string, fields ...Field) {
	if !globalLevel.Enabled(ErrorLevel) {
		return
	}
	pc, _, _, _ := runtime.Caller(1)
	if !ratedAllow(pc, limit, &fields) {
		return
	}
	logger, fields := l.prepareLog(ctx, fields)
	logger.Error(msg, fields...)
}
