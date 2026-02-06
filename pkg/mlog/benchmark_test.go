//go:build test

package mlog

import (
	"context"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
)

// discardWriteSyncer is a WriteSyncer that discards all output.
type discardWriteSyncer struct{}

func (d discardWriteSyncer) Write(p []byte) (int, error) { return len(p), nil }
func (d discardWriteSyncer) Sync() error                 { return nil }

// newBenchLogger creates a zap logger that encodes JSON but discards output.
func newBenchLogger() *zap.Logger {
	enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(enc, discardWriteSyncer{}, zapcore.DebugLevel)
	return zap.New(core)
}

// newBenchLoggerWithCaller creates a zap logger with caller enabled.
func newBenchLoggerWithCaller() *zap.Logger {
	enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(enc, discardWriteSyncer{}, zapcore.DebugLevel)
	return zap.New(core, zap.AddCaller())
}

// setupBench initializes mlog with a discard logger for benchmarks.
func setupBench() {
	SetLevel(DebugLevel)
	Init(newBenchLogger())
}

// ---------------------------------------------------------------------------
// Baseline: native zap.Logger
// ---------------------------------------------------------------------------

func BenchmarkZapInfo(b *testing.B) {
	logger := newBenchLogger()
	b.ResetTimer()
	for b.Loop() {
		logger.Info("benchmark message")
	}
}

func BenchmarkZapInfoWithFields(b *testing.B) {
	logger := newBenchLogger()
	b.ResetTimer()
	for b.Loop() {
		logger.Info("benchmark message",
			zap.String("key1", "value1"),
			zap.Int64("key2", 42),
			zap.String("key3", "value3"),
		)
	}
}

func BenchmarkZapInfoWithCaller(b *testing.B) {
	logger := newBenchLoggerWithCaller()
	b.ResetTimer()
	for b.Loop() {
		logger.Info("benchmark message")
	}
}

func BenchmarkZapInfoWithCallerAndFields(b *testing.B) {
	logger := newBenchLoggerWithCaller()
	b.ResetTimer()
	for b.Loop() {
		logger.Info("benchmark message",
			zap.String("key1", "value1"),
			zap.Int64("key2", 42),
			zap.String("key3", "value3"),
		)
	}
}

func BenchmarkZapInfoDisabledLevel(b *testing.B) {
	enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	core := zapcore.NewCore(enc, discardWriteSyncer{}, zapcore.WarnLevel)
	logger := zap.New(core)
	b.ResetTimer()
	for b.Loop() {
		logger.Info("benchmark message")
	}
}

// ---------------------------------------------------------------------------
// Package-level functions
// ---------------------------------------------------------------------------

func BenchmarkMlogInfo(b *testing.B) {
	setupBench()
	defer resetLogger()
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		Info(ctx, "benchmark message")
	}
}

func BenchmarkMlogInfoWithFields(b *testing.B) {
	setupBench()
	defer resetLogger()
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		Info(ctx, "benchmark message",
			String("key1", "value1"),
			Int64("key2", 42),
			String("key3", "value3"),
		)
	}
}

func BenchmarkMlogInfoWithContextFields(b *testing.B) {
	setupBench()
	defer resetLogger()
	ctx := WithFields(context.Background(),
		String("trace_id", "abc-123"),
		Int64("node_id", 1),
	)
	b.ResetTimer()
	for b.Loop() {
		Info(ctx, "benchmark message")
	}
}

func BenchmarkMlogInfoWithContextAndCallFields(b *testing.B) {
	setupBench()
	defer resetLogger()
	ctx := WithFields(context.Background(),
		String("trace_id", "abc-123"),
		Int64("node_id", 1),
	)
	b.ResetTimer()
	for b.Loop() {
		Info(ctx, "benchmark message",
			String("key1", "value1"),
			Int64("key2", 42),
			String("key3", "value3"),
		)
	}
}

func BenchmarkMlogInfoDisabledLevel(b *testing.B) {
	setupBench()
	defer resetLogger()
	SetLevel(WarnLevel)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		Info(ctx, "benchmark message")
	}
}

func BenchmarkMlogInfoNilContext(b *testing.B) {
	setupBench()
	defer resetLogger()
	b.ResetTimer()
	for b.Loop() {
		Info(nil, "benchmark message")
	}
}

// ---------------------------------------------------------------------------
// Logger methods
// ---------------------------------------------------------------------------

func BenchmarkMlogLoggerInfo(b *testing.B) {
	setupBench()
	defer resetLogger()
	l := With(String("component", "benchmark"))
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		l.Info(ctx, "benchmark message")
	}
}

func BenchmarkMlogLoggerInfoWithFields(b *testing.B) {
	setupBench()
	defer resetLogger()
	l := With(String("component", "benchmark"))
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		l.Info(ctx, "benchmark message",
			String("key1", "value1"),
			Int64("key2", 42),
			String("key3", "value3"),
		)
	}
}

func BenchmarkMlogLoggerInfoWithContextFields(b *testing.B) {
	setupBench()
	defer resetLogger()
	l := With(String("component", "benchmark"))
	ctx := WithFields(context.Background(),
		String("trace_id", "abc-123"),
		Int64("node_id", 1),
	)
	b.ResetTimer()
	for b.Loop() {
		l.Info(ctx, "benchmark message")
	}
}

func BenchmarkMlogLoggerInfoDisabledLevel(b *testing.B) {
	setupBench()
	defer resetLogger()
	SetLevel(WarnLevel)
	l := With(String("component", "benchmark"))
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		l.Info(ctx, "benchmark message")
	}
}

// ---------------------------------------------------------------------------
// Rated functions
// ---------------------------------------------------------------------------

func BenchmarkMlogRatedInfoAllowed(b *testing.B) {
	setupBench()
	defer resetLogger()
	resetRatedRegistry()
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		RatedInfo(ctx, rate.Inf, "benchmark message")
	}
}

func BenchmarkMlogRatedInfoSuppressed(b *testing.B) {
	setupBench()
	defer resetLogger()
	resetRatedRegistry()
	ctx := context.Background()
	// First call goes through, rest are suppressed with rate=0
	RatedInfo(ctx, 0, "benchmark message")
	b.ResetTimer()
	for b.Loop() {
		RatedInfo(ctx, 0, "benchmark message")
	}
}

func BenchmarkMlogRatedInfoDisabledLevel(b *testing.B) {
	setupBench()
	defer resetLogger()
	SetLevel(WarnLevel)
	resetRatedRegistry()
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		RatedInfo(ctx, rate.Inf, "benchmark message")
	}
}

func BenchmarkMlogLoggerRatedInfoAllowed(b *testing.B) {
	setupBench()
	defer resetLogger()
	resetRatedRegistry()
	l := With(String("component", "benchmark"))
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		l.RatedInfo(ctx, rate.Inf, "benchmark message")
	}
}

func BenchmarkMlogLoggerRatedInfoSuppressed(b *testing.B) {
	setupBench()
	defer resetLogger()
	resetRatedRegistry()
	l := With(String("component", "benchmark"))
	ctx := context.Background()
	l.RatedInfo(ctx, 0, "benchmark message")
	b.ResetTimer()
	for b.Loop() {
		l.RatedInfo(ctx, 0, "benchmark message")
	}
}
