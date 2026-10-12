package mlog

import (
	"bytes"
	"sync"
)

// TestSink is an in-memory zapcore.WriteSyncer for unit tests that assert on
// log output.
//
// It has to serialize its own access because InitLoggerWithWriteSyncer does not
// wrap the syncer in zapcore.Lock - textIOCore writes straight through to it.
// A test that installs its sink as the global logger therefore shares that sink
// with every background goroutine that logs, while the test itself reads the
// captured bytes back. A bare bytes.Buffer is not safe for that, and the race
// detector reports both write/write and read/write races on it.
type TestSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// NewTestSink returns an empty TestSink.
func NewTestSink() *TestSink {
	return &TestSink{}
}

// Write implements zapcore.WriteSyncer.
func (s *TestSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

// Sync implements zapcore.WriteSyncer. The sink is in memory, so there is
// nothing to flush.
func (s *TestSink) Sync() error {
	return nil
}

// String returns everything written to the sink so far. It is safe to call
// while other goroutines are still logging.
func (s *TestSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// TestingTB is the subset of *testing.T that CaptureGlobalLogs needs, declared
// here so that mlog does not have to import "testing". It mirrors how
// InitTestLogger takes a zaptest.TestingT.
type TestingTB interface {
	Helper()
	Cleanup(func())
	Fatalf(format string, args ...any)
}

// CaptureGlobalLogs installs a TestSink as the sink of the global logger and
// restores the previous logger when the test finishes.
//
// Prefer this over a per-package log buffer: the returned sink is safe to read
// while unrelated goroutines keep writing to the global logger.
func CaptureGlobalLogs(t TestingTB, cfg *Config, opts ...Option) *TestSink {
	t.Helper()

	sink := NewTestSink()
	oldLogger := L()
	oldLevel := GetAtomicLevel()
	logger, props, err := InitLoggerWithWriteSyncer(cfg, sink, opts...)
	if err != nil {
		t.Fatalf("mlog: init logger with test sink: %v", err)
		return nil
	}
	ReplaceGlobals(logger, props)
	t.Cleanup(func() {
		ReplaceGlobals(oldLogger, &ZapProperties{Level: oldLevel})
	})
	return sink
}
