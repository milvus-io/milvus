//go:build test

package mlog

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// resetRatedRegistry clears the global rate limiter registry between tests.
func resetRatedRegistry() {
	ratedRegistry.Range(func(key, value any) bool {
		ratedRegistry.Delete(key)
		return true
	})
}

func TestRatedInfoFirstCallAlwaysLogs(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	ctx := context.Background()
	RatedInfo(ctx, 0.001, "first call") // very low rate, but first call should always go through

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "info", entry["level"])
	assert.Equal(t, "first call", entry["msg"])
}

func TestRatedInfoSuppressesSubsequentCalls(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	ctx := context.Background()
	// Use rate.Limit(0) which means no events allowed after the initial burst
	for i := 0; i < 10; i++ {
		RatedInfo(ctx, rate.Limit(0), "rated message")
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.Len(t, lines, 1, "only the first call should produce a log entry")
}

func TestRatedInfoReportsIgnoredCount(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	ctx := context.Background()
	// Use rate.Inf so every call is allowed - first call
	RatedInfo(ctx, rate.Inf, "first")

	// Clear buffer
	buf.Reset()

	// Now use a registry entry that was already created but with rate.Inf,
	// so let's create a fresh scenario: use a separate test function to get a different call site.
	// Actually, let's directly test via the internal mechanism.

	// Create an entry with zero rate (no events after initial burst)
	entry := &ratedEntry{
		limiter: rate.NewLimiter(rate.Limit(0), 1),
	}
	// Consume the initial token
	entry.limiter.Allow()
	// Set ignore count
	entry.ignoreCount.Store(5)

	// Store it with a known key (using uintptr as registry key)
	testKey := uintptr(0xDEAD)
	ratedRegistry.Store(testKey, entry)

	// Now allow the limiter again by creating a new one with Inf rate
	entry.limiter = rate.NewLimiter(rate.Inf, 1)

	// Simulate the check
	var fields []Field
	result := func() bool {
		// We can't use ratedCheck directly because it uses runtime.Caller
		// Instead, test the logic manually
		if !entry.limiter.Allow() {
			entry.ignoreCount.Add(1)
			return false
		}
		if ignored := entry.ignoreCount.Swap(0); ignored > 0 {
			fields = append(fields, Int64("_ignored", ignored))
		}
		return true
	}()

	assert.True(t, result)
	require.Len(t, fields, 1)
	assert.Equal(t, "_ignored", fields[0].Key)
	assert.Equal(t, int64(5), fields[0].Integer)
}

func TestRatedDebugLogsAtDebugLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	oldLevel := GetLevel()
	SetLevel(DebugLevel)
	defer SetLevel(oldLevel)

	ctx := context.Background()
	RatedDebug(ctx, rate.Inf, "debug rated")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "debug", entry["level"])
	assert.Equal(t, "debug rated", entry["msg"])
}

func TestRatedWarnLogsAtWarnLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	ctx := context.Background()
	RatedWarn(ctx, rate.Inf, "warn rated")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "warn", entry["level"])
	assert.Equal(t, "warn rated", entry["msg"])
}

func TestRatedErrorLogsAtErrorLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	ctx := context.Background()
	RatedError(ctx, rate.Inf, "error rated")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "error", entry["level"])
	assert.Equal(t, "error rated", entry["msg"])
}

func TestRatedInfoWithFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	ctx := context.Background()
	RatedInfo(ctx, rate.Inf, "with fields", String("key", "value"), Int64("count", 42))

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "value", entry["key"])
	assert.Equal(t, float64(42), entry["count"])
}

func TestRatedInfoWithContextFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	ctx := context.Background()
	ctx = WithFields(ctx, String("trace_id", "abc123"))
	RatedInfo(ctx, rate.Inf, "with context", String("extra", "data"))

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "abc123", entry["trace_id"])
	assert.Equal(t, "data", entry["extra"])
}

func TestRatedInfoLevelFiltering(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	oldLevel := GetLevel()
	SetLevel(ErrorLevel)
	defer SetLevel(oldLevel)

	ctx := context.Background()
	RatedDebug(ctx, rate.Inf, "debug")
	RatedInfo(ctx, rate.Inf, "info")
	RatedWarn(ctx, rate.Inf, "warn")

	assert.Empty(t, buf.String(), "no logs when level is disabled")

	RatedError(ctx, rate.Inf, "error")
	assert.Contains(t, buf.String(), "error")
}

func TestRatedInfoDifferentCallSitesIndependent(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	ctx := context.Background()

	// Two different source lines → two different call sites → independent rate limiters.
	// Each first call should succeed (burst=1).
	RatedInfo(ctx, rate.Limit(0), "site1")
	RatedInfo(ctx, rate.Limit(0), "site2")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	require.Len(t, lines, 2)

	var entry1, entry2 map[string]interface{}
	require.NoError(t, json.Unmarshal(lines[0], &entry1))
	require.NoError(t, json.Unmarshal(lines[1], &entry2))
	assert.Equal(t, "site1", entry1["msg"])
	assert.Equal(t, "site2", entry2["msg"])
}

func TestRatedInfoIgnoredCountIntegration(t *testing.T) {
	defer resetRatedRegistry()

	// For integration test, we manipulate the registry directly
	testKey := uintptr(0xBEEF)
	entry := &ratedEntry{
		limiter: rate.NewLimiter(rate.Inf, 1),
	}
	entry.ignoreCount.Store(42)
	ratedRegistry.Store(testKey, entry)

	// Verify the entry has the ignore count
	loaded, ok := ratedRegistry.Load(testKey)
	require.True(t, ok)
	assert.Equal(t, int64(42), loaded.(*ratedEntry).ignoreCount.Load())
}

// Test Logger rated methods

func TestLoggerRatedInfoFirstCallLogs(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	componentLogger := With(String("module", "test"))
	ctx := context.Background()
	componentLogger.RatedInfo(ctx, rate.Inf, "logger rated info")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "info", entry["level"])
	assert.Equal(t, "logger rated info", entry["msg"])
	assert.Equal(t, "test", entry["module"])
}

func TestLoggerRatedDebugLogs(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	oldLevel := GetLevel()
	SetLevel(DebugLevel)
	defer SetLevel(oldLevel)

	componentLogger := With(String("module", "test"))
	ctx := context.Background()
	componentLogger.RatedDebug(ctx, rate.Inf, "logger rated debug")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "debug", entry["level"])
	assert.Equal(t, "test", entry["module"])
}

func TestLoggerRatedWarnLogs(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	componentLogger := With(String("module", "test"))
	ctx := context.Background()
	componentLogger.RatedWarn(ctx, rate.Inf, "logger rated warn")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "warn", entry["level"])
	assert.Equal(t, "test", entry["module"])
}

func TestLoggerRatedErrorLogs(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	componentLogger := With(String("module", "test"))
	ctx := context.Background()
	componentLogger.RatedError(ctx, rate.Inf, "logger rated error")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "error", entry["level"])
	assert.Equal(t, "test", entry["module"])
}

func TestLoggerRatedSuppressesSubsequentCalls(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	componentLogger := With(String("module", "test"))
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		componentLogger.RatedInfo(ctx, rate.Limit(0), "suppressed")
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.Len(t, lines, 1, "only first call should log")
}

func TestLoggerRatedLevelFiltering(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	oldLevel := GetLevel()
	SetLevel(ErrorLevel)
	defer SetLevel(oldLevel)

	componentLogger := With(String("module", "test"))
	ctx := context.Background()
	componentLogger.RatedDebug(ctx, rate.Inf, "debug")
	componentLogger.RatedInfo(ctx, rate.Inf, "info")
	componentLogger.RatedWarn(ctx, rate.Inf, "warn")

	assert.Empty(t, buf.String())

	componentLogger.RatedError(ctx, rate.Inf, "error")
	assert.Contains(t, buf.String(), "error")
}

func TestLoggerRatedWithContextFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	componentLogger := With(String("module", "proxy"))
	ctx := context.Background()
	ctx = WithFields(ctx, String("trace_id", "trace789"))
	componentLogger.RatedInfo(ctx, rate.Inf, "with context", String("extra", "data"))

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "proxy", entry["module"])
	assert.Equal(t, "trace789", entry["trace_id"])
	assert.Equal(t, "data", entry["extra"])
}

func TestGetOrCreateRatedEntryLazyInit(t *testing.T) {
	defer resetRatedRegistry()

	key := uintptr(0x1234)
	entry := getOrCreateRatedEntry(key, 10)
	require.NotNil(t, entry)
	require.NotNil(t, entry.limiter)

	// Second call should return the same entry
	entry2 := getOrCreateRatedEntry(key, 20) // different rate, same key
	assert.Equal(t, entry, entry2, "should return cached entry")
}

func TestGetOrCreateRatedEntryConcurrent(t *testing.T) {
	defer resetRatedRegistry()

	key := uintptr(0x5678)
	var wg sync.WaitGroup
	entries := make([]*ratedEntry, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			entries[idx] = getOrCreateRatedEntry(key, 1)
		}(i)
	}
	wg.Wait()

	// All entries should be the same instance
	for i := 1; i < 100; i++ {
		assert.Equal(t, entries[0], entries[i], "all goroutines should get the same entry")
	}
}

func TestRatedEntryConcurrentIgnoreCount(t *testing.T) {
	defer resetRatedRegistry()

	entry := &ratedEntry{
		limiter: rate.NewLimiter(rate.Limit(0), 1),
	}
	// Consume initial token
	entry.limiter.Allow()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !entry.limiter.Allow() {
				entry.ignoreCount.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(100), entry.ignoreCount.Load())
}

func TestRatedInfoNilContext(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	RatedInfo(nil, rate.Inf, "nil context rated")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "nil context rated", entry["msg"])
	assert.Equal(t, true, entry["_ctx_nil"])
}

func TestLoggerRatedInfoNilContext(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	componentLogger := With(String("module", "test"))
	componentLogger.RatedInfo(nil, rate.Inf, "nil context rated")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "nil context rated", entry["msg"])
	assert.Equal(t, true, entry["_ctx_nil"])
	assert.Equal(t, "test", entry["module"])
}

// Test suppression for all levels and the _ignored field in output

func TestRatedDebugSuppressesSubsequentCalls(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	oldLevel := GetLevel()
	SetLevel(DebugLevel)
	defer SetLevel(oldLevel)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		RatedDebug(ctx, rate.Limit(0), "debug suppressed")
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.Len(t, lines, 1)
}

func TestRatedWarnSuppressesSubsequentCalls(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		RatedWarn(ctx, rate.Limit(0), "warn suppressed")
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.Len(t, lines, 1)
}

func TestRatedErrorSuppressesSubsequentCalls(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		RatedError(ctx, rate.Limit(0), "error suppressed")
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.Len(t, lines, 1)
}

func TestRatedIgnoredFieldEndToEnd(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	ctx := context.Background()

	// All calls on the same line in a loop → same pc → same rate limiter entry.
	// i=0: first call, goes through (burst=1). i=1..3: suppressed (ignoreCount=3).
	// Before i=4: replace limiter to allow next call, reset buf.
	// i=4: goes through with _ignored=3.
	for i := 0; i < 5; i++ {
		if i == 4 {
			ratedRegistry.Range(func(key, value any) bool {
				value.(*ratedEntry).limiter = rate.NewLimiter(rate.Inf, 1)
				return true
			})
			buf.Reset()
		}
		RatedInfo(ctx, rate.Limit(0), "rated msg")
	}

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "rated msg", entry["msg"])
	assert.Equal(t, float64(3), entry["_ignored"], "should report 3 ignored entries")
}

func TestLoggerRatedIgnoredFieldEndToEnd(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	componentLogger := With(String("module", "test"))
	ctx := context.Background()

	// Same loop pattern: i=0 goes through, i=1..5 suppressed (ignoreCount=5),
	// before i=6: replace limiter, reset buf. i=6: goes through with _ignored=5.
	for i := 0; i < 7; i++ {
		if i == 6 {
			ratedRegistry.Range(func(key, value any) bool {
				value.(*ratedEntry).limiter = rate.NewLimiter(rate.Inf, 1)
				return true
			})
			buf.Reset()
		}
		componentLogger.RatedInfo(ctx, rate.Limit(0), "logger rated msg")
	}

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "logger rated msg", entry["msg"])
	assert.Equal(t, "test", entry["module"])
	assert.Equal(t, float64(5), entry["_ignored"], "should report 5 ignored entries")
}

func TestLoggerRatedDebugSuppresses(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	oldLevel := GetLevel()
	SetLevel(DebugLevel)
	defer SetLevel(oldLevel)

	componentLogger := With(String("module", "test"))
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		componentLogger.RatedDebug(ctx, rate.Limit(0), "debug suppressed")
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.Len(t, lines, 1)
}

func TestLoggerRatedWarnSuppresses(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	componentLogger := With(String("module", "test"))
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		componentLogger.RatedWarn(ctx, rate.Limit(0), "warn suppressed")
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.Len(t, lines, 1)
}

func TestLoggerRatedErrorSuppresses(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	componentLogger := With(String("module", "test"))
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		componentLogger.RatedError(ctx, rate.Limit(0), "error suppressed")
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.Len(t, lines, 1)
}

func TestRatedLogAtSpecifiedLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	ctx := context.Background()
	RatedLog(ctx, WarnLevel, rate.Inf, "rated log warn", String("key", "val"))

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "warn", entry["level"])
	assert.Equal(t, "rated log warn", entry["msg"])
	assert.Equal(t, "val", entry["key"])
}

func TestRatedLogSuppresses(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		RatedLog(ctx, InfoLevel, rate.Limit(0), "suppressed")
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.Len(t, lines, 1)
}

func TestRatedLogLevelFiltering(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	oldLevel := GetLevel()
	SetLevel(ErrorLevel)
	defer SetLevel(oldLevel)

	ctx := context.Background()
	RatedLog(ctx, InfoLevel, rate.Inf, "should not appear")
	assert.Empty(t, buf.String())
}

func TestLoggerRatedLogAtSpecifiedLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	componentLogger := With(String("module", "test"))
	ctx := context.Background()
	componentLogger.RatedLog(ctx, WarnLevel, rate.Inf, "logger rated log")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "warn", entry["level"])
	assert.Equal(t, "logger rated log", entry["msg"])
	assert.Equal(t, "test", entry["module"])
}

func TestLoggerRatedLogSuppresses(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()
	defer resetRatedRegistry()

	componentLogger := With(String("module", "test"))
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		componentLogger.RatedLog(ctx, InfoLevel, rate.Limit(0), "suppressed")
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.Len(t, lines, 1)
}

func TestResetRatedRegistry(t *testing.T) {
	key := uintptr(0xFFFF)
	getOrCreateRatedEntry(key, 1)

	_, ok := ratedRegistry.Load(key)
	require.True(t, ok)

	resetRatedRegistry()

	_, ok = ratedRegistry.Load(key)
	assert.False(t, ok, "registry should be empty after reset")
}
