//go:build test

package mlog

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// testLogEntry represents a parsed JSON log entry
type testLogEntry struct {
	Level   string `json:"level"`
	Message string `json:"msg"`
	// Additional fields are captured dynamically
}

// createTestLogger creates a zap logger that writes to a buffer for testing
func createTestLogger(buf *bytes.Buffer) *zap.Logger {
	encoderConfig := zapcore.EncoderConfig{
		MessageKey:  "msg",
		LevelKey:    "level",
		TimeKey:     "time",
		EncodeLevel: zapcore.LowercaseLevelEncoder,
		EncodeTime:  zapcore.ISO8601TimeEncoder,
	}
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(buf),
		zapcore.DebugLevel,
	)
	return zap.New(core)
}

func TestInfoLogsAtInfoLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	ctx := context.Background()
	Info(ctx, "test message")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "info", entry["level"])
	assert.Equal(t, "test message", entry["msg"])
}

func TestDebugLogsAtDebugLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Set global level to Debug to enable debug logs
	oldLevel := GetLevel()
	SetLevel(DebugLevel)
	defer SetLevel(oldLevel)

	ctx := context.Background()
	Debug(ctx, "debug message")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "debug", entry["level"])
	assert.Equal(t, "debug message", entry["msg"])
}

func TestWarnLogsAtWarnLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	ctx := context.Background()
	Warn(ctx, "warn message")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "warn", entry["level"])
	assert.Equal(t, "warn message", entry["msg"])
}

func TestErrorLogsAtErrorLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	ctx := context.Background()
	Error(ctx, "error message")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "error", entry["level"])
	assert.Equal(t, "error message", entry["msg"])
}

func TestLogIncludesCallSiteFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	ctx := context.Background()
	Info(ctx, "test", String("key", "value"), Int64("count", 42))

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "value", entry["key"])
	assert.Equal(t, float64(42), entry["count"]) // JSON numbers are float64
}

func TestLogIncludesContextFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	ctx := context.Background()
	ctx = WithFields(ctx, String("request_id", "abc123"))
	Info(ctx, "test")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "abc123", entry["request_id"])
}

func TestLogCombinesContextAndCallSiteFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	ctx := context.Background()
	ctx = WithFields(ctx, String("ctx_field", "ctx_value"))
	Info(ctx, "test", String("call_field", "call_value"))

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "ctx_value", entry["ctx_field"])
	assert.Equal(t, "call_value", entry["call_field"])
}

func TestNilContextAddsWarningField(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	Info(nil, "test with nil context")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, true, entry["_ctx_nil"])
}

func TestLogLevelFiltering(t *testing.T) {
	buf := &bytes.Buffer{}

	// Create logger with Info level
	encoderConfig := zapcore.EncoderConfig{
		MessageKey:  "msg",
		LevelKey:    "level",
		EncodeLevel: zapcore.LowercaseLevelEncoder,
	}
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(buf),
		zapcore.InfoLevel, // Only Info and above
	)
	logger := zap.New(core)
	Init(logger)
	defer resetLogger()

	ctx := context.Background()
	Debug(ctx, "debug message") // Should be filtered
	Info(ctx, "info message")   // Should be logged

	// Only one line should be logged
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.Len(t, lines, 1)

	var entry map[string]interface{}
	err := json.Unmarshal(lines[0], &entry)
	require.NoError(t, err)
	assert.Equal(t, "info message", entry["msg"])
}

func TestLogFunction(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	ctx := context.Background()
	Log(ctx, WarnLevel, "log function test")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "warn", entry["level"])
	assert.Equal(t, "log function test", entry["msg"])
}

// resetLogger restores the default logger after test
func resetLogger() {
	cfg := zap.NewProductionConfig()
	cfg.Level = globalLevel
	logger, _ := cfg.Build(zap.AddCallerSkip(1))
	globalLogger.Store(logger)
}

func TestInitNodeSetsNodeId(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	InitNode(logger, 12345)
	defer resetLogger()

	ctx := context.Background()
	Info(ctx, "test message")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, float64(12345), entry[KeyNodeID])
}

func TestInitNodeFieldIncludedInAllLogs(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	InitNode(logger, 99)
	defer resetLogger()

	// Set global level to Debug to enable all logs
	oldLevel := GetLevel()
	SetLevel(DebugLevel)
	defer SetLevel(oldLevel)

	ctx := context.Background()
	Debug(ctx, "debug msg")
	Info(ctx, "info msg")
	Warn(ctx, "warn msg")
	Error(ctx, "error msg")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.Len(t, lines, 4)

	for _, line := range lines {
		var entry map[string]interface{}
		err := json.Unmarshal(line, &entry)
		require.NoError(t, err)
		assert.Equal(t, float64(99), entry[KeyNodeID], "nodeId should be in all log entries")
	}
}

func TestEarlyReturnWhenLevelDisabled(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Set level to Error, so Debug/Info/Warn should be skipped
	oldLevel := GetLevel()
	SetLevel(ErrorLevel)
	defer SetLevel(oldLevel)

	ctx := context.Background()

	// These should return early without any processing
	Debug(ctx, "debug message")
	Info(ctx, "info message")
	Warn(ctx, "warn message")

	// Buffer should be empty
	assert.Empty(t, buf.String(), "no logs should be written when level is disabled")

	// Error should still work
	Error(ctx, "error message")
	assert.Contains(t, buf.String(), "error message")
}

// Tests for component Logger

func TestWithCreatesLoggerWithFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	componentLogger := With(String("module", "querynode"), Int64("node_id", 123))
	ctx := context.Background()
	componentLogger.Info(ctx, "component log")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "querynode", entry["module"])
	assert.Equal(t, float64(123), entry["node_id"])
	assert.Equal(t, "component log", entry["msg"])
}

func TestLoggerCombinesWithContextFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	componentLogger := With(String("module", "datanode"))
	ctx := context.Background()
	ctx = WithFields(ctx, String("trace_id", "abc123"), Int64("collection_id", 456))
	componentLogger.Info(ctx, "combined log")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "datanode", entry["module"])
	assert.Equal(t, "abc123", entry["trace_id"])
	assert.Equal(t, float64(456), entry["collection_id"])
}

func TestLoggerWith(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	baseLogger := With(String("module", "proxy"))
	childLogger := baseLogger.With(String("component", "search"))

	ctx := context.Background()
	childLogger.Info(ctx, "child log")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "proxy", entry["module"])
	assert.Equal(t, "search", entry["component"])
}

func TestLoggerWithLazy(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	baseLogger := With(String("module", "indexnode"))
	childLogger := baseLogger.WithLazy(String("task", "build"))

	ctx := context.Background()
	childLogger.Info(ctx, "lazy log")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "indexnode", entry["module"])
	assert.Equal(t, "build", entry["task"])
}

func TestLoggerLevel(t *testing.T) {
	componentLogger := With()

	oldLevel := GetLevel()
	SetLevel(WarnLevel)
	defer SetLevel(oldLevel)

	assert.Equal(t, WarnLevel, componentLogger.Level())
}

func TestLoggerAllLevels(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	oldLevel := GetLevel()
	SetLevel(DebugLevel)
	defer SetLevel(oldLevel)

	componentLogger := With(String("module", "test"))
	ctx := context.Background()

	componentLogger.Debug(ctx, "debug msg")
	componentLogger.Info(ctx, "info msg")
	componentLogger.Warn(ctx, "warn msg")
	componentLogger.Error(ctx, "error msg")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	assert.Len(t, lines, 4)

	expectedLevels := []string{"debug", "info", "warn", "error"}
	for i, line := range lines {
		var entry map[string]interface{}
		err := json.Unmarshal(line, &entry)
		require.NoError(t, err)
		assert.Equal(t, expectedLevels[i], entry["level"])
		assert.Equal(t, "test", entry["module"])
	}
}

func TestLoggerNilContext(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	componentLogger := With(String("module", "test"))
	componentLogger.Info(nil, "nil context log")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "test", entry["module"])
	assert.Equal(t, true, entry["_ctx_nil"])
}

func TestLoggerOptimizationUsesCtxLoggerWhenMoreFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Component has 1 field
	componentLogger := With(String("module", "test"))

	// Context has 3 fields (more than component)
	ctx := context.Background()
	ctx = WithFields(ctx,
		String("trace_id", "trace123"),
		String("span_id", "span456"),
		Int64("collection_id", 789),
	)

	componentLogger.Info(ctx, "optimized log", String("extra", "value"))

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)

	// All fields should be present
	assert.Equal(t, "test", entry["module"])
	assert.Equal(t, "trace123", entry["trace_id"])
	assert.Equal(t, "span456", entry["span_id"])
	assert.Equal(t, float64(789), entry["collection_id"])
	assert.Equal(t, "value", entry["extra"])
}

func TestLoggerOptimizationUsesComponentLoggerWhenMoreFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Component has 3 fields
	componentLogger := With(
		String("module", "querynode"),
		Int64("node_id", 123),
		String("role", "worker"),
	)

	// Context has 1 field (less than component)
	ctx := context.Background()
	ctx = WithFields(ctx, String("trace_id", "trace123"))

	componentLogger.Info(ctx, "optimized log")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)

	// All fields should be present
	assert.Equal(t, "querynode", entry["module"])
	assert.Equal(t, float64(123), entry["node_id"])
	assert.Equal(t, "worker", entry["role"])
	assert.Equal(t, "trace123", entry["trace_id"])
}

func TestLoggerEarlyReturnWhenDisabled(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	oldLevel := GetLevel()
	SetLevel(ErrorLevel)
	defer SetLevel(oldLevel)

	componentLogger := With(String("module", "test"))
	ctx := context.Background()

	componentLogger.Debug(ctx, "debug")
	componentLogger.Info(ctx, "info")
	componentLogger.Warn(ctx, "warn")

	assert.Empty(t, buf.String())

	componentLogger.Error(ctx, "error")
	assert.Contains(t, buf.String(), "error")
}

func TestLoggerWithEmptyFields(t *testing.T) {
	componentLogger := With(String("module", "test"))

	// With empty fields should return the same logger
	sameLogger := componentLogger.With()
	assert.Equal(t, componentLogger, sameLogger)

	sameLazyLogger := componentLogger.WithLazy()
	assert.Equal(t, componentLogger, sameLazyLogger)
}

// Test package-level WithLazy function
func TestPackageLevelWithLazy(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Test WithLazy with fields
	componentLogger := WithLazy(String("module", "lazytest"), Int64("node_id", 999))
	ctx := context.Background()
	componentLogger.Info(ctx, "lazy log message")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "lazytest", entry["module"])
	assert.Equal(t, float64(999), entry["node_id"])
}

// Test package-level WithLazy with empty fields
func TestPackageLevelWithLazyEmptyFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Test WithLazy with no fields
	componentLogger := WithLazy()
	ctx := context.Background()
	componentLogger.Info(ctx, "no fields message")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "no fields message", entry["msg"])
}

// Test package-level With with empty fields
func TestPackageLevelWithEmptyFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Test With with no fields
	componentLogger := With()
	ctx := context.Background()
	componentLogger.Info(ctx, "no fields message")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "no fields message", entry["msg"])
}

// Test Logger.log with ctx logger having more fields (branch: len(l.fields) == 0 && len(fields) == 0)
func TestLoggerLogCtxMoreFieldsNoComponentNoExtra(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Component has 0 fields
	componentLogger := With()

	// Context has fields
	ctx := context.Background()
	ctx = WithFields(ctx, String("trace_id", "trace123"))

	// Log with no extra fields
	componentLogger.Info(ctx, "test message")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "trace123", entry["trace_id"])
}

// Test Logger.log with ctx logger having more fields (branch: len(l.fields) == 0 && len(fields) > 0)
func TestLoggerLogCtxMoreFieldsNoComponentWithExtra(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Component has 0 fields
	componentLogger := With()

	// Context has fields
	ctx := context.Background()
	ctx = WithFields(ctx, String("trace_id", "trace123"))

	// Log with extra fields
	componentLogger.Info(ctx, "test message", String("extra", "value"))

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "trace123", entry["trace_id"])
	assert.Equal(t, "value", entry["extra"])
}

// Test Logger.log with ctx logger having more fields (branch: len(l.fields) > 0 && len(fields) == 0)
func TestLoggerLogCtxMoreFieldsWithComponentNoExtra(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Component has 1 field
	componentLogger := With(String("module", "test"))

	// Context has more fields (3 fields)
	ctx := context.Background()
	ctx = WithFields(ctx,
		String("trace_id", "trace123"),
		String("span_id", "span456"),
		Int64("collection_id", 789),
	)

	// Log with no extra fields
	componentLogger.Info(ctx, "test message")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "test", entry["module"])
	assert.Equal(t, "trace123", entry["trace_id"])
}

// Test Logger.log with component having more fields (branch: len(ctxFields) == 0 && len(fields) == 0)
func TestLoggerLogComponentMoreFieldsNoCtxNoExtra(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Component has fields
	componentLogger := With(String("module", "test"), Int64("node_id", 123))

	// Context has no fields
	ctx := context.Background()

	// Log with no extra fields
	componentLogger.Info(ctx, "test message")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "test", entry["module"])
	assert.Equal(t, float64(123), entry["node_id"])
}

// Test Logger.log with component having more fields (branch: len(ctxFields) == 0 && len(fields) > 0)
func TestLoggerLogComponentMoreFieldsNoCtxWithExtra(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Component has fields
	componentLogger := With(String("module", "test"), Int64("node_id", 123))

	// Context has no fields
	ctx := context.Background()

	// Log with extra fields
	componentLogger.Info(ctx, "test message", String("extra", "value"))

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "test", entry["module"])
	assert.Equal(t, "value", entry["extra"])
}

// Test Logger.log with component having more fields (branch: len(ctxFields) > 0 && len(fields) == 0)
func TestLoggerLogComponentMoreFieldsWithCtxNoExtra(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Component has 3 fields (more than ctx)
	componentLogger := With(
		String("module", "querynode"),
		Int64("node_id", 123),
		String("role", "worker"),
	)

	// Context has 1 field
	ctx := context.Background()
	ctx = WithFields(ctx, String("trace_id", "trace123"))

	// Log with no extra fields
	componentLogger.Info(ctx, "test message")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "querynode", entry["module"])
	assert.Equal(t, "trace123", entry["trace_id"])
}

// Test Logger.log nil context with fields
func TestLoggerLogNilContextWithFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	componentLogger := With(String("module", "test"))

	// Log with nil context and extra fields
	componentLogger.Info(nil, "nil context message", String("extra", "value"))

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "test", entry["module"])
	assert.Equal(t, "value", entry["extra"])
	assert.Equal(t, true, entry["_ctx_nil"])
}

// Test Logger.log nil context without fields
func TestLoggerLogNilContextNoFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	componentLogger := With(String("module", "test"))

	// Log with nil context and no extra fields
	componentLogger.Info(nil, "nil context message")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "test", entry["module"])
	assert.Equal(t, true, entry["_ctx_nil"])
}

// Test global log function with context that has cached logger
func TestGlobalLogWithCachedLogger(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Create context with fields (this creates a cached logger)
	ctx := context.Background()
	ctx = WithFields(ctx, String("trace_id", "trace123"))

	// Log using global function - should use cached logger
	Info(ctx, "message with cached logger")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "trace123", entry["trace_id"])
}

// Test Logger.log with component having more fields, ctx has some fields, AND extra fields passed (default branch)
func TestLoggerLogComponentMoreFieldsWithCtxAndExtra(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Component has 3 fields (more than ctx)
	componentLogger := With(
		String("module", "querynode"),
		Int64("node_id", 123),
		String("role", "worker"),
	)

	// Context has 1 field (less than component)
	ctx := context.Background()
	ctx = WithFields(ctx, String("trace_id", "trace123"))

	// Log with extra fields - this should hit the default branch
	componentLogger.Info(ctx, "test message", String("extra", "value"), Int64("count", 42))

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "querynode", entry["module"])
	assert.Equal(t, float64(123), entry["node_id"])
	assert.Equal(t, "trace123", entry["trace_id"])
	assert.Equal(t, "value", entry["extra"])
	assert.Equal(t, float64(42), entry["count"])
}

// Test global log function with context that has fields but uses FieldsFromContext path
// This tests the branch where ctx != nil, loggerFromContext returns nil, and ctxFields > 0
func TestGlobalLogWithContextFieldsNoCache(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Create context with fields
	ctx := context.Background()
	ctx = WithFields(ctx, String("trace_id", "trace456"), Int64("user_id", 789))

	// Log with extra fields
	Info(ctx, "message with context fields", String("extra", "data"))

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "trace456", entry["trace_id"])
	assert.Equal(t, float64(789), entry["user_id"])
	assert.Equal(t, "data", entry["extra"])
}

// Test global log function with context that has no cached logger but has fields
// This is a defensive code path that covers the branch at logger.go:67-68
func TestGlobalLogNoCachedLoggerWithFields(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := createTestLogger(buf)
	Init(logger)
	defer resetLogger()

	// Directly create a context with logContext that has fields but nil logger
	// This simulates a potential edge case
	field := String("manual_field", "manual_value")
	lc := &logContext{
		fieldKeys: map[string]*Field{
			"manual_field": &field,
		},
		logger: nil, // explicitly nil
	}
	ctx := context.WithValue(context.Background(), fieldsKey, lc)

	// Log - should use FieldsFromContext path
	Info(ctx, "message with manual fields")

	var entry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err)
	assert.Equal(t, "manual_value", entry["manual_field"])
}
