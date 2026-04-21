package log

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func TestExporterV2(t *testing.T) {
	ts := newTestLogSpy(t)
	conf := &Config{Level: "debug", DisableTimestamp: true}
	logger, properties, _ := InitTestLogger(ts, conf)
	ReplaceGlobals(logger, properties)

	replaceLeveledLoggers(logger)
	ctx := WithTraceID(context.TODO(), "mock-trace")

	Ctx(ctx).Info("Info Test")
	Ctx(ctx).Debug("Debug Test")
	Ctx(ctx).Warn("Warn Test")
	Ctx(ctx).Error("Error Test")
	Ctx(ctx).Sync()

	ts.assertMessagesContains("log/mlogger_test.go")
	ts.assertMessagesContains("traceID=mock-trace")

	ts.CleanBuffer()
	// nolint
	Ctx(nil).Info("empty context")
	ts.assertMessagesNotContains("traceID")

	fieldCtx := WithFields(ctx, zap.String("field", "test"))
	reqCtx := WithReqID(ctx, 123456)
	modCtx := WithModule(ctx, "test")

	Ctx(fieldCtx).Info("Info Test")
	Ctx(fieldCtx).Sync()
	ts.assertLastMessageContains("field=test")
	ts.assertLastMessageContains("traceID=mock-trace")

	Ctx(reqCtx).Info("Info Test")
	Ctx(reqCtx).Sync()
	ts.assertLastMessageContains("reqID=123456")
	ts.assertLastMessageContains("traceID=mock-trace")
	ts.assertLastMessageNotContains("field=test")

	Ctx(modCtx).Info("Info Test")
	Ctx(modCtx).Sync()
	ts.assertLastMessageContains("module=test")
	ts.assertLastMessageContains("traceID=mock-trace")
	ts.assertLastMessageNotContains("reqID=123456")
	ts.assertLastMessageNotContains("field=test")
}

func TestMLoggerRatedLog(t *testing.T) {
	ts := newTestLogSpy(t)
	conf := &Config{Level: "debug", DisableTimestamp: true}
	logger, p, _ := InitTestLogger(ts, conf)
	ReplaceGlobals(logger, p)

	ctx := WithTraceID(context.TODO(), "test-trace")
	time.Sleep(time.Duration(1) * time.Second)
	success := Ctx(ctx).RatedDebug(1.0, "debug test")
	assert.True(t, success)

	time.Sleep(time.Duration(1) * time.Second)
	success = Ctx(ctx).RatedDebug(100.0, "debug test")
	assert.False(t, success)

	time.Sleep(time.Duration(1) * time.Second)
	success = Ctx(ctx).RatedInfo(1.0, "info test")
	assert.True(t, success)

	time.Sleep(time.Duration(1) * time.Second)
	success = Ctx(ctx).RatedWarn(1.0, "warn test")
	assert.True(t, success)

	time.Sleep(time.Duration(1) * time.Second)
	success = Ctx(ctx).RatedWarn(100.0, "warn test")
	assert.False(t, success)

	time.Sleep(time.Duration(1) * time.Second)
	success = Ctx(ctx).RatedInfo(100.0, "info test")
	assert.False(t, success)

	successNum := 0
	for i := 0; i < 1000; i++ {
		if Ctx(ctx).RatedInfo(1.0, "info test") {
			successNum++
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	assert.True(t, successNum < 1000)
	assert.True(t, successNum > 10)

	time.Sleep(time.Duration(3) * time.Second)
	success = Ctx(ctx).RatedInfo(3.0, "info test")
	assert.True(t, success)
	Ctx(ctx).Sync()
}

func TestMLoggerWithContext(t *testing.T) {
	ts := newTestLogSpy(t)
	logger, p, _ := InitTestLogger(ts, &Config{Level: "debug", DisableTimestamp: true})
	ReplaceGlobals(logger, p)
	replaceLeveledLoggers(logger)

	t.Run("nil context returns receiver unchanged", func(t *testing.T) {
		m := &MLogger{Logger: ctxL()}
		//nolint:staticcheck // deliberately testing nil ctx path
		assert.Same(t, m, m.WithContext(nil))
	})

	t.Run("context without span returns receiver unchanged", func(t *testing.T) {
		m := &MLogger{Logger: ctxL()}
		assert.Same(t, m, m.WithContext(context.Background()))
	})

	t.Run("valid span context attaches traceID field", func(t *testing.T) {
		traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
		spanID, _ := trace.SpanIDFromHex("0102030405060708")
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
		})
		ctx := trace.ContextWithSpanContext(context.Background(), sc)

		ts.CleanBuffer()
		m := (&MLogger{Logger: ctxL()}).WithContext(ctx)
		m.Info("with-context message")
		_ = m.Sync()
		ts.assertLastMessageContains("traceID=" + traceID.String())
	})
}

func TestNewIntentContext(t *testing.T) {
	ts := newTestLogSpy(t)
	conf := &Config{Level: "debug", DisableTimestamp: true}
	logger, p, _ := InitTestLogger(ts, conf)
	ReplaceGlobals(logger, p)

	replaceLeveledLoggers(logger)
	testName := "testRole"
	testIntent := "testIntent"
	ctx, span := NewIntentContext(testName, testIntent)
	traceID := span.SpanContext().TraceID().String()
	assert.NotNil(t, ctx)
	assert.NotNil(t, span)
	assert.NotNil(t, ctx.Value(CtxLogKey))
	mLogger, ok := ctx.Value(CtxLogKey).(*MLogger)
	assert.True(t, ok)
	assert.NotNil(t, mLogger)

	Ctx(ctx).Info("Info Test")
	Ctx(ctx).Debug("Debug Test")
	Ctx(ctx).Warn("Warn Test")
	Ctx(ctx).Error("Error Test")
	Ctx(ctx).Sync()

	ts.assertLastMessageContains(fmt.Sprintf("role=%s", testName))
	ts.assertLastMessageContains(fmt.Sprintf("intent=%s", testIntent))
	ts.assertLastMessageContains(fmt.Sprintf("traceID=%s", traceID))
}
