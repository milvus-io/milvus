package log

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
