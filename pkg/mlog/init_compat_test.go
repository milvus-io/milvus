package mlog

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type compatMessage string

func (m compatMessage) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("value", string(m))
	return nil
}

func TestInitLoggerWithWriteSyncerReplacesMlogGlobalLogger(t *testing.T) {
	var buf bytes.Buffer
	cfg := &Config{Level: "debug", DisableTimestamp: true}

	logger, props, err := InitLoggerWithWriteSyncer(cfg, zapcore.AddSync(&buf), zap.AddCallerSkip(1))
	require.NoError(t, err)
	ReplaceGlobals(logger, props)
	defer resetCompatLogger()

	Info(context.Background(), "mlog initialized", String("key", "value"))
	require.NoError(t, logger.Sync())

	assert.Contains(t, buf.String(), `[INFO]`)
	assert.Contains(t, buf.String(), `["mlog initialized"]`)
	assert.Contains(t, buf.String(), `[key=value]`)
	assert.Equal(t, DebugLevel, GetLevel())
}

func resetCompatLogger() {
	cfg := zap.NewProductionConfig()
	cfg.Level = GetAtomicLevel()
	logger, _ := cfg.Build(zap.AddCallerSkip(1))
	globalLogger.Store(logger)
}

func TestTextEncoderAndCoreAreAvailableFromMlog(t *testing.T) {
	var buf bytes.Buffer
	cfg := &Config{Level: "debug", DisableTimestamp: true}

	encoder := NewTextEncoderByConfig(cfg)
	core := NewTextCore(encoder, zapcore.AddSync(&buf), zapcore.InfoLevel)
	logger := zap.New(core)

	logger.Info("text core")
	require.NoError(t, logger.Sync())

	assert.Equal(t, `[INFO] ["text core"]`+"\n", buf.String())
}

func TestCompatibilityFieldHelpers(t *testing.T) {
	fields := []Field{
		FieldComponent("streaming"),
		FieldMessage(compatMessage("one")),
		FieldMessages([]compatMessage{compatMessage("one"), compatMessage("two")}),
	}

	assert.Equal(t, "component", fields[0].Key)
	assert.Equal(t, "message", fields[1].Key)
	assert.Equal(t, "messages", fields[2].Key)
}
