//go:build test

package metricsutil

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestAppendMetricsDoneUsesProvidedTraceContext(t *testing.T) {
	paramtable.Init()

	buf := &bytes.Buffer{}
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			MessageKey: "msg",
			LevelKey:   "level",
		}),
		zapcore.AddSync(buf),
		zapcore.DebugLevel,
	))
	mlog.ReplaceGlobals(logger, nil)
	oldLevel := mlog.GetLevel()
	mlog.SetLevel(mlog.DebugLevel)
	defer func() {
		mlog.SetLevel(oldLevel)
		mlog.ReplaceGlobals(zap.NewNop(), nil)
	}()

	traceID, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	}))

	writeMetrics := NewWriteMetrics(types.PChannelInfo{Name: "pchannel-test", Term: 1}, message.WALNameTest)
	msg := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		WithAllVChannel().
		MustBuildMutable()
	appendMetrics := writeMetrics.StartAppend(msg)
	appendMetrics.Done(ctx, &types.AppendResult{
		MessageID:              walimplstest.NewTestMessageID(1),
		LastConfirmedMessageID: walimplstest.NewTestMessageID(1),
		TimeTick:               1,
	}, nil)

	var entry map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
		var current map[string]any
		require.NoError(t, json.Unmarshal(line, &current))
		if current["msg"] == "append message into wal" {
			entry = current
			break
		}
	}
	require.NotNil(t, entry)
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", entry["traceID"])
	assert.Equal(t, "0102030405060708", entry["spanID"])
}
