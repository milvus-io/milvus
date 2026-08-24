//go:build test

package metricsutil

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestAppendMetricsDoneUsesProvidedTraceContext(t *testing.T) {
	paramtable.Init()

	logDir := t.TempDir()
	logFile := filepath.Join(logDir, "wal.log")
	logger, props, err := mlog.InitLogger(&mlog.Config{
		Level:             "debug",
		Format:            "json",
		DisableTimestamp:  true,
		DisableCaller:     true,
		DisableStacktrace: true,
		File: mlog.FileLogConfig{
			RootPath: logDir,
			Filename: "wal.log",
		},
	})
	require.NoError(t, err)
	mlog.ReplaceGlobals(logger, props)
	defer func() {
		_ = logger.Sync()
		restoreLogger, restoreProps, err := mlog.InitTestLogger(t, &mlog.Config{
			Level:             "info",
			DisableTimestamp:  true,
			DisableCaller:     true,
			DisableStacktrace: true,
		})
		require.NoError(t, err)
		mlog.ReplaceGlobals(restoreLogger, restoreProps)
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
	require.NoError(t, logger.Sync())

	content, err := os.ReadFile(logFile)
	require.NoError(t, err)
	var entry map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(content)), "\n") {
		var current map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &current))
		if current["traceID"] == "0102030405060708090a0b0c0d0e0f10" {
			entry = current
			break
		}
	}
	require.NotNil(t, entry, string(content))
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", entry["traceID"])
	assert.Equal(t, "0102030405060708", entry["spanID"])
}
