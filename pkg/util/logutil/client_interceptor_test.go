package logutil

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/stretchr/testify/assert"
)

func withSpan(ctx context.Context, h string) context.Context {
	traceID, err := trace.TraceIDFromHex(h)
	if err != nil {
		panic(err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
	})
	return trace.ContextWithSpanContext(ctx, sc)
}

func Test_wrapFields(t *testing.T) {
	ctx := withSpan(context.TODO(), "15613d04dbf2d0374387f6aedf98a372")
	newCtx := withLevelAndTrace(withMetaData(ctx, zapcore.DebugLevel))
	newCtx2 := wrapFields(newCtx)
	md, ok := metadata.FromOutgoingContext(newCtx2)
	assert.True(t, ok)
	traceIDs := md.Get(strings.ToLower(log.TraceIDKey))
	assert.Equal(t, 1, len(traceIDs))
	log.Ctx(newCtx).Info("test wrap fields", zap.String("traceID", traceIDs[0]))
}
