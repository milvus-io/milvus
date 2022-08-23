package logutil

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/metadata"
)

func TestCtxWithLevelAndTrace(t *testing.T) {
	t.Run("debug level", func(t *testing.T) {
		ctx := withMetaData(context.TODO(), zapcore.DebugLevel)
		newctx := withLevelAndTrace(ctx)

		assert.Equal(t, log.Ctx(log.WithDebugLevel(context.TODO())), log.Ctx(newctx))
	})

	t.Run("info level", func(t *testing.T) {
		ctx := context.TODO()
		newctx := withLevelAndTrace(withMetaData(ctx, zapcore.InfoLevel))
		assert.Equal(t, log.Ctx(log.WithInfoLevel(ctx)), log.Ctx(newctx))
	})

	t.Run("warn level", func(t *testing.T) {
		ctx := context.TODO()
		newctx := withLevelAndTrace(withMetaData(ctx, zapcore.WarnLevel))
		assert.Equal(t, log.Ctx(log.WithWarnLevel(ctx)), log.Ctx(newctx))
	})

	t.Run("error level", func(t *testing.T) {
		ctx := context.TODO()
		newctx := withLevelAndTrace(withMetaData(ctx, zapcore.ErrorLevel))
		assert.Equal(t, log.Ctx(log.WithErrorLevel(ctx)), log.Ctx(newctx))
	})

	t.Run("fatal level", func(t *testing.T) {
		ctx := context.TODO()
		newctx := withLevelAndTrace(withMetaData(ctx, zapcore.FatalLevel))
		assert.Equal(t, log.Ctx(log.WithFatalLevel(ctx)), log.Ctx(newctx))
	})

	t.Run(("pass through variables"), func(t *testing.T) {
		md := metadata.New(map[string]string{
			logLevelRPCMetaKey: zapcore.ErrorLevel.String(),
			clientRequestIDKey: "client-req-id",
		})
		ctx := metadata.NewIncomingContext(context.TODO(), md)
		newctx := withLevelAndTrace(ctx)
		md, ok := metadata.FromOutgoingContext(newctx)
		assert.True(t, ok)
		assert.Equal(t, "client-req-id", md.Get(clientRequestIDKey)[0])
		assert.Equal(t, zapcore.ErrorLevel.String(), md.Get(logLevelRPCMetaKey)[0])
	})
}

func withMetaData(ctx context.Context, level zapcore.Level) context.Context {
	md := metadata.New(map[string]string{
		logLevelRPCMetaKey: level.String(),
	})
	return metadata.NewIncomingContext(context.TODO(), md)
}
