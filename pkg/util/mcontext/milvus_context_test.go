package mcontext

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/metadata"
)

func TestNewMilvusContext(t *testing.T) {
	ctx := context.Background()
	mctx := FromContext(ctx)
	assert.Equal(t, defaultMilvusContext, *mctx)

	ctx, _ = NewMilvusContext(ctx)
	assert.Equal(t, defaultMilvusContext, *mctx)
	ctx = AppendOutgoingContext(ctx)

	ctx = context.Background()
	md := metadata.New(map[string]string{
		sourceNodeIDKey:        "123",
		DestinationServerIDKey: "456",
		DestinationClusterKey:  "test-cluster",
		logLevelRPCMetaKey:     "debug",
	})
	ctx = metadata.NewIncomingContext(ctx, md)
	ctx, _ = NewMilvusContext(ctx)
	mctx = FromContext(ctx)

	assert.Equal(t, int64(123), mctx.SourceNodeID)
	assert.Equal(t, int64(456), mctx.DestinationNodeID)
	assert.Equal(t, "test-cluster", mctx.DestinationCluster)
	assert.Equal(t, zapcore.DebugLevel, mctx.LogLevel)

	paramtable.SetNodeID(345)
	ctx = AppendOutgoingContext(ctx)
	md, _ = metadata.FromOutgoingContext(ctx)
	assert.Equal(t, paramtable.GetStringNodeID(), md.Get(sourceNodeIDKey)[0])
	assert.Equal(t, paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), md.Get(DestinationClusterKey)[0])
	assert.Equal(t, "debug", md.Get(logLevelRPCMetaKey)[0])
}

func TestFromContext(t *testing.T) {
	t.Run("returns default context when no milvus context", func(t *testing.T) {
		ctx := context.Background()
		mctx := FromContext(ctx)
		assert.Equal(t, &defaultMilvusContext, mctx)
	})

	t.Run("returns milvus context when present", func(t *testing.T) {
		expected := &MilvusContext{
			SourceNodeID:       100,
			DestinationNodeID:  200,
			DestinationCluster: "my-cluster",
			LogLevel:           zapcore.InfoLevel,
		}
		ctx := context.WithValue(context.Background(), MilvusContextKeyValue, expected)
		mctx := FromContext(ctx)
		assert.Equal(t, expected, mctx)
	})
}

func TestNewMilvusContextWithLegacyLogLevel(t *testing.T) {
	t.Run("parses legacy log level key", func(t *testing.T) {
		md := metadata.New(map[string]string{
			logLevelRPCMetaKeyLegacy: "warn",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		ctx, mctx := NewMilvusContext(ctx)
		assert.Equal(t, zapcore.WarnLevel, mctx.LogLevel)
		assert.NotNil(t, ctx)
	})

	t.Run("prefers new log level key over legacy", func(t *testing.T) {
		md := metadata.New(map[string]string{
			logLevelRPCMetaKey:       "error",
			logLevelRPCMetaKeyLegacy: "debug",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, mctx := NewMilvusContext(ctx)
		assert.Equal(t, zapcore.ErrorLevel, mctx.LogLevel)
	})
}

func TestNewMilvusContextWithInvalidValues(t *testing.T) {
	t.Run("handles invalid source node ID", func(t *testing.T) {
		md := metadata.New(map[string]string{
			sourceNodeIDKey: "not-a-number",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, mctx := NewMilvusContext(ctx)
		assert.Equal(t, int64(0), mctx.SourceNodeID)
	})

	t.Run("handles invalid destination node ID", func(t *testing.T) {
		md := metadata.New(map[string]string{
			DestinationServerIDKey: "invalid",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, mctx := NewMilvusContext(ctx)
		assert.Equal(t, int64(0), mctx.DestinationNodeID)
	})

	t.Run("handles invalid log level", func(t *testing.T) {
		md := metadata.New(map[string]string{
			logLevelRPCMetaKey: "invalid-level",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, mctx := NewMilvusContext(ctx)
		assert.Equal(t, zapcore.InvalidLevel, mctx.LogLevel)
	})
}

func TestNewMilvusContextWithEmptyMetadata(t *testing.T) {
	t.Run("handles nil metadata", func(t *testing.T) {
		ctx := context.Background()
		_, mctx := NewMilvusContext(ctx)
		assert.Equal(t, int64(0), mctx.SourceNodeID)
		assert.Equal(t, int64(0), mctx.DestinationNodeID)
		assert.Equal(t, "", mctx.DestinationCluster)
		// LogLevel should be the default value when no metadata is present
		// (either InvalidLevel or the zero value depending on zap version)
		assert.True(t, mctx.LogLevel == zapcore.InvalidLevel || mctx.LogLevel == 0)
	})

	t.Run("handles empty metadata", func(t *testing.T) {
		md := metadata.New(map[string]string{})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, mctx := NewMilvusContext(ctx)
		assert.Equal(t, int64(0), mctx.SourceNodeID)
		assert.Equal(t, int64(0), mctx.DestinationNodeID)
		assert.Equal(t, "", mctx.DestinationCluster)
		// LogLevel should be the default value when no metadata is present
		assert.True(t, mctx.LogLevel == zapcore.InvalidLevel || mctx.LogLevel == 0)
	})
}

func TestNewMilvusContextIdempotent(t *testing.T) {
	t.Run("calling NewMilvusContext twice returns same context", func(t *testing.T) {
		md := metadata.New(map[string]string{
			sourceNodeIDKey:        "123",
			DestinationServerIDKey: "456",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		ctx1, mctx1 := NewMilvusContext(ctx)
		ctx2, mctx2 := NewMilvusContext(ctx1)

		// Second call should return the same context (no double wrapping)
		assert.Equal(t, ctx1, ctx2)
		// mctx2 should be defaultMilvusContext since context already has milvus context
		assert.Equal(t, &defaultMilvusContext, mctx2)
		assert.NotEqual(t, mctx1, mctx2)
	})
}

func TestUnmarshalMetadata(t *testing.T) {
	t.Run("returns first matching key", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"key1": "value1",
			"key2": "value2",
		})
		result := unmarshalMetadata(md, func(s string) string { return s }, "key1", "key2")
		assert.Equal(t, "value1", result)
	})

	t.Run("returns second key when first not found", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"key2": "value2",
		})
		result := unmarshalMetadata(md, func(s string) string { return s }, "key1", "key2")
		assert.Equal(t, "value2", result)
	})

	t.Run("returns unmarshaled empty string when no key found", func(t *testing.T) {
		md := metadata.New(map[string]string{})
		result := unmarshalMetadata(md, func(s string) string {
			if s == "" {
				return "default"
			}
			return s
		}, "missing")
		assert.Equal(t, "default", result)
	})

	t.Run("handles nil metadata", func(t *testing.T) {
		result := unmarshalMetadata(nil, func(s string) string {
			if s == "" {
				return "default"
			}
			return s
		}, "key")
		assert.Equal(t, "default", result)
	})
}

func TestAppendOutgoingContext(t *testing.T) {
	paramtable.Init()

	t.Run("appends all required metadata", func(t *testing.T) {
		ctx := context.Background()
		ctx = AppendOutgoingContext(ctx)

		md, ok := metadata.FromOutgoingContext(ctx)
		assert.True(t, ok)
		assert.NotEmpty(t, md.Get(logLevelRPCMetaKey))
		assert.NotEmpty(t, md.Get(sourceNodeIDKey))
		assert.NotEmpty(t, md.Get(DestinationClusterKey))
	})

	t.Run("preserves existing milvus context log level", func(t *testing.T) {
		mctx := &MilvusContext{
			LogLevel: zapcore.WarnLevel,
		}
		ctx := context.WithValue(context.Background(), MilvusContextKeyValue, mctx)
		ctx = AppendOutgoingContext(ctx)

		md, _ := metadata.FromOutgoingContext(ctx)
		assert.Equal(t, "warn", md.Get(logLevelRPCMetaKey)[0])
	})
}
