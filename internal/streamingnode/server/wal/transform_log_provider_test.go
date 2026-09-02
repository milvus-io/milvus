package wal

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

type providerWAL struct{ WAL }

func (*providerWAL) TransformLog() TransformLogAccesser { panic("mockey") }
func TestTransformLogProviderBoundary(t *testing.T) {
	_, err := TransformLogFor(nil).AcquireStream(context.Background(), "p")
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
	expected := NewTransformLogErrorAccesser(context.DeadlineExceeded)
	patch := mockey.Mock((*providerWAL).TransformLog).Return(expected).Build()
	defer patch.UnPatch()
	_, err = TransformLogFor(&providerWAL{}).AcquireStream(context.Background(), "p")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
