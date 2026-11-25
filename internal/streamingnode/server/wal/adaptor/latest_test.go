package adaptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestLatestRejectedBeforeConsumerInitialization(t *testing.T) {
	policy := &streamingpb.DeliverPolicy{Policy: &streamingpb.DeliverPolicy_Latest{}}
	s, err := (&roWALAdaptorImpl{}).Read(context.Background(), wal.ReadOption{DeliverPolicy: policy})
	require.Nil(t, s)
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsInvalidArgument())
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
}
