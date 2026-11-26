package handler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestLatestRejectedBeforeConsumerInitialization(t *testing.T) {
	policy := &streamingpb.DeliverPolicy{Policy: &streamingpb.DeliverPolicy_Latest{}}
	handler := &handlerClientImpl{lifetime: typeutil.NewLifetime()}
	c, err := handler.CreateConsumer(context.Background(), &ConsumerOptions{DeliverPolicy: policy})
	require.Nil(t, c)
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsInvalidArgument())
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
}
