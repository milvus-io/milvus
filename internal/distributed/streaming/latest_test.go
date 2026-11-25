package streaming

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestLatestRejectedBeforeConsumerInitialization(t *testing.T) {
	policy := &streamingpb.DeliverPolicy{Policy: &streamingpb.DeliverPolicy_Latest{}}
	s := (&walAccesserImpl{}).Read(context.Background(), ReadOption{DeliverPolicy: policy})
	select {
	case <-s.Done():
	default:
		t.Fatal("deprecated policy must return a completed error scanner")
	}
	err := s.Error()
	s.Close()
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsInvalidArgument())
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
}
