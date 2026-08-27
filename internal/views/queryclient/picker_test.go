package queryclient

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPrimaryReplicaPicker(t *testing.T) {
	picker := NewPrimaryReplicaPicker()
	primary := testShard("v0")

	actual, err := picker.Pick(context.Background(), &resolver.ShardReplicas{
		VChannel:       primary.VChannel,
		PrimaryShardID: primary,
	})
	require.NoError(t, err)
	require.Equal(t, primary, actual)

	_, err = picker.Pick(context.Background(), nil)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)

	_, err = picker.Pick(context.Background(), &resolver.ShardReplicas{VChannel: "v0"})
	require.ErrorIs(t, err, merr.ErrServiceNotReady)

	_, err = picker.Pick(context.Background(), &resolver.ShardReplicas{
		VChannel:       "v0",
		PrimaryShardID: qviews.ShardID{ReplicaID: 1, VChannel: "v1"},
	})
	require.True(t, errors.Is(err, merr.ErrServiceInternal))
}
