package replicates

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/stretchr/testify/assert"
)

func TestNonReplicateManager(t *testing.T) {
	rm, err := RecoverReplicateManager(&ReplicateManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{
			Name: "test",
			Term: 1,
		},
		CurrentClusterID: "test",
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			Checkpoint: &utility.WALCheckpoint{
				MessageID:           walimplstest.NewTestMessageID(1),
				TimeTick:            1,
				ReplicateCheckpoint: nil,
				ReplicateConfig:     nil,
			},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, rm.Role(), replicateutil.RolePrimary)

	ctx := context.Background()
	g, err := rm.BeginReplicateMessage(ctx, message.NewCreateSegmentMessageBuilderV2().
		WithHeader(&message.CreateSegmentMessageHeader{}).
		WithBody(&message.CreateSegmentMessageBody{}).
		WithVChannel("v1").
		MustBuildMutable())
	assert.ErrorIs(t, err, ErrNotReplicateMessage)
	assert.Nil(t, g)
}
