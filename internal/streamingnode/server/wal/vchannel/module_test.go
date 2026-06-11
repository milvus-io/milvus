package vchannel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestCleanupDeleteSnapshotKeepsStableInFlightView(t *testing.T) {
	module := NewModule("p1", map[string]*streamingpb.VChannelMeta{
		"v1": {
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED,
			CheckpointTimeTick: 10,
			TombstoneTimeTick:  10,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	})
	module.NotifyCheckpointPersisted(11, 11)

	first := module.ConsumeDirtySnapshots()
	require.Len(t, first, 1)
	assert.Equal(t, moduleapi.SnapshotOpDelete, first[0].Op())

	second := module.ConsumeDirtySnapshots()
	require.Len(t, second, 1)
	assert.Same(t, first[0], second[0])
	assert.Len(t, module.snapshotViews(), 1)

	first[0].MarkPersisted()
	assert.Empty(t, module.ConsumeDirtySnapshots())
	assert.Empty(t, module.snapshotViews())
}
