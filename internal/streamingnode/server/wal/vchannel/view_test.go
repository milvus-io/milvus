package vchannel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestConsumeDirtySnapshotKeepsStableInFlightView(t *testing.T) {
	view := NewVChannelView(
		&streamingpb.VChannelMeta{
			Vchannel:           "v1",
			CheckpointTimeTick: 10,
		},
		10,
		false,
		runtimeConfig{},
	)

	view.mu.Lock()
	view.meta.CheckpointTimeTick = 20
	view.dirty = true
	view.mu.Unlock()

	first := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, first)
	assert.Equal(t, uint64(20), first.GetCheckpointTimeTick())

	view.mu.Lock()
	view.meta.CheckpointTimeTick = 30
	view.dirty = true
	view.mu.Unlock()

	inFlight := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, inFlight)
	assert.Equal(t, uint64(20), inFlight.GetCheckpointTimeTick())

	view.MarkSnapshotPersisted(first)

	next := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, next)
	assert.Equal(t, uint64(30), next.GetCheckpointTimeTick())

	view.MarkSnapshotPersisted(next)
	assert.Nil(t, view.ConsumeDirtyAndGetSnapshot())
}
