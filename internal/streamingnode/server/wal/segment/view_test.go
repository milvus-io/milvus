package segment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestConsumeDirtySnapshotKeepsStableInFlightView(t *testing.T) {
	view := newSegmentView(
		&streamingpb.SegmentAssignmentMeta{
			SegmentId:              1,
			CheckpointTimeTick:     10,
			DataCheckpointTimeTick: 10,
			PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
		},
		10,
		10,
		false,
		writeOnlyInsertBuffer{},
		nil,
		runtimeConfig{},
	)

	view.mu.Lock()
	view.meta.CheckpointTimeTick = 20
	view.meta.DataCheckpointTimeTick = 20
	view.dirty = true
	view.mu.Unlock()

	first := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, first)
	assert.Equal(t, uint64(20), first.GetCheckpointTimeTick())
	assert.Equal(t, uint64(20), first.GetDataCheckpointTimeTick())

	view.mu.Lock()
	view.meta.CheckpointTimeTick = 30
	view.meta.DataCheckpointTimeTick = 30
	view.dirty = true
	view.mu.Unlock()

	inFlight := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, inFlight)
	assert.Equal(t, uint64(20), inFlight.GetCheckpointTimeTick())
	assert.Equal(t, uint64(20), inFlight.GetDataCheckpointTimeTick())

	view.MarkSnapshotPersisted(first)

	next := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, next)
	assert.Equal(t, uint64(30), next.GetCheckpointTimeTick())
	assert.Equal(t, uint64(30), next.GetDataCheckpointTimeTick())

	view.MarkSnapshotPersisted(next)
	assert.Nil(t, view.ConsumeDirtyAndGetSnapshot())
}
