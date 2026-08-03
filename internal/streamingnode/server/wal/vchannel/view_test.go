package vchannel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestVChannelDataVersionSummaryTracksCurrentAndPersistedSnapshots(t *testing.T) {
	view := NewVChannelView(
		&streamingpb.VChannelMeta{
			Vchannel:                  "v1",
			CheckpointTimeTick:        10,
			SegmentDataVersionSummary: &viewpb.DataVersion{StreamingVersion: 5},
		},
		10,
		false,
		runtimeConfig{},
	)

	assert.False(t, view.AdvanceSegmentDataVersionSummary(qviews.DataVersion{StreamingVersion: 4}))
	assert.True(t, view.AdvanceSegmentDataVersionSummary(qviews.DataVersion{StreamingVersion: 7}))
	assert.Equal(t, qviews.DataVersion{StreamingVersion: 7}, view.SegmentDataVersionSummary())
	assert.Equal(t, qviews.DataVersion{StreamingVersion: 5}, view.PersistedSegmentDataVersionSummary())

	first, saveSchemas := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, first)
	assert.False(t, saveSchemas)
	assert.Equal(t, int64(7), first.GetSegmentDataVersionSummary().GetStreamingVersion())

	assert.True(t, view.AdvanceSegmentDataVersionSummary(qviews.DataVersion{StreamingVersion: 9}))
	view.MarkSnapshotPersisted(first)
	assert.Equal(t, qviews.DataVersion{StreamingVersion: 7}, view.PersistedSegmentDataVersionSummary())
	assert.Equal(t, qviews.DataVersion{StreamingVersion: 9}, view.SegmentDataVersionSummary())

	second, saveSchemas := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, second)
	assert.False(t, saveSchemas)
	assert.Equal(t, int64(9), second.GetSegmentDataVersionSummary().GetStreamingVersion())
	view.MarkSnapshotPersisted(second)
	assert.Equal(t, qviews.DataVersion{StreamingVersion: 9}, view.PersistedSegmentDataVersionSummary())
}

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

	first, _ := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, first)
	assert.Equal(t, uint64(20), first.GetCheckpointTimeTick())

	view.mu.Lock()
	view.meta.CheckpointTimeTick = 30
	view.dirty = true
	view.mu.Unlock()

	inFlight, _ := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, inFlight)
	assert.Equal(t, uint64(20), inFlight.GetCheckpointTimeTick())

	view.MarkSnapshotPersisted(first)

	next, _ := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, next)
	assert.Equal(t, uint64(30), next.GetCheckpointTimeTick())

	view.MarkSnapshotPersisted(next)
	final, _ := view.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, final)
}

func TestVChannelDirtySnapshotPersistsSchemasOnlyWhenRequired(t *testing.T) {
	meta := &streamingpb.VChannelMeta{
		Vchannel:           "v1",
		CheckpointTimeTick: 10,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{{
				State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				CheckpointTimeTick: 10,
			}},
		},
	}
	persisted := NewVChannelView(meta, 10, false, runtimeConfig{})
	require.True(t, persisted.AdvanceSegmentDataVersionSummary(qviews.DataVersion{StreamingVersion: 1}))
	_, saveSchemas := persisted.ConsumeDirtyAndGetSnapshot()
	assert.False(t, saveSchemas)

	created := NewVChannelView(meta, 0, true, runtimeConfig{})
	_, saveSchemas = created.ConsumeDirtyAndGetSnapshot()
	assert.True(t, saveSchemas)
}

func TestRecoveredVChannelViewAdoptsOwnedMetaButSnapshotsClone(t *testing.T) {
	meta := &streamingpb.VChannelMeta{Vchannel: "v1", CheckpointTimeTick: 10}
	view := newVChannelViewFromOwnedMeta(meta)

	assert.Same(t, meta, view.meta)
	snapshot := view.AssignmentMeta()
	assert.NotSame(t, meta, snapshot)
	assert.Equal(t, meta, snapshot)
}
