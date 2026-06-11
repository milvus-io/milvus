package checkpoint

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

type testBarrier struct {
	timetick uint64
}

func (b *testBarrier) TimeTick() uint64 {
	return b.timetick
}

func TestCheckpointManagerAdvanceMetaCheckpointByOrderedBarrierPrefix(t *testing.T) {
	manager := NewManager(&utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  1,
		},
	})
	first := &testBarrier{timetick: 2}
	second := &testBarrier{timetick: 2}

	manager.AddMetaBarrier(newTestConsumeCheckpoint(2), first)
	manager.AddMetaBarrier(newTestConsumeCheckpoint(3), second)

	snapshot := manager.Snapshot()
	assert.True(t, walimplstest.NewTestMessageID(2).EQ(snapshot.MessageID))
	assert.Equal(t, uint64(2), snapshot.TimeTick)
	assert.True(t, manager.ConsumeDirty())
	assert.False(t, manager.ConsumeDirty())

	second.timetick = 3
	manager.TryAdvanceMetaCheckpoint()

	snapshot = manager.Snapshot()
	assert.True(t, walimplstest.NewTestMessageID(3).EQ(snapshot.MessageID))
	assert.Equal(t, uint64(3), snapshot.TimeTick)
	assert.True(t, manager.ConsumeDirty())
}

func TestCheckpointManagerAdvanceDataCheckpointByOrderedBarrierPrefix(t *testing.T) {
	manager := NewManager(&utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  10,
		DataCheckpoint: &utility.WALConsumeCheckpoint{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  1,
		},
	})
	first := &testBarrier{timetick: 1}
	second := &testBarrier{timetick: 3}
	third := &testBarrier{timetick: 4}

	manager.AddDataBarrier(newTestConsumeCheckpoint(2), first)
	manager.AddDataBarrier(newTestConsumeCheckpoint(3), second)
	manager.AddDataBarrier(newTestConsumeCheckpoint(4), third)

	snapshot := manager.Snapshot()
	assert.True(t, walimplstest.NewTestMessageID(1).EQ(snapshot.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(1), snapshot.DataCheckpoint.TimeTick)
	assert.False(t, manager.ConsumeDirty())

	first.timetick = 2
	manager.TryAdvanceDataCheckpoint()

	snapshot = manager.Snapshot()
	assert.True(t, walimplstest.NewTestMessageID(4).EQ(snapshot.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(4), snapshot.DataCheckpoint.TimeTick)
	assert.True(t, manager.ConsumeDirty())
}

func TestCheckpointManagerImmediateBarriers(t *testing.T) {
	manager := NewManager(&utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
	})

	manager.AddImmediateMetaBarrier(newTestConsumeCheckpoint(2))
	manager.AddImmediateDataBarrier(newTestConsumeCheckpoint(2))

	snapshot := manager.Snapshot()
	require.NotNil(t, snapshot.DataCheckpoint)
	assert.True(t, walimplstest.NewTestMessageID(2).EQ(snapshot.MessageID))
	assert.Equal(t, uint64(2), snapshot.TimeTick)
	assert.True(t, walimplstest.NewTestMessageID(2).EQ(snapshot.DataCheckpoint.MessageID))
	assert.Equal(t, uint64(2), snapshot.DataCheckpoint.TimeTick)
	assert.True(t, manager.ConsumeDirty())
}

func TestCompositeBarriers(t *testing.T) {
	barrier := NewCompositeBarrier(
		&testBarrier{timetick: 10},
		&testBarrier{timetick: 8},
		&testBarrier{timetick: 12},
	)
	assert.Equal(t, uint64(8), barrier.TimeTick())

	barrier = NewCompositeBarrier(
		&testBarrier{timetick: 4},
		&testBarrier{timetick: 6},
		&testBarrier{timetick: 5},
	)
	assert.Equal(t, uint64(4), barrier.TimeTick())
	assert.Nil(t, NewCompositeBarrier(nil, nil))
}

func TestCheckpointManagerAdvanceMetaCheckpointInMemoryDoesNotDirty(t *testing.T) {
	manager := NewManager(&utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
	})

	manager.AdvanceMetaCheckpointInMemory(newTestConsumeCheckpoint(2))

	snapshot := manager.Snapshot()
	assert.True(t, walimplstest.NewTestMessageID(2).EQ(snapshot.MessageID))
	assert.Equal(t, uint64(2), snapshot.TimeTick)
	assert.False(t, manager.HasDirty())
}

func newTestConsumeCheckpoint(timetick uint64) utility.WALConsumeCheckpoint {
	return utility.WALConsumeCheckpoint{
		MessageID: walimplstest.NewTestMessageID(int64(timetick)),
		TimeTick:  timetick,
	}
}
