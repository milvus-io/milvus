package checkpoint

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// Barrier reports the latest timetick whose module state is durable for a
// checkpoint lane.
type Barrier interface {
	TimeTick() uint64
}

type BarrierFunc func() uint64

func (f BarrierFunc) TimeTick() uint64 {
	return f()
}

type compositeBarrier []Barrier

func NewCompositeBarrier(barriers ...Barrier) Barrier {
	nonNil := make([]Barrier, 0, len(barriers))
	for _, barrier := range barriers {
		if barrier != nil {
			nonNil = append(nonNil, barrier)
		}
	}
	if len(nonNil) == 0 {
		return nil
	}
	return compositeBarrier(nonNil)
}

func (b compositeBarrier) TimeTick() uint64 {
	minTimeTick := b[0].TimeTick()
	for _, barrier := range b[1:] {
		if timetick := barrier.TimeTick(); timetick < minTimeTick {
			minTimeTick = timetick
		}
	}
	return minTimeTick
}

type barrierEntry struct {
	point   utility.WALConsumeCheckpoint
	barrier Barrier
}

type Manager struct {
	checkpoint *utility.WALCheckpoint

	metaBarriers []barrierEntry
	dataBarriers []barrierEntry

	checkpointDirty bool
}

func NewManager(checkpoint *utility.WALCheckpoint) *Manager {
	if checkpoint == nil {
		checkpoint = &utility.WALCheckpoint{}
	}
	return &Manager{
		checkpoint: checkpoint.Clone(),
	}
}

func (m *Manager) Checkpoint() *utility.WALCheckpoint {
	return m.checkpoint
}

func (m *Manager) AddMetaBarrier(point utility.WALConsumeCheckpoint, barrier Barrier) {
	m.metaBarriers = append(m.metaBarriers, barrierEntry{
		point:   point,
		barrier: barrier,
	})
	m.TryAdvanceMetaCheckpoint()
}

func (m *Manager) AddImmediateMetaBarrier(point utility.WALConsumeCheckpoint) {
	m.AddMetaBarrier(point, nil)
}

func (m *Manager) TryAdvanceMetaCheckpoint() {
	for len(m.metaBarriers) > 0 {
		entry := m.metaBarriers[0]
		if entry.barrier != nil && entry.barrier.TimeTick() < entry.point.TimeTick {
			return
		}
		m.advanceMetaCheckpoint(entry.point)
		m.metaBarriers = m.metaBarriers[1:]
	}
}

func (m *Manager) AddDataBarrier(point utility.WALConsumeCheckpoint, barrier Barrier) {
	m.dataBarriers = append(m.dataBarriers, barrierEntry{
		point:   point,
		barrier: barrier,
	})
	m.TryAdvanceDataCheckpoint()
}

func (m *Manager) AddImmediateDataBarrier(point utility.WALConsumeCheckpoint) {
	m.AddDataBarrier(point, nil)
}

func (m *Manager) TryAdvanceDataCheckpoint() {
	for len(m.dataBarriers) > 0 {
		entry := m.dataBarriers[0]
		if !checkpointReached(m.checkpoint.MessageID, m.checkpoint.TimeTick, entry.point) {
			return
		}
		if entry.barrier != nil && entry.barrier.TimeTick() < entry.point.TimeTick {
			return
		}
		m.advanceDataCheckpoint(entry.point)
		m.dataBarriers = m.dataBarriers[1:]
	}
}

func (m *Manager) Snapshot() *utility.WALCheckpoint {
	return m.checkpoint.Clone()
}

func (m *Manager) HasDirty() bool {
	return m.checkpointDirty
}

func (m *Manager) MarkDirty() {
	m.checkpointDirty = true
}

func (m *Manager) ConsumeDirty() bool {
	dirty := m.checkpointDirty
	m.checkpointDirty = false
	return dirty
}

func (m *Manager) AdvanceMetaCheckpointInMemory(point utility.WALConsumeCheckpoint) {
	if !ShouldAdvance(m.checkpoint.MessageID, m.checkpoint.TimeTick, point) {
		return
	}
	m.checkpoint.MessageID = point.MessageID
	m.checkpoint.TimeTick = point.TimeTick
}

func (m *Manager) advanceMetaCheckpoint(point utility.WALConsumeCheckpoint) {
	if !ShouldAdvance(m.checkpoint.MessageID, m.checkpoint.TimeTick, point) {
		return
	}
	m.checkpoint.MessageID = point.MessageID
	m.checkpoint.TimeTick = point.TimeTick
	m.checkpointDirty = true
}

func (m *Manager) advanceDataCheckpoint(point utility.WALConsumeCheckpoint) {
	if m.checkpoint.DataCheckpoint == nil {
		m.checkpoint.DataCheckpoint = point.Clone()
		m.checkpointDirty = true
		return
	}
	if !ShouldAdvance(m.checkpoint.DataCheckpoint.MessageID, m.checkpoint.DataCheckpoint.TimeTick, point) {
		return
	}
	m.checkpoint.DataCheckpoint.MessageID = point.MessageID
	m.checkpoint.DataCheckpoint.TimeTick = point.TimeTick
	m.checkpointDirty = true
}

func ShouldAdvance(currentMessageID message.MessageID, currentTimeTick uint64, point utility.WALConsumeCheckpoint) bool {
	if currentMessageID == nil {
		return true
	}
	if point.MessageID != nil {
		return currentMessageID.LT(point.MessageID)
	}
	return point.TimeTick > currentTimeTick
}

func checkpointReached(currentMessageID message.MessageID, currentTimeTick uint64, point utility.WALConsumeCheckpoint) bool {
	if currentMessageID == nil {
		return false
	}
	if point.MessageID != nil {
		return point.MessageID.LTE(currentMessageID)
	}
	return currentTimeTick >= point.TimeTick
}
