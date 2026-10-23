package mvcc

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func NewQueryMVCCManager(recoveryTimeTick uint64) *QueryMVCCManager {
	return &QueryMVCCManager{
		through:   recoveryTimeTick,
		frontiers: make(map[string]QueryVChannelMVCC),
		pending:   make(map[string]uint64),
		changed:   make(chan struct{}),
	}
}

// QueryMVCCManager owns query snapshots and their freshness bounds. Only ordered
// WAB publication changes executable frontiers and channel membership. Append
// callbacks merely raise a minimum prefix for Strong reads; they may run after
// publication because Acker.Ack precedes the callback.
type QueryMVCCManager struct {
	mu         sync.Mutex
	frontiers  map[string]QueryVChannelMVCC
	pending    map[string]uint64
	pendingAll uint64
	through    uint64
	changed    chan struct{}
}

// ApplyRecoveryBarrier runs for live channels before append admission opens.
// Recovery has consumed the complete prefix through this owner's opening fence.
func (cm *QueryMVCCManager) ApplyRecoveryBarrier(channel string, tick uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.frontiers[channel] = QueryVChannelMVCC{GrowingTimetick: tick, TransformingTimetick: tick}
	cm.through = max(cm.through, tick)
}

// ObserveAppend records completed, query-relevant writes without changing the
// snapshot. A callback already covered by publication cannot recreate state,
// including a delayed Create callback after an ordered Create/Drop prefix.
func (cm *QueryMVCCManager) ObserveAppend(msg message.MutableMessage) {
	if !msg.IsPersisted() || (msg.TxnContext() != nil && msg.MessageType() != message.MessageTypeCommitTxn) {
		return
	}
	kind, channel, tick := msg.MessageType(), msg.VChannel(), msg.TimeTick()
	if kind != message.MessageTypeInsert && !messageutil.AdvancesQueryTransformMVCC(msg) {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if tick <= cm.through {
		return
	}
	if messageutil.IsPChannelTransformBarrier(kind) && (channel == "" || msg.IsPChannelLevel()) {
		cm.pendingAll = max(cm.pendingAll, tick)
	} else if channel != "" && !msg.IsPChannelLevel() && !funcutil.IsControlChannel(channel) {
		cm.pending[channel] = max(cm.pending[channel], tick)
	}
}

// Minimum fixes the required prefix once per query. Eventually may reuse the
// current complete snapshot; a channel whose Create is still pending must first
// become visible. Strong also covers completed channel and global publications.
func (cm *QueryMVCCManager) Minimum(channel string, strong bool) uint64 {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	minimum := cm.through
	if _, live := cm.frontiers[channel]; strong || !live {
		minimum = max(minimum, cm.pending[channel], cm.pendingAll)
	}
	return minimum
}

// Snapshot returns one pair from one complete prefix, never append-time maxima.
// A ready zero pair denotes an absent channel. The caller's minimum is fixed
// while waiting, so concurrent writes cannot keep moving the wait target.
func (cm *QueryMVCCManager) Snapshot(channel string, minimum uint64) (QueryVChannelMVCC, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.frontiers[channel], cm.through >= minimum
}

func (cm *QueryMVCCManager) WaitSnapshot(ctx context.Context, channel string, minimum uint64) (QueryVChannelMVCC, error) {
	for {
		cm.mu.Lock()
		snapshot, live := cm.frontiers[channel]
		ready, changed := cm.through >= minimum, cm.changed
		cm.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return QueryVChannelMVCC{}, err
		}
		if ready {
			if !live {
				return QueryVChannelMVCC{}, merr.WrapErrChannelNotFound(channel)
			}
			return snapshot, nil
		}
		select {
		case <-ctx.Done():
			return QueryVChannelMVCC{}, ctx.Err()
		case <-changed:
		}
	}
}

// Publish consumes successful ACK messages in TimeTick order after WAB.Append.
// The serialized sync operator supplies exactly one newly completed prefix.
func (cm *QueryMVCCManager) Publish(messages []message.ImmutableMessage, confirmation message.ImmutableMessage) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for _, msg := range messages {
		cm.applyLocked(msg)
		channel := msg.VChannel()
		if cm.pending[channel] <= confirmation.TimeTick() {
			delete(cm.pending, channel)
		}
	}
	if confirmation.MessageType() == message.MessageTypeMVCCBarrier {
		for _, channel := range message.MustAsImmutableMVCCBarrierMessageV2(confirmation).Header().GetVchannels() {
			if current, live := cm.frontiers[channel]; live {
				current.GrowingTimetick = max(current.GrowingTimetick, confirmation.TimeTick())
				current.TransformingTimetick = max(current.TransformingTimetick, confirmation.TimeTick())
				cm.frontiers[channel] = current
			}
		}
	}
	cm.through = confirmation.TimeTick()
	if cm.pendingAll <= cm.through {
		cm.pendingAll = 0
	}
	close(cm.changed)
	cm.changed = make(chan struct{})
}

func (cm *QueryMVCCManager) applyLocked(msg message.ImmutableMessage) {
	if !msg.IsPersisted() || (msg.TxnContext() != nil && msg.MessageType() != message.MessageTypeCommitTxn) {
		return
	}
	channel, kind, tick := msg.VChannel(), msg.MessageType(), msg.TimeTick()
	if channel == "" || msg.IsPChannelLevel() {
		if messageutil.IsPChannelTransformBarrier(kind) {
			for channel, current := range cm.frontiers {
				current.TransformingTimetick = max(current.TransformingTimetick, tick)
				cm.frontiers[channel] = current
			}
		}
		return
	}
	if funcutil.IsControlChannel(channel) {
		return
	}
	if kind == message.MessageTypeDropCollection {
		delete(cm.frontiers, channel)
		return
	}
	current, live := cm.frontiers[channel]
	if !live && kind != message.MessageTypeCreateCollection {
		return
	}
	switch kind {
	case message.MessageTypeCreateCollection:
		current.GrowingTimetick = tick
		current.TransformingTimetick = tick
	case message.MessageTypeInsert:
		current.GrowingTimetick = max(current.GrowingTimetick, tick)
	case message.MessageTypeDelete, message.MessageTypeCommitTxn:
		current.TransformingTimetick = max(current.TransformingTimetick, tick)
		current.GrowingTimetick = max(current.GrowingTimetick, tick)
	default:
		if !messageutil.AdvancesQueryTransformMVCC(msg) {
			return
		}
		current.TransformingTimetick = max(current.TransformingTimetick, tick)
	}
	cm.frontiers[channel] = current
}

// QueryVChannelMVCC is the executable projection of one complete WAL prefix.
// Pure growing writes leave the Transform frontier unchanged.
type QueryVChannelMVCC struct {
	GrowingTimetick      uint64
	TransformingTimetick uint64
}
