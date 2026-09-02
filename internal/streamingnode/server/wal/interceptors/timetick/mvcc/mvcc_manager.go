package mvcc

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

// NewMVCCManager creates a new per-vchannel query MVCC manager.
func NewMVCCManager(_ uint64) *MVCCManager {
	return &MVCCManager{
		vchannelMVCCs:        make(map[string]VChannelMVCC),
		unconfirmedVChannels: make(map[string]struct{}),
	}
}

// MVCCManager is the manager that manages all the mvcc state of one wal.
// It tracks the persisted query-plan frontiers of each recovered vchannel.
type MVCCManager struct {
	mu                   sync.Mutex
	vchannelMVCCs        map[string]VChannelMVCC
	unconfirmedVChannels map[string]struct{}
}

// GetMVCCOfVChannel gets the query MVCC frontiers of the vchannel.
func (cm *MVCCManager) GetMVCCOfVChannel(vchannel string) VChannelMVCC {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if mvcc, ok := cm.vchannelMVCCs[vchannel]; ok {
		return mvcc
	}
	return VChannelMVCC{}
}

// ApplyRecoveryBarrier initializes or advances the recovered query MVCC baseline
// of one live vchannel.
func (cm *MVCCManager) ApplyRecoveryBarrier(vchannel string, timetick uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	mvcc := cm.vchannelMVCCs[vchannel]
	mvcc.GrowingTimetick = max(mvcc.GrowingTimetick, timetick)
	mvcc.TransformingTimetick = max(mvcc.TransformingTimetick, timetick)
	mvcc.Confirmed = true
	cm.vchannelMVCCs[vchannel] = mvcc
	delete(cm.unconfirmedVChannels, vchannel)
}

// UpdateMVCC updates the mvcc state by incoming message.
func (cm *MVCCManager) UpdateMVCC(msg message.MutableMessage) {
	if !msg.IsPersisted() {
		// Compatibility guard for externally constructed non-persisted messages.
		// The WAL no longer produces non-persisted TimeTick messages.
		return
	}

	tt := msg.TimeTick()
	msgType := msg.MessageType()
	vchannel := msg.VChannel()
	isTxn := msg.TxnContext() != nil

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if messageutil.IsTimeTickConfirmBarrier(msgType) {
		cm.sync(tt)
		return
	}

	// If the message belongs to a transaction, the query MVCC frontiers cannot
	// move forward until the transaction is committed.
	// because of an unconfirmed transaction may be rollback and cannot be seen at read side.
	if isTxn && msgType != message.MessageTypeCommitTxn {
		return
	}
	if vchannel == "" {
		if isPChannelTransformBarrier(msgType) {
			cm.advanceTransformingAllLocked(tt)
		}
		return
	}
	mvcc := cm.vchannelMVCCs[vchannel]
	switch msgType {
	case message.MessageTypeCreateCollection:
		if tt <= max(mvcc.GrowingTimetick, mvcc.TransformingTimetick) {
			return
		}
		mvcc.GrowingTimetick = tt
		mvcc.TransformingTimetick = tt
	case message.MessageTypeInsert:
		if tt <= mvcc.GrowingTimetick {
			return
		}
		mvcc.GrowingTimetick = tt
	case message.MessageTypeDelete:
		if tt <= mvcc.TransformingTimetick {
			return
		}
		mvcc.TransformingTimetick = tt
		mvcc.GrowingTimetick = max(mvcc.GrowingTimetick, mvcc.TransformingTimetick)
	case message.MessageTypeCommitTxn:
		if tt <= mvcc.TransformingTimetick {
			return
		}
		mvcc.TransformingTimetick = tt
		mvcc.GrowingTimetick = max(mvcc.GrowingTimetick, mvcc.TransformingTimetick)
	case message.MessageTypeCommitImport:
		// Import commit behaves like a flush barrier: it publishes sealed
		// segments to the query view at its commit fence, so it advances the
		// transforming frontier only. Growing MVCC must NOT move — imported
		// rows live in sealed segments served by QueryNode (which filters by
		// the transforming frontier), and advancing the growing frontier would
		// stall streamingnode WaitMVCCVisible on vchannels with no insert
		// traffic.
		if tt <= mvcc.TransformingTimetick {
			return
		}
		mvcc.TransformingTimetick = tt
	case message.MessageTypeFlush,
		message.MessageTypeManualFlush,
		message.MessageTypeDropPartition,
		message.MessageTypeDropCollection,
		message.MessageTypeTruncateCollection,
		message.MessageTypeFlushAll,
		message.MessageTypeAlterWAL:
		if tt <= mvcc.TransformingTimetick {
			return
		}
		mvcc.TransformingTimetick = tt
	case message.MessageTypeAlterCollection:
		alter := message.MustAsMutableAlterCollectionMessageV2(msg)
		if !messageutil.IsSchemaChange(alter.Header()) || tt <= mvcc.TransformingTimetick {
			return
		}
		mvcc.TransformingTimetick = tt
	default:
		return
	}
	mvcc.Confirmed = false
	cm.vchannelMVCCs[vchannel] = mvcc
	cm.unconfirmedVChannels[vchannel] = struct{}{}
}

// sync confirms the unconfirmed vchannel MVCC states covered by the incoming timetick message.
func (cm *MVCCManager) sync(tt uint64) {
	for vchannel := range cm.unconfirmedVChannels {
		mvcc := cm.vchannelMVCCs[vchannel]
		if max(mvcc.GrowingTimetick, mvcc.TransformingTimetick) <= tt {
			mvcc.Confirmed = true
			cm.vchannelMVCCs[vchannel] = mvcc
			delete(cm.unconfirmedVChannels, vchannel)
		}
	}
}

func (cm *MVCCManager) advanceTransformingAllLocked(tt uint64) {
	for vchannel, mvcc := range cm.vchannelMVCCs {
		if tt <= mvcc.TransformingTimetick {
			continue
		}
		mvcc.TransformingTimetick = tt
		mvcc.Confirmed = false
		cm.vchannelMVCCs[vchannel] = mvcc
		cm.unconfirmedVChannels[vchannel] = struct{}{}
	}
}

func isPChannelTransformBarrier(msgType message.MessageType) bool {
	return msgType == message.MessageTypeFlushAll ||
		msgType == message.MessageTypeAlterWAL
}

// VChannelMVCC is a mvcc of one vchannel
// which is used to identify the maximum query-plan timeticks persisted into the wal of one vchannel.
// The state of mvcc that is confirmed if the timetick is synced by timeticksync message,
// otherwise, the mvcc is not confirmed.
type VChannelMVCC struct {
	GrowingTimetick      uint64
	TransformingTimetick uint64
	Confirmed            bool
}

func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}
