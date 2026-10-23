package mvcc

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// NewMVCCManager creates a new mvcc timestamp manager.
func NewMVCCManager(lastConfirmedTimeTick uint64) *MVCCManager {
	return &MVCCManager{
		query:                  NewQueryMVCCManager(lastConfirmedTimeTick),
		pchannelMVCCTimestamp:  lastConfirmedTimeTick,
		recoveryTimeTick:       lastConfirmedTimeTick,
		vchannelMVCCTimestamps: make(map[string]uint64),
	}
}

// MVCCManager is the manager that manages all the mvcc state of one wal.
// It tracks PChannel confirmation separately from each live VChannel's latest
// relevant persisted write. VChannel entries survive confirmation and are freed
// on DropCollection, so the map size is bounded by the number of live VChannels.
type MVCCManager struct {
	query                  *QueryMVCCManager
	mu                     sync.Mutex
	retiredTimeTick        uint64            // shared fence for retired/unknown identities, without per-shard tombstones.
	recoveryTimeTick       uint64            // fixed WAL opening timestamp; query targets never precede recovery.
	pchannelMVCCTimestamp  uint64            // the last confirmed timetick of the pchannel.
	vchannelMVCCTimestamps map[string]uint64 // map the vchannel to the maximum timetick that is persisted into the wal.
}

// GetMVCCOfVChannel gets the mvcc of the vchannel.
func (cm *MVCCManager) GetMVCCOfVChannel(vchannel string) VChannelMVCC {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	tt, live := cm.vchannelMVCCTimestamps[vchannel]
	if !live {
		// A query may already be in flight when Drop removes write admission.
		// Do not lower its target just because the per-shard entry was reclaimed.
		tt = cm.retiredTimeTick
	}
	tt = max(cm.recoveryTimeTick, tt)
	return VChannelMVCC{Timetick: tt, Confirmed: tt <= cm.pchannelMVCCTimestamp}
}

// GetConfirmedTimeTick returns physical WAL progress, independently of query targets.
func (cm *MVCCManager) GetConfirmedTimeTick() uint64 {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.pchannelMVCCTimestamp
}

// UpdateMVCC updates the mvcc state by incoming message.
func (cm *MVCCManager) UpdateMVCC(msg message.MutableMessage) {
	cm.query.ObserveAppend(msg)
	if !msg.IsPersisted() {
		// Volatile confirmations do not change durable MVCC. Scoped query
		// progress is handled separately by QueryMVCCManager.
		return
	}

	tt := msg.TimeTick()
	msgType := msg.MessageType()
	vchannel := msg.VChannel()
	isTxn := msg.TxnContext() != nil

	cm.mu.Lock()
	defer cm.mu.Unlock()
	// Confirmation can race ahead of this callback: Acker.Ack runs before
	// UpdateMVCC. Never discard a successful write merely because tt <= C.
	if messageutil.IsTimeTickConfirmBarrier(msgType) {
		cm.pchannelMVCCTimestamp = max(cm.pchannelMVCCTimestamp, tt)
		return
	}

	// Only the commit makes a transaction visible, including bodies recovered
	// from before this WAL's opening fence.
	if isTxn && msgType != message.MessageTypeCommitTxn {
		return
	}
	// PChannel/control operations do not change this VChannel's visible data.
	// Query-plan transform barriers are handled independently by cm.query above.
	if vchannel == "" || msg.IsPChannelLevel() || funcutil.IsControlChannel(vchannel) {
		return
	}
	switch msgType {
	case message.MessageTypeCreateSegment, message.MessageTypeFlush:
		// These storage operations do not change query-visible data and are not
		// delivered to the delegator. Tracking them would require another tick
		// to advance tSafe. The query-plan frontier is handled independently above.
		return
	case message.MessageTypeCreateCollection:
		cm.vchannelMVCCTimestamps[vchannel] = max(cm.vchannelMVCCTimestamps[vchannel], tt)
	case message.MessageTypeDropCollection:
		// The outer VChannel exclusive append lock covers this callback. Earlier
		// appends have finished publishing their targets, and the shard has stopped
		// accepting DML. Collection IDs in VChannel names are never reused.
		cm.retiredTimeTick = max(cm.retiredTimeTick, cm.vchannelMVCCTimestamps[vchannel], tt)
		delete(cm.vchannelMVCCTimestamps, vchannel)
	default:
		// Only recovery and CreateCollection register a live shard. In particular,
		// late internal/control messages after Drop must not resurrect its entry.
		if previous, live := cm.vchannelMVCCTimestamps[vchannel]; live {
			cm.vchannelMVCCTimestamps[vchannel] = max(previous, tt)
		}
	}
}

// VChannelMVCC is a mvcc of one vchannel
// which is used to identify the maximum timetick that is persisted into the wal of one vchannel.
// The state of mvcc that is confirmed if the timetick is synced by timeticksync message,
// otherwise, the mvcc is not confirmed.
type VChannelMVCC struct {
	Timetick  uint64 // the timetick of the mvcc.
	Confirmed bool   // the mvcc is confirmed by the timeticksync operation.
}

func (cm *MVCCManager) QueryMVCC() *QueryMVCCManager {
	return cm.query
}

// ApplyRecoveryBarrier registers live shards before WAL append admission opens.
// It does not confirm a concurrent newer write or move the physical watermark.
func (cm *MVCCManager) ApplyRecoveryBarrier(vchannel string, timetick uint64) {
	cm.mu.Lock()
	cm.vchannelMVCCTimestamps[vchannel] = max(cm.vchannelMVCCTimestamps[vchannel], timetick)
	cm.mu.Unlock()
	cm.query.ApplyRecoveryBarrier(vchannel, timetick)
}
