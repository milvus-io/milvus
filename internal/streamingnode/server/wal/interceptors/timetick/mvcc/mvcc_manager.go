package mvcc

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

// NewMVCCManager creates a new mvcc timestamp manager.
func NewMVCCManager(lastConfirmedTimeTick uint64) *MVCCManager {
	return &MVCCManager{
		pchannelMVCCTimestamp:  lastConfirmedTimeTick,
		vchannelMVCCTimestamps: make(map[string]uint64),
	}
}

// MVCCManager is the manager that manages all the mvcc state of one wal.
// It keeps the last confirmed timestamp as mvcc of one pchannel and maximum timetick persisted into the wal of each vchannel.
type MVCCManager struct {
	mu                     sync.Mutex
	pchannelMVCCTimestamp  uint64            // the last confirmed timetick of the pchannel.
	vchannelMVCCTimestamps map[string]uint64 // map the vchannel to the maximum timetick that is persisted into the wal.
}

// GetMVCCOfVChannel gets the mvcc of the vchannel.
func (cm *MVCCManager) GetMVCCOfVChannel(vchannel string) VChannelMVCC {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if tt, ok := cm.vchannelMVCCTimestamps[vchannel]; ok {
		return VChannelMVCC{
			Timetick:  tt,
			Confirmed: false,
		}
	}
	// if the mvcc of vchannel is not found which means that
	// the mvcc of vchannel is equal to the pchannel level lastConfirmedTimeTick.
	// and that mvcc timestamp is already confirmed.
	return VChannelMVCC{
		Timetick:  cm.pchannelMVCCTimestamp,
		Confirmed: true,
	}
}

// UpdateMVCC updates the mvcc state by incoming message.
func (cm *MVCCManager) UpdateMVCC(msg message.MutableMessage) {
	tt := msg.TimeTick()
	msgType := msg.MessageType()
	vchannel := msg.VChannel()
	isTxn := msg.TxnContext() != nil

	cm.mu.Lock()
	defer cm.mu.Unlock()
	if tt <= cm.pchannelMVCCTimestamp {
		return
	}

	if msgType == message.MessageTypeTimeTick {
		cm.sync(tt)
		return
	}

	// If the message is belong to a transaction.
	// the mvcc timestamp cannot be push forward if the message is not committed.
	// because of an unconfirmed transaction may be rollback and cannot be seen at read side.
	if isTxn && msgType != message.MessageTypeCommitTxn {
		return
	}
	if exists, ok := cm.vchannelMVCCTimestamps[vchannel]; !ok || exists < tt {
		cm.vchannelMVCCTimestamps[vchannel] = tt
	}
}

// sync syncs the mvcc state by the incoming timetick message, push forward the wal mvcc state
// and clear the vchannel mvcc state.
func (cm *MVCCManager) sync(tt uint64) {
	// clear all existing vchannel mvcc that are are confirmed.
	// which means the mvcc of vchannel is equal to the pchannel level mvcc.
	for vchannel, existTimeTick := range cm.vchannelMVCCTimestamps {
		if existTimeTick <= tt {
			delete(cm.vchannelMVCCTimestamps, vchannel)
		}
	}
	cm.pchannelMVCCTimestamp = tt
}

// VChannelMVCC is a mvcc of one vchannel
// which is used to identify the maximum timetick that is persisted into the wal of one vchannel.
// The state of mvcc that is confirmed if the timetick is synced by timeticksync message,
// otherwise, the mvcc is not confirmed.
type VChannelMVCC struct {
	Timetick  uint64 // the timetick of the mvcc.
	Confirmed bool   // the mvcc is confirmed by the timeticksync operation.
}
