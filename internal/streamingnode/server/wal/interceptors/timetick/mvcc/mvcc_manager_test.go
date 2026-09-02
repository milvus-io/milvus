package mvcc

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestNewMVCCManager(t *testing.T) {
	cm := NewMVCCManager(100)
	v := cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{}, v)

	cm.ApplyRecoveryBarrier("vc1", 100)
	cm.ApplyRecoveryBarrier("vc2", 100)
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createTestMessage(t, 101, "vc1", message.MessageTypeInsert, false, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 101, TransformingTimetick: 100, Confirmed: false}, v)
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createTestMessage(t, 102, "", message.MessageTypeTimeTick, false, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 101, TransformingTimetick: 100, Confirmed: true}, v)
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createTestMessage(t, 103, "vc1", message.MessageTypeInsert, true, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 101, TransformingTimetick: 100, Confirmed: true}, v)
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createTestMessage(t, 104, "vc1", message.MessageTypeCommitTxn, true, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 104, TransformingTimetick: 104, Confirmed: false}, v)
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createTestMessage(t, 104, "", message.MessageTypeTimeTick, false, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 104, TransformingTimetick: 104, Confirmed: true}, v)
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createTestMessage(t, 101, "", message.MessageTypeTimeTick, false, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 104, TransformingTimetick: 104, Confirmed: true}, v)
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createTestMessage(t, 1000, "", message.MessageTypeTimeTick, false, false))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 104, TransformingTimetick: 104, Confirmed: true}, v)
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)
}

func TestQueryPlanMVCCTracksGrowingAndTransformingSeparately(t *testing.T) {
	cm := NewMVCCManager(100)
	cm.ApplyRecoveryBarrier("vc1", 120)

	mvcc := cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 120, Confirmed: true}, mvcc)

	cm.UpdateMVCC(createTestMessage(t, 130, "vc1", message.MessageTypeInsert, false, true))
	mvcc = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 130, TransformingTimetick: 120, Confirmed: false}, mvcc)

	cm.UpdateMVCC(createTestMessage(t, 140, "vc1", message.MessageTypeDelete, false, true))
	mvcc = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 140, TransformingTimetick: 140, Confirmed: false}, mvcc)

	cm.UpdateMVCC(createTestMessage(t, 150, "", message.MessageTypeTimeTick, false, true))
	mvcc = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 140, TransformingTimetick: 140, Confirmed: true}, mvcc)

	missing := cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, VChannelMVCC{}, missing)
}

func TestCreateCollectionInitializesQueryPlanMVCC(t *testing.T) {
	cm := NewMVCCManager(100)

	cm.UpdateMVCC(createTestMessage(t, 120, "vc1", message.MessageTypeCreateCollection, false, true))

	mvcc := cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 120, Confirmed: false}, mvcc)

	cm.UpdateMVCC(createTestMessage(t, 120, "", message.MessageTypeTimeTick, false, true))

	mvcc = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 120, Confirmed: true}, mvcc)
}

func TestRecoveryBarrierConfirmsQueryPlanMVCC(t *testing.T) {
	cm := NewMVCCManager(100)
	cm.ApplyRecoveryBarrier("vc1", 120)
	cm.UpdateMVCC(createTestMessage(t, 130, "vc1", message.MessageTypeInsert, false, true))

	mvcc := cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 130, TransformingTimetick: 120, Confirmed: false}, mvcc)

	cm.UpdateMVCC(createTestMessage(t, 130, "", message.MessageTypeRecoveryBarrier, false, true))

	mvcc = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 130, TransformingTimetick: 120, Confirmed: true}, mvcc)
}

func TestMVCCManagerTracksUnconfirmedVChannels(t *testing.T) {
	cm := NewMVCCManager(100)
	cm.ApplyRecoveryBarrier("vc1", 120)
	cm.ApplyRecoveryBarrier("vc2", 120)
	assert.Empty(t, cm.unconfirmedVChannels)

	cm.UpdateMVCC(createTestMessage(t, 130, "vc1", message.MessageTypeInsert, false, true))
	assert.Equal(t, map[string]struct{}{"vc1": {}}, cm.unconfirmedVChannels)

	cm.UpdateMVCC(createTestMessage(t, 129, "", message.MessageTypeTimeTick, false, true))
	assert.Equal(t, map[string]struct{}{"vc1": {}}, cm.unconfirmedVChannels)

	cm.UpdateMVCC(createTestMessage(t, 130, "", message.MessageTypeTimeTick, false, true))
	assert.Empty(t, cm.unconfirmedVChannels)

	cm.UpdateMVCC(createTestMessage(t, 140, "vc1", message.MessageTypeInsert, false, true))
	cm.ApplyRecoveryBarrier("vc1", 140)
	assert.Empty(t, cm.unconfirmedVChannels)

	cm.UpdateMVCC(createTestMessage(t, 150, "", message.MessageTypeFlushAll, false, true))
	assert.Equal(t, map[string]struct{}{"vc1": {}, "vc2": {}}, cm.unconfirmedVChannels)

	cm.UpdateMVCC(createTestMessage(t, 150, "", message.MessageTypeTimeTick, false, true))
	assert.Empty(t, cm.unconfirmedVChannels)
}

func TestTransformBarrierMessagesAdvanceTransformingMVCC(t *testing.T) {
	cm := NewMVCCManager(100)
	cm.ApplyRecoveryBarrier("vc1", 120)

	cm.UpdateMVCC(createTestMessage(t, 130, "vc1", message.MessageTypeManualFlush, false, true))
	mvcc := cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 130, Confirmed: false}, mvcc)

	cm.UpdateMVCC(createTestMessage(t, 130, "", message.MessageTypeTimeTick, false, true))
	mvcc = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 130, Confirmed: true}, mvcc)
}

func TestCommitImportAdvancesQueryPlanMVCC(t *testing.T) {
	cm := NewMVCCManager(100)
	cm.ApplyRecoveryBarrier("vc1", 120)

	// CommitImport behaves like a flush barrier: it must advance the
	// transforming frontier (QueryNode filters sealed rows by it) and leave
	// the growing frontier untouched (imported rows never enter growing
	// segments; moving it would stall WaitMVCCVisible on insert-less
	// vchannels).
	cm.UpdateMVCC(createTestMessage(t, 130, "vc1", message.MessageTypeCommitImport, false, true))
	mvcc := cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 130, Confirmed: false}, mvcc)

	cm.UpdateMVCC(createTestMessage(t, 130, "", message.MessageTypeTimeTick, false, true))
	mvcc = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 130, Confirmed: true}, mvcc)

	// A later CommitImport with a smaller timetick must be a no-op.
	cm.UpdateMVCC(createTestMessage(t, 129, "vc1", message.MessageTypeCommitImport, false, true))
	mvcc = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, VChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 130, Confirmed: true}, mvcc)
}

func createTestMessage(
	t *testing.T,
	tt uint64,
	vchannel string,
	msgType message.MessageType,
	txTxn bool,
	persist bool,
) message.MutableMessage {
	msg := mock_message.NewMockMutableMessage(t)
	msg.EXPECT().IsPersisted().Return(persist)
	msg.EXPECT().TimeTick().Return(tt).Maybe()
	msg.EXPECT().VChannel().Return(vchannel).Maybe()
	msg.EXPECT().MessageType().Return(msgType).Maybe()
	if txTxn {
		msg.EXPECT().TxnContext().Return(&message.TxnContext{}).Maybe()
		return msg
	}
	msg.EXPECT().TxnContext().Return(nil).Maybe()
	return msg
}
