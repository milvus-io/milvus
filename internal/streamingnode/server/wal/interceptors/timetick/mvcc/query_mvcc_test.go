package mvcc

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestQueryNewQueryMVCCManager(t *testing.T) {
	cm := NewQueryMVCCManager(100)
	v := cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{}, v)

	cm.ApplyRecoveryBarrier("vc1", 100)
	cm.ApplyRecoveryBarrier("vc2", 100)
	v = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createQueryTestMessage(t, 101, "vc1", message.MessageTypeInsert, false, true))
	v = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 101, TransformingTimetick: 100, Confirmed: false}, v)
	v = cm.GetQueryMVCCOfVChannel("vc2")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createQueryTestMessage(t, 102, "", message.MessageTypeTimeTick, false, true))
	v = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 101, TransformingTimetick: 100, Confirmed: true}, v)
	v = cm.GetQueryMVCCOfVChannel("vc2")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createQueryTestMessage(t, 103, "vc1", message.MessageTypeInsert, true, true))
	v = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 101, TransformingTimetick: 100, Confirmed: true}, v)
	v = cm.GetQueryMVCCOfVChannel("vc2")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createQueryTestMessage(t, 104, "vc1", message.MessageTypeCommitTxn, true, true))
	v = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 104, TransformingTimetick: 104, Confirmed: false}, v)
	v = cm.GetQueryMVCCOfVChannel("vc2")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createQueryTestMessage(t, 104, "", message.MessageTypeTimeTick, false, true))
	v = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 104, TransformingTimetick: 104, Confirmed: true}, v)
	v = cm.GetQueryMVCCOfVChannel("vc2")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createQueryTestMessage(t, 101, "", message.MessageTypeTimeTick, false, true))
	v = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 104, TransformingTimetick: 104, Confirmed: true}, v)
	v = cm.GetQueryMVCCOfVChannel("vc2")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)

	cm.UpdateMVCC(createQueryTestMessage(t, 1000, "", message.MessageTypeTimeTick, false, false))
	v = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 104, TransformingTimetick: 104, Confirmed: true}, v)
	v = cm.GetQueryMVCCOfVChannel("vc2")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 100, TransformingTimetick: 100, Confirmed: true}, v)
}

func TestQueryQueryPlanMVCCTracksGrowingAndTransformingSeparately(t *testing.T) {
	cm := NewQueryMVCCManager(100)
	cm.ApplyRecoveryBarrier("vc1", 120)

	mvcc := cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 120, Confirmed: true}, mvcc)

	cm.UpdateMVCC(createQueryTestMessage(t, 130, "vc1", message.MessageTypeInsert, false, true))
	mvcc = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 130, TransformingTimetick: 120, Confirmed: false}, mvcc)

	cm.UpdateMVCC(createQueryTestMessage(t, 140, "vc1", message.MessageTypeDelete, false, true))
	mvcc = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 140, TransformingTimetick: 140, Confirmed: false}, mvcc)

	cm.UpdateMVCC(createQueryTestMessage(t, 150, "", message.MessageTypeTimeTick, false, true))
	mvcc = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 140, TransformingTimetick: 140, Confirmed: true}, mvcc)

	missing := cm.GetQueryMVCCOfVChannel("vc2")
	assert.Equal(t, QueryVChannelMVCC{}, missing)
}

func TestQueryCreateCollectionInitializesQueryPlanMVCC(t *testing.T) {
	cm := NewQueryMVCCManager(100)

	cm.UpdateMVCC(createQueryTestMessage(t, 120, "vc1", message.MessageTypeCreateCollection, false, true))

	mvcc := cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 120, Confirmed: false}, mvcc)

	cm.UpdateMVCC(createQueryTestMessage(t, 120, "", message.MessageTypeTimeTick, false, true))

	mvcc = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 120, Confirmed: true}, mvcc)
}

func TestQueryRecoveryBarrierConfirmsQueryPlanMVCC(t *testing.T) {
	cm := NewQueryMVCCManager(100)
	cm.ApplyRecoveryBarrier("vc1", 120)
	cm.UpdateMVCC(createQueryTestMessage(t, 130, "vc1", message.MessageTypeInsert, false, true))

	mvcc := cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 130, TransformingTimetick: 120, Confirmed: false}, mvcc)

	cm.UpdateMVCC(createQueryTestMessage(t, 130, "", message.MessageTypeRecoveryBarrier, false, true))

	mvcc = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 130, TransformingTimetick: 120, Confirmed: true}, mvcc)
}

func TestQueryMVCCManagerTracksUnconfirmedVChannels(t *testing.T) {
	cm := NewQueryMVCCManager(100)
	cm.ApplyRecoveryBarrier("vc1", 120)
	cm.ApplyRecoveryBarrier("vc2", 120)
	assert.Empty(t, cm.unconfirmedVChannels)

	cm.UpdateMVCC(createQueryTestMessage(t, 130, "vc1", message.MessageTypeInsert, false, true))
	assert.Equal(t, map[string]struct{}{"vc1": {}}, cm.unconfirmedVChannels)

	cm.UpdateMVCC(createQueryTestMessage(t, 129, "", message.MessageTypeTimeTick, false, true))
	assert.Equal(t, map[string]struct{}{"vc1": {}}, cm.unconfirmedVChannels)

	cm.UpdateMVCC(createQueryTestMessage(t, 130, "", message.MessageTypeTimeTick, false, true))
	assert.Empty(t, cm.unconfirmedVChannels)

	cm.UpdateMVCC(createQueryTestMessage(t, 140, "vc1", message.MessageTypeInsert, false, true))
	cm.ApplyRecoveryBarrier("vc1", 140)
	assert.Empty(t, cm.unconfirmedVChannels)

	cm.UpdateMVCC(createQueryTestMessage(t, 150, "", message.MessageTypeFlushAll, false, true))
	assert.Equal(t, map[string]struct{}{"vc1": {}, "vc2": {}}, cm.unconfirmedVChannels)

	cm.UpdateMVCC(createQueryTestMessage(t, 150, "", message.MessageTypeTimeTick, false, true))
	assert.Empty(t, cm.unconfirmedVChannels)
}

func TestQueryTransformBarrierMessagesAdvanceTransformingMVCC(t *testing.T) {
	cm := NewQueryMVCCManager(100)
	cm.ApplyRecoveryBarrier("vc1", 120)

	cm.UpdateMVCC(createQueryTestMessage(t, 130, "vc1", message.MessageTypeManualFlush, false, true))
	mvcc := cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 130, Confirmed: false}, mvcc)

	cm.UpdateMVCC(createQueryTestMessage(t, 130, "", message.MessageTypeTimeTick, false, true))
	mvcc = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 130, Confirmed: true}, mvcc)
}

func TestQueryCommitImportAdvancesQueryPlanMVCC(t *testing.T) {
	cm := NewQueryMVCCManager(100)
	cm.ApplyRecoveryBarrier("vc1", 120)

	// CommitImport behaves like a flush barrier: it must advance the
	// transforming frontier (QueryNode filters sealed rows by it) and leave
	// the growing frontier untouched (imported rows never enter growing
	// segments; moving it would stall WaitMVCCVisible on insert-less
	// vchannels).
	cm.UpdateMVCC(createQueryTestMessage(t, 130, "vc1", message.MessageTypeCommitImport, false, true))
	mvcc := cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 130, Confirmed: false}, mvcc)

	cm.UpdateMVCC(createQueryTestMessage(t, 130, "", message.MessageTypeTimeTick, false, true))
	mvcc = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 130, Confirmed: true}, mvcc)

	// A later CommitImport with a smaller timetick must be a no-op.
	cm.UpdateMVCC(createQueryTestMessage(t, 129, "vc1", message.MessageTypeCommitImport, false, true))
	mvcc = cm.GetQueryMVCCOfVChannel("vc1")
	assert.Equal(t, QueryVChannelMVCC{GrowingTimetick: 120, TransformingTimetick: 130, Confirmed: true}, mvcc)
}

func createQueryTestMessage(t *testing.T, tt uint64, vc string, typ message.MessageType, txn bool, persisted bool) message.MutableMessage {
	t.Helper()
	properties := map[string]string{"_t": strconv.FormatInt(int64(typ), 10), "_vc": vc}
	if !persisted {
		properties["_np"] = ""
	}
	msg := message.NewMutableMessageBeforeAppend(nil, properties).WithTimeTick(tt)
	if txn {
		msg = msg.WithTxnContext(message.TxnContext{})
	}
	return msg
}
