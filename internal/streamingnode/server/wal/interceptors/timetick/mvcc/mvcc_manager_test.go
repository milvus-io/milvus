package mvcc

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestNewMVCCManager(t *testing.T) {
	cm := NewMVCCManager(100)
	v := cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 100, Confirmed: true})

	cm.UpdateMVCC(createTestMessage(t, 101, "vc1", message.MessageTypeInsert, false, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 101, Confirmed: false})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 100, Confirmed: true})

	cm.UpdateMVCC(createTestMessage(t, 102, "", message.MessageTypeTimeTick, false, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})

	cm.UpdateMVCC(createTestMessage(t, 103, "vc1", message.MessageTypeInsert, true, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})

	cm.UpdateMVCC(createTestMessage(t, 104, "vc1", message.MessageTypeCommitTxn, true, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: false})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})

	cm.UpdateMVCC(createTestMessage(t, 104, "", message.MessageTypeTimeTick, false, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})

	cm.UpdateMVCC(createTestMessage(t, 101, "", message.MessageTypeTimeTick, false, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})

	cm.UpdateMVCC(createTestMessage(t, 1000, "", message.MessageTypeTimeTick, false, false))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})
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
