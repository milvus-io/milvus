package mvcc

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

func TestNewMVCCManager(t *testing.T) {
	cm := NewMVCCManager(100)
	v := cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 100, Confirmed: true})

	cm.UpdateMVCC(createTestMessage(t, 101, "vc1", message.MessageTypeInsert, false))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 101, Confirmed: false})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 100, Confirmed: true})

	cm.UpdateMVCC(createTestMessage(t, 102, "", message.MessageTypeTimeTick, false))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})

	cm.UpdateMVCC(createTestMessage(t, 103, "vc1", message.MessageTypeInsert, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})

	cm.UpdateMVCC(createTestMessage(t, 104, "vc1", message.MessageTypeCommitTxn, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: false})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})

	cm.UpdateMVCC(createTestMessage(t, 104, "", message.MessageTypeTimeTick, false))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})

	cm.UpdateMVCC(createTestMessage(t, 101, "", message.MessageTypeTimeTick, false))
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
) message.MutableMessage {
	msg := mock_message.NewMockMutableMessage(t)
	msg.EXPECT().TimeTick().Return(tt)
	msg.EXPECT().VChannel().Return(vchannel)
	msg.EXPECT().MessageType().Return(msgType)
	if txTxn {
		msg.EXPECT().TxnContext().Return(&message.TxnContext{})
		return msg
	}
	msg.EXPECT().TxnContext().Return(nil)
	return msg
}
