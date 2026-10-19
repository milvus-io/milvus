package mvcc

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestNewMVCCManager(t *testing.T) {
	cm := NewMVCCManager(100)
	v := cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 100, Confirmed: true})

	cm.UpdateMVCC(createQueryTestMessage(t, 101, "vc1", message.MessageTypeInsert, false, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 101, Confirmed: false})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 100, Confirmed: true})

	cm.UpdateMVCC(createQueryTestMessage(t, 102, "", message.MessageTypeTimeTick, false, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})

	cm.UpdateMVCC(createQueryTestMessage(t, 103, "vc1", message.MessageTypeInsert, true, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})

	cm.UpdateMVCC(createQueryTestMessage(t, 104, "vc1", message.MessageTypeCommitTxn, true, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: false})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 102, Confirmed: true})

	cm.UpdateMVCC(createQueryTestMessage(t, 104, "", message.MessageTypeTimeTick, false, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})

	cm.UpdateMVCC(createQueryTestMessage(t, 101, "", message.MessageTypeTimeTick, false, true))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})

	cm.UpdateMVCC(createQueryTestMessage(t, 1000, "", message.MessageTypeTimeTick, false, false))
	v = cm.GetMVCCOfVChannel("vc1")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})
	v = cm.GetMVCCOfVChannel("vc2")
	assert.Equal(t, v, VChannelMVCC{Timetick: 104, Confirmed: true})
}

func TestCommitImportAdvancesMVCCWithoutInserts(t *testing.T) {
	manager := NewMVCCManager(100)
	commit := message.NewCommitImportMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.CommitImportMessageHeader{CollectionId: 1, JobId: 10}).
		WithBody(&message.CommitImportMessageBody{}).MustBuildMutable().WithTimeTick(200)
	manager.UpdateMVCC(commit)
	assert.Equal(t, VChannelMVCC{Timetick: 200}, manager.GetMVCCOfVChannel("v1"))
	tick := message.NewTimeTickMessageBuilderV1().WithAllVChannel().
		WithHeader(&message.TimeTickMessageHeader{}).WithBody(&message.TimeTickMsg{}).
		MustBuildMutable().WithTimeTick(200)
	manager.UpdateMVCC(tick)
	assert.Equal(t, VChannelMVCC{Timetick: 200, Confirmed: true}, manager.GetMVCCOfVChannel("v1"))
}
