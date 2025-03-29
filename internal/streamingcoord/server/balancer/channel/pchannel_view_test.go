package channel

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

func TestPChannelView(t *testing.T) {
	metas := []*PChannelMeta{
		newPChannelMetaFromProto(&streamingpb.PChannelMeta{
			Channel: &streamingpb.PChannelInfo{Name: "test", Term: 1},
			State:   streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
		}),
		newPChannelMetaFromProto(&streamingpb.PChannelMeta{
			Channel: &streamingpb.PChannelInfo{Name: "test2", Term: 1},
			State:   streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
		}),
		newPChannelMetaFromProto(&streamingpb.PChannelMeta{
			Channel: &streamingpb.PChannelInfo{Name: "test", Term: 1, AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY, ReplicaId: 1},
			State:   streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
		}),
	}
	view := newPChannelView(metas)
	assert.Equal(t, 3, view.Count())
	pchannel, ok := view.GetChannel(ChannelID{Name: "test", ReplicaID: 0})
	assert.True(t, ok)
	assert.NotNil(t, pchannel)
	assert.Equal(t, "test:0", pchannel.ChannelID().String())

	pchannel, ok = view.GetChannel(ChannelID{Name: "test", ReplicaID: 1})
	assert.True(t, ok)
	assert.NotNil(t, pchannel)
	assert.Equal(t, "test:1", pchannel.ChannelID().String())

	assign := view.GetAssignment()
	assert.Len(t, assign, 0)
	view.Update(newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{Name: "test", Term: 1},
		State:   streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		Node:    &streamingpb.StreamingNodeInfo{ServerId: 2},
	}))
	view.Update(newPChannelMetaFromProto(&streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{Name: "test", Term: 1, AccessMode: streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READONLY, ReplicaId: 1},
		State:   streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED,
		Node:    &streamingpb.StreamingNodeInfo{ServerId: 1},
	}))
	assign = view.GetAssignment()
	assert.Len(t, assign, 2)
}
