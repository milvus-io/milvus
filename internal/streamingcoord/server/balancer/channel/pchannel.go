package channel

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// newPChannelMeta creates a new PChannelMeta.
func newPChannelMeta(name string) *PChannelMeta {
	return &PChannelMeta{
		inner: &streamingpb.PChannelMeta{
			Channel: &streamingpb.PChannelInfo{
				Name:       name,
				Term:       1,
				AccessMode: streamingpb.PChannelAccessMode(types.AccessModeRW),
			},
			Node:      nil,
			State:     streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
			Histories: make([]*streamingpb.PChannelAssignmentLog, 0),
		},
	}
}

// newPChannelMetaFromProto creates a new PChannelMeta from proto.
func newPChannelMetaFromProto(channel *streamingpb.PChannelMeta) *PChannelMeta {
	return &PChannelMeta{
		inner: channel,
	}
}

// PChannelMeta is the read only version of PChannelInfo, to be used in balancer,
// If you need to update PChannelMeta, please use CopyForWrite to get mutablePChannel.
type PChannelMeta struct {
	inner *streamingpb.PChannelMeta
}

// IsRWChannel check if the pchannel is a rw channel.
func (c *PChannelMeta) IsRWChannel() bool {
	return c.inner.Channel.GetAccessMode() == streamingpb.PChannelAccessMode_PCHANNEL_ACCESS_READWRITE
}

// Name returns the name of the channel.
func (c *PChannelMeta) Name() string {
	return c.inner.GetChannel().GetName()
}

// ChannelID returns the channel id.
func (c *PChannelMeta) ChannelID() types.ChannelID {
	return types.ChannelID{Name: c.inner.Channel.Name}
}

// ChannelInfo returns the channel info.
func (c *PChannelMeta) ChannelInfo() types.PChannelInfo {
	return types.NewPChannelInfoFromProto(c.inner.Channel)
}

// Term returns the current term of the channel.
func (c *PChannelMeta) CurrentTerm() int64 {
	return c.inner.GetChannel().GetTerm()
}

// CurrentServerID returns the server id of the channel.
// If the channel is not assigned to any server, return -1.
func (c *PChannelMeta) CurrentServerID() int64 {
	return c.inner.GetNode().GetServerId()
}

// CurrentAssignment returns the current assignment of the channel.
func (c *PChannelMeta) CurrentAssignment() types.PChannelInfoAssigned {
	return types.PChannelInfoAssigned{
		Channel: types.NewPChannelInfoFromProto(c.inner.Channel),
		Node:    types.NewStreamingNodeInfoFromProto(c.inner.Node),
	}
}

// AssignHistories returns the history of the channel assignment.
func (c *PChannelMeta) AssignHistories() []types.PChannelInfoAssigned {
	history := make([]types.PChannelInfoAssigned, 0, len(c.inner.Histories))
	for _, h := range c.inner.Histories {
		history = append(history, types.PChannelInfoAssigned{
			Channel: types.PChannelInfo{
				Name:       c.inner.GetChannel().GetName(),
				Term:       h.Term,
				AccessMode: types.AccessMode(h.AccessMode),
			},
			Node: types.NewStreamingNodeInfoFromProto(h.Node),
		})
	}
	return history
}

// IsAssigned returns if the channel is assigned to a server.
func (c *PChannelMeta) IsAssigned() bool {
	return c.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED
}

// State returns the state of the channel.
func (c *PChannelMeta) State() streamingpb.PChannelMetaState {
	return c.inner.State
}

// CopyForWrite returns mutablePChannel to modify pchannel
// but didn't affect other replicas.
func (c *PChannelMeta) CopyForWrite() *mutablePChannel {
	return &mutablePChannel{
		PChannelMeta: &PChannelMeta{
			inner: proto.Clone(c.inner).(*streamingpb.PChannelMeta),
		},
	}
}

// mutablePChannel is a mutable version of PChannel.
// use to update the channel info.
type mutablePChannel struct {
	*PChannelMeta
}

// TryAssignToServerID assigns the channel to a server.
func (m *mutablePChannel) TryAssignToServerID(streamingNode types.StreamingNodeInfo) bool {
	if m.CurrentServerID() == streamingNode.ServerID && m.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED {
		// if the channel is already assigned to the server, return false.
		return false
	}
	if m.inner.State != streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED {
		// if the channel is already initialized, add the history.
		m.inner.Histories = append(m.inner.Histories, &streamingpb.PChannelAssignmentLog{
			Term:       m.inner.Channel.Term,
			Node:       m.inner.Node,
			AccessMode: m.inner.Channel.AccessMode,
		})
	}

	// otherwise update the channel into assgining state.
	m.inner.Channel.Term++
	m.inner.Node = types.NewProtoFromStreamingNodeInfo(streamingNode)
	m.inner.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING
	return true
}

// AssignToServerDone assigns the channel to the server done.
func (m *mutablePChannel) AssignToServerDone() {
	if m.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING {
		m.inner.Histories = make([]*streamingpb.PChannelAssignmentLog, 0)
		m.inner.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED
	}
}

// MarkAsUnavailable marks the channel as unavailable.
func (m *mutablePChannel) MarkAsUnavailable(term int64) {
	if m.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED && m.CurrentTerm() == term {
		m.inner.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNAVAILABLE
	}
}

// IntoRawMeta returns the raw meta, no longger available after call.
func (m *mutablePChannel) IntoRawMeta() *streamingpb.PChannelMeta {
	c := m.PChannelMeta
	m.PChannelMeta = nil
	return c.inner
}
