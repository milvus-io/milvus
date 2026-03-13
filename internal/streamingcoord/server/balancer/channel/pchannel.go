package channel

import (
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
)

// NewPChannelMeta creates a new PChannelMeta.
// By default, the channel is available in replication.
func NewPChannelMeta(name string, accessMode types.AccessMode) *PChannelMeta {
	return newPChannelMetaWithAvailability(name, accessMode, true)
}

// newPChannelMetaWithAvailability creates a new PChannelMeta with explicit availability in replication.
func newPChannelMetaWithAvailability(name string, accessMode types.AccessMode, availableInReplication bool) *PChannelMeta {
	return &PChannelMeta{
		inner: &streamingpb.PChannelMeta{
			Channel: &streamingpb.PChannelInfo{
				Name:       name,
				Term:       1,
				AccessMode: streamingpb.PChannelAccessMode(accessMode),
			},
			Node:      nil,
			State:     streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
			Histories: make([]*streamingpb.PChannelAssignmentLog, 0),
		},
		availableInReplication: availableInReplication,
	}
}

// newPChannelMetaFromProto creates a new PChannelMeta from proto.
// The availableInReplication flag is computed from the given replicateConfig.
func newPChannelMetaFromProto(channel *streamingpb.PChannelMeta, replicateConfig *replicateutil.ConfigHelper) *PChannelMeta {
	return &PChannelMeta{
		inner:                  channel,
		availableInReplication: isChannelAvailableInReplication(channel.GetChannel().GetName(), replicateConfig),
	}
}

// PChannelMeta is the read only version of PChannelInfo, to be used in balancer,
// If you need to update PChannelMeta, please use CopyForWrite to get mutablePChannel.
type PChannelMeta struct {
	inner                  *streamingpb.PChannelMeta
	availableInReplication bool
}

// AvailableInReplication returns whether the channel is available for VChannel allocation
// and DDL broadcasts. Dynamically-added PChannels are gated until they appear in ReplicateConfig.
func (c *PChannelMeta) AvailableInReplication() bool {
	return c.availableInReplication
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

// IsAssignedOrAssigning returns if the channel is assigned or assigning to a server.
func (c *PChannelMeta) IsAssignedOrAssigning() bool {
	return c.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED || c.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING
}

// LastAssignTimestamp returns the last assigned timestamp.
func (c *PChannelMeta) LastAssignTimestamp() time.Time {
	return time.Unix(int64(c.inner.LastAssignTimestampSeconds), 0)
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
			inner:                  proto.Clone(c.inner).(*streamingpb.PChannelMeta),
			availableInReplication: c.availableInReplication,
		},
	}
}

// mutablePChannel is a mutable version of PChannel.
// use to update the channel info.
type mutablePChannel struct {
	*PChannelMeta
}

// TryAssignToServerID assigns the channel to a server.
func (m *mutablePChannel) TryAssignToServerID(accessMode types.AccessMode, streamingNode types.StreamingNodeInfo) bool {
	if m.ChannelInfo().AccessMode == accessMode && m.CurrentServerID() == streamingNode.ServerID && m.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED {
		// if the channel is already assigned to the server, return false.
		return false
	}
	if m.inner.State != streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED {
		m.updateOrAppendAssignHistory()
	}

	// otherwise update the channel into assgining state.
	m.inner.Channel.AccessMode = streamingpb.PChannelAccessMode(accessMode)
	m.inner.Channel.Term++
	m.inner.Node = types.NewProtoFromStreamingNodeInfo(streamingNode)
	m.inner.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING
	return true
}

// updateOrAppendAssignHistory updates the assign history of the channel if channel is assigned at previous term at target node,
// otherwise, append the history directly.
func (m *mutablePChannel) updateOrAppendAssignHistory() {
	// if the node has been assigned to, update the history directly.
	// e.g. the node 10 is assigned to the channel at term 1 but open failed,
	// we have history record like:
	// (term 1, node 10, access mode RW)
	// (term 2, node 11, access mode RW)
	// the the node is reassigned to the channel at term 3.
	// the the history can be compacted into
	// (term 3, node 10, access mode RW)
	// (term 2, node 11, access mode RW)
	// to make the history smaller.
	for _, h := range m.inner.Histories {
		if h.Node.ServerId == m.inner.Node.ServerId && h.AccessMode == m.inner.Channel.AccessMode {
			h.Term = m.inner.Channel.Term
			return
		}
	}
	// otherwise, append the history directly.
	m.inner.Histories = append(m.inner.Histories, &streamingpb.PChannelAssignmentLog{
		Term:       m.inner.Channel.Term,
		Node:       m.inner.Node,
		AccessMode: m.inner.Channel.AccessMode,
	})
}

// AssignToServerDone assigns the channel to the server done.
func (m *mutablePChannel) AssignToServerDone() {
	if m.inner.State == streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNING {
		m.inner.Histories = make([]*streamingpb.PChannelAssignmentLog, 0)
		m.inner.State = streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED
		m.inner.LastAssignTimestampSeconds = uint64(time.Now().Unix())
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
