package channel

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// ChannelID is the unique id of a channel.
type ChannelID = types.ChannelID

// newPChannelView creates a new pchannel view.
func newPChannelView(metas []*PChannelMeta) *PChannelView {
	view := &PChannelView{
		Channels: make(map[ChannelID]*PChannelMeta),
	}
	for _, meta := range metas {
		id := meta.ChannelInfo().ChannelID()
		if id.ReplicaID == 0 && meta.ChannelInfo().AccessMode == types.AccessModeRO ||
			id.ReplicaID != 0 && meta.ChannelInfo().AccessMode == types.AccessModeRW {
			// assertion failed.
			panic(fmt.Sprintf("inconsistent channel meta: %s", id.String()))
		}
		if _, ok := view.Channels[id]; ok {
			panic(fmt.Sprintf("duplicate rw channel: %s", id.String()))
		}
		view.Channels[id] = meta
	}
	return view
}

// PChannelView is the view of current pchannels.
type PChannelView struct {
	Channels map[ChannelID]*PChannelMeta
}

func (v *PChannelView) Count() int {
	return len(v.Channels)
}

// GetChannel returns the channel by id.
func (v *PChannelView) GetChannel(id ChannelID) (*PChannelMeta, bool) {
	c, ok := v.Channels[id]
	return c, ok
}

// Update updates the channel.
func (v *PChannelView) Update(channel *PChannelMeta) {
	v.Channels[channel.ChannelInfo().ChannelID()] = channel
}

// Clone clones the pchannel view.
func (v *PChannelView) Clone() *PChannelView {
	// because all pchannel meta should be immutable.
	// so we only need to copy the reference.
	newView := &PChannelView{
		Channels: make(map[ChannelID]*PChannelMeta, len(v.Channels)),
	}
	for k, v := range v.Channels {
		newView.Channels[k] = v
	}
	return newView
}

// GetAssignment returns the current assignment of all channels.
func (v *PChannelView) GetAssignment() []types.PChannelInfoAssigned {
	assignment := make([]types.PChannelInfoAssigned, 0, len(v.Channels))
	for _, c := range v.Channels {
		if c.IsAssigned() {
			assignment = append(assignment, c.CurrentAssignment())
		}
	}
	return assignment
}
