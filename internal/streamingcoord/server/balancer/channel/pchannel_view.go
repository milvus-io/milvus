package channel

import (
	"fmt"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// ChannelID is the unique id of a channel.
type ChannelID = types.ChannelID

// newPChannelView creates a new pchannel view.
func newPChannelView(metas map[ChannelID]*PChannelMeta) *PChannelView {
	view := newPChannelMetaView(metas)
	view.fillStats()
	return view
}

// newPChannelMetaView is newPChannelView without Stats: it copies no vchannel
// and does not wait for the pchannel stats manager.
func newPChannelMetaView(metas map[ChannelID]*PChannelMeta) *PChannelView {
	view := &PChannelView{
		Channels: make(map[ChannelID]*PChannelMeta, len(metas)),
	}
	for _, meta := range metas {
		id := meta.ChannelInfo().ChannelID()
		if _, ok := view.Channels[id]; ok {
			panic(fmt.Sprintf("duplicate rw channel: %s", id.String()))
		}
		view.Channels[id] = meta
	}
	return view
}

// fillStats copies the stats of every pchannel of the view. The metas are
// immutable snapshots and the stats have their own locks, so it needs no
// ChannelManager lock.
func (v *PChannelView) fillStats() {
	v.Stats = make(map[ChannelID]PChannelStatsView, len(v.Channels))
	for id, meta := range v.Channels {
		stat := StaticPChannelStatsManager.Get().GetPChannelStats(id).View()
		stat.LastAssignTimestamp = meta.LastAssignTimestamp()
		v.Stats[id] = stat
	}
}

// PChannelView is the view of current pchannels.
type PChannelView struct {
	Channels map[ChannelID]*PChannelMeta
	Stats    map[ChannelID]PChannelStatsView
}

// PChannelStatsView is the view of the pchannel stats.
type PChannelStatsView struct {
	LastAssignTimestamp time.Time
	VChannels           map[string]int64
}
