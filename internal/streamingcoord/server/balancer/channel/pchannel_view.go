package channel

import (
	"fmt"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

// ChannelID is the unique id of a channel.
type ChannelID = types.ChannelID

// newPChannelView creates a new pchannel view.
func newPChannelView(metas map[ChannelID]*PChannelMeta) *PChannelView {
	view := &PChannelView{
		Channels: make(map[ChannelID]*PChannelMeta, len(metas)),
		Stats:    make(map[ChannelID]PChannelStatsView, len(metas)),
	}
	for _, meta := range metas {
		id := meta.ChannelInfo().ChannelID()
		if _, ok := view.Channels[id]; ok {
			panic(fmt.Sprintf("duplicate rw channel: %s", id.String()))
		}
		view.Channels[id] = meta
		stat := StaticPChannelStatsManager.Get().GetPChannelStats(id).View()
		stat.LastAssignTimestamp = meta.LastAssignTimestamp()
		view.Stats[id] = stat
	}
	return view
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
