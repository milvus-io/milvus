package channel

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func TestPChannelView(t *testing.T) {
	ResetStaticPChannelStatsManager()
	RecoverPChannelStatsManager([]string{})

	metas := map[ChannelID]*PChannelMeta{
		types.ChannelID{
			Name: "test",
		}: newPChannelMetaFromProto(&streamingpb.PChannelMeta{
			Channel: &streamingpb.PChannelInfo{Name: "test", Term: 1},
			State:   streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
		}),
		types.ChannelID{
			Name: "test2",
		}: newPChannelMetaFromProto(&streamingpb.PChannelMeta{
			Channel: &streamingpb.PChannelInfo{Name: "test2", Term: 1},
			State:   streamingpb.PChannelMetaState_PCHANNEL_META_STATE_UNINITIALIZED,
		}),
	}
	view := newPChannelView(metas)
	assert.Len(t, view.Channels, 2)
	assert.Len(t, view.Stats, 2)
}
