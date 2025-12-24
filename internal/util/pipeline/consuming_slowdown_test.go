package pipeline

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/util/mock_pipeline"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func TestConsumingSlowdown(t *testing.T) {
	g := mock_pipeline.NewMockLastestMVCCTimeTickGetter(t)

	var latestRequiredMVCCTimeTick uint64

	g.EXPECT().GetLatestRequiredMVCCTimeTick().RunAndReturn(func() uint64 {
		return latestRequiredMVCCTimeTick
	})

	sd := newEmptyTimeTickSlowdowner(g, "vchannel")

	now := time.Now().UnixMilli()
	ts := tsoutil.ComposeTS(now, 0)
	filtered := sd.Filter(&msgstream.MsgPack{EndTs: ts})
	require.False(t, filtered)

	ts = tsoutil.ComposeTS(now+1, 0)
	latestRequiredMVCCTimeTick = ts + 5

	filtered = sd.Filter(&msgstream.MsgPack{EndTs: ts})
	require.False(t, filtered)

	filtered = sd.Filter(&msgstream.MsgPack{EndTs: ts + 4})
	require.False(t, filtered)

	filtered = sd.Filter(&msgstream.MsgPack{EndTs: ts + 5})
	require.False(t, filtered)

	latestRequiredMVCCTimeTick = ts + 10

	filtered = sd.Filter(&msgstream.MsgPack{EndTs: ts + 7})
	require.False(t, filtered)

	filtered = sd.Filter(&msgstream.MsgPack{EndTs: ts + 10})
	require.False(t, filtered)

	filtered = sd.Filter(&msgstream.MsgPack{EndTs: ts + 11})
	require.True(t, filtered)

	filtered = sd.Filter(&msgstream.MsgPack{EndTs: ts + 12, Msgs: make([]msgstream.TsMsg, 10)})
	require.False(t, filtered)

	filtered = sd.Filter(&msgstream.MsgPack{EndTs: ts + 13})
	require.True(t, filtered)

	filtered = sd.Filter(&msgstream.MsgPack{EndTs: tsoutil.ComposeTS(now+int64(30*time.Millisecond), 0)})
	require.False(t, filtered)
}
