package growingruntime

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
)

func TestQueryBarrierAdvancesBothFrontiers(t *testing.T) {
	r := newRuntime()
	r.markGrowingTimeTick(90)
	r.markTransformTimeTick(10)
	event := walview.VChannelResourceEvent{QueryBarrier: &walview.QueryBarrierEvent{TimeTick: 100}}
	r.ApplyLiveEvent(t.Context(), event)
	require.NoError(t, r.WaitMVCCVisible(t.Context(), 100, 100))
	event.QueryBarrier.TimeTick = 80
	r.ApplyLiveEvent(t.Context(), event)
	require.EqualValues(t, 100, r.AppliedGrowingTimeTick())
	require.EqualValues(t, 100, r.AppliedTransformTimeTick())
}
