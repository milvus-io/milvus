package coordview

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestMemoryLifecycleSnapshotsRemainImmutable(t *testing.T) {
	view := buildTestView(2)
	view.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	sm := NewCoordQueryViewStateMachine(view)
	preparing := sm.ConsumeFlush()
	expected := proto.Clone(preparing.Persist)
	require.Same(t, view.QueryNode[0], preparing.Persist.QueryNode[0])
	require.Same(t, view.StreamingNode, preparing.Persist.StreamingNode)
	require.NotSame(t, view.Meta, preparing.Persist.Meta)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			// The queued persistence/sync work can marshal while later transitions run.
			_, err := proto.Marshal(preparing.Persist)
			assert.NoError(t, err)
			for _, target := range preparing.Sync {
				assert.Equal(t, qviews.QueryViewStatePreparing, target.State())
				_ = target.IntoProto()
			}
		}
	}()
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(qnReport(view, 2, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateReady))
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateUp))
	up := sm.ConsumeFlush()
	sm.EnterDown()
	down := sm.ConsumeFlush()
	control := &viewpb.QueryViewControl{PreemptServingLease: true}
	sm.EnterDownWithControl(control)
	controlled := sm.ConsumeFlush()
	control.PreemptServingLease = false
	require.False(t, down.Persist.Meta.GetControl().GetPreemptServingLease())
	require.True(t, controlled.Persist.Meta.GetControl().GetPreemptServingLease())
	require.True(t, controlled.Sync[0].IntoProto().Meta.GetControl().GetPreemptServingLease())
	require.Equal(t, viewpb.QueryViewState_QueryViewStateUp, up.Persist.Meta.State)
	sm.EnterDownWithControl(nil)
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDown))
	sm.OnNodeStateReported(qnReport(view, 1, qviews.QueryViewStateDropped))
	sm.OnQueryNodeLost(qviews.QueryNode{ID: 2})
	sm.OnNodeStateReported(snReport(view, qviews.QueryViewStateDropped))
	require.Equal(t, qviews.QueryViewStateDropped, sm.State())
	wg.Wait()
	require.True(t, proto.Equal(expected, preparing.Persist))
	require.EqualValues(t, []byte{0xa0, 0x06, 0x01}, controlled.Persist.ProtoReflect().GetUnknown())
}
