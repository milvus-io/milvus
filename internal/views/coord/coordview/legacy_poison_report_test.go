package coordview

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestLegacyPoisonFieldDoesNotTriggerCoordRecovery(t *testing.T) {
	for _, state := range []qviews.QueryViewState{qviews.QueryViewStatePreparing, qviews.QueryViewStateReady, qviews.QueryViewStateUp} {
		t.Run(state.String(), func(t *testing.T) {
			view := buildTestView(1)
			sm := NewCoordQueryViewStateMachine(view)
			sm.state = state
			report := qnReport(view, 1, qviews.QueryViewStateReady, 1000).IntoProto()
			wire, err := proto.Marshal(report.QueryNode[0])
			require.NoError(t, err)
			// Simulate an older node sending retired field 3, containing one
			// PoisonedSegment {segment_id:1000, generation:7, failed_timetick:100}.
			var legacy []byte
			for i, value := range []uint64{1000, 7, 100} {
				legacy = protowire.AppendTag(legacy, protowire.Number(i+1), protowire.VarintType)
				legacy = protowire.AppendVarint(legacy, value)
			}
			wire = protowire.AppendTag(wire, 3, protowire.BytesType)
			wire = protowire.AppendBytes(wire, legacy)
			decoded := &viewpb.QueryViewOfQueryNode{}
			require.NoError(t, proto.Unmarshal(wire, decoded))
			report.QueryNode[0] = decoded
			sm.OnNodeStateReported(qviews.NewQueryViewAtWorkNodeFromProto(report))
			require.NotEqual(t, qviews.QueryViewStateUnrecoverable, sm.State())
			require.Equal(t, []int64{1000}, sm.QNReadySegments()[1])
			descriptor := decoded.ProtoReflect().Descriptor()
			require.Nil(t, descriptor.Fields().ByNumber(3))
			require.True(t, descriptor.ReservedRanges().Has(3))
			require.True(t, descriptor.ReservedNames().Has("poisoned_segments"))
		})
	}
}
