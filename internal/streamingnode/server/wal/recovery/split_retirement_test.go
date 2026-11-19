package recovery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestRecoverSplitRetiredVChannel(t *testing.T) {
	meta := &streamingpb.VChannelMeta{
		Vchannel: "p0_1v0", State: streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, CheckpointTimeTick: 300,
		SplitLineage: &streamingpb.VChannelSplitLineage{SplitTaskId: 7, SwitchTimeTick: 200, TargetVchannels: []string{"p1_1v1", "p2_1v2"}},
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 1, Schemas: []*streamingpb.CollectionSchemaOfVChannel{
			{Schema: &schemapb.CollectionSchema{}, CheckpointTimeTick: 10, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL},
		}},
	}
	metas := map[string]*streamingpb.VChannelMeta{meta.Vchannel: meta}
	require.NoError(t, validateRecoveredViewMeta(metas, nil, false))
	state, tick, closed := closedVChannelState(meta)
	require.Equal(t, "split", state)
	require.Equal(t, uint64(200), tick, "data must precede the original fence, not local retirement")
	require.True(t, closed)
	meta.SplitLineage = nil
	require.ErrorContains(t, validateRecoveredViewMeta(metas, nil, false), "split lineage")
}
