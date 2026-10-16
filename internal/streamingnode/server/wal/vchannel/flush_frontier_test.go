package vchannel

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestFlushCertificateAndMaterializationRequireFinalPublication(t *testing.T) {
	makeSegment := func(id, partition int64, created uint64, published bool) *segment.SegmentView {
		meta := &streamingpb.SegmentAssignmentMeta{
			SegmentId: id, PartitionId: partition, Vchannel: "v1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
			CheckpointTimeTick: 150, // registered and packs durable, but not finally published
			Stat:               &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: created},
		}
		if published {
			meta.SealedAtDataVersion = &viewpb.DataVersion{StreamingVersion: 2}
		}
		return segment.NewSegmentViewFromMetaWithConfig(meta, nil, segment.ViewConfig{})
	}
	m := &VChannelRecoveryModule{
		queryObservedTimeTick: 200,
		segments:              map[int64]*segment.SegmentView{1: makeSegment(1, 1, 40, false), 2: makeSegment(2, 2, 90, false)},
	}
	require.Equal(t, uint64(40), m.GrowingTransformStartAfterTimeTick(2), "other partitions remain blockers")
	require.Equal(t, uint64(90), m.GrowingTransformStartAfterTimeTick(1), "exclude only the current atomic publication")
	require.False(t, m.earlierSegmentsFlushed(200), "registration is not final Flush")
	require.True(t, m.earlierSegmentsFlushed(40), "creation at the boundary has no earlier rows")
	m.segments[2] = makeSegment(2, 2, 90, true)
	require.False(t, m.earlierSegmentsFlushed(200), "one published partition cannot release another")
	require.Equal(t, uint64(200), m.GrowingTransformStartAfterTimeTick(1))
	m.segments[1] = makeSegment(1, 1, 40, true)
	require.True(t, m.earlierSegmentsFlushed(200))
	m.segments[3] = makeSegment(3, 3, 250, false)
	require.Equal(t, uint64(200), m.GrowingTransformStartAfterTimeTick(0), "never certify beyond the observed prefix")
	m.queryObservedTimeTick = 300
	require.Equal(t, uint64(250), m.GrowingTransformStartAfterTimeTick(0))
	require.False(t, m.earlierSegmentsFlushed(300))
}
