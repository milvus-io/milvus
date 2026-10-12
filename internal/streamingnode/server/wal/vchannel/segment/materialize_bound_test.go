package segment

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestL1MaterializationBlockerTimeTick(t *testing.T) {
	blocked := newSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{
		Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 100},
	}, nil)
	timetick, blocks := blocked.L1MaterializationBlockerTimeTick()
	assert.True(t, blocks)
	assert.Equal(t, uint64(100), timetick)

	committed := newSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{
		SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 1},
		Stat:                &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 200},
	}, nil)
	timetick, blocks = committed.L1MaterializationBlockerTimeTick()
	assert.False(t, blocks)
	assert.Zero(t, timetick)
}
