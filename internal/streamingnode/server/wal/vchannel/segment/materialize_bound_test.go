package segment

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestL1MaterializationBlockerTimeTick(t *testing.T) {
	blocked := NewSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{
		Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 100},
	}, nil)
	timetick, blocks := blocked.L1MaterializationBlockerTimeTick()
	assert.True(t, blocks)
	assert.Equal(t, uint64(100), timetick)

	committed := NewSegmentViewFromMeta(&streamingpb.SegmentAssignmentMeta{
		L1CommitDone: true,
		Stat:         &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 200},
	}, nil)
	timetick, blocks = committed.L1MaterializationBlockerTimeTick()
	assert.False(t, blocks)
	assert.Zero(t, timetick)
}
