package datacoord

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

func TestCompactionTo(t *testing.T) {
	t.Run("mix_2_to_1", func(t *testing.T) {
		segments := NewSegmentsInfo()
		segment := NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1,
		})
		segments.SetSegment(segment.GetID(), segment)

		compactTos, ok := segments.GetCompactionTo(1)
		assert.True(t, ok)
		assert.Nil(t, compactTos)

		segment = NewSegmentInfo(&datapb.SegmentInfo{
			ID: 2,
		})
		segments.SetSegment(segment.GetID(), segment)
		segment = NewSegmentInfo(&datapb.SegmentInfo{
			ID:             3,
			CompactionFrom: []int64{1, 2},
		})
		segments.SetSegment(segment.GetID(), segment)

		getCompactToIDs := func(segments []*SegmentInfo) []int64 {
			return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 { return segment.GetID() })
		}

		compactTos, ok = segments.GetCompactionTo(3)
		assert.Nil(t, compactTos)
		assert.True(t, ok)
		compactTos, ok = segments.GetCompactionTo(1)
		assert.True(t, ok)
		assert.NotNil(t, compactTos)
		assert.ElementsMatch(t, []int64{3}, getCompactToIDs(compactTos))
		compactTos, ok = segments.GetCompactionTo(2)
		assert.True(t, ok)
		assert.NotNil(t, compactTos)
		assert.ElementsMatch(t, []int64{3}, getCompactToIDs(compactTos))

		// should be droped.
		segments.DropSegment(1)
		compactTos, ok = segments.GetCompactionTo(1)
		assert.False(t, ok)
		assert.Nil(t, compactTos)
		compactTos, ok = segments.GetCompactionTo(2)
		assert.True(t, ok)
		assert.NotNil(t, compactTos)
		assert.ElementsMatch(t, []int64{3}, getCompactToIDs(compactTos))
		compactTos, ok = segments.GetCompactionTo(3)
		assert.Nil(t, compactTos)
		assert.True(t, ok)

		segments.DropSegment(3)
		compactTos, ok = segments.GetCompactionTo(2)
		assert.True(t, ok)
		assert.Nil(t, compactTos)
	})

	t.Run("split_1_to_2", func(t *testing.T) {
		segments := NewSegmentsInfo()
		segment := NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1,
		})
		segments.SetSegment(segment.GetID(), segment)

		compactTos, ok := segments.GetCompactionTo(1)
		assert.True(t, ok)
		assert.Nil(t, compactTos)

		segment = NewSegmentInfo(&datapb.SegmentInfo{
			ID:             2,
			CompactionFrom: []int64{1},
		})
		segments.SetSegment(segment.GetID(), segment)
		segment = NewSegmentInfo(&datapb.SegmentInfo{
			ID:             3,
			CompactionFrom: []int64{1},
		})
		segments.SetSegment(segment.GetID(), segment)

		getCompactToIDs := func(segments []*SegmentInfo) []int64 {
			return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 { return segment.GetID() })
		}

		compactTos, ok = segments.GetCompactionTo(2)
		assert.Nil(t, compactTos)
		assert.True(t, ok)
		compactTos, ok = segments.GetCompactionTo(3)
		assert.Nil(t, compactTos)
		assert.True(t, ok)
		compactTos, ok = segments.GetCompactionTo(1)
		assert.True(t, ok)
		assert.NotNil(t, compactTos)
		assert.ElementsMatch(t, []int64{2, 3}, getCompactToIDs(compactTos))
	})
}

func TestGetSegmentSize(t *testing.T) {
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			Binlogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogID:      1,
							MemorySize: 1,
						},
					},
				},
			},
			Statslogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogID:      1,
							MemorySize: 1,
						},
					},
				},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogID:      1,
							MemorySize: 1,
						},
					},
				},
			},
		},
	}

	assert.Equal(t, int64(3), segment.getSegmentSize())
	assert.Equal(t, int64(3), segment.getSegmentSize())
}

func TestIsDeltaLogExists(t *testing.T) {
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogID: 1,
						},
						{
							LogID: 2,
						},
					},
				},
			},
		},
	}
	assert.True(t, segment.IsDeltaLogExists(1))
	assert.True(t, segment.IsDeltaLogExists(2))
	assert.False(t, segment.IsDeltaLogExists(3))
	assert.False(t, segment.IsDeltaLogExists(0))
}

func TestIsStatsLogExists(t *testing.T) {
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			Statslogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogID: 1,
						},
						{
							LogID: 2,
						},
					},
				},
			},
		},
	}
	assert.True(t, segment.IsStatsLogExists(1))
	assert.True(t, segment.IsStatsLogExists(2))
	assert.False(t, segment.IsStatsLogExists(3))
	assert.False(t, segment.IsStatsLogExists(0))
}
