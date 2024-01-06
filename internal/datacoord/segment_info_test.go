package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

func TestIsDeltaLogExists(t *testing.T) {
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogPath: "test",
						},
						{
							LogPath: "test2",
						},
					},
				},
			},
		},
	}
	assert.True(t, segment.IsDeltaLogExists("test"))
	assert.True(t, segment.IsDeltaLogExists("test2"))
	assert.False(t, segment.IsDeltaLogExists("test3"))
	assert.False(t, segment.IsDeltaLogExists(""))
}

func TestIsStatsLogExists(t *testing.T) {
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			Statslogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{
							LogPath: "test",
						},
						{
							LogPath: "test2",
						},
					},
				},
			},
		},
	}
	assert.True(t, segment.IsStatsLogExists("test"))
	assert.True(t, segment.IsStatsLogExists("test2"))
	assert.False(t, segment.IsStatsLogExists("test3"))
	assert.False(t, segment.IsStatsLogExists(""))
}
