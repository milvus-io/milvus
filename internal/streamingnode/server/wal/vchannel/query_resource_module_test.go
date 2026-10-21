package vchannel

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestDeleteReplayStartIncludesLegacySegments(t *testing.T) {
	for _, tc := range []struct {
		name    string
		created []uint64
		want    uint64
	}{
		{"empty", nil, 0},
		{"ordered", []uint64{20, 10}, 9},
		{"legacy first", []uint64{0, 20}, 0},
		{"legacy last", []uint64{20, 0}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := walview.VisibleSegmentSnapshot{}
			for _, tt := range tc.created {
				snapshot.Segments = append(snapshot.Segments, walview.VisibleSegment{Assignment: &streamingpb.SegmentAssignmentMeta{Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: tt}}})
			}
			require.Equal(t, tc.want, deleteReplayStartAfter(snapshot))
		})
	}
}
