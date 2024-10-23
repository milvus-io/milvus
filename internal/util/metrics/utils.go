package metrics

import (
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func PruneFieldIndexInfo(f *querypb.FieldIndexInfo) *querypb.FieldIndexInfo {
	return &querypb.FieldIndexInfo{
		FieldID:   f.FieldID,
		IndexID:   f.IndexID,
		BuildID:   f.BuildID,
		IndexSize: f.IndexSize,
		NumRows:   f.NumRows,
	}
}

func PruneSegmentInfo(s *datapb.SegmentInfo) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:        s.ID,
		NumOfRows: s.NumOfRows,
		State:     s.State,
		Compacted: s.Compacted,
		Level:     s.Level,
	}
}

func PruneVChannelInfo(channel *datapb.VchannelInfo) *datapb.VchannelInfo {
	return &datapb.VchannelInfo{
		ChannelName: channel.ChannelName,
		UnflushedSegments: lo.Map(channel.UnflushedSegments, func(s *datapb.SegmentInfo, i int) *datapb.SegmentInfo {
			return PruneSegmentInfo(s)
		}),
		FlushedSegments: lo.Map(channel.FlushedSegments, func(s *datapb.SegmentInfo, i int) *datapb.SegmentInfo {
			return PruneSegmentInfo(s)
		}),
		DroppedSegments: lo.Map(channel.DroppedSegments, func(s *datapb.SegmentInfo, i int) *datapb.SegmentInfo {
			return PruneSegmentInfo(s)
		}),
		IndexedSegments: lo.Map(channel.IndexedSegments, func(s *datapb.SegmentInfo, i int) *datapb.SegmentInfo {
			return PruneSegmentInfo(s)
		}),
	}
}
