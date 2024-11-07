package metrics

import (
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
)

func NewSegmentFrom(segment *datapb.SegmentInfo) *metricsinfo.Segment {
	return &metricsinfo.Segment{
		SegmentID:    segment.GetID(),
		CollectionID: segment.GetCollectionID(),
		PartitionID:  segment.GetPartitionID(),
		Channel:      segment.GetInsertChannel(),
		NumOfRows:    segment.GetNumOfRows(),
		State:        segment.GetState().String(),
		IsImporting:  segment.GetIsImporting(),
		Compacted:    segment.GetCompacted(),
		Level:        segment.GetLevel().String(),
		IsSorted:     segment.GetIsSorted(),
		IsInvisible:  segment.GetIsInvisible(),
	}
}

func NewDMChannelFrom(channel *datapb.VchannelInfo) *metricsinfo.DmChannel {
	return &metricsinfo.DmChannel{
		CollectionID:           channel.GetCollectionID(),
		ChannelName:            channel.GetChannelName(),
		UnflushedSegmentIds:    channel.GetUnflushedSegmentIds(),
		FlushedSegmentIds:      channel.GetFlushedSegmentIds(),
		DroppedSegmentIds:      channel.GetDroppedSegmentIds(),
		LevelZeroSegmentIds:    channel.GetLevelZeroSegmentIds(),
		PartitionStatsVersions: channel.GetPartitionStatsVersions(),
	}
}
