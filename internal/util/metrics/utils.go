package metrics

import (
	"strconv"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
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
		CollectionID: channel.GetCollectionID(),
		ChannelName:  channel.GetChannelName(),
		UnflushedSegmentIds: lo.Map(channel.GetUnflushedSegmentIds(), func(t int64, i int) string {
			return strconv.FormatInt(t, 10)
		}),
		FlushedSegmentIds: lo.Map(channel.GetFlushedSegmentIds(), func(t int64, i int) string {
			return strconv.FormatInt(t, 10)
		}),
		DroppedSegmentIds: lo.Map(channel.GetDroppedSegmentIds(), func(t int64, i int) string {
			return strconv.FormatInt(t, 10)
		}),
		LevelZeroSegmentIds: lo.Map(channel.GetLevelZeroSegmentIds(), func(t int64, i int) string {
			return strconv.FormatInt(t, 10)
		}),
	}
}
