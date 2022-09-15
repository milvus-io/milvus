package job

import (
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/samber/lo"
)

// waitCollectionReleased blocks until
// all channels and segments of given collection(partitions) are released,
// empty partition list means wait for collection released
func waitCollectionReleased(dist *meta.DistributionManager, collection int64, partitions ...int64) {
	partitionSet := typeutil.NewUniqueSet(partitions...)
	for {
		var (
			channels []*meta.DmChannel
			segments []*meta.Segment = dist.SegmentDistManager.GetByCollection(collection)
		)
		if partitionSet.Len() > 0 {
			segments = lo.Filter(segments, func(segment *meta.Segment, _ int) bool {
				return partitionSet.Contain(segment.GetPartitionID())
			})
		} else {
			channels = dist.ChannelDistManager.GetByCollection(collection)
		}

		if len(channels)+len(segments) == 0 {
			break
		}

		time.Sleep(200 * time.Millisecond)
	}
}
