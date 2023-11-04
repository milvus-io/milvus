package writebuffer

import (
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type SyncPolicy func(buffers []*segmentBuffer, ts typeutil.Timestamp) []int64

func SyncFullBuffer(buffers []*segmentBuffer, _ typeutil.Timestamp) []int64 {
	return lo.FilterMap(buffers, func(buf *segmentBuffer, _ int) (int64, bool) {
		return buf.segmentID, buf.IsFull()
	})
}

func GetSyncStaleBufferPolicy(staleDuration time.Duration) SyncPolicy {
	return func(buffers []*segmentBuffer, ts typeutil.Timestamp) []int64 {
		current := tsoutil.PhysicalTime(ts)
		return lo.FilterMap(buffers, func(buf *segmentBuffer, _ int) (int64, bool) {
			minTs := buf.MinTimestamp()
			start := tsoutil.PhysicalTime(minTs)

			return buf.segmentID, current.Sub(start) > staleDuration
		})
	}
}

func GetFlushingSegmentsPolicy(meta metacache.MetaCache) SyncPolicy {
	return func(_ []*segmentBuffer, _ typeutil.Timestamp) []int64 {
		return meta.GetSegmentIDsBy(metacache.WithSegmentState(commonpb.SegmentState_Flushing))
	}
}
