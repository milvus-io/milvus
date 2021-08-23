package segmentfilter

import (
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

// SegmentFilter is used to know which segments may have data corresponding
// to the primary key
type SegmentFilter struct {
	segmentInfos []*datapb.SegmentInfo
	bloomFilters []*bloom.BloomFilter
}

func NewSegmentFilter(segmentInfos []*datapb.SegmentInfo) *SegmentFilter {
	return &SegmentFilter{
		segmentInfos: segmentInfos,
	}
}

func (sf *SegmentFilter) init() {
	panic("This method has not been implemented")
}

// GetSegmentByPK pass a list of primary key and retrun an map of
// <segmentID, []string{primary_key}>
func (sf *SegmentFilter) GetSegmentByPK(pk []string) map[int64][]string {
	panic("This method has not been implemented")
}
