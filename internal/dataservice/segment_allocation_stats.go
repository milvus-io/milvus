package dataservice

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap"
)

type segAllocStatus struct {
	id             UniqueID
	collectionID   UniqueID
	partitionID    UniqueID
	sealed         bool
	total          int64
	insertChannel  string
	allocations    []*allocation
	lastExpireTime Timestamp
}
type allocation struct {
	rowNums    int64
	expireTime Timestamp
}

func (s *segAllocStatus) getAllocationSize() int64 {
	var totalOfAllocations int64
	for _, allocation := range s.allocations {
		totalOfAllocations += allocation.rowNums
	}
	return totalOfAllocations
}

func (s *segAllocStatus) appendAllocation(rowNums int64, expireTime Timestamp) {
	alloc := &allocation{
		rowNums:    rowNums,
		expireTime: expireTime,
	}
	s.lastExpireTime = expireTime
	s.allocations = append(s.allocations, alloc)
}

type segAllocStats struct {
	meta  *meta
	stats map[UniqueID]*segAllocStatus //segment id -> status
}

func newAllocStats(meta *meta) *segAllocStats {
	s := &segAllocStats{
		meta:  meta,
		stats: make(map[UniqueID]*segAllocStatus),
	}
	s.loadSegmentsFromMeta()
	return s
}

func (s *segAllocStats) loadSegmentsFromMeta() {
	// load unflushed segments from meta
	segments := s.meta.GetUnFlushedSegments()
	for _, seg := range segments {
		stat := &segAllocStatus{
			id:             seg.ID,
			collectionID:   seg.CollectionID,
			partitionID:    seg.PartitionID,
			total:          seg.MaxRowNum,
			allocations:    []*allocation{},
			insertChannel:  seg.InsertChannel,
			lastExpireTime: seg.LastExpireTime,
			sealed: seg.State == commonpb.SegmentState_Sealed ||
				seg.State == commonpb.SegmentState_Flushing,
		}
		s.stats[seg.ID] = stat
	}
}

func (s *segAllocStats) getSegments(collectionID UniqueID, partitionID UniqueID, channelName string) []*segAllocStatus {
	ret := make([]*segAllocStatus, 0)
	for _, segment := range s.stats {
		if segment.sealed || segment.collectionID != collectionID || segment.partitionID != partitionID || segment.insertChannel != channelName {
			continue
		}
		ret = append(ret, segment)
	}
	return ret
}

func (s *segAllocStats) appendAllocation(segmentID UniqueID, numRows int64, expireTime Timestamp) error {
	segStatus := s.stats[segmentID]
	segStatus.appendAllocation(numRows, expireTime)
	return s.meta.SetLastExpireTime(segStatus.id, expireTime)
}

func (s *segAllocStats) sealSegment(id UniqueID) error {
	s.stats[id].sealed = true
	return s.meta.SealSegment(id)
}

func (s *segAllocStats) sealSegmentsBy(collectionID UniqueID) error {
	for _, status := range s.stats {
		if status.collectionID == collectionID {
			if status.sealed {
				continue
			}
			if err := s.meta.SealSegment(status.id); err != nil {
				return err
			}
			status.sealed = true
		}
	}
	return nil
}
func (s *segAllocStats) dropSegment(id UniqueID) {
	delete(s.stats, id)
}

func (s *segAllocStats) expire(t Timestamp) {
	for _, segStatus := range s.stats {
		for i := 0; i < len(segStatus.allocations); i++ {
			if t < segStatus.allocations[i].expireTime {
				continue
			}
			log.Debug("dataservice::ExpireAllocations: ",
				zap.Any("segStatus.id", segStatus.id),
				zap.Any("segStatus.allocations.rowNums", segStatus.allocations[i].rowNums))
			segStatus.allocations = append(segStatus.allocations[:i], segStatus.allocations[i+1:]...)
			i--
		}
	}
}

func (s *segAllocStats) getAllSegments() []*segAllocStatus {
	ret := make([]*segAllocStatus, 0)
	for _, status := range s.stats {
		ret = append(ret, status)
	}
	return ret
}

func (s *segAllocStats) getSegmentBy(id UniqueID) *segAllocStatus {
	return s.stats[id]
}

func (s *segAllocStats) addSegment(segment *datapb.SegmentInfo) error {
	s.stats[segment.ID] = &segAllocStatus{
		id:             segment.ID,
		collectionID:   segment.CollectionID,
		partitionID:    segment.PartitionID,
		sealed:         false,
		total:          segment.MaxRowNum,
		insertChannel:  segment.InsertChannel,
		allocations:    []*allocation{},
		lastExpireTime: segment.LastExpireTime,
	}
	return s.meta.AddSegment(segment)
}
