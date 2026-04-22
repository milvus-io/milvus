package datacoord

import (
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// CachedSegmentsInfo is a concurrent, version-tracked segment metadata store
// with secondary indexes by collection, channel, and compaction relationship.
type CachedSegmentsInfo struct {
	segments *Cache[UniqueID, *SegmentInfo]

	// secondary indexes: collection/channel -> set of segment IDs
	coll2Segments    *typeutil.ConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, struct{}]]
	channel2Segments *typeutil.ConcurrentMap[string, *typeutil.ConcurrentMap[UniqueID, struct{}]]
	compactionTo     *typeutil.ConcurrentMap[UniqueID, []UniqueID]
}

func NewCachedSegmentsInfo() *CachedSegmentsInfo {
	return &CachedSegmentsInfo{
		segments: NewCache[UniqueID, *SegmentInfo](func(s *SegmentInfo) *SegmentInfo {
			return s.Clone()
		}),
		coll2Segments:    typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, struct{}]](),
		channel2Segments: typeutil.NewConcurrentMap[string, *typeutil.ConcurrentMap[UniqueID, struct{}]](),
		compactionTo:     typeutil.NewConcurrentMap[UniqueID, []UniqueID](),
	}
}

func (s *CachedSegmentsInfo) GetSegment(segmentID UniqueID) *SegmentInfo {
	v, ok := s.segments.Lookup(segmentID)
	if !ok {
		return nil
	}
	return v
}

// GetSegmentWithVersion returns the live SegmentInfo and its current version
// atomically, so callers can use the version as the CAS precondition for a
// subsequent persist Update. Returns (nil, 0) for missing or tombstoned segments.
func (s *CachedSegmentsInfo) GetSegmentWithVersion(segmentID UniqueID) (*SegmentInfo, int64) {
	v, ver, ok := s.segments.LookupWithVersion(segmentID)
	if !ok {
		return nil, 0
	}
	return v, ver
}

func (s *CachedSegmentsInfo) GetSegments() []*SegmentInfo {
	var result []*SegmentInfo
	s.segments.Range(func(_ UniqueID, v *SegmentInfo) bool {
		result = append(result, v)
		return true
	})
	return result
}

func (s *CachedSegmentsInfo) GetSegmentsBySelector(filters ...SegmentFilter) []*SegmentInfo {
	criterion := &segmentCriterion{}
	for _, filter := range filters {
		filter.AddFilter(criterion)
	}

	candidates := s.getCandidates(criterion)
	result := make([]*SegmentInfo, 0, len(candidates))
	for _, seg := range candidates {
		if criterion.Match(seg) {
			result = append(result, seg)
		}
	}
	return result
}

func (s *CachedSegmentsInfo) getCandidates(criterion *segmentCriterion) []*SegmentInfo {
	if criterion.collectionID > 0 {
		idSet, ok := s.coll2Segments.Get(criterion.collectionID)
		if !ok {
			return nil
		}
		var result []*SegmentInfo
		idSet.Range(func(id UniqueID, _ struct{}) bool {
			if seg := s.GetSegment(id); seg != nil {
				if criterion.channel == "" || seg.InsertChannel == criterion.channel {
					result = append(result, seg)
				}
			}
			return true
		})
		return result
	}

	if criterion.channel != "" {
		idSet, ok := s.channel2Segments.Get(criterion.channel)
		if !ok {
			return nil
		}
		var result []*SegmentInfo
		idSet.Range(func(id UniqueID, _ struct{}) bool {
			if seg := s.GetSegment(id); seg != nil {
				result = append(result, seg)
			}
			return true
		})
		return result
	}

	return s.GetSegments()
}

func (s *CachedSegmentsInfo) GetRealSegmentsForChannel(channel string) []*SegmentInfo {
	idSet, ok := s.channel2Segments.Get(channel)
	if !ok {
		return nil
	}
	var result []*SegmentInfo
	idSet.Range(func(id UniqueID, _ struct{}) bool {
		if seg := s.GetSegment(id); seg != nil && !seg.GetIsFake() {
			result = append(result, seg)
		}
		return true
	})
	return result
}

func (s *CachedSegmentsInfo) GetCompactionTo(fromSegmentID int64) ([]*SegmentInfo, bool) {
	exist := s.GetSegment(fromSegmentID) != nil
	compactTos, ok := s.compactionTo.Get(fromSegmentID)
	if !ok {
		return nil, exist
	}
	var result []*SegmentInfo
	for _, toID := range compactTos {
		to := s.GetSegment(toID)
		if to == nil {
			log.Warn("compactionTo relation is broken", zap.Int64("from", fromSegmentID), zap.Int64("to", toID))
			return nil, exist
		}
		result = append(result, to)
	}
	return result, exist
}

func (s *CachedSegmentsInfo) SetSegment(segmentID UniqueID, segment *SegmentInfo, version int64) (old *SegmentInfo, existed bool) {
	old, existed = s.segments.Insert(segmentID, segment, version)
	if existed {
		s.removeSecondaryIndex(old)
		s.deleteCompactTo(old)
	}
	s.addSecondaryIndex(segment)
	s.addCompactTo(segment)
	return old, existed
}

// DropSegment marks the segment as a tombstone; the version blocks later stale writes.
func (s *CachedSegmentsInfo) DropSegment(segmentID UniqueID, version int64) {
	if old, ok := s.segments.Lookup(segmentID); ok {
		s.removeSecondaryIndex(old)
		s.deleteCompactTo(old)
	}
	s.segments.Erase(segmentID, version)
}

// PruneSegment removes a tombstone entry, freeing memory.
func (s *CachedSegmentsInfo) PruneSegment(segmentID UniqueID) {
	s.segments.Prune(segmentID)
}

// Local-only updates below bypass versioning and are not persisted.

func (s *CachedSegmentsInfo) SetRowCount(segmentID UniqueID, rowCount int64) {
	s.updateSegment(segmentID, SetRowCount(rowCount))
}

func (s *CachedSegmentsInfo) SetDmlPosition(segmentID UniqueID, pos *msgpb.MsgPosition) {
	s.updateSegment(segmentID, SetDmlPosition(pos))
}

func (s *CachedSegmentsInfo) SetStartPosition(segmentID UniqueID, pos *msgpb.MsgPosition) {
	s.updateSegment(segmentID, SetStartPosition(pos))
}

func (s *CachedSegmentsInfo) SetAllocations(segmentID UniqueID, allocations []*Allocation) {
	s.updateSegment(segmentID, SetAllocations(allocations))
}

func (s *CachedSegmentsInfo) AddAllocation(segmentID UniqueID, allocation *Allocation) {
	s.updateSegment(segmentID, AddAllocation(allocation))
}

func (s *CachedSegmentsInfo) SetLastWrittenTime(segmentID UniqueID) {
	s.updateSegment(segmentID, SetLastWrittenTime())
}

func (s *CachedSegmentsInfo) SetFlushTime(segmentID UniqueID, t time.Time) {
	s.updateSegment(segmentID, SetFlushTime(t))
}

func (s *CachedSegmentsInfo) SetIsCompacting(segmentID UniqueID, isCompacting bool) {
	s.updateSegment(segmentID, SetIsCompacting(isCompacting))
}

func (s *CachedSegmentsInfo) SetLevel(segmentID UniqueID, level datapb.SegmentLevel) {
	s.updateSegment(segmentID, SetLevel(level))
}

func (s *CachedSegmentsInfo) SetLastExpire(segmentID UniqueID, lastExpire uint64) {
	s.updateSegment(segmentID, SetExpireTime(lastExpire))
}

func (s *CachedSegmentsInfo) GetSegmentsByChannel(channel string) []*SegmentInfo {
	idSet, ok := s.channel2Segments.Get(channel)
	if !ok {
		return nil
	}
	var result []*SegmentInfo
	idSet.Range(func(id UniqueID, _ struct{}) bool {
		if seg := s.GetSegment(id); seg != nil {
			result = append(result, seg)
		}
		return true
	})
	return result
}

func (s *CachedSegmentsInfo) Len() int {
	return s.segments.Len()
}

func (s *CachedSegmentsInfo) updateSegment(segmentID UniqueID, opt SegmentInfoOption) {
	s.segments.Update(segmentID, func(seg *SegmentInfo) bool {
		opt(seg)
		return true
	}, 0)
}

func (s *CachedSegmentsInfo) addSecondaryIndex(segment *SegmentInfo) {
	collID := segment.GetCollectionID()
	channel := segment.GetInsertChannel()

	collSet, _ := s.coll2Segments.GetOrInsert(collID, typeutil.NewConcurrentMap[UniqueID, struct{}]())
	collSet.Insert(segment.GetID(), struct{}{})

	chSet, _ := s.channel2Segments.GetOrInsert(channel, typeutil.NewConcurrentMap[UniqueID, struct{}]())
	chSet.Insert(segment.GetID(), struct{}{})
}

func (s *CachedSegmentsInfo) removeSecondaryIndex(segment *SegmentInfo) {
	collID := segment.GetCollectionID()
	channel := segment.GetInsertChannel()

	if collSet, ok := s.coll2Segments.Get(collID); ok {
		collSet.Remove(segment.GetID())
	}
	if chSet, ok := s.channel2Segments.Get(channel); ok {
		chSet.Remove(segment.GetID())
	}
}

func (s *CachedSegmentsInfo) addCompactTo(segment *SegmentInfo) {
	for _, from := range segment.GetCompactionFrom() {
		existing, _ := s.compactionTo.GetOrInsert(from, nil)
		s.compactionTo.Insert(from, append(existing, segment.GetID()))
	}
}

func (s *CachedSegmentsInfo) deleteCompactTo(segment *SegmentInfo) {
	for _, from := range segment.GetCompactionFrom() {
		s.compactionTo.Remove(from)
	}
}
