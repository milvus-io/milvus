package segments

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	pkoracle "github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ Segment = (*viewQueryGrowingSegment)(nil)

type ViewQueryGrowingSegmentInfo struct {
	CollectionID int64
	PartitionID  int64
	VChannel     string
}

func NewGrowingSegmentForViewQuery(info ViewQueryGrowingSegmentInfo, csegment segcore.CSegment) Segment {
	return &viewQueryGrowingSegment{
		info:     info,
		csegment: csegment,
	}
}

type viewQueryGrowingSegment struct {
	info     ViewQueryGrowingSegmentInfo
	csegment segcore.CSegment
}

func (s *viewQueryGrowingSegment) ID() int64 {
	if s.csegment == nil {
		return 0
	}
	return s.csegment.ID()
}

func (s *viewQueryGrowingSegment) DatabaseName() string { return "" }

func (s *viewQueryGrowingSegment) ResourceGroup() string { return "" }

func (s *viewQueryGrowingSegment) Collection() int64 { return s.info.CollectionID }

func (s *viewQueryGrowingSegment) Partition() int64 { return s.info.PartitionID }

func (s *viewQueryGrowingSegment) Shard() metautil.Channel {
	return metautil.Channel{}
}

func (s *viewQueryGrowingSegment) Version() int64 { return 0 }

func (s *viewQueryGrowingSegment) CASVersion(int64, int64) bool { return false }

func (s *viewQueryGrowingSegment) StartPosition() *msgpb.MsgPosition { return nil }

func (s *viewQueryGrowingSegment) Type() SegmentType { return SegmentTypeGrowing }

func (s *viewQueryGrowingSegment) Level() datapb.SegmentLevel { return datapb.SegmentLevel_L1 }

func (s *viewQueryGrowingSegment) IsSorted() bool { return false }

func (s *viewQueryGrowingSegment) LoadInfo() *querypb.SegmentLoadInfo {
	return &querypb.SegmentLoadInfo{
		SegmentID:     s.ID(),
		CollectionID:  s.info.CollectionID,
		PartitionID:   s.info.PartitionID,
		InsertChannel: s.info.VChannel,
	}
}

func (s *viewQueryGrowingSegment) PinIfNotReleased() error { return nil }

func (s *viewQueryGrowingSegment) Unpin() {}

func (s *viewQueryGrowingSegment) InsertCount() int64 { return s.RowNum() }

func (s *viewQueryGrowingSegment) RowNum() int64 {
	if s.csegment == nil {
		return 0
	}
	return s.csegment.RowNum()
}

func (s *viewQueryGrowingSegment) MemSize() int64 {
	if s.csegment == nil {
		return 0
	}
	return s.csegment.MemSize()
}

func (s *viewQueryGrowingSegment) ResourceUsageEstimate() ResourceUsage {
	return ResourceUsage{MemorySize: uint64(s.MemSize())}
}

func (s *viewQueryGrowingSegment) GetIndexByID(int64) *IndexedFieldInfo { return nil }

func (s *viewQueryGrowingSegment) GetIndex(int64) []*IndexedFieldInfo { return nil }

func (s *viewQueryGrowingSegment) ExistIndex(int64) bool { return false }

func (s *viewQueryGrowingSegment) Indexes() []*IndexedFieldInfo { return nil }

func (s *viewQueryGrowingSegment) HasRawData(fieldID int64) bool {
	return s.csegment != nil && s.csegment.HasRawData(fieldID)
}

func (s *viewQueryGrowingSegment) DropIndex(context.Context, int64) error {
	return merr.WrapErrServiceInternalMsg("view query growing segment does not support DropIndex")
}

func (s *viewQueryGrowingSegment) Insert(context.Context, []int64, []typeutil.Timestamp, *segcorepb.InsertRecord) error {
	return merr.WrapErrServiceInternalMsg("view query growing segment does not support Insert")
}

func (s *viewQueryGrowingSegment) Delete(context.Context, storage.PrimaryKeys, []typeutil.Timestamp) error {
	return merr.WrapErrServiceInternalMsg("view query growing segment does not support Delete")
}

func (s *viewQueryGrowingSegment) LoadDeltaData(context.Context, *storage.DeltaData) error {
	return merr.WrapErrServiceInternalMsg("view query growing segment does not support LoadDeltaData")
}

func (s *viewQueryGrowingSegment) LastDeltaTimestamp() uint64 { return 0 }

func (s *viewQueryGrowingSegment) Load(context.Context) error { return nil }

func (s *viewQueryGrowingSegment) Release(context.Context, ...releaseOption) {}

func (s *viewQueryGrowingSegment) Reopen(context.Context, *querypb.SegmentLoadInfo) error {
	return merr.WrapErrServiceInternalMsg("view query growing segment does not support Reopen")
}

func (s *viewQueryGrowingSegment) SetPKCandidate(pkoracle.Candidate) {}

func (s *viewQueryGrowingSegment) PkCandidateExist() bool { return false }

func (s *viewQueryGrowingSegment) UpdatePkCandidate([]storage.PrimaryKey) {}

func (s *viewQueryGrowingSegment) Stats() *storage.PkStatistics { return nil }

func (s *viewQueryGrowingSegment) Charge() {}

func (s *viewQueryGrowingSegment) Refund() {}

func (s *viewQueryGrowingSegment) MayPkExist(*storage.LocationsCache) bool { return true }

func (s *viewQueryGrowingSegment) BatchPkExist(lc *storage.BatchLocationsCache) []bool {
	if lc == nil {
		return nil
	}
	hits := make([]bool, lc.Size())
	for i := range hits {
		hits[i] = true
	}
	return hits
}

func (s *viewQueryGrowingSegment) GetMinPk() *storage.PrimaryKey { return nil }

func (s *viewQueryGrowingSegment) GetMaxPk() *storage.PrimaryKey { return nil }

func (s *viewQueryGrowingSegment) UpdateBM25Stats(map[int64]*storage.BM25Stats) {}

func (s *viewQueryGrowingSegment) GetBM25Stats() map[int64]*storage.BM25Stats { return nil }

func (s *viewQueryGrowingSegment) Search(ctx context.Context, searchReq *segcore.SearchRequest) (*segcore.SearchResult, error) {
	if s.csegment == nil {
		return nil, merr.WrapErrSegmentNotLoaded(s.ID(), "nil growing segment")
	}
	return s.csegment.Search(ctx, searchReq)
}

func (s *viewQueryGrowingSegment) Retrieve(ctx context.Context, plan *segcore.RetrievePlan) (*segcorepb.RetrieveResults, error) {
	if s.csegment == nil {
		return nil, merr.WrapErrSegmentNotLoaded(s.ID(), "nil growing segment")
	}
	result, err := s.csegment.Retrieve(ctx, plan)
	if err != nil {
		return nil, err
	}
	defer result.Release()
	return result.GetResult()
}

func (s *viewQueryGrowingSegment) RetrieveByOffsets(ctx context.Context, plan *segcore.RetrievePlanWithOffsets) (*segcorepb.RetrieveResults, error) {
	if s.csegment == nil {
		return nil, merr.WrapErrSegmentNotLoaded(s.ID(), "nil growing segment")
	}
	result, err := s.csegment.RetrieveByOffsets(ctx, plan)
	if err != nil {
		return nil, err
	}
	defer result.Release()
	return result.GetResult()
}

func (s *viewQueryGrowingSegment) FlushData(context.Context, int64, int64, *FlushConfig) (*FlushResult, error) {
	return nil, merr.WrapErrServiceInternalMsg("view query growing segment does not support FlushData")
}

func (s *viewQueryGrowingSegment) IsLazyLoad() bool { return false }

func (s *viewQueryGrowingSegment) ResetIndexesLazyLoad(bool) {}

func (s *viewQueryGrowingSegment) NeedUpdatedVersion() int64 { return 0 }

func (s *viewQueryGrowingSegment) RemoveUnusedFieldFiles() error { return nil }

func (s *viewQueryGrowingSegment) GetFieldJSONIndexStats() map[int64]*querypb.JsonStatsInfo {
	return nil
}
