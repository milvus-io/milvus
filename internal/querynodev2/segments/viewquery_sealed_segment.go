package segments

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// NewSealedSegmentForViewQuery borrows an already pinned native segment.
// The caller retains its owning handle through execution and result cleanup.
func NewSealedSegmentForViewQuery(info *querypb.SegmentLoadInfo, native segcore.CSegment, databaseName string) Segment {
	return &viewQuerySealedSegment{
		viewQueryGrowingSegment: &viewQueryGrowingSegment{
			info: ViewQueryGrowingSegmentInfo{
				CollectionID: info.GetCollectionID(),
				PartitionID:  info.GetPartitionID(),
				VChannel:     info.GetInsertChannel(),
			},
			csegment: native,
		},
		loadInfo:     info,
		databaseName: databaseName,
	}
}

type viewQuerySealedSegment struct {
	*viewQueryGrowingSegment
	loadInfo     *querypb.SegmentLoadInfo
	databaseName string
}

func (s *viewQuerySealedSegment) Type() SegmentType                  { return SegmentTypeSealed }
func (s *viewQuerySealedSegment) Level() datapb.SegmentLevel         { return s.loadInfo.GetLevel() }
func (s *viewQuerySealedSegment) IsSorted() bool                     { return s.loadInfo.GetIsSorted() }
func (s *viewQuerySealedSegment) LoadInfo() *querypb.SegmentLoadInfo { return s.loadInfo }
func (s *viewQuerySealedSegment) DatabaseName() string               { return s.databaseName }
func (s *viewQuerySealedSegment) Shard() metautil.Channel {
	channel, _ := metautil.ParseChannel(s.loadInfo.GetInsertChannel(), metautil.NewDynChannelMapper())
	return channel
}

func (s *viewQuerySealedSegment) Indexes() []*IndexedFieldInfo {
	result := make([]*IndexedFieldInfo, 0, len(s.loadInfo.GetIndexInfos()))
	for _, index := range s.loadInfo.GetIndexInfos() {
		result = append(result, &IndexedFieldInfo{IndexInfo: index, IsLoaded: true})
	}
	return result
}

func (s *viewQuerySealedSegment) GetIndexByID(id int64) *IndexedFieldInfo {
	for _, index := range s.Indexes() {
		if index.IndexInfo.GetIndexID() == id {
			return index
		}
	}
	return nil
}

func (s *viewQuerySealedSegment) GetIndex(fieldID int64) []*IndexedFieldInfo {
	result := make([]*IndexedFieldInfo, 0)
	for _, index := range s.Indexes() {
		if index.IndexInfo.GetFieldID() == fieldID {
			result = append(result, index)
		}
	}
	return result
}
func (s *viewQuerySealedSegment) ExistIndex(fieldID int64) bool { return len(s.GetIndex(fieldID)) > 0 }
func (s *viewQuerySealedSegment) Retrieve(ctx context.Context, plan *segcore.RetrievePlan) (*segcorepb.RetrieveResults, error) {
	return retrySegmentReadGate(ctx, SegmentTypeSealed, func() (*segcorepb.RetrieveResults, error) { return s.viewQueryGrowingSegment.Retrieve(ctx, plan) }, waitSegmentReadGateRetry)
}

func (s *viewQuerySealedSegment) RetrieveByOffsets(ctx context.Context, plan *segcore.RetrievePlanWithOffsets) (*segcorepb.RetrieveResults, error) {
	return retrySegmentReadGate(ctx, SegmentTypeSealed, func() (*segcorepb.RetrieveResults, error) {
		return s.viewQueryGrowingSegment.RetrieveByOffsets(ctx, plan)
	}, waitSegmentReadGateRetry)
}

// NativeSegment is only borrowed while the owner's query reference is held.
func (s *viewQueryGrowingSegment) NativeSegment() segcore.CSegment { return s.csegment }
func (s *LocalSegment) NativeSegment() segcore.CSegment            { return s.csegment }
func borrowNativeSegment(segment Segment) (segcore.CSegment, func(), error) {
	native, ok := segment.(interface{ NativeSegment() segcore.CSegment })
	if !ok {
		return nil, nil, merr.WrapErrServiceInternalMsg("segment %d does not expose native query capability", segment.ID())
	}
	if native.NativeSegment() == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("segment %d has nil CSegment", segment.ID())
	}
	if err := segment.PinIfNotReleased(); err != nil {
		return nil, nil, err
	}
	return native.NativeSegment(), segment.Unpin, nil
}
