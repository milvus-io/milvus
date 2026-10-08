package segments

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type adapterNativeSegment struct{ segcore.CSegment }

func (*adapterNativeSegment) ID() int64 { return 10 }

func TestViewQueryAdaptersSupportArrowAndBoost(t *testing.T) {
	native := &adapterNativeSegment{}
	growing := NewGrowingSegmentForViewQuery(ViewQueryGrowingSegmentInfo{CollectionID: 1}, native)
	sealed := NewSealedSegmentForViewQuery(&querypb.SegmentLoadInfo{CollectionID: 1, SegmentID: 10, IndexInfos: []*querypb.FieldIndexInfo{{FieldID: 100, IndexID: 20}}}, native, "db")
	require.Equal(t, SegmentTypeSealed, sealed.Type())
	require.Equal(t, "db", sealed.DatabaseName())
	require.True(t, sealed.ExistIndex(100))
	require.False(t, sealed.ExistIndex(101))
	require.NotNil(t, sealed.GetIndexByID(20))
	require.Nil(t, sealed.GetIndexByID(21))
	sentinel := merr.WrapErrServiceInternalMsg("native invocation reached")
	fill := mockey.Mock(segcore.FillRetrieveFieldsOrdered).To(func(_ context.Context, natives []segcore.CSegment, _ *segcore.RetrievePlan, indices []int32, offsets []int64) (arrow.Record, error) {
		require.Equal(t, []segcore.CSegment{native, native}, natives)
		require.Equal(t, []int32{1, 0}, indices)
		require.Equal(t, []int64{7, 3}, offsets)
		return nil, sentinel
	}).Build()
	defer fill.UnPatch()
	_, err := fetchFieldsAsRecord(context.Background(), []Segment{growing, sealed}, nil, &MergedResultWithOffsets{Selections: []OffsetSelection{{SegmentIndex: 1, Offset: 7}, {SegmentIndex: 0, Offset: 3}}})
	require.ErrorIs(t, err, sentinel)
	syncScore := mockey.Mock(segcore.ComputeScorerScoresOnChunkedOffsets).To(func(s segcore.CSegment, _ *segcore.SearchRequest, _ *planpb.ScoreFunction, _ *arrow.Chunked) (*arrow.Chunked, error) {
		require.Same(t, native, s)
		return nil, sentinel
	}).Build()
	defer syncScore.UnPatch()
	asyncScore := mockey.Mock(segcore.AsyncComputeScorerScoresOnChunkedOffsets).To(func(_ context.Context, s segcore.CSegment, _ *segcore.SearchRequest, _ *planpb.ScoreFunction, _ *arrow.Chunked) (*arrow.Chunked, error) {
		require.Same(t, native, s)
		return nil, sentinel
	}).Build()
	defer asyncScore.UnPatch()
	for _, segment := range []Segment{growing, sealed} {
		_, err := ComputeScorerScoresOnChunkedOffsets(context.Background(), segment, nil, nil, nil)
		require.ErrorIs(t, err, sentinel)
		_, err = AsyncComputeScorerScoresOnChunkedOffsets(context.Background(), segment, nil, nil, nil)
		require.ErrorIs(t, err, sentinel)
		segment.Release(context.Background()) // Must not call native Release.
		require.NoError(t, segment.PinIfNotReleased())
	}
	require.Equal(t, 2, syncScore.Times())
	require.Equal(t, 2, asyncScore.Times())
}

func (*adapterNativeSegment) RowNum() int64         { panic("mockey") }
func (*adapterNativeSegment) MemSize() int64        { panic("mockey") }
func (*adapterNativeSegment) HasRawData(int64) bool { panic("mockey") }
func (*adapterNativeSegment) Search(context.Context, *segcore.SearchRequest) (*segcore.SearchResult, error) {
	panic("mockey")
}

func (*adapterNativeSegment) Retrieve(context.Context, *segcore.RetrievePlan) (*segcore.RetrieveResult, error) {
	panic("mockey")
}

func (*adapterNativeSegment) RetrieveByOffsets(context.Context, *segcore.RetrievePlanWithOffsets) (*segcore.RetrieveResult, error) {
	panic("mockey")
}

func TestViewQueryAdapterReadResultOwnership(t *testing.T) {
	native := &adapterNativeSegment{}
	result := &segcore.RetrieveResult{}
	payload := &segcorepb.RetrieveResults{}
	failure := merr.WrapErrServiceInternalMsg("read failed")
	currentError := failure
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*adapterNativeSegment).Retrieve).To(func(*adapterNativeSegment, context.Context, *segcore.RetrievePlan) (*segcore.RetrieveResult, error) {
			return result, currentError
		}).Build(),
		mockey.Mock((*adapterNativeSegment).RetrieveByOffsets).To(func(*adapterNativeSegment, context.Context, *segcore.RetrievePlanWithOffsets) (*segcore.RetrieveResult, error) {
			return result, currentError
		}).Build(),
		mockey.Mock((*adapterNativeSegment).Search).Return(nil, failure).Build(),
		mockey.Mock((*segcore.RetrieveResult).GetResult).Return(payload, nil).Build(),
	} {
		p := patch
		t.Cleanup(func() { p.UnPatch() })
	}
	released := mockey.Mock((*segcore.RetrieveResult).Release).Return().Build()
	defer released.UnPatch()
	for _, segment := range []Segment{
		NewGrowingSegmentForViewQuery(ViewQueryGrowingSegmentInfo{}, native),
		NewSealedSegmentForViewQuery(&querypb.SegmentLoadInfo{}, native, "db"),
	} {
		currentError = failure
		_, err := segment.Search(context.Background(), nil)
		require.ErrorIs(t, err, failure)
		_, err = segment.Retrieve(context.Background(), nil)
		require.ErrorIs(t, err, failure)
		_, err = segment.RetrieveByOffsets(context.Background(), nil)
		require.ErrorIs(t, err, failure)
		currentError = nil
		got, err := segment.Retrieve(context.Background(), nil)
		require.NoError(t, err)
		require.Same(t, payload, got)
		got, err = segment.RetrieveByOffsets(context.Background(), nil)
		require.NoError(t, err)
		require.Same(t, payload, got)
	}
	require.Equal(t, 4, released.Times(), "each successful native retrieve result must be released exactly once")
	missing := NewGrowingSegmentForViewQuery(ViewQueryGrowingSegmentInfo{}, nil)
	_, err := missing.Search(context.Background(), nil)
	require.Error(t, err)
	_, err = missing.Retrieve(context.Background(), nil)
	require.Error(t, err)
	_, err = missing.RetrieveByOffsets(context.Background(), nil)
	require.Error(t, err)
	require.Zero(t, missing.RowNum())
	require.Zero(t, missing.MemSize())
}

func TestViewQueryAdapterCannotOwnOrMutateNativeSegment(t *testing.T) {
	native := &adapterNativeSegment{}
	for _, patch := range []*mockey.Mocker{
		mockey.Mock((*adapterNativeSegment).RowNum).Return(int64(5)).Build(),
		mockey.Mock((*adapterNativeSegment).MemSize).Return(int64(20)).Build(),
		mockey.Mock((*adapterNativeSegment).HasRawData).Return(true).Build(),
	} {
		p := patch
		t.Cleanup(func() { p.UnPatch() })
	}
	segment := NewGrowingSegmentForViewQuery(ViewQueryGrowingSegmentInfo{CollectionID: 1, PartitionID: 2, VChannel: "v1"}, native)
	ctx := context.Background()
	for _, err := range []error{
		segment.Insert(ctx, nil, nil, nil),
		segment.Delete(ctx, nil, nil),
		segment.LoadDeltaData(ctx, nil),
		segment.Reopen(ctx, nil),
	} {
		require.Error(t, err)
	}
	_, err := segment.FlushData(ctx, 0, 0, nil)
	require.Error(t, err)
	require.NoError(t, segment.Load(ctx))
	segment.Release(ctx)
	require.NoError(t, segment.PinIfNotReleased())
	segment.Unpin()
	require.EqualValues(t, 5, segment.InsertCount(), "wrapper release must not affect the owner")
	require.EqualValues(t, 20, segment.ResourceUsageEstimate().MemorySize)
	require.True(t, segment.HasRawData(100))
	require.EqualValues(t, 1, segment.Collection())
	require.EqualValues(t, 2, segment.Partition())
	require.EqualValues(t, 10, segment.LoadInfo().SegmentID)
	require.Equal(t, "v1", segment.LoadInfo().InsertChannel)
	require.Equal(t, SegmentTypeGrowing, segment.Type())
	require.False(t, segment.IsSorted())
	require.Equal(t, datapb.SegmentLevel_L1, segment.Level())
	require.Empty(t, segment.DatabaseName())
	require.Empty(t, segment.ResourceGroup())
	require.Empty(t, segment.Shard())
	require.Zero(t, segment.Version())
	require.False(t, segment.CASVersion(0, 1))
	require.Nil(t, segment.StartPosition())
	require.Zero(t, segment.LastDeltaTimestamp())
	require.False(t, segment.ExistIndex(100))
	require.Empty(t, segment.Indexes())
	require.Empty(t, segment.GetIndex(100))
	require.Nil(t, segment.GetIndexByID(10))
	require.Nil(t, segment.Stats())
	require.Nil(t, segment.GetMinPk())
	require.Nil(t, segment.GetMaxPk())
	require.Nil(t, segment.GetBM25Stats())
	require.False(t, segment.IsLazyLoad())
	require.Zero(t, segment.NeedUpdatedVersion())
	require.Empty(t, segment.GetFieldJSONIndexStats())
	require.False(t, segment.PkCandidateExist())
	require.True(t, segment.MayPkExist(nil), "a borrowed wrapper cannot exclude deletes using an absent Bloom filter")
	require.Nil(t, segment.BatchPkExist(nil))
	require.Equal(t, []bool{true, true}, segment.BatchPkExist(storage.NewBatchLocationsCache([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1), storage.NewInt64PrimaryKey(2)})))
	sealed := NewSealedSegmentForViewQuery(&querypb.SegmentLoadInfo{IsSorted: true, Level: datapb.SegmentLevel_L2, InsertChannel: "by-dev-rootcoord-dml_0_100v0"}, native, "db")
	require.True(t, sealed.IsSorted())
	require.Equal(t, datapb.SegmentLevel_L2, sealed.Level())
	require.NotEmpty(t, sealed.Shard())
	require.NotNil(t, sealed.LoadInfo())
}

func TestViewQueryCollectionRejectsMissingOwner(t *testing.T) {
	collection, err := NewCollectionFromCCollectionForViewQuery(nil)
	require.Error(t, err)
	require.Nil(t, collection)
}
