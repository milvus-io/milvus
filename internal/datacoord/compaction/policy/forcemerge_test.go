package policy

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestForceMergeCompactionPolicySuite(t *testing.T) {
	suite.Run(t, new(ForceMergeCompactionPolicySuite))
}

type ForceMergeCompactionPolicySuite struct {
	suite.Suite

	mockAlloc   *allocator.MockAllocator
	mockHandler *meta.NMockHandler
	mockQuerier *view.MockCollectionTopologyQuerier
	testLabel   *view.CompactionGroupLabel

	policy *ForceMergeCompactionPolicy
}

func (s *ForceMergeCompactionPolicySuite) SetupTest() {
	s.testLabel = &view.CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	segments := GenSegmentsForMeta(s.testLabel)
	mt, dm, err := index.NewMemoryMetaWithIndex(s.T())
	s.Require().NoError(err)
	for id, segment := range segments {
		if segment.GetLevel() != datapb.SegmentLevel_L0 && segment.GetState() == commonpb.SegmentState_Flushed {
			segment.IsSorted = true
		}
		mt.Segments.SetSegment(id, segment)
	}

	s.mockAlloc = allocator.NewMockAllocator(s.T())
	s.mockHandler = meta.NewNMockHandler(s.T())
	s.mockQuerier = view.NewMockCollectionTopologyQuerier(s.T())

	s.policy = NewForceMergeCompactionPolicy(mt, dm.IndexMeta, s.mockAlloc, s.mockHandler)
	s.policy.SetTopologyQuerier(s.mockQuerier)
}

func (s *ForceMergeCompactionPolicySuite) TestNewForceMergeCompactionPolicy() {
	mt, dm, err := index.NewMemoryMetaWithIndex(s.T())
	s.Require().NoError(err)
	policy := NewForceMergeCompactionPolicy(mt, dm.IndexMeta, s.mockAlloc, s.mockHandler)

	s.NotNil(policy)
	s.Equal(mt, policy.meta)
	s.Equal(s.mockAlloc, policy.allocator)
	s.Equal(s.mockHandler, policy.handler)
	s.Nil(policy.topologyQuerier)
}

func (s *ForceMergeCompactionPolicySuite) TestSetTopologyQuerier() {
	policy := NewForceMergeCompactionPolicy(s.policy.meta, s.policy.IndexMeta, s.mockAlloc, s.mockHandler)
	s.Nil(policy.topologyQuerier)

	policy.SetTopologyQuerier(s.mockQuerier)
	s.Equal(s.mockQuerier, policy.topologyQuerier)
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_Success() {
	ctx := context.Background()
	collectionID := int64(1)
	targetSize := int64(1024 * 2) // 2GB in MB
	triggerID := int64(100)

	coll := &meta.CollectionInfo{
		ID:         collectionID,
		Schema:     meta.NewTestSchema(),
		Properties: nil,
	}

	topology := &view.CollectionTopology{
		CollectionID: collectionID,
		NumReplicas:  1,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(triggerID, nil)
	s.mockQuerier.EXPECT().GetCollectionTopology(mock.Anything, collectionID).Return(topology, nil)

	views, gotTriggerID, err := s.policy.TriggerOneCollection(ctx, collectionID, targetSize)

	s.NoError(err)
	s.Equal(triggerID, gotTriggerID)
	s.NotNil(views)
	s.Greater(len(views), 0)

	for _, cv := range views {
		s.NotNil(cv)
		s.NotNil(cv.GetGroupLabel())
	}
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_GetCollectionError() {
	ctx := context.Background()
	collectionID := int64(999)
	targetSize := int64(1024 * 2) // 2GB in MB

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(nil, merr.ErrCollectionNotFound)

	views, triggerID, err := s.policy.TriggerOneCollection(ctx, collectionID, targetSize)

	s.Error(err)
	s.Nil(views)
	s.Equal(int64(0), triggerID)
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_AllocIDError() {
	ctx := context.Background()
	collectionID := int64(1)
	targetSize := int64(1024 * 2) // 2GB in MB

	coll := &meta.CollectionInfo{
		ID:         collectionID,
		Schema:     meta.NewTestSchema(),
		Properties: nil,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(0), merr.ErrServiceUnavailable)

	views, triggerID, err := s.policy.TriggerOneCollection(ctx, collectionID, targetSize)

	s.Error(err)
	s.Nil(views)
	s.Equal(int64(0), triggerID)
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_NoEligibleSegments() {
	ctx := context.Background()
	collectionID := int64(999)
	targetSize := int64(1024 * 2) // 2GB in MB
	triggerID := int64(100)

	coll := &meta.CollectionInfo{
		ID:         collectionID,
		Schema:     meta.NewTestSchema(),
		Properties: nil,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(triggerID, nil)

	views, gotTriggerID, err := s.policy.TriggerOneCollection(ctx, collectionID, targetSize)

	s.NoError(err)
	s.Equal(int64(0), gotTriggerID)
	s.Nil(views)
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_TopologyError() {
	ctx := context.Background()
	collectionID := int64(1)
	targetSize := int64(1024 * 2) // 2GB in MB
	triggerID := int64(100)

	coll := &meta.CollectionInfo{
		ID:         collectionID,
		Schema:     meta.NewTestSchema(),
		Properties: nil,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(triggerID, nil)
	s.mockQuerier.EXPECT().GetCollectionTopology(mock.Anything, collectionID).Return(nil, merr.ErrServiceUnavailable)

	views, gotTriggerID, err := s.policy.TriggerOneCollection(ctx, collectionID, targetSize)

	s.Error(err)
	s.Nil(views)
	s.Equal(int64(0), gotTriggerID)
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_WithCollectionProperties() {
	ctx := context.Background()
	collectionID := int64(1)
	targetSize := int64(1024 * 2) // 2GB in MB
	triggerID := int64(100)

	coll := &meta.CollectionInfo{
		ID:     collectionID,
		Schema: meta.NewTestSchema(),
		Properties: map[string]string{
			"collection.ttl.seconds": "86400",
		},
	}

	topology := &view.CollectionTopology{
		CollectionID: collectionID,
		NumReplicas:  2,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(triggerID, nil)
	s.mockQuerier.EXPECT().GetCollectionTopology(mock.Anything, collectionID).Return(topology, nil)

	views, gotTriggerID, err := s.policy.TriggerOneCollection(ctx, collectionID, targetSize)

	s.NoError(err)
	s.Equal(triggerID, gotTriggerID)
	s.NotNil(views)
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_TargetSizeTooSmall() {
	ctx := context.Background()
	collectionID := int64(1)
	// Default configMaxSize is 1024 MB (SegmentMaxSize default), so 512 MB should fail
	targetSize := int64(512) // 512 MB, smaller than default configMaxSize (1024 MB)

	coll := &meta.CollectionInfo{
		ID:     collectionID,
		Schema: meta.NewTestSchema(),
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(100), nil)

	views, triggerID, err := s.policy.TriggerOneCollection(ctx, collectionID, targetSize)

	s.Error(err)
	s.Contains(err.Error(), "should be greater than or equal to")
	s.Nil(views)
	s.Equal(int64(0), triggerID)
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_AutoCalculateMode() {
	// Test that max_int64 (auto-calculate mode) doesn't cause overflow
	ctx := context.Background()
	collectionID := int64(1)
	targetSize := int64(math.MaxInt64) // Auto-calculate mode
	triggerID := int64(100)

	coll := &meta.CollectionInfo{
		ID:         collectionID,
		Schema:     meta.NewTestSchema(),
		Properties: nil,
	}

	topology := &view.CollectionTopology{
		CollectionID: collectionID,
		NumReplicas:  1,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(triggerID, nil)
	s.mockQuerier.EXPECT().GetCollectionTopology(mock.Anything, collectionID).Return(topology, nil)

	views, gotTriggerID, err := s.policy.TriggerOneCollection(ctx, collectionID, targetSize)

	// Should not overflow and should succeed
	s.NoError(err)
	s.Equal(triggerID, gotTriggerID)
	s.NotNil(views)
	s.Greater(len(views), 0)
}

func (s *ForceMergeCompactionPolicySuite) TestGroupByPartitionChannel_EmptySegments() {
	segments := []*meta.SegmentInfo{}
	result := groupByPartitionChannel(segments)

	s.NotNil(result)
	s.Equal(0, len(result))
}

func (s *ForceMergeCompactionPolicySuite) TestGroupByPartitionChannel_SingleGroup() {
	segmentInfo := GenTestSegmentInfo(s.testLabel, 100, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)
	segments := []*meta.SegmentInfo{segmentInfo}

	result := groupByPartitionChannel(segments)

	s.NotNil(result)
	s.Equal(1, len(result))

	for label, segs := range result {
		s.Equal(s.testLabel.CollectionID, label.CollectionID)
		s.Equal(s.testLabel.PartitionID, label.PartitionID)
		s.Equal(s.testLabel.Channel, label.Channel)
		s.Equal(1, len(segs))
	}
}

func (s *ForceMergeCompactionPolicySuite) TestGroupByPartitionChannel_MultipleGroups() {
	label1 := &view.CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}
	label2 := &view.CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  11,
		Channel:      "ch-1",
	}
	label3 := &view.CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-2",
	}

	seg1 := GenTestSegmentInfo(label1, 100, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)
	seg2 := GenTestSegmentInfo(label2, 101, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)
	seg3 := GenTestSegmentInfo(label3, 102, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)

	segments := []*meta.SegmentInfo{seg1, seg2, seg3}
	result := groupByPartitionChannel(segments)

	s.NotNil(result)
	s.Equal(3, len(result))
}

func (s *ForceMergeCompactionPolicySuite) TestGroupByPartitionChannel_SameGroupMultipleSegments() {
	seg1 := GenTestSegmentInfo(s.testLabel, 100, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)
	seg2 := GenTestSegmentInfo(s.testLabel, 101, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)
	seg3 := GenTestSegmentInfo(s.testLabel, 102, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)

	segments := []*meta.SegmentInfo{seg1, seg2, seg3}
	result := groupByPartitionChannel(segments)

	s.NotNil(result)
	s.Equal(1, len(result))

	for label, segs := range result {
		s.Equal(s.testLabel.Key(), label.Key())
		s.Equal(3, len(segs))
	}
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_FilterSegments() {
	ctx := context.Background()
	collectionID := int64(1)
	targetSize := int64(1024 * 2) // 2GB in MB
	triggerID := int64(100)

	coll := &meta.CollectionInfo{
		ID:         collectionID,
		Schema:     meta.NewTestSchema(),
		Properties: nil,
	}

	topology := &view.CollectionTopology{
		CollectionID: collectionID,
		NumReplicas:  1,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(triggerID, nil)
	s.mockQuerier.EXPECT().GetCollectionTopology(mock.Anything, collectionID).Return(topology, nil)

	views, gotTriggerID, err := s.policy.TriggerOneCollection(ctx, collectionID, targetSize)

	s.NoError(err)
	s.Equal(triggerID, gotTriggerID)
	s.NotNil(views)

	for _, cv := range views {
		for _, seg := range cv.GetSegmentsView() {
			s.NotEqual(datapb.SegmentLevel_L0, seg.Level)
			s.Equal(commonpb.SegmentState_Flushed, seg.State)
		}
	}
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_AlignsTargetSizeWithManualCompactionSelection() {
	ctx := context.Background()
	collectionID := int64(1)
	targetSize := int64(1024 * 2) // 2GB in MB
	triggerID := int64(100)

	mt, dm, err := index.NewMemoryMetaWithIndex(s.T())
	s.Require().NoError(err)
	policy := NewForceMergeCompactionPolicy(mt, dm.IndexMeta, s.mockAlloc, s.mockHandler)
	policy.SetTopologyQuerier(s.mockQuerier)

	newSegment := func(id int64, level datapb.SegmentLevel, sorted bool, sortedByNamespace bool, compacting bool) *meta.SegmentInfo {
		return &meta.SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:                  id,
				CollectionID:        collectionID,
				PartitionID:         s.testLabel.PartitionID,
				InsertChannel:       s.testLabel.Channel,
				State:               commonpb.SegmentState_Flushed,
				Level:               level,
				IsSorted:            sorted,
				IsSortedByNamespace: sortedByNamespace,
				Binlogs:             GenTestBinlogs(1, 10*meta.MB),
			},
			IsCompacting: compacting,
		}
	}

	for _, segment := range []*meta.SegmentInfo{
		newSegment(10, datapb.SegmentLevel_L1, true, false, false),
		newSegment(11, datapb.SegmentLevel_L1, false, true, false),
		newSegment(12, datapb.SegmentLevel_L1, false, false, false),
		newSegment(13, datapb.SegmentLevel_L2, true, false, false),
		newSegment(14, datapb.SegmentLevel_L1, true, false, true),
	} {
		mt.Segments.SetSegment(segment.GetID(), segment)
	}

	coll := &meta.CollectionInfo{
		ID:         collectionID,
		Schema:     meta.NewTestSchema(),
		Properties: nil,
	}
	topology := &view.CollectionTopology{
		CollectionID: collectionID,
		NumReplicas:  1,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(triggerID, nil)
	s.mockQuerier.EXPECT().GetCollectionTopology(mock.Anything, collectionID).Return(topology, nil)

	views, gotTriggerID, err := policy.TriggerOneCollection(ctx, collectionID, targetSize)

	s.NoError(err)
	s.Equal(triggerID, gotTriggerID)
	s.Require().Len(views, 1)

	gotSegmentIDs := make([]int64, 0, len(views[0].GetSegmentsView()))
	for _, segment := range views[0].GetSegmentsView() {
		gotSegmentIDs = append(gotSegmentIDs, segment.ID)
	}
	s.ElementsMatch([]int64{10, 11}, gotSegmentIDs)
}
