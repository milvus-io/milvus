package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestForceMergeCompactionPolicySuite(t *testing.T) {
	suite.Run(t, new(ForceMergeCompactionPolicySuite))
}

type ForceMergeCompactionPolicySuite struct {
	suite.Suite

	mockAlloc   *allocator.MockAllocator
	mockHandler *NMockHandler
	mockQuerier *MockCollectionTopologyQuerier
	testLabel   *CompactionGroupLabel

	policy *forceMergeCompactionPolicy
}

func (s *ForceMergeCompactionPolicySuite) SetupTest() {
	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	segments := genSegmentsForMeta(s.testLabel)
	meta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}

	s.mockAlloc = allocator.NewMockAllocator(s.T())
	s.mockHandler = NewNMockHandler(s.T())
	s.mockQuerier = NewMockCollectionTopologyQuerier(s.T())

	s.policy = newForceMergeCompactionPolicy(meta, s.mockAlloc, s.mockHandler)
	s.policy.SetTopologyQuerier(s.mockQuerier)
}

func (s *ForceMergeCompactionPolicySuite) TestNewForceMergeCompactionPolicy() {
	meta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	policy := newForceMergeCompactionPolicy(meta, s.mockAlloc, s.mockHandler)

	s.NotNil(policy)
	s.Equal(meta, policy.meta)
	s.Equal(s.mockAlloc, policy.allocator)
	s.Equal(s.mockHandler, policy.handler)
	s.Nil(policy.topologyQuerier)
}

func (s *ForceMergeCompactionPolicySuite) TestSetTopologyQuerier() {
	policy := newForceMergeCompactionPolicy(s.policy.meta, s.mockAlloc, s.mockHandler)
	s.Nil(policy.topologyQuerier)

	policy.SetTopologyQuerier(s.mockQuerier)
	s.Equal(s.mockQuerier, policy.topologyQuerier)
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_Success() {
	ctx := context.Background()
	collectionID := int64(1)
	targetSize := int64(1024 * 1024 * 1024 * 2)
	triggerID := int64(100)

	coll := &collectionInfo{
		ID:         collectionID,
		Schema:     newTestSchema(),
		Properties: nil,
	}

	topology := &CollectionTopology{
		CollectionID: collectionID,
		NumReplicas:  1,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(triggerID, nil)
	s.mockQuerier.EXPECT().GetCollectionTopology(mock.Anything, collectionID).Return(topology, nil)

	views, gotTriggerID, err := s.policy.triggerOneCollection(ctx, collectionID, targetSize)

	s.NoError(err)
	s.Equal(triggerID, gotTriggerID)
	s.NotNil(views)
	s.Greater(len(views), 0)

	for _, view := range views {
		s.NotNil(view)
		s.NotNil(view.GetGroupLabel())
	}
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_GetCollectionError() {
	ctx := context.Background()
	collectionID := int64(999)
	targetSize := int64(1024 * 1024 * 512)

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(nil, merr.ErrCollectionNotFound)

	views, triggerID, err := s.policy.triggerOneCollection(ctx, collectionID, targetSize)

	s.Error(err)
	s.Nil(views)
	s.Equal(int64(0), triggerID)
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_AllocIDError() {
	ctx := context.Background()
	collectionID := int64(1)
	targetSize := int64(1024 * 1024 * 1024 * 2)

	coll := &collectionInfo{
		ID:         collectionID,
		Schema:     newTestSchema(),
		Properties: nil,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(0), merr.ErrServiceUnavailable)

	views, triggerID, err := s.policy.triggerOneCollection(ctx, collectionID, targetSize)

	s.Error(err)
	s.Nil(views)
	s.Equal(int64(0), triggerID)
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_NoEligibleSegments() {
	ctx := context.Background()
	collectionID := int64(999)
	targetSize := int64(1024 * 1024 * 1024 * 2)
	triggerID := int64(100)

	coll := &collectionInfo{
		ID:         collectionID,
		Schema:     newTestSchema(),
		Properties: nil,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(triggerID, nil)

	views, gotTriggerID, err := s.policy.triggerOneCollection(ctx, collectionID, targetSize)

	s.NoError(err)
	s.Equal(int64(0), gotTriggerID)
	s.Nil(views)
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_TopologyError() {
	ctx := context.Background()
	collectionID := int64(1)
	targetSize := int64(1024 * 1024 * 1024 * 2)
	triggerID := int64(100)

	coll := &collectionInfo{
		ID:         collectionID,
		Schema:     newTestSchema(),
		Properties: nil,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(triggerID, nil)
	s.mockQuerier.EXPECT().GetCollectionTopology(mock.Anything, collectionID).Return(nil, merr.ErrServiceUnavailable)

	views, gotTriggerID, err := s.policy.triggerOneCollection(ctx, collectionID, targetSize)

	s.Error(err)
	s.Nil(views)
	s.Equal(int64(0), gotTriggerID)
}

func (s *ForceMergeCompactionPolicySuite) TestTriggerOneCollection_WithCollectionProperties() {
	ctx := context.Background()
	collectionID := int64(1)
	targetSize := int64(1024 * 1024 * 1024 * 2)
	triggerID := int64(100)

	coll := &collectionInfo{
		ID:     collectionID,
		Schema: newTestSchema(),
		Properties: map[string]string{
			"collection.ttl.seconds": "86400",
		},
	}

	topology := &CollectionTopology{
		CollectionID: collectionID,
		NumReplicas:  2,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(triggerID, nil)
	s.mockQuerier.EXPECT().GetCollectionTopology(mock.Anything, collectionID).Return(topology, nil)

	views, gotTriggerID, err := s.policy.triggerOneCollection(ctx, collectionID, targetSize)

	s.NoError(err)
	s.Equal(triggerID, gotTriggerID)
	s.NotNil(views)
}

func (s *ForceMergeCompactionPolicySuite) TestGroupByPartitionChannel_EmptySegments() {
	segments := []*SegmentView{}
	result := groupByPartitionChannel(segments)

	s.NotNil(result)
	s.Equal(0, len(result))
}

func (s *ForceMergeCompactionPolicySuite) TestGroupByPartitionChannel_SingleGroup() {
	segmentInfo := genTestSegmentInfo(s.testLabel, 100, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)
	segments := GetViewsByInfo(segmentInfo)

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
	label1 := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}
	label2 := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  11,
		Channel:      "ch-1",
	}
	label3 := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-2",
	}

	seg1 := genTestSegmentInfo(label1, 100, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)
	seg2 := genTestSegmentInfo(label2, 101, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)
	seg3 := genTestSegmentInfo(label3, 102, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)

	segments := GetViewsByInfo(seg1, seg2, seg3)
	result := groupByPartitionChannel(segments)

	s.NotNil(result)
	s.Equal(3, len(result))
}

func (s *ForceMergeCompactionPolicySuite) TestGroupByPartitionChannel_SameGroupMultipleSegments() {
	seg1 := genTestSegmentInfo(s.testLabel, 100, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)
	seg2 := genTestSegmentInfo(s.testLabel, 101, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)
	seg3 := genTestSegmentInfo(s.testLabel, 102, datapb.SegmentLevel_L1, commonpb.SegmentState_Flushed)

	segments := GetViewsByInfo(seg1, seg2, seg3)
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
	targetSize := int64(1024 * 1024 * 1024 * 2)
	triggerID := int64(100)

	coll := &collectionInfo{
		ID:         collectionID,
		Schema:     newTestSchema(),
		Properties: nil,
	}

	topology := &CollectionTopology{
		CollectionID: collectionID,
		NumReplicas:  1,
	}

	s.mockHandler.EXPECT().GetCollection(mock.Anything, collectionID).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(triggerID, nil)
	s.mockQuerier.EXPECT().GetCollectionTopology(mock.Anything, collectionID).Return(topology, nil)

	views, gotTriggerID, err := s.policy.triggerOneCollection(ctx, collectionID, targetSize)

	s.NoError(err)
	s.Equal(triggerID, gotTriggerID)
	s.NotNil(views)

	for _, view := range views {
		for _, seg := range view.GetSegmentsView() {
			s.NotEqual(datapb.SegmentLevel_L0, seg.Level)
			s.Equal(commonpb.SegmentState_Flushed, seg.State)
		}
	}
}
