package segments

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type ManagerSuite struct {
	suite.Suite

	// Data
	segmentIDs    []int64
	collectionIDs []int64
	partitionIDs  []int64
	channels      []string
	types         []SegmentType
	segments      []*LocalSegment

	mgr *segmentManager
}

func (s *ManagerSuite) SetupSuite() {
	paramtable.Init()
	s.segmentIDs = []int64{1, 2, 3}
	s.collectionIDs = []int64{100, 200, 300}
	s.partitionIDs = []int64{10, 11, 12}
	s.channels = []string{"dml1", "dml2", "dml3"}
	s.types = []SegmentType{SegmentTypeSealed, SegmentTypeGrowing, SegmentTypeSealed}
}

func (s *ManagerSuite) SetupTest() {
	s.mgr = NewSegmentManager()

	for i, id := range s.segmentIDs {
		schema := GenTestCollectionSchema("manager-suite", schemapb.DataType_Int64)
		segment, err := NewSegment(
			NewCollection(s.collectionIDs[i], schema, GenTestIndexMeta(s.collectionIDs[i], schema), querypb.LoadType_LoadCollection),
			id,
			s.partitionIDs[i],
			s.collectionIDs[i],
			s.channels[i],
			s.types[i],
			0,
			nil,
			nil,
		)
		s.Require().NoError(err)
		s.segments = append(s.segments, segment)

		s.mgr.Put(s.types[i], segment)
	}
}

func (s *ManagerSuite) TestGetBy() {
	for i, partitionID := range s.partitionIDs {
		segments := s.mgr.GetBy(WithPartition(partitionID))
		s.Contains(segments, s.segments[i])
	}

	for i, channel := range s.channels {
		segments := s.mgr.GetBy(WithChannel(channel))
		s.Contains(segments, s.segments[i])
	}

	for i, typ := range s.types {
		segments := s.mgr.GetBy(WithType(typ))
		s.Contains(segments, s.segments[i])
	}
	s.mgr.Clear()

	for _, typ := range s.types {
		segments := s.mgr.GetBy(WithType(typ))
		s.Len(segments, 0)
	}
}

func (s *ManagerSuite) TestRemoveGrowing() {
	for i, id := range s.segmentIDs {
		isGrowing := s.types[i] == SegmentTypeGrowing

		s.mgr.Remove(id, querypb.DataScope_Streaming)
		s.Equal(s.mgr.Get(id) == nil, isGrowing)
	}
}

func (s *ManagerSuite) TestRemoveSealed() {
	for i, id := range s.segmentIDs {
		isSealed := s.types[i] == SegmentTypeSealed

		s.mgr.Remove(id, querypb.DataScope_Historical)
		s.Equal(s.mgr.Get(id) == nil, isSealed)
	}
}

func (s *ManagerSuite) TestRemoveAll() {
	for _, id := range s.segmentIDs {
		s.mgr.Remove(id, querypb.DataScope_All)
		s.Nil(s.mgr.Get(id))
	}
}

func (s *ManagerSuite) TestRemoveBy() {
	for _, id := range s.segmentIDs {
		s.mgr.RemoveBy(WithID(id))
		s.Nil(s.mgr.Get(id))
	}
}

func (s *ManagerSuite) TestUpdateBy() {
	action := IncreaseVersion(1)

	s.Equal(2, s.mgr.UpdateSegmentBy(action, WithType(SegmentTypeSealed)))
	s.Equal(1, s.mgr.UpdateSegmentBy(action, WithType(SegmentTypeGrowing)))

	segments := s.mgr.GetBy()
	for _, segment := range segments {
		s.Equal(int64(1), segment.Version())
	}
}

func (s *ManagerSuite) TestIncreaseVersion() {
	action := IncreaseVersion(1)

	segment := NewMockSegment(s.T())
	segment.EXPECT().ID().Return(100)
	segment.EXPECT().Type().Return(commonpb.SegmentState_Sealed)
	segment.EXPECT().Version().Return(1)

	s.False(action(segment), "version already gte version")
	segment.AssertExpectations(s.T())

	segment = NewMockSegment(s.T())
	segment.EXPECT().ID().Return(100)
	segment.EXPECT().Type().Return(commonpb.SegmentState_Sealed)
	segment.EXPECT().Version().Return(0)
	segment.EXPECT().CASVersion(int64(0), int64(1)).Return(true)

	s.True(action(segment), "version lt execute CAS")
	segment.AssertExpectations(s.T())
}

func TestManager(t *testing.T) {
	suite.Run(t, new(ManagerSuite))
}
