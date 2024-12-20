package segments

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ManagerSuite struct {
	suite.Suite

	// Data
	segmentIDs    []int64
	collectionIDs []int64
	partitionIDs  []int64
	channels      []string
	types         []SegmentType
	segments      []Segment
	levels        []datapb.SegmentLevel

	mgr *segmentManager
}

func (s *ManagerSuite) SetupSuite() {
	paramtable.Init()
	s.segmentIDs = []int64{1, 2, 3, 4}
	s.collectionIDs = []int64{100, 200, 300, 400}
	s.partitionIDs = []int64{10, 11, 12, 13}
	s.channels = []string{"by-dev-rootcoord-dml_0_100v0", "by-dev-rootcoord-dml_1_200v0", "by-dev-rootcoord-dml_2_300v0", "by-dev-rootcoord-dml_3_400v0"}
	s.types = []SegmentType{SegmentTypeSealed, SegmentTypeGrowing, SegmentTypeSealed, SegmentTypeSealed}
	s.levels = []datapb.SegmentLevel{datapb.SegmentLevel_Legacy, datapb.SegmentLevel_Legacy, datapb.SegmentLevel_L1, datapb.SegmentLevel_L0}
	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	initcore.InitLocalChunkManager(typeutil.QueryNodeRole, localDataRootPath)
	initcore.InitMmapManager(paramtable.Get())
}

func (s *ManagerSuite) SetupTest() {
	s.mgr = NewSegmentManager()
	s.segments = nil

	for i, id := range s.segmentIDs {
		schema := mock_segcore.GenTestCollectionSchema("manager-suite", schemapb.DataType_Int64, true)
		segment, err := NewSegment(
			context.Background(),
			NewCollection(s.collectionIDs[i], schema, mock_segcore.GenTestIndexMeta(s.collectionIDs[i], schema), &querypb.LoadMetaInfo{
				LoadType: querypb.LoadType_LoadCollection,
			}),
			s.types[i],
			0,
			&querypb.SegmentLoadInfo{
				SegmentID:     id,
				PartitionID:   s.partitionIDs[i],
				CollectionID:  s.collectionIDs[i],
				InsertChannel: s.channels[i],
				Level:         s.levels[i],
			},
			nil,
		)
		s.Require().NoError(err)
		s.segments = append(s.segments, segment)

		s.mgr.Put(context.Background(), s.types[i], segment)
	}
}

func (s *ManagerSuite) TestExist() {
	for _, segment := range s.segments {
		s.True(s.mgr.Exist(segment.ID(), segment.Type()))
		s.mgr.removeSegmentWithType(segment.Type(), segment.ID())
		s.True(s.mgr.Exist(segment.ID(), segment.Type()))
		s.mgr.release(context.Background(), segment)
		s.False(s.mgr.Exist(segment.ID(), segment.Type()))
	}

	s.False(s.mgr.Exist(10086, SegmentTypeGrowing))
	s.False(s.mgr.Exist(10086, SegmentTypeSealed))
}

func (s *ManagerSuite) TestGetBy() {
	for i, partitionID := range s.partitionIDs {
		segments := s.mgr.GetBy(WithPartition(partitionID))
		s.Contains(
			lo.Map(segments, func(segment Segment, _ int) int64 { return segment.ID() }), s.segmentIDs[i])
	}

	for i, channel := range s.channels {
		segments := s.mgr.GetBy(WithChannel(channel))
		s.Contains(lo.Map(segments, func(segment Segment, _ int) int64 { return segment.ID() }), s.segmentIDs[i])
	}

	for i, typ := range s.types {
		segments := s.mgr.GetBy(WithType(typ))
		s.Contains(lo.Map(segments, func(segment Segment, _ int) int64 { return segment.ID() }), s.segmentIDs[i])
	}
	s.mgr.Clear(context.Background())

	for _, typ := range s.types {
		segments := s.mgr.GetBy(WithType(typ))
		s.Len(segments, 0)
	}
}

func (s *ManagerSuite) TestGetAndPin() {
	// get and pin will ignore L0 segment
	segments, err := s.mgr.GetAndPin(lo.Filter(s.segmentIDs, func(_ int64, id int) bool { return s.levels[id] == datapb.SegmentLevel_L0 }))
	s.NoError(err)
	s.Equal(len(segments), 0)
}

func (s *ManagerSuite) TestRemoveGrowing() {
	for i, id := range s.segmentIDs {
		isGrowing := s.types[i] == SegmentTypeGrowing

		s.mgr.Remove(context.Background(), id, querypb.DataScope_Streaming)
		s.Equal(s.mgr.Get(id) == nil, isGrowing)
	}
}

func (s *ManagerSuite) TestRemoveSealed() {
	for i, id := range s.segmentIDs {
		isSealed := s.types[i] == SegmentTypeSealed

		s.mgr.Remove(context.Background(), id, querypb.DataScope_Historical)
		s.Equal(s.mgr.Get(id) == nil, isSealed)
	}
}

func (s *ManagerSuite) TestRemoveAll() {
	for _, id := range s.segmentIDs {
		s.mgr.Remove(context.Background(), id, querypb.DataScope_All)
		s.Nil(s.mgr.Get(id))
	}
}

func (s *ManagerSuite) TestRemoveBy() {
	for _, id := range s.segmentIDs {
		s.mgr.RemoveBy(context.Background(), WithID(id))
		s.Nil(s.mgr.Get(id))
	}
}

func (s *ManagerSuite) TestUpdateBy() {
	action := IncreaseVersion(1)

	s.Equal(lo.Count(s.types, SegmentTypeSealed), s.mgr.UpdateBy(action, WithType(SegmentTypeSealed)))
	s.Equal(lo.Count(s.types, SegmentTypeGrowing), s.mgr.UpdateBy(action, WithType(SegmentTypeGrowing)))

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
