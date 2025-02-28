package datacoord

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

func TestLevelZeroSegmentsViewSuite(t *testing.T) {
	suite.Run(t, new(LevelZeroSegmentsViewSuite))
}

type LevelZeroSegmentsViewSuite struct {
	suite.Suite
	v *LevelZeroSegmentsView
}

func genTestL0SegmentView(ID UniqueID, label *CompactionGroupLabel, posTime Timestamp) *SegmentView {
	return &SegmentView{
		ID:     ID,
		label:  label,
		dmlPos: &msgpb.MsgPosition{Timestamp: posTime},
		Level:  datapb.SegmentLevel_L0,
		State:  commonpb.SegmentState_Flushed,
	}
}

func (s *LevelZeroSegmentsViewSuite) SetupTest() {
	label := &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}
	segments := []*SegmentView{
		genTestL0SegmentView(100, label, 10000),
		genTestL0SegmentView(101, label, 10000),
		genTestL0SegmentView(102, label, 10000),
	}

	targetView := &LevelZeroSegmentsView{
		label, segments, &msgpb.MsgPosition{Timestamp: 10000},
	}

	s.True(label.Equal(targetView.GetGroupLabel()))
	log.Info("LevelZeroSegmentsView", zap.String("view", targetView.String()))

	s.v = targetView
}

func (s *LevelZeroSegmentsViewSuite) TestEqual() {
	label := s.v.GetGroupLabel()

	tests := []struct {
		description string

		input  []*SegmentView
		output bool
	}{
		{"Different segment numbers", []*SegmentView{genTestL0SegmentView(100, label, 10000)}, false},
		{"Same number, diff segmentIDs", []*SegmentView{
			genTestL0SegmentView(100, label, 10000),
			genTestL0SegmentView(101, label, 10000),
			genTestL0SegmentView(200, label, 10000),
		}, false},
		{"Same", s.v.GetSegmentsView(), true},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			got := s.v.Equal(test.input)
			s.Equal(test.output, got)
		})
	}
}

func (s *LevelZeroSegmentsViewSuite) TestTrigger() {
	label := s.v.GetGroupLabel()
	views := []*SegmentView{
		genTestL0SegmentView(100, label, 20000),
		genTestL0SegmentView(101, label, 10000),
		genTestL0SegmentView(102, label, 30000),
		genTestL0SegmentView(103, label, 40000),
	}

	s.v.segments = views
	tests := []struct {
		description string

		prepSizeEach  float64
		prepCountEach int
		prepEarliestT Timestamp

		expectedSegs []UniqueID
	}{
		{
			"No valid segments by earliest growing segment pos",
			64,
			20,
			10000,
			nil,
		},
		{
			"Not qualified",
			1,
			1,
			30000,
			nil,
		},
		{
			"Trigger by > TriggerDeltaSize",
			8 * 1024 * 1024,
			1,
			30000,
			[]UniqueID{100, 101},
		},
		{
			"Trigger by > TriggerDeltaCount",
			1,
			10,
			30000,
			[]UniqueID{100, 101},
		},
		{
			"Trigger by > maxDeltaSize",
			128 * 1024 * 1024,
			1,
			30000,
			[]UniqueID{100},
		},
		{
			"Trigger by > maxDeltaCount",
			1,
			24,
			30000,
			[]UniqueID{100},
		},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			s.v.earliestGrowingSegmentPos.Timestamp = test.prepEarliestT
			for _, view := range s.v.GetSegmentsView() {
				if view.dmlPos.Timestamp < test.prepEarliestT {
					view.DeltalogCount = test.prepCountEach
					view.DeltaSize = test.prepSizeEach
					view.DeltaRowCount = 1
				}
			}
			log.Info("LevelZeroSegmentsView", zap.String("view", s.v.String()))

			gotView, reason := s.v.Trigger()
			if len(test.expectedSegs) == 0 {
				s.Nil(gotView)
			} else {
				levelZeroView, ok := gotView.(*LevelZeroSegmentsView)
				s.True(ok)
				s.NotNil(levelZeroView)

				gotSegIDs := lo.Map(levelZeroView.GetSegmentsView(), func(v *SegmentView, _ int) int64 {
					return v.ID
				})
				s.ElementsMatch(gotSegIDs, test.expectedSegs)
				log.Info("output view", zap.String("view", levelZeroView.String()), zap.String("trigger reason", reason))
			}
		})
	}
}

func (s *LevelZeroSegmentsViewSuite) TestMinCountSizeTrigger() {
	label := s.v.GetGroupLabel()
	tests := []struct {
		description string
		segIDs      []int64
		segCounts   []int
		segSize     []float64

		expectedIDs []int64
	}{
		{"donot trigger", []int64{100, 101, 102}, []int{1, 1, 1}, []float64{1, 1, 1}, nil},
		{"trigger by count=15", []int64{100, 101, 102}, []int{5, 5, 5}, []float64{1, 1, 1}, []int64{100, 101, 102}},
		{"trigger by count=10", []int64{100, 101, 102}, []int{5, 3, 2}, []float64{1, 1, 1}, []int64{100, 101, 102}},
		{"trigger by count=50", []int64{100, 101, 102}, []int{32, 10, 8}, []float64{1, 1, 1}, []int64{100}},
		{"trigger by size=24MB", []int64{100, 101, 102}, []int{1, 1, 1}, []float64{8 * 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024}, []int64{100, 101, 102}},
		{"trigger by size=8MB", []int64{100, 101, 102}, []int{1, 1, 1}, []float64{3 * 1024 * 1024, 3 * 1024 * 1024, 2 * 1024 * 1024}, []int64{100, 101, 102}},
		{"trigger by size=128MB", []int64{100, 101, 102}, []int{1, 1, 1}, []float64{100 * 1024 * 1024, 20 * 1024 * 1024, 8 * 1024 * 1024}, []int64{100}},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			views := []*SegmentView{}
			for idx, ID := range test.segIDs {
				seg := genTestL0SegmentView(ID, label, 10000)
				seg.DeltaSize = test.segSize[idx]
				seg.DeltalogCount = test.segCounts[idx]

				views = append(views, seg)
			}

			picked, reason := s.v.minCountSizeTrigger(views)
			s.ElementsMatch(lo.Map(picked, func(view *SegmentView, _ int) int64 {
				return view.ID
			}), test.expectedIDs)

			if len(picked) > 0 {
				s.NotEmpty(reason)
			}

			log.Info("test minCountSizeTrigger", zap.Any("trigger reason", reason))
		})
	}
}

func (s *LevelZeroSegmentsViewSuite) TestForceTrigger() {
	label := s.v.GetGroupLabel()
	tests := []struct {
		description string
		segIDs      []int64
		segCounts   []int
		segSize     []float64

		expectedIDs []int64
	}{
		{"force trigger", []int64{100, 101, 102}, []int{1, 1, 1}, []float64{1, 1, 1}, []int64{100, 101, 102}},
		{"trigger by count=15", []int64{100, 101, 102}, []int{5, 5, 5}, []float64{1, 1, 1}, []int64{100, 101, 102}},
		{"trigger by count=10", []int64{100, 101, 102}, []int{5, 3, 2}, []float64{1, 1, 1}, []int64{100, 101, 102}},
		{"trigger by count=50", []int64{100, 101, 102}, []int{32, 10, 8}, []float64{1, 1, 1}, []int64{100}},
		{"trigger by size=24MB", []int64{100, 101, 102}, []int{1, 1, 1}, []float64{8 * 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024}, []int64{100, 101, 102}},
		{"trigger by size=8MB", []int64{100, 101, 102}, []int{1, 1, 1}, []float64{3 * 1024 * 1024, 3 * 1024 * 1024, 2 * 1024 * 1024}, []int64{100, 101, 102}},
		{"trigger by size=128MB", []int64{100, 101, 102}, []int{1, 1, 1}, []float64{100 * 1024 * 1024, 20 * 1024 * 1024, 8 * 1024 * 1024}, []int64{100}},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			views := []*SegmentView{}
			for idx, ID := range test.segIDs {
				seg := genTestL0SegmentView(ID, label, 10000)
				seg.DeltaSize = test.segSize[idx]
				seg.DeltalogCount = test.segCounts[idx]

				views = append(views, seg)
			}

			picked, reason := s.v.forceTrigger(views)
			s.ElementsMatch(lo.Map(picked, func(view *SegmentView, _ int) int64 {
				return view.ID
			}), test.expectedIDs)
			log.Info("test forceTrigger", zap.Any("trigger reason", reason))
		})
	}
}
