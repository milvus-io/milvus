package datacoord

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
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
			8,
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
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			s.v.earliestGrowingSegmentPos.Timestamp = test.prepEarliestT
			for _, view := range s.v.GetSegmentsView() {
				if view.dmlPos.Timestamp < test.prepEarliestT {
					view.DeltalogCount = test.prepCountEach
					view.DeltaSize = test.prepSizeEach
				}
			}
			log.Info("LevelZeroSegmentsView", zap.String("view", s.v.String()))

			gotView := s.v.Trigger()
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
			}
		})
	}
}
