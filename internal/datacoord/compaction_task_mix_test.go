package datacoord

import (
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func (s *CompactionTaskSuite) TestProcessRefreshPlan_NormalMix() {
	channel := "Ch-1"
	binLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			Level:         datapb.SegmentLevel_L1,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Binlogs:       binLogs,
		}}
	}).Times(2)
	task := &mixCompactionTask{
		CompactionTask: &datapb.CompactionTask{
			PlanID:         1,
			TriggerID:      19530,
			CollectionID:   1,
			PartitionID:    10,
			Type:           datapb.CompactionType_MixCompaction,
			NodeID:         1,
			State:          datapb.CompactionTaskState_executing,
			InputSegments:  []int64{200, 201},
			ResultSegments: []int64{100, 200},
		},
		// plan: plan,
		meta: s.mockMeta,
	}
	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
	task.allocator = alloc
	plan, err := task.BuildCompactionRequest()
	s.Require().NoError(err)

	s.Equal(2, len(plan.GetSegmentBinlogs()))
	segIDs := lo.Map(plan.GetSegmentBinlogs(), func(b *datapb.CompactionSegmentBinlogs, _ int) int64 {
		return b.GetSegmentID()
	})
	s.ElementsMatch([]int64{200, 201}, segIDs)
}

func (s *CompactionTaskSuite) TestProcessRefreshPlan_MixSegmentNotFound() {
	channel := "Ch-1"
	s.Run("segment_not_found", func() {
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
			return nil
		}).Once()
		task := &mixCompactionTask{
			CompactionTask: &datapb.CompactionTask{
				PlanID:         1,
				TriggerID:      19530,
				CollectionID:   1,
				PartitionID:    10,
				Channel:        channel,
				Type:           datapb.CompactionType_MixCompaction,
				State:          datapb.CompactionTaskState_executing,
				NodeID:         1,
				InputSegments:  []int64{200, 201},
				ResultSegments: []int64{100, 200},
			},
			meta: s.mockMeta,
		}
		alloc := allocator.NewMockAllocator(s.T())
		alloc.EXPECT().AllocN(int64(1)).Return(19530, 99999, nil)
		task.allocator = alloc
		_, err := task.BuildCompactionRequest()
		s.Error(err)
		s.ErrorIs(err, merr.ErrSegmentNotFound)
	})
}

func (s *CompactionTaskSuite) TestCompactionTimeout() {
	channel := "Ch-1"
	binLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			Level:         datapb.SegmentLevel_L1,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Binlogs:       binLogs,
		}}
	}).Times(2)
	s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil)
	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything)
	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
	task := &mixCompactionTask{
		CompactionTask: &datapb.CompactionTask{
			PlanID:           1,
			TriggerID:        19530,
			CollectionID:     1,
			PartitionID:      10,
			Type:             datapb.CompactionType_MixCompaction,
			NodeID:           1,
			State:            datapb.CompactionTaskState_executing,
			InputSegments:    []int64{200, 201},
			ResultSegments:   []int64{100, 200},
			TimeoutInSeconds: 1,
		},
		meta:      s.mockMeta,
		sessions:  s.mockSessMgr,
		allocator: alloc,
	}
	plan, err := task.BuildCompactionRequest()
	task.plan = plan
	s.Require().NoError(err)
	time.Sleep(time.Second * 2)

	s.mockSessMgr.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		State: datapb.CompactionTaskState_executing,
	}, nil)
	end := task.processExecuting()
	s.Equal(true, end)
	s.Equal(datapb.CompactionTaskState_cleaned, task.State)
}
