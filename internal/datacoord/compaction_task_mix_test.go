package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestMixCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(MixCompactionTaskSuite))
}

type MixCompactionTaskSuite struct {
	suite.Suite

	mockMeta *MockCompactionMeta
}

func (s *MixCompactionTaskSuite) SetupTest() {
	s.mockMeta = NewMockCompactionMeta(s.T())
}

func (s *MixCompactionTaskSuite) TestProcessRefreshPlan_NormalMix() {
	channel := "Ch-1"
	binLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			Level:         datapb.SegmentLevel_L1,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Binlogs:       binLogs,
		}}
	}).Times(2)
	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:         1,
		TriggerID:      19530,
		CollectionID:   1,
		PartitionID:    10,
		Type:           datapb.CompactionType_MixCompaction,
		NodeID:         1,
		State:          datapb.CompactionTaskState_executing,
		InputSegments:  []int64{200, 201},
		ResultSegments: []int64{100, 200},
	}, nil, s.mockMeta, newMockVersionManager())
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

func (s *MixCompactionTaskSuite) TestProcessRefreshPlan_MixSegmentNotFound() {
	channel := "Ch-1"
	s.Run("segment_not_found", func() {
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
			return nil
		}).Once()
		task := newMixCompactionTask(&datapb.CompactionTask{
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
		}, nil, s.mockMeta, newMockVersionManager())
		_, err := task.BuildCompactionRequest()
		s.Error(err)
		s.ErrorIs(err, merr.ErrSegmentNotFound)
	})
}

func (s *MixCompactionTaskSuite) TestProcess() {
	s.Run("test process states", func() {
		testCases := []struct {
			state         datapb.CompactionTaskState
			processResult bool
		}{
			{state: datapb.CompactionTaskState_unknown, processResult: false},
			{state: datapb.CompactionTaskState_pipelining, processResult: false},
			{state: datapb.CompactionTaskState_executing, processResult: false},
			{state: datapb.CompactionTaskState_failed, processResult: true},
			{state: datapb.CompactionTaskState_timeout, processResult: true},
		}

		for _, tc := range testCases {
			task := newMixCompactionTask(&datapb.CompactionTask{
				PlanID: 1,
				State:  tc.state,
			}, nil, s.mockMeta, newMockVersionManager())
			res := task.Process()
			s.Equal(tc.processResult, res)
		}
	})
}

func (s *MixCompactionTaskSuite) TestQueryTaskOnWorker() {
	cluster := session.NewMockCluster(s.T())

	t1 := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:    1,
		Type:      datapb.CompactionType_MixCompaction,
		StartTime: time.Now().Unix(),
		Channel:   "ch-1",
		State:     datapb.CompactionTaskState_executing,
		NodeID:    111,
	}, nil, s.mockMeta, newMockVersionManager())

	s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
	cluster.EXPECT().QueryCompaction(mock.Anything, mock.Anything).Return(
		&datapb.CompactionPlanResult{PlanID: 1, State: datapb.CompactionTaskState_timeout}, nil).Once()

	t1.QueryTaskOnWorker(cluster)

	s.Equal(taskcommon.Retry, t1.GetTaskState())
}

func (s *MixCompactionTaskSuite) TestGetTaskSlot() {
	// Test backward compatibility - GetTaskSlot returns int64
	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_MixCompaction,
		MaxSize:       512 * 1024 * 1024, // 512MB
		InputSegments: []int64{200},
	}, nil, s.mockMeta, newMockVersionManager())

	slot := task.GetTaskSlot()
	s.Greater(slot, int64(0))
	s.IsType(int64(0), slot)
}

func (s *MixCompactionTaskSuite) TestGetTaskSlotV2_MixCompaction() {
	// Test new GetTaskSlotV2 returns (float64, float64)
	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_MixCompaction,
		MaxSize:       512 * 1024 * 1024, // 512MB
		InputSegments: []int64{200},
	}, nil, s.mockMeta, newMockVersionManager())

	cpuSlot, memorySlot := task.GetTaskSlotV2()
	s.Greater(cpuSlot, 0.0)
	s.Greater(memorySlot, 0.0)
}

func (s *MixCompactionTaskSuite) TestGetTaskSlotV2_SortCompaction() {
	// Test SortCompaction uses calculateStatsTaskSlotV2
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(&SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID: 200,
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{
							LogID: 10, MemorySize: 512 * 1024 * 1024, // 512MB},
						},
					},
				},
			},
			Statslogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{LogID: 10, MemorySize: 100},
					},
				},
			},
		},
	}).Once()

	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_SortCompaction,
		MaxSize:       512 * 1024 * 1024, // 512MB
		InputSegments: []int64{200},
	}, nil, s.mockMeta, newMockVersionManager())

	cpuSlot, memorySlot := task.GetTaskSlotV2()
	s.Greater(cpuSlot, 0.0)
	s.Greater(memorySlot, 0.0)
}

func (s *MixCompactionTaskSuite) TestGetSlotUsage() {
	// Test GetSlotUsage delegates to GetTaskSlot
	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_MixCompaction,
		MaxSize:       512 * 1024 * 1024,
		InputSegments: []int64{200},
	}, nil, s.mockMeta, newMockVersionManager())

	slotUsage := task.GetSlotUsage()
	taskSlot := task.GetTaskSlot()
	s.Equal(taskSlot, slotUsage)
}

func (s *MixCompactionTaskSuite) TestGetTaskSlotV2_DifferentSizes() {
	// Test that different task sizes return different memory slots
	testCases := []struct {
		name    string
		maxSize int64
	}{
		{"small_100MB", 100 * 1024 * 1024},
		{"medium_500MB", 500 * 1024 * 1024},
		{"large_1GB", 1024 * 1024 * 1024},
		{"xlarge_5GB", 5 * 1024 * 1024 * 1024},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			task := newMixCompactionTask(&datapb.CompactionTask{
				PlanID:        1,
				Type:          datapb.CompactionType_MixCompaction,
				MaxSize:       tc.maxSize,
				InputSegments: []int64{200},
			}, nil, s.mockMeta, newMockVersionManager())

			cpuSlot, memorySlot := task.GetTaskSlotV2()
			s.Greater(cpuSlot, 0.0)
			s.Greater(memorySlot, 0.0)

			// Memory should scale with size
			expectedMemory := float64(tc.maxSize) / 1024 / 1024 / 1024
			s.Greater(memorySlot, expectedMemory*0.5) // At least 0.5x the size
		})
	}
}

func (s *MixCompactionTaskSuite) TestGetTaskSlot_CachedValue() {
	// Test that slot value is cached after first calculation
	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_MixCompaction,
		MaxSize:       512 * 1024 * 1024,
		InputSegments: []int64{200},
	}, nil, s.mockMeta, newMockVersionManager())

	// First call
	slot1 := task.GetTaskSlot()
	// Second call should return same cached value
	slot2 := task.GetTaskSlot()
	s.Equal(slot1, slot2)
}

func (s *MixCompactionTaskSuite) TestGetTaskSlotV2_SortCompaction_NoSegment() {
	// Test SortCompaction when segment is not found
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(nil).Once()

	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_SortCompaction,
		MaxSize:       512 * 1024 * 1024,
		InputSegments: []int64{200},
	}, nil, s.mockMeta, newMockVersionManager())

	cpuSlot, memorySlot := task.GetTaskSlotV2()
	// Should fallback to default calculation
	s.Greater(cpuSlot, 0.0)
	s.Greater(memorySlot, 0.0)
}
