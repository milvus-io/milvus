package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
		Schema:         &schemapb.CollectionSchema{Version: 1},
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
			Schema:         &schemapb.CollectionSchema{Version: 1},
		}, nil, s.mockMeta, newMockVersionManager())
		_, err := task.BuildCompactionRequest()
		s.Error(err)
		s.ErrorIs(err, merr.ErrSegmentNotFound)
	})
}

func (s *MixCompactionTaskSuite) TestBuildCompactionRequestSchemaVersionGuard() {
	s.Run("nil_schema", func() {
		task := newMixCompactionTask(&datapb.CompactionTask{
			PlanID:        1,
			Type:          datapb.CompactionType_MixCompaction,
			InputSegments: []int64{200},
		}, nil, NewMockCompactionMeta(s.T()), newMockVersionManager())

		_, err := task.BuildCompactionRequest()
		s.Error(err)
		s.ErrorIs(err, merr.ErrIllegalCompactionPlan)
	})

	s.Run("mix_task_schema_older_than_input", func() {
		meta := NewMockCompactionMeta(s.T())
		meta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            200,
			State:         commonpb.SegmentState_Flushed,
			SchemaVersion: 3,
		}}).Once()
		task := newMixCompactionTask(&datapb.CompactionTask{
			PlanID:        1,
			Type:          datapb.CompactionType_MixCompaction,
			InputSegments: []int64{200},
			Schema:        &schemapb.CollectionSchema{Version: 2},
		}, nil, meta, newMockVersionManager())

		_, err := task.BuildCompactionRequest()
		s.Error(err)
		s.ErrorIs(err, merr.ErrIllegalCompactionPlan)
	})

	s.Run("sort_task_schema_older_than_input", func() {
		meta := NewMockCompactionMeta(s.T())
		meta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            200,
			State:         commonpb.SegmentState_Flushed,
			SchemaVersion: 3,
		}}).Once()
		task := newMixCompactionTask(&datapb.CompactionTask{
			PlanID:        1,
			Type:          datapb.CompactionType_SortCompaction,
			InputSegments: []int64{200},
			Schema:        &schemapb.CollectionSchema{Version: 2},
		}, nil, meta, newMockVersionManager())
		task.slotUsage.Store(1)

		_, err := task.BuildCompactionRequest()
		s.Error(err)
		s.ErrorIs(err, merr.ErrIllegalCompactionPlan)
	})

	for _, test := range []struct {
		name           string
		compactionType datapb.CompactionType
		taskSchema     int32
		inputSchema    int32
		storeSlotUsage bool
		expectedSchema int32
	}{
		{
			name:           "mix_task_schema_newer_than_mixed_inputs_allowed",
			compactionType: datapb.CompactionType_MixCompaction,
			taskSchema:     4,
			inputSchema:    3,
			expectedSchema: 4,
		},
		{
			name:           "sort_task_schema_equal_input_allowed",
			compactionType: datapb.CompactionType_SortCompaction,
			taskSchema:     3,
			inputSchema:    3,
			storeSlotUsage: true,
			expectedSchema: 3,
		},
		{
			name:           "sort_task_schema_newer_than_input_allowed",
			compactionType: datapb.CompactionType_SortCompaction,
			taskSchema:     4,
			inputSchema:    3,
			storeSlotUsage: true,
			expectedSchema: 4,
		},
	} {
		s.Run(test.name, func() {
			meta := NewMockCompactionMeta(s.T())
			meta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            200,
				State:         commonpb.SegmentState_Flushed,
				SchemaVersion: test.inputSchema,
				Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(101, 1)},
			}}).Once()
			task := newMixCompactionTask(&datapb.CompactionTask{
				PlanID:        1,
				Type:          test.compactionType,
				InputSegments: []int64{200},
				Schema:        &schemapb.CollectionSchema{Version: test.taskSchema},
			}, nil, meta, newMockVersionManager())
			if test.storeSlotUsage {
				task.slotUsage.Store(1)
			}
			alloc := allocator.NewMockAllocator(s.T())
			alloc.EXPECT().AllocN(mock.Anything).Return(int64(100), int64(200), nil).Once()
			task.allocator = alloc

			plan, err := task.BuildCompactionRequest()
			s.NoError(err)
			s.EqualValues(test.expectedSchema, plan.GetSchema().GetVersion())
			s.Len(plan.GetSegmentBinlogs(), 1)
		})
	}
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
