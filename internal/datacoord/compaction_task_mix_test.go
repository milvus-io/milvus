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
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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

func (s *MixCompactionTaskSuite) TestBuildCompactionRequest_MixFileResources() {
	channel := "Ch-1"
	binLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}
	expectedResources := []*internalpb.FileResourceInfo{
		{Id: 7, Name: "dict", Path: "dict.jieba"},
	}

	for _, testCase := range []struct {
		name              string
		mode              string
		expectResources   bool
		expectResourceGet bool
	}{
		{name: "ref", mode: "ref", expectResources: true, expectResourceGet: true},
		{name: "sync", mode: "sync", expectResources: false, expectResourceGet: false},
	} {
		s.Run(testCase.name, func() {
			paramtable.Get().Save(Params.CommonCfg.DNFileResourceMode.Key, testCase.mode)
			s.T().Cleanup(func() {
				paramtable.Get().Reset(Params.CommonCfg.DNFileResourceMode.Key)
			})

			mockMeta := NewMockCompactionMeta(s.T())
			mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
				return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
					ID:            segID,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
					State:         commonpb.SegmentState_Flushed,
					Binlogs:       binLogs,
				}}
			}).Once()
			if testCase.expectResourceGet {
				mockMeta.EXPECT().GetFileResources(mock.Anything, mock.Anything).Return(expectedResources, nil).Once()
			}

			task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
				PlanID:        1,
				TriggerID:     19530,
				CollectionID:  1,
				PartitionID:   10,
				Type:          datapb.CompactionType_MixCompaction,
				NodeID:        1,
				State:         datapb.CompactionTaskState_executing,
				InputSegments: []int64{200},
				Schema: &schemapb.CollectionSchema{
					FileResourceIds: []int64{7},
				},
			}, nil, mockMeta, newMockVersionManager())
			alloc := allocator.NewMockAllocator(s.T())
			alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
			task.allocator = alloc

			plan, err := task.BuildCompactionRequest()
			s.Require().NoError(err)
			if testCase.expectResources {
				s.Equal(expectedResources, plan.GetFileResources())
			} else {
				s.Empty(plan.GetFileResources())
			}
		})
	}
}

func (s *MixCompactionTaskSuite) TestProcessRefreshPlan_MixSegmentNotFound() {
	channel := "Ch-1"
	s.Run("segment_not_found", func() {
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
			return nil
		}).Once()
		task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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
		task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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
		task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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
		task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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
			task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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
			task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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

	t1 := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
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

	// The attempt is over and the inspector owns Retry directly. The global
	// scheduler releases every state other than Init/InProgress.
	s.Equal(taskcommon.Retry, t1.GetTaskState())
}

// What cleanup owes a terminated sort task is the segment lock, and only that.
// canTriggerSortCompaction requires !isCompacting, so releasing it is what makes
// the input eligible for another sort. IsInvisible must survive: it means "this
// segment still owes a sort", and cleanup clearing it would publish an unsorted,
// unindexable segment on the sealed path instead of letting the trigger retry.
func (s *MixCompactionTaskSuite) TestCleanReArmsSortInputWithoutPublishingIt() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	collectionID, segmentID := int64(10), int64(100)
	s.Require().NoError(realMeta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Flushed,
		IsInvisible:  true,
	})))
	realMeta.SetSegmentsCompacting(context.Background(), []int64{segmentID}, true)
	s.Require().False(canTriggerSortCompaction(realMeta.GetSegment(context.Background(), segmentID)),
		"a segment held by a running task is not eligible")

	task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:        1,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_failed,
		InputSegments: []int64{segmentID},
	}, nil, s.mockMeta, newMockVersionManager())

	s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, saved *datapb.CompactionTask) error {
			s.Equal(datapb.CompactionTaskState_cleaned, saved.GetState())
			s.True(realMeta.GetSegment(ctx, segmentID).isCompacting,
				"the lock is released only after cleaned is durable")
			return nil
		}).Once()
	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{segmentID}, false).
		Run(func(ctx context.Context, segmentIDs []int64, compacting bool) {
			realMeta.SetSegmentsCompacting(ctx, segmentIDs, compacting)
		}).Once()

	s.True(task.Clean())

	segment := realMeta.GetSegment(context.Background(), segmentID)
	s.True(segment.GetIsInvisible(), "the input still owes a sort and must not be published")
	s.False(segment.isCompacting)
	s.True(canTriggerSortCompaction(segment), "and it must now be eligible for another sort")
}

// A DataNode leaves the cluster (it deregisters from etcd before tearing down
// its compactions) while it holds a sort plan. The plan must not be handed to a
// second node -- both would write the same output segment IDs -- so the attempt
// is abandoned, and cleanup must hand the input's lock back so the trigger can
// try again with a fresh plan.
func (s *MixCompactionTaskSuite) TestNodeGoneAbandonsAttemptAndReArmsInput() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	collectionID, segmentID := int64(10), int64(100)
	s.Require().NoError(realMeta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Flushed,
		IsInvisible:  true,
	})))
	realMeta.SetSegmentsCompacting(context.Background(), []int64{segmentID}, true)

	task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:        1,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_executing,
		NodeID:        11,
		InputSegments: []int64{segmentID},
	}, nil, s.mockMeta, newMockVersionManager())

	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().QueryCompaction(int64(11), mock.Anything).
		Return(nil, merr.WrapErrNodeNotFound(11)).Once()
	s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, saved *datapb.CompactionTask) error {
			switch saved.GetState() {
			case datapb.CompactionTaskState_retrying:
				s.EqualValues(11, saved.GetNodeID(), "the plan is abandoned, never re-dispatched")
			case datapb.CompactionTaskState_cleaned:
			default:
				s.Fail("unexpected saved compaction state", saved.GetState().String())
			}
			return nil
		}).Twice()
	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{segmentID}, false).
		Run(func(ctx context.Context, segmentIDs []int64, compacting bool) {
			realMeta.SetSegmentsCompacting(ctx, segmentIDs, compacting)
		}).Once()

	task.QueryTaskOnWorker(cluster)
	s.Equal(datapb.CompactionTaskState_retrying, task.GetTask().GetState())
	s.EqualValues(11, task.GetTask().GetNodeID())

	s.True(task.Process(), "a timed-out task ends the state machine")
	s.True(task.Clean())

	segment := realMeta.GetSegment(context.Background(), segmentID)
	s.True(segment.GetIsInvisible(), "the input still owes a sort")
	s.True(canTriggerSortCompaction(segment), "and is eligible for a fresh plan")
}

func (s *MixCompactionTaskSuite) TestCleanRetriesWhenCleanedStatePersistenceFails() {
	realMeta, err := newMemoryMeta(s.T())
	s.Require().NoError(err)
	collectionID := int64(10)
	segmentID := int64(100)
	s.Require().NoError(realMeta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Flushed,
		IsInvisible:  true,
	})))
	realMeta.SetSegmentsCompacting(context.Background(), []int64{segmentID}, true)

	task := newMixCompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:        1,
		CollectionID:  collectionID,
		Type:          datapb.CompactionType_SortCompaction,
		State:         datapb.CompactionTaskState_timeout,
		InputSegments: []int64{segmentID},
	}, nil, s.mockMeta, newMockVersionManager())
	s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).
		Return(merr.WrapErrServiceInternalMsg("mock task persistence failure")).Once()
	s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
	s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{segmentID}, false).
		Run(func(ctx context.Context, segmentIDs []int64, compacting bool) {
			realMeta.SetSegmentsCompacting(ctx, segmentIDs, compacting)
		}).Once()

	s.False(task.Clean())
	s.Equal(datapb.CompactionTaskState_timeout, task.GetTask().GetState())
	segment := realMeta.GetSegment(context.Background(), segmentID)
	s.True(segment.isCompacting, "the segment lock must remain held until cleaned is durable")

	s.True(task.Clean())
	s.Equal(datapb.CompactionTaskState_cleaned, task.GetTask().GetState())
	segment = realMeta.GetSegment(context.Background(), segmentID)
	s.True(segment.GetIsInvisible(), "cleanup never publishes an unsorted input")
	s.False(segment.isCompacting)
}
