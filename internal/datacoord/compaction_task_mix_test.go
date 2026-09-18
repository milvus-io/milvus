package datacoord

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/common"
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

// A shard split rewrite plan carries the targets and the modulus their
// residues are taken against; the datanode routes every row by both.
func (s *MixCompactionTaskSuite) TestBuildCompactionRequest_HashSplitRouting() {
	targets := []*datapb.SplitShardTaskTarget{
		{Vchannel: "t0", Buckets: []uint64{0}},
		{Vchannel: "t1", Buckets: []uint64{1}},
	}
	meta := NewMockCompactionMeta(s.T())
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            200,
		Level:         datapb.SegmentLevel_L1,
		InsertChannel: "src",
		State:         commonpb.SegmentState_Flushed,
	}}).Once()
	meta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:           1,
		Type:             datapb.CompactionType_HashSplitCompaction,
		Channel:          "src",
		InputSegments:    []int64{200},
		Schema:           &schemapb.CollectionSchema{Version: 1},
		HashSplitTargets: targets,
		HashSplitModulus: 2,
	}, nil, meta, newMockVersionManager())
	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocN(mock.Anything).Return(int64(100), int64(200), nil).Once()
	task.allocator = alloc

	plan, err := task.BuildCompactionRequest()
	s.Require().NoError(err)
	s.Equal(datapb.CompactionType_HashSplitCompaction, plan.GetType())
	s.Equal("src", plan.GetChannel(), "the plan runs on the source, where its input lives")
	s.Len(plan.GetHashSplitTargets(), 2)
	s.EqualValues(2, plan.GetHashSplitModulus())
}

// Every rewrite plan carries the source channel's L0 deltalogs, so the datanode
// can fold the deletes that never reached an L1 deltalog.
func (s *MixCompactionTaskSuite) TestBuildCompactionRequest_HashSplitCarriesSourceLevelZero() {
	const (
		srcChannel  = "src"
		partitionID = int64(10)
	)
	input := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 200, Level: datapb.SegmentLevel_L1, InsertChannel: srcChannel,
		PartitionID: partitionID, State: commonpb.SegmentState_Flushed,
	}}
	levelZero := func(id, partition int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: id, Level: datapb.SegmentLevel_L0, InsertChannel: srcChannel,
			PartitionID: partition, State: commonpb.SegmentState_Flushed,
			ManifestPath: fmt.Sprintf("manifest/%d", id),
			Deltalogs:    []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogPath: fmt.Sprintf("delta/%d", id)}}}},
		}}
	}
	// SelectSegments applies the caller's filters, exactly as the real meta does,
	// so the partition and level rules below are the production ones.
	selectFrom := func(pool []*SegmentInfo) func(context.Context, ...SegmentFilter) []*SegmentInfo {
		return func(_ context.Context, filters ...SegmentFilter) []*SegmentInfo {
			return lo.Filter(pool, func(info *SegmentInfo, _ int) bool {
				for _, filter := range filters {
					if !filter.Match(info) {
						return false
					}
				}
				return true
			})
		}
	}
	buildPlan := func(pool []*SegmentInfo) *datapb.CompactionPlan {
		meta := NewMockCompactionMeta(s.T())
		meta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(input).Once()
		meta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(selectFrom(pool))
		task := newMixCompactionTask(&datapb.CompactionTask{
			PlanID:           1,
			Type:             datapb.CompactionType_HashSplitCompaction,
			Channel:          srcChannel,
			PartitionID:      partitionID,
			InputSegments:    []int64{200},
			Schema:           &schemapb.CollectionSchema{Version: 1},
			HashSplitTargets: []*datapb.SplitShardTaskTarget{{Vchannel: "t0"}, {Vchannel: "t1"}},
			HashSplitModulus: 2,
		}, nil, meta, newMockVersionManager())
		alloc := allocator.NewMockAllocator(s.T())
		alloc.EXPECT().AllocN(mock.Anything).Return(int64(100), int64(200), nil).Once()
		task.allocator = alloc
		plan, err := task.BuildCompactionRequest()
		s.Require().NoError(err)
		return plan
	}

	s.Run("the source L0s ride along, in id order, with their deltalogs", func() {
		plan := buildPlan([]*SegmentInfo{input, levelZero(302, partitionID), levelZero(301, partitionID)})
		s.Require().Len(plan.GetSegmentBinlogs(), 3)
		s.EqualValues(200, plan.GetSegmentBinlogs()[0].GetSegmentID())
		s.Equal(datapb.SegmentLevel_L0, plan.GetSegmentBinlogs()[1].GetLevel())
		s.EqualValues([]int64{301, 302}, lo.Map(plan.GetSegmentBinlogs()[1:],
			func(seg *datapb.CompactionSegmentBinlogs, _ int) int64 { return seg.GetSegmentID() }))
		s.Equal("delta/301", plan.GetSegmentBinlogs()[1].GetDeltalogs()[0].GetBinlogs()[0].GetLogPath())
		s.Equal("manifest/301", plan.GetSegmentBinlogs()[1].GetManifest())
	})

	s.Run("the L0s are delete sources, not inputs", func() {
		meta := NewMockCompactionMeta(s.T())
		meta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(input).Once()
		meta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(selectFrom([]*SegmentInfo{input, levelZero(301, partitionID)}))
		task := newMixCompactionTask(&datapb.CompactionTask{
			PlanID:           1,
			Type:             datapb.CompactionType_HashSplitCompaction,
			Channel:          srcChannel,
			PartitionID:      partitionID,
			InputSegments:    []int64{200},
			Schema:           &schemapb.CollectionSchema{Version: 1},
			HashSplitTargets: []*datapb.SplitShardTaskTarget{{Vchannel: "t0"}, {Vchannel: "t1"}},
			HashSplitModulus: 2,
		}, nil, meta, newMockVersionManager())
		alloc := allocator.NewMockAllocator(s.T())
		alloc.EXPECT().AllocN(mock.Anything).Return(int64(100), int64(200), nil).Once()
		task.allocator = alloc

		plan, err := task.BuildCompactionRequest()
		s.Require().NoError(err)
		s.Len(plan.GetSegmentBinlogs(), 2)
		// Nothing put the L0 on the task's InputSegments, which is the only list
		// the commit retires and the inspector marks compacting. Several plans of
		// one task share these L0s; retiring them per plan would pull them out
		// from under the plans still running.
		s.EqualValues([]int64{200}, task.GetTaskProto().GetInputSegments())
	})

	s.Run("another partition's L0 is left behind", func() {
		plan := buildPlan([]*SegmentInfo{input, levelZero(301, partitionID+1), levelZero(302, common.AllPartitionsID)})
		s.Require().Len(plan.GetSegmentBinlogs(), 2)
		s.EqualValues(302, plan.GetSegmentBinlogs()[1].GetSegmentID())
	})

	s.Run("a dropped L0 is not a delete source", func() {
		dropped := levelZero(301, partitionID)
		dropped.State = commonpb.SegmentState_Dropped
		plan := buildPlan([]*SegmentInfo{input, dropped})
		s.Len(plan.GetSegmentBinlogs(), 1)
	})

	s.Run("no source L0 leaves the plan exactly as before", func() {
		plan := buildPlan([]*SegmentInfo{input})
		s.Len(plan.GetSegmentBinlogs(), 1)
	})
}

// A mix compaction must not start carrying L0s: its own commit would retire them
// as inputs, and L0 compaction is what folds deletes for it.
func (s *MixCompactionTaskSuite) TestBuildCompactionRequest_MixCarriesNoLevelZero() {
	meta := NewMockCompactionMeta(s.T())
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 200, Level: datapb.SegmentLevel_L1, InsertChannel: "src", State: commonpb.SegmentState_Flushed,
	}}).Once()
	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_MixCompaction,
		Channel:       "src",
		InputSegments: []int64{200},
		Schema:        &schemapb.CollectionSchema{Version: 1},
	}, nil, meta, newMockVersionManager())
	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocN(mock.Anything).Return(int64(100), int64(200), nil).Once()
	task.allocator = alloc

	plan, err := task.BuildCompactionRequest()
	s.Require().NoError(err)
	s.Len(plan.GetSegmentBinlogs(), 1)
	// SelectSegments was never called: mockery fails the test on an unexpected call.
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

			task := newMixCompactionTask(&datapb.CompactionTask{
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
	s.Equal("DataNode reported compaction timeout", t1.GetTaskProto().GetFailReason())
}
