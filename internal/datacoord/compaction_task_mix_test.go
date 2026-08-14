package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/util/taskresource"
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
		// Called once per input segment by GetTaskSlot (to size the
		// requirement estimate) and once more per input segment when
		// BuildCompactionRequest assembles the plan's segment binlogs.
	}).Times(4)
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
				// Once from GetTaskSlot's own segment scan, once more when
				// BuildCompactionRequest assembles the plan's segment binlogs.
			}).Times(2)
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
		}).Times(3) // GetTaskSlot scans both nil segments (skipping each), then
		// BuildCompactionRequest's own loop hits segment 200 again and fails fast.
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
		}}).Times(2) // once from GetTaskSlot, once from the segment binlog loop
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
			// A pre-stored slotUsage (sort cases below) short-circuits
			// GetTaskSlot's own segment scan, so only the segment binlog
			// loop in BuildCompactionRequest calls GetHealthySegment; a
			// mix task with no stored slotUsage pays for both.
			wantCalls := 2
			if test.storeSlotUsage {
				wantCalls = 1
			}
			meta := NewMockCompactionMeta(s.T())
			meta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            200,
				State:         commonpb.SegmentState_Flushed,
				SchemaVersion: test.inputSchema,
				Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(101, 1)},
			}}).Times(wantCalls)
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

// newMixCompactionTaskForTest builds a *mixCompactionTask with a single input
// segment (ID 200) carrying the given storage version and uncompressed
// binlog size, wired to a mock CompactionMeta so GetTaskSlot's
// taskresource.EstimateCompaction path can run end to end.
func newMixCompactionTaskForTest(t *testing.T, storageVersion int64, memorySize int64) *mixCompactionTask {
	meta := NewMockCompactionMeta(t)
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:             200,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storageVersion,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: memorySize}}},
		},
	}}).Once()

	return newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_MixCompaction,
		InputSegments: []int64{200},
	}, nil, meta, newMockVersionManager())
}

// issue #52180 incident 1: eight mix compactions totalling 36GiB of input
// were each charged the flat mixCompactionUsage of 4.
func TestMixCompactionSlotScalesWithInput(t *testing.T) {
	paramtable.Init()

	small := newMixCompactionTaskForTest(t, 3 /* storageVersion */, 100*1024*1024)
	large := newMixCompactionTaskForTest(t, 3 /* storageVersion */, 8*1024*1024*1024)

	assert.Greater(t, large.GetTaskSlot(), small.GetTaskSlot(),
		"a 8GiB compaction must not cost the same as a 100MiB one")
}

func TestMixCompactionSlotIsPositive(t *testing.T) {
	paramtable.Init()

	task := newMixCompactionTaskForTest(t, 2, 0)
	assert.Greater(t, task.GetTaskSlot(), int64(0))
}

// GetTaskSlot must aggregate across every input segment, not just the first
// or last one it happens to see: storage version and delete payload take the
// max (a single v3/heavy-delete segment dominates the memory profile per the
// design doc), while memory size sums (the reader holds all of it). Segment
// order is varied to catch an implementation that silently reads only the
// first segment in the slice, which "max across inputs" logic can easily
// regress to.
func TestMixCompactionSlotAggregatesAcrossSegments(t *testing.T) {
	paramtable.Init()

	newSeg := func(id, storageVersion, memorySize, deltaSize int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:             id,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: storageVersion,
			Binlogs: []*datapb.FieldBinlog{
				{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: memorySize}}},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{Binlogs: []*datapb.Binlog{{MemorySize: deltaSize}}},
			},
		}}
	}

	const (
		mem1   = 50 * 1024 * 1024
		mem2   = 30 * 1024 * 1024
		delta1 = 1 * 1024 * 1024
		delta2 = 3 * 1024 * 1024
	)
	// want is computed from the real estimator with the aggregated inputs a
	// correct implementation must derive (max storage version 3, summed
	// memory, max delete payload) -- an independent check that does not just
	// re-run GetTaskSlot's own arithmetic.
	want := memoryToSlots(taskresource.EstimateCompaction(taskresource.CompactionInput{
		Type:                  datapb.CompactionType_MixCompaction,
		StorageVersion:        3,
		TotalMemorySize:       mem1 + mem2,
		MaxSegmentDeleteBytes: delta2,
	}).Memory)

	for _, order := range [][]int64{{200, 201}, {201, 200}} {
		meta := NewMockCompactionMeta(t)
		meta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(newSeg(200, 1, mem1, delta1)).Once()
		meta.EXPECT().GetHealthySegment(mock.Anything, int64(201)).Return(newSeg(201, 3, mem2, delta2)).Once()

		task := newMixCompactionTask(&datapb.CompactionTask{
			PlanID:        1,
			Type:          datapb.CompactionType_MixCompaction,
			InputSegments: order,
		}, nil, meta, newMockVersionManager())

		assert.Equal(t, want, task.GetTaskSlot(), "input order %v must not change the aggregated result", order)
	}
}
