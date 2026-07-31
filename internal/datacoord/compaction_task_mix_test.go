package datacoord

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
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

// Covers the FileResources branch in BuildCompactionRequest for MixCompaction
// plans (previously only SortCompaction was wired). Without this, mix-compacted
// segments with custom analyzers in ref mode would build text indexes using
// default tokenization → silent search regressions.
func (s *MixCompactionTaskSuite) TestBuildCompactionRequest_MixFileResourcesInRefMode() {
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.DNFileResourceMode.Key, "ref")
	defer pt.Reset(pt.CommonCfg.DNFileResourceMode.Key)

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
	}).Once()

	expectedResources := []*internalpb.FileResourceInfo{
		{Id: 7, Name: "dict", Path: "dict.jieba"},
	}
	s.mockMeta.EXPECT().GetFileResources(mock.Anything, mock.Anything).Return(expectedResources, nil).Once()

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
	}, nil, s.mockMeta, newMockVersionManager())
	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
	task.allocator = alloc

	plan, err := task.BuildCompactionRequest()
	s.Require().NoError(err)
	s.Equal(expectedResources, plan.GetFileResources(),
		"FileResources must flow through for MixCompaction plans (issue #50145, PR #50140)")
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

// A failed dispatch must release the Executing gauge under the label the enqueue
// filed it under. compactionInspector.schedule increments Executing while NodeID
// is still NullNodeID, and the reserve write stamps the real nodeID before the
// RPC — so reading NodeID after that write would credit a bucket that was never
// incremented, inflating node=-1 and driving the real node negative.
func (s *MixCompactionTaskSuite) TestCreateTaskOnWorkerFailureReleasesEnqueuedNodeLabel() {
	const (
		planID = int64(1)
		nodeID = int64(111)
	)
	compactionType := datapb.CompactionType_MixCompaction

	meta := NewMockCompactionMeta(s.T())
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(200)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            200,
		State:         commonpb.SegmentState_Flushed,
		SchemaVersion: 3,
		Binlogs:       []*datapb.FieldBinlog{getFieldBinlogIDs(101, 1)},
	}}).Once()
	meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)

	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:        planID,
		Type:          compactionType,
		InputSegments: []int64{200},
		Schema:        &schemapb.CollectionSchema{Version: 3},
		State:         datapb.CompactionTaskState_pipelining,
		NodeID:        NullNodeID,
	}, nil, meta, newMockVersionManager())
	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocN(mock.Anything).Return(int64(100), int64(200), nil).Once()
	task.allocator = alloc

	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().CreateCompaction(nodeID, mock.Anything, mock.Anything).
		Return(merr.WrapErrServiceUnavailableMsg("slot limit")).Once()
	cluster.EXPECT().DropCompaction(nodeID, planID).Return(nil).Once()

	enqueued := metrics.DataCoordCompactionTaskNum.WithLabelValues(
		fmt.Sprintf("%d", NullNodeID), compactionType.String(), metrics.Executing)
	dispatched := metrics.DataCoordCompactionTaskNum.WithLabelValues(
		fmt.Sprintf("%d", nodeID), compactionType.String(), metrics.Executing)
	beforeEnqueued := testutil.ToFloat64(enqueued)
	beforeDispatched := testutil.ToFloat64(dispatched)

	task.CreateTaskOnWorker(nodeID, cluster)

	s.Equal(datapb.CompactionTaskState_pipelining, task.GetTaskProto().GetState())
	s.EqualValues(NullNodeID, task.GetTaskProto().GetNodeID())
	s.Equal(beforeEnqueued-1, testutil.ToFloat64(enqueued),
		"the enqueue-side Executing entry must be released")
	s.Equal(beforeDispatched, testutil.ToFloat64(dispatched),
		"the dispatched node never had an Executing entry to release")
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

func (s *MixCompactionTaskSuite) TestQueryErrorRequiresSuccessfulCancelBeforeRetry() {
	for _, compactionType := range []datapb.CompactionType{
		datapb.CompactionType_MixCompaction,
		datapb.CompactionType_SortCompaction,
	} {
		s.Run(compactionType.String()+"_cancel_failed", func() {
			cluster := session.NewMockCluster(s.T())
			task := newMixCompactionTask(&datapb.CompactionTask{
				PlanID: 1,
				Type:   compactionType,
				State:  datapb.CompactionTaskState_executing,
				NodeID: 111,
			}, nil, s.mockMeta, newMockVersionManager())

			cluster.EXPECT().QueryCompaction(mock.Anything, mock.Anything).
				Return(nil, merr.WrapErrServiceUnavailableMsg("query failed"))
			cluster.EXPECT().DropCompaction(int64(111), int64(1)).
				Return(merr.WrapErrServiceUnavailableMsg("cancel failed"))

			task.QueryTaskOnWorker(cluster)
			s.Equal(taskcommon.InProgress, task.GetTaskState())
		})

		s.Run(compactionType.String()+"_cancel_succeeded", func() {
			cluster := session.NewMockCluster(s.T())
			task := newMixCompactionTask(&datapb.CompactionTask{
				PlanID: 2,
				Type:   compactionType,
				State:  datapb.CompactionTaskState_executing,
				NodeID: 112,
			}, nil, s.mockMeta, newMockVersionManager())

			cluster.EXPECT().QueryCompaction(mock.Anything, mock.Anything).
				Return(nil, merr.WrapErrServiceUnavailableMsg("query failed"))
			cluster.EXPECT().DropCompaction(int64(112), int64(2)).Return(nil)
			s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)

			task.QueryTaskOnWorker(cluster)
			s.Equal(taskcommon.Init, task.GetTaskState())
		})
	}
}
