package datacoord

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	globaltask "github.com/milvus-io/milvus/internal/datacoord/task"
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
}

func TestMixCompactionTaskMetricsRetryDoesNotDrift(t *testing.T) {
	const (
		retryNodeID    = int64(99001)
		normalNodeID   = int64(99002)
		retryPlanID    = int64(99001)
		normalPlanID   = int64(99002)
		compactionType = datapb.CompactionType_SortCompaction
	)

	taskType := compactionType.String()
	pending := metrics.DataCoordCompactionTaskNum.WithLabelValues("-1", taskType, metrics.Pending)
	nullExecuting := metrics.DataCoordCompactionTaskNum.WithLabelValues("-1", taskType, metrics.Executing)
	retryExecuting := metrics.DataCoordCompactionTaskNum.WithLabelValues("99001", taskType, metrics.Executing)
	retryDone := metrics.DataCoordCompactionTaskNum.WithLabelValues("99001", taskType, metrics.Done)
	normalExecuting := metrics.DataCoordCompactionTaskNum.WithLabelValues("99002", taskType, metrics.Executing)
	normalDone := metrics.DataCoordCompactionTaskNum.WithLabelValues("99002", taskType, metrics.Done)
	initialPending := testutil.ToFloat64(pending)
	initialNullExecuting := testutil.ToFloat64(nullExecuting)
	initialRetryExecuting := testutil.ToFloat64(retryExecuting)
	initialRetryDone := testutil.ToFloat64(retryDone)
	initialNormalExecuting := testutil.ToFloat64(normalExecuting)
	initialNormalDone := testutil.ToFloat64(normalDone)
	t.Cleanup(func() {
		pending.Set(initialPending)
		nullExecuting.Set(initialNullExecuting)
		retryExecuting.Set(initialRetryExecuting)
		retryDone.Set(initialRetryDone)
		normalExecuting.Set(initialNormalExecuting)
		normalDone.Set(initialNormalDone)
	})

	meta := NewMockCompactionMeta(t)
	meta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, segmentID int64) *SegmentInfo {
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:    segmentID,
				State: commonpb.SegmentState_Flushed,
			}}
		},
	).Maybe()
	meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	meta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Times(2)
	meta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).Return(
		nil, &segMetricMutation{}, nil).Times(2)
	meta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, false).Return().Times(2)

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).Return(int64(100), int64(200), nil).Maybe()
	newTask := func(planID int64) *mixCompactionTask {
		return newMixCompactionTask(&datapb.CompactionTask{
			PlanID:        planID,
			CollectionID:  1,
			Type:          compactionType,
			State:         datapb.CompactionTaskState_pipelining,
			NodeID:        NullNodeID,
			InputSegments: []int64{planID},
			Schema:        &schemapb.CollectionSchema{},
		}, alloc, meta, newMockVersionManager())
	}

	compactionTask := newTask(retryPlanID)

	scheduler := globaltask.NewMockGlobalScheduler(t)
	scheduler.EXPECT().Enqueue(compactionTask).Once()
	handler := newCompactionInspector(nil, nil, nil, scheduler, scheduler, newMockVersionManager())
	require.NoError(t, handler.submitTask(compactionTask))
	require.Equal(t, initialPending+1, testutil.ToFloat64(pending))

	// Moving a task into the generic scheduler does not mean that a worker has
	// accepted it yet. It must remain pending until CreateTaskOnWorker succeeds.
	handler.schedule()
	require.Equal(t, initialPending+1, testutil.ToFloat64(pending))
	require.Equal(t, initialNullExecuting, testutil.ToFloat64(nullExecuting))

	cluster := session.NewMockCluster(t)
	cluster.EXPECT().CreateCompaction(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	compactionTask.CreateTaskOnWorker(retryNodeID, cluster)
	require.Equal(t, initialPending, testutil.ToFloat64(pending))
	require.Equal(t, initialRetryExecuting+1, testutil.ToFloat64(retryExecuting))

	// A worker query error returns the task to pending. Repeating the failed
	// Create path must not apply another metric transition.
	cluster.EXPECT().QueryCompaction(retryNodeID, mock.Anything).Return(nil, merr.WrapErrNodeNotFound(retryNodeID)).Once()
	compactionTask.QueryTaskOnWorker(cluster)
	require.Equal(t, datapb.CompactionTaskState_pipelining, compactionTask.GetTaskProto().GetState())
	require.EqualValues(t, NullNodeID, compactionTask.GetTaskProto().GetNodeID())
	require.Equal(t, initialPending+1, testutil.ToFloat64(pending))
	require.Equal(t, initialRetryExecuting, testutil.ToFloat64(retryExecuting))
	for i := 0; i < 5; i++ {
		cluster.EXPECT().CreateCompaction(mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("compaction already exists")).Once()
		compactionTask.CreateTaskOnWorker(retryNodeID, cluster)
	}
	require.Equal(t, initialPending+1, testutil.ToFloat64(pending))
	require.Equal(t, initialRetryExecuting, testutil.ToFloat64(retryExecuting))
	require.Equal(t, initialNullExecuting, testutil.ToFloat64(nullExecuting))

	cluster.EXPECT().CreateCompaction(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	compactionTask.CreateTaskOnWorker(retryNodeID, cluster)
	cluster.EXPECT().QueryCompaction(retryNodeID, mock.Anything).Return(
		&datapb.CompactionPlanResult{PlanID: retryPlanID, State: datapb.CompactionTaskState_completed}, nil).Once()
	compactionTask.QueryTaskOnWorker(cluster)
	require.Equal(t, initialPending, testutil.ToFloat64(pending))
	require.Equal(t, initialRetryExecuting, testutil.ToFloat64(retryExecuting))
	require.Equal(t, initialRetryDone+1, testutil.ToFloat64(retryDone))

	// A separate task with no retry must still follow pending -> executing -> done.
	normalTask := newTask(normalPlanID)
	require.NoError(t, handler.submitTask(normalTask))
	require.Equal(t, initialPending+1, testutil.ToFloat64(pending))

	normalCluster := session.NewMockCluster(t)
	normalCluster.EXPECT().CreateCompaction(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	normalTask.CreateTaskOnWorker(normalNodeID, normalCluster)
	require.Equal(t, initialPending, testutil.ToFloat64(pending))
	require.Equal(t, initialNormalExecuting+1, testutil.ToFloat64(normalExecuting))

	normalCluster.EXPECT().QueryCompaction(normalNodeID, mock.Anything).Return(
		&datapb.CompactionPlanResult{PlanID: normalPlanID, State: datapb.CompactionTaskState_completed}, nil).Once()
	normalTask.QueryTaskOnWorker(normalCluster)
	require.Equal(t, datapb.CompactionTaskState_completed, normalTask.GetTaskProto().GetState())
	require.Equal(t, initialPending, testutil.ToFloat64(pending))
	require.Equal(t, initialNormalExecuting, testutil.ToFloat64(normalExecuting))
	require.Equal(t, initialNormalDone+1, testutil.ToFloat64(normalDone))
}

func TestMixCompactionTaskMetricsConcurrentCompletionDoesNotDrift(t *testing.T) {
	const (
		nodeID         = int64(99003)
		compactionType = datapb.CompactionType_SortCompaction
	)

	taskType := compactionType.String()
	executing := metrics.DataCoordCompactionTaskNum.WithLabelValues("99003", taskType, metrics.Executing)
	done := metrics.DataCoordCompactionTaskNum.WithLabelValues("99003", taskType, metrics.Done)
	initialExecuting := testutil.ToFloat64(executing)
	initialDone := testutil.ToFloat64(done)
	t.Cleanup(func() {
		executing.Set(initialExecuting)
		done.Set(initialDone)
	})

	firstSaveStarted := make(chan struct{})
	releaseFirstSave := make(chan struct{})
	var saveCalls atomic.Int32
	meta := NewMockCompactionMeta(t)
	meta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, *datapb.CompactionTask) error {
			if saveCalls.Add(1) == 1 {
				close(firstSaveStarted)
				<-releaseFirstSave
			}
			return nil
		},
	).Twice()

	task := newMixCompactionTask(&datapb.CompactionTask{
		PlanID:  99003,
		Type:    compactionType,
		State:   datapb.CompactionTaskState_meta_saved,
		NodeID:  nodeID,
		Channel: "concurrent-completion",
	}, nil, meta, newMockVersionManager())
	incCompactionTaskMetric(task.GetTaskProto())

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- task.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed))
	}()
	<-firstSaveStarted

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- task.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_completed))
	}()

	var secondErr error
	secondFinished := false
	select {
	case secondErr = <-secondDone:
		secondFinished = true
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirstSave)
	require.NoError(t, <-firstDone)
	if !secondFinished {
		secondErr = <-secondDone
	}
	require.NoError(t, secondErr)

	require.Equal(t, initialExecuting, testutil.ToFloat64(executing))
	require.Equal(t, initialDone+1, testutil.ToFloat64(done))
}
