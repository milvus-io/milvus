// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestL0CompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(L0CompactionTaskSuite))
}

type L0CompactionTaskSuite struct {
	suite.Suite

	mockAlloc          *allocator.MockAllocator
	mockMeta           *ctask.MockCompactionMeta
	mockCatalog        *mocks.DataCoordCatalog
	compactionTaskMeta *ctask.CompactionTaskMeta
}

func (s *L0CompactionTaskSuite) SetupTest() {
	s.mockMeta = ctask.NewMockCompactionMeta(s.T())
	s.mockCatalog = mocks.NewDataCoordCatalog(s.T())
	s.compactionTaskMeta = ctask.NewCompactionTaskMetaOverCatalog(s.mockCatalog)
	s.mockAlloc = allocator.NewMockAllocator(s.T())
	// s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything).Return(nil)
}

func (s *L0CompactionTaskSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *L0CompactionTaskSuite) TestSaveSegmentMetaUsesAtomicDeltalogOperator() {
	actualDeltaPath := "/tmp/milvus/insert_log/1/10/200/_delta/not-log-id-suffix"

	task := s.generateTestL0Task(datapb.CompactionTaskState_executing)
	output := []*datapb.CompactionSegment{{
		SegmentID: 200,
		Deltalogs: []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{LogID: 9001, LogPath: actualDeltaPath, EntriesNum: 3}},
		}},
	}}
	// A legacy/non-manifest destination continues through the compatibility
	// operator. StorageV3 destinations are committed by meta directly.
	s.mockMeta.EXPECT().GetSegment(mock.Anything, int64(200)).Return(nil).Once()

	s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, operators ...meta.UpdateOperator) error {
			s.Len(operators, 5)
			s.Equal(actualDeltaPath, output[0].GetDeltalogs()[0].GetBinlogs()[0].GetLogPath())
			s.EqualValues(9001, output[0].GetDeltalogs()[0].GetBinlogs()[0].GetLogID())
			return nil
		},
	).Once()

	s.NoError(task.SaveSegmentMeta(output))
}

func (s *L0CompactionTaskSuite) TestSaveSegmentMetaUpdatesLatestDataViewWithoutVersionAdvance() {
	ctx := context.Background()
	mt, err := meta.NewMemoryMeta(s.T())
	s.Require().NoError(err)
	manifestBasePath := "/tmp/milvus/insert_log/1/10/200"
	for _, segment := range []*datapb.SegmentInfo{
		{
			ID:            100,
			CollectionID:  1,
			PartitionID:   10,
			InsertChannel: "ch-1",
			State:         commonpb.SegmentState_Flushed,
			Level:         datapb.SegmentLevel_L0,
		},
		{
			ID:            101,
			CollectionID:  1,
			PartitionID:   10,
			InsertChannel: "ch-1",
			State:         commonpb.SegmentState_Flushed,
			Level:         datapb.SegmentLevel_L0,
		},
		{
			ID:            200,
			CollectionID:  1,
			PartitionID:   10,
			InsertChannel: "ch-1",
			State:         commonpb.SegmentState_Flushed,
			Level:         datapb.SegmentLevel_L1,
			ManifestPath:  packed.MarshalManifestPath(manifestBasePath, 3),
		},
	} {
		s.Require().NoError(mt.AddSegment(ctx, meta.NewSegmentInfo(segment)))
	}
	dataViewCatalog, ok := mt.Catalog.(dataview.Catalog)
	s.Require().True(ok)
	mt.DataViewManager = dataview.NewManager(dataViewCatalog, nil)
	_, err = mt.DataViewManager.OnCreateCollection(ctx, CreateCollectionDataViewEvent{
		CollectionID: 1,
		VChannels:    []string{"ch-1"},
	})
	s.Require().NoError(err)
	_, commit, abort, err := mt.DataViewManager.PrepareFlush(ctx, FlushDataViewEvent{
		CollectionID: 1,
		Segments: []dataview.LoadableSegment{{
			SegmentID:       200,
			VChannel:        "ch-1",
			PartitionID:     10,
			ManifestVersion: 0,
		}},
	})
	s.Require().NoError(err)
	s.Require().NotNil(commit)
	s.Require().NotNil(abort)
	commit()

	task := ctask.NewL0CompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        meta.NullNodeID,
		State:         datapb.CompactionTaskState_executing,
		Channel:       "ch-1",
		InputSegments: []int64{100, 101},
		Pos:           &msgpb.MsgPosition{Timestamp: 800},
	}, nil, mt, s.compactionTaskMeta)
	task.CommittedV3Manifests[200] = packed.MarshalManifestPath(manifestBasePath, 4)
	output := []*datapb.CompactionSegment{{
		SegmentID: 200,
		Deltalogs: []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{LogID: 9001, LogPath: "/tmp/milvus/delta/9001", EntriesNum: 3}},
		}},
	}}

	s.Require().NoError(task.SaveSegmentMeta(output))

	// saveSegmentMeta enqueues an async recompute; drive the queue worker's
	// projection. The L0 manifest bump (3 -> 4) hard-triggers compact_version
	// +1 even though the membership is unchanged (a Manifest-version change
	// has no dedicated counter).
	segments, err := mt.LoadableProjection(ctx, 1)
	s.Require().NoError(err)
	_, err = mt.DataViewManager.RecomputeNow(ctx, 1, func(_ context.Context, _ int64) ([]dataview.LoadableSegment, error) {
		return segments, nil
	})
	s.Require().NoError(err)

	ref, err := mt.DataViewManager.Latest(ctx, 1)
	s.Require().NoError(err)
	s.Require().NotNil(ref)
	defer ref.Deref()
	cv := ref.DataView()
	s.Equal(int64(2), cv.GetDataVersion().GetStreamingVersion())
	s.Equal(int64(1), cv.GetDataVersion().GetCompactVersion())
	s.Equal([]int64{4}, cv.GetShards()[0].GetPartitions()[0].GetSegmentManifestVersions())
}

func (s *L0CompactionTaskSuite) TestProcessRefreshPlan_NormalL0() {
	channel := "Ch-1"
	deltaLogs := []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 3)}

	s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(
		[]*meta.SegmentInfo{
			{SegmentInfo: &datapb.SegmentInfo{
				ID:            200,
				Level:         datapb.SegmentLevel_L1,
				InsertChannel: channel,
			}},
			{SegmentInfo: &datapb.SegmentInfo{
				ID:            201,
				Level:         datapb.SegmentLevel_L1,
				InsertChannel: channel,
			}},
			{SegmentInfo: &datapb.SegmentInfo{
				ID:            202,
				Level:         datapb.SegmentLevel_L1,
				InsertChannel: channel,
			}},
		},
	)

	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *meta.SegmentInfo {
		return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			Level:         datapb.SegmentLevel_L0,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Deltalogs:     deltaLogs,
		}}
	}).Times(2)
	task := ctask.NewL0CompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        1,
		State:         datapb.CompactionTaskState_executing,
		InputSegments: []int64{100, 101},
	}, nil, s.mockMeta, s.compactionTaskMeta)
	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
	task.Allocator = alloc
	plan, err := task.BuildCompactionRequest()
	s.Require().NoError(err)

	s.Equal(5, len(plan.GetSegmentBinlogs()))
	segIDs := lo.Map(plan.GetSegmentBinlogs(), func(b *datapb.CompactionSegmentBinlogs, _ int) int64 {
		return b.GetSegmentID()
	})

	s.ElementsMatch([]int64{200, 201, 202, 100, 101}, segIDs)
}

func (s *L0CompactionTaskSuite) TestProcessRefreshPlan_SegmentNotFoundL0() {
	channel := "Ch-1"
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *meta.SegmentInfo {
		return nil
	}).Once()
	task := ctask.NewL0CompactionTask(&datapb.CompactionTask{
		InputSegments: []int64{102},
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Channel:       channel,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        1,
		State:         datapb.CompactionTaskState_executing,
	}, nil, s.mockMeta, s.compactionTaskMeta)

	_, err := task.BuildCompactionRequest()
	s.Error(err)
	s.ErrorIs(err, merr.ErrSegmentNotFound)
}

func (s *L0CompactionTaskSuite) TestProcessRefreshPlan_SelectZeroSegmentsL0() {
	channel := "Ch-1"
	deltaLogs := []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 3)}
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *meta.SegmentInfo {
		return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			Level:         datapb.SegmentLevel_L0,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Deltalogs:     deltaLogs,
		}}
	}).Times(2)
	s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	task := ctask.NewL0CompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        1,
		State:         datapb.CompactionTaskState_executing,
		InputSegments: []int64{100, 101},
	}, nil, s.mockMeta, s.compactionTaskMeta)
	plan, err := task.BuildCompactionRequest()
	// Fast finish: should return a plan with only L0 segments (no error)
	s.NoError(err)
	s.Require().NotNil(plan)
	// Verify plan only contains L0 input segments (2 segments)
	s.Equal(2, len(plan.GetSegmentBinlogs()))
	segIDs := lo.Map(plan.GetSegmentBinlogs(), func(b *datapb.CompactionSegmentBinlogs, _ int) int64 {
		return b.GetSegmentID()
	})
	s.ElementsMatch([]int64{100, 101}, segIDs)
	// Verify no binlog IDs were allocated for fast finish
	s.Nil(plan.GetPreAllocatedLogIDs())
}

func (s *L0CompactionTaskSuite) TestBuildCompactionRequestFailed_AllocFailed() {
	channel := "Ch-1"
	deltaLogs := []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 3)}

	s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(
		[]*meta.SegmentInfo{
			{SegmentInfo: &datapb.SegmentInfo{
				ID:            200,
				Level:         datapb.SegmentLevel_L1,
				InsertChannel: channel,
			}},
			{SegmentInfo: &datapb.SegmentInfo{
				ID:            201,
				Level:         datapb.SegmentLevel_L1,
				InsertChannel: channel,
			}},
			{SegmentInfo: &datapb.SegmentInfo{
				ID:            202,
				Level:         datapb.SegmentLevel_L1,
				InsertChannel: channel,
			}},
		},
	)

	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *meta.SegmentInfo {
		return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			Level:         datapb.SegmentLevel_L0,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Deltalogs:     deltaLogs,
		}}
	}).Times(2)
	task := ctask.NewL0CompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        1,
		State:         datapb.CompactionTaskState_executing,
		InputSegments: []int64{100, 101},
	}, s.mockAlloc, s.mockMeta, s.compactionTaskMeta)

	s.mockAlloc.EXPECT().AllocN(mock.Anything).Return(0, 0, errors.New("mock alloc err"))

	_, err := task.BuildCompactionRequest()
	s.T().Logf("err=%v", err)
	s.Error(err)
}

func (s *L0CompactionTaskSuite) generateTestL0Task(state datapb.CompactionTaskState) *ctask.L0CompactionTask {
	return ctask.NewL0CompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        meta.NullNodeID,
		State:         state,
		Channel:       "ch-1",
		InputSegments: []int64{100, 101},
	}, s.mockAlloc, s.mockMeta, s.compactionTaskMeta)
}

func (s *L0CompactionTaskSuite) TestPorcessStateTrans() {
	s.Run("test pipelining Compaction failed", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		s.mockAlloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		channel := "ch-1"
		deltaLogs := []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 3)}

		s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(
			[]*meta.SegmentInfo{
				{SegmentInfo: &datapb.SegmentInfo{
					ID:            200,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
				}},
			},
		)

		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *meta.SegmentInfo {
			return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				Level:         datapb.SegmentLevel_L0,
				InsertChannel: channel,
				State:         commonpb.SegmentState_Flushed,
				Deltalogs:     deltaLogs,
			}}
		}).Twice()
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateCompaction(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(nodeID int64, plan *datapb.CompactionPlan, collectionID int64) error {
			s.Require().EqualValues(t.GetTaskProto().NodeID, nodeID)
			s.Require().EqualValues(t.GetTaskProto().GetCollectionID(), collectionID)
			return errors.New("mock error")
		})

		t.CreateTaskOnWorker(100, cluster)
		s.Equal(datapb.CompactionTaskState_pipelining, t.GetTaskProto().State)
		s.EqualValues(meta.NullNodeID, t.GetTaskProto().NodeID)
	})

	s.Run("test pipelining success", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		s.mockAlloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		channel := "ch-1"
		deltaLogs := []*datapb.FieldBinlog{meta.GetFieldBinlogIDs(101, 3)}

		s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(
			[]*meta.SegmentInfo{
				{SegmentInfo: &datapb.SegmentInfo{
					ID:            200,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
				}},
			},
		)

		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *meta.SegmentInfo {
			return &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				Level:         datapb.SegmentLevel_L0,
				InsertChannel: channel,
				State:         commonpb.SegmentState_Flushed,
				Deltalogs:     deltaLogs,
			}}
		}).Twice()

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateCompaction(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(nodeID int64, plan *datapb.CompactionPlan, collectionID int64) error {
			s.Require().EqualValues(t.GetTaskProto().NodeID, nodeID)
			s.Require().EqualValues(t.GetTaskProto().GetCollectionID(), collectionID)
			return nil
		})

		t.CreateTaskOnWorker(100, cluster)
		s.Equal(datapb.CompactionTaskState_executing, t.GetTaskProto().GetState())
	})

	// stay in executing state when GetCompactionPlanResults error except ErrNodeNotFound
	s.Run("test executing GetCompactionPlanResult fail NodeNotFound", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTaskProto().NodeID, mock.Anything).Return(nil, merr.WrapErrNodeNotFound(t.GetTaskProto().NodeID)).Once()

		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_pipelining, t.GetTaskProto().GetState())
		s.EqualValues(meta.NullNodeID, t.GetTaskProto().GetNodeID())
	})

	// stay in executing state when GetCompactionPlanResults error except ErrNodeNotFound
	s.Run("test executing GetCompactionPlanResult fail mock error", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTaskProto().NodeID, mock.Anything).Return(nil, errors.New("mock error"))
		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_pipelining, t.GetTaskProto().GetState())
		s.EqualValues(-1, t.GetTaskProto().GetNodeID())
	})

	s.Run("test executing with result executing", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTaskProto().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTaskProto().GetPlanID(),
				State:  datapb.CompactionTaskState_executing,
			}, nil).Once()

		t.QueryTaskOnWorker(cluster)
	})

	s.Run("test executing with result completed", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTaskProto().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTaskProto().GetPlanID(),
				State:  datapb.CompactionTaskState_completed,
			}, nil).Once()

		s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Times(2)
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, false).Return().Once()

		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_completed, t.GetTaskProto().GetState())
	})
	s.Run("test executing with result completed save segment meta failed", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTaskProto().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTaskProto().GetPlanID(),
				State:  datapb.CompactionTaskState_completed,
			}, nil).Once()

		s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("mock error")).Once()

		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_executing, t.GetTaskProto().GetState())
	})
	s.Run("test executing with result completed save compaction meta failed", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTaskProto().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTaskProto().GetPlanID(),
				State:  datapb.CompactionTaskState_completed,
			}, nil).Once()

		s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_executing, t.GetTaskProto().GetState())
	})

	s.Run("test executing with result failed", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTaskProto().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTaskProto().GetPlanID(),
				State:  datapb.CompactionTaskState_failed,
			}, nil).Once()

		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_failed, t.GetTaskProto().GetState())
		s.Equal("compaction failed in datanode", t.GetTaskProto().GetFailReason())
	})
	s.Run("test executing with result failed save compaction meta failed", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTaskProto().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTaskProto().GetPlanID(),
				State:  datapb.CompactionTaskState_failed,
			}, nil).Once()
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_executing, t.GetTaskProto().GetState())
	})

	s.Run("test metaSaved success", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_meta_saved)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, false).RunAndReturn(func(ctx context.Context, segIDs []int64, isCompacting bool) {
			s.ElementsMatch(segIDs, t.GetTaskProto().GetInputSegments())
		}).Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetTaskProto().GetState())
	})

	s.Run("test metaSaved failed", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		t := s.generateTestL0Task(datapb.CompactionTaskState_meta_saved)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_meta_saved, t.GetTaskProto().GetState())
	})

	s.Run("test complete drop failed", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_completed)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, false).RunAndReturn(func(ctx context.Context, segIDs []int64, isCompacting bool) {
			s.ElementsMatch(segIDs, t.GetTaskProto().GetInputSegments())
		}).Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetTaskProto().GetState())
	})

	s.Run("test complete success", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_completed)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, false).RunAndReturn(func(ctx context.Context, segIDs []int64, isCompacting bool) {
			s.ElementsMatch(segIDs, t.GetTaskProto().GetInputSegments())
		}).Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetTaskProto().GetState())
	})

	s.Run("test process failed success", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_failed)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_failed, t.GetTaskProto().GetState())
	})

	s.Run("test process failed failed", func() {
		s.mockCatalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_failed)
		t.UpdateAndSaveTaskMeta(ctask.SetNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_failed, t.GetTaskProto().GetState())
	})

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
			t := s.generateTestL0Task(tc.state)
			res := t.Process()
			s.Equal(tc.processResult, res)
		}
	})
}

// TestSelectFlushedSegment_ForceSelectAllFlag exercises the flag end-to-end:
// build an L0 view, call Trigger() with the flag off / on, feed the resulting
// latestDeletePos into an l0CompactionTask (the same wiring
// compaction_trigger_v2.go uses), and verify selectFlushedSegment's output
// actually changes based on the flag. A high-StartPosition segment (the
// shape silently dropped by the import-position bug) is excluded with the
// flag off and included with the flag on.
func (s *L0CompactionTaskSuite) TestSelectFlushedSegment_ForceSelectAllFlag() {
	paramtable.Init()
	const flagKey = "dataCoord.compaction.levelzero.forceSelectAllSegments"

	channel := "ch-1"
	collectionID := int64(1)
	partitionID := int64(10)
	label := &view.CompactionGroupLabel{
		CollectionID: collectionID,
		PartitionID:  partitionID,
		Channel:      channel,
	}

	// Flushed L1 segments visible to selectFlushedSegment. The one with
	// StartPosition > realL0DmlTs is the case we care about — it would
	// normally be filtered out by `startPos < taskPos`.
	const realL0DmlTs = uint64(5000)
	lowPosSeg := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            200,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channel,
		Level:         datapb.SegmentLevel_L1,
		State:         commonpb.SegmentState_Flushed,
		StartPosition: &msgpb.MsgPosition{ChannelName: channel, Timestamp: 3000},
	}}
	highPosSeg := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            201,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channel,
		Level:         datapb.SegmentLevel_L1,
		State:         commonpb.SegmentState_Flushed,
		StartPosition: &msgpb.MsgPosition{ChannelName: channel, Timestamp: 20000},
	}}

	// Mockery's SelectSegments returns whatever we tell it without applying
	// filters. We need the real filter logic to run so the `startPos <
	// taskPos` predicate is actually exercised — install RunAndReturn that
	// evaluates each SegmentFilter.Match against our fixed segment set.
	installFilteringMock := func() {
		s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, filters ...meta.SegmentFilter) []*meta.SegmentInfo {
				all := []*meta.SegmentInfo{lowPosSeg, highPosSeg}
				result := make([]*meta.SegmentInfo, 0, len(all))
				for _, seg := range all {
					matched := true
					for _, f := range filters {
						if !f.Match(seg) {
							matched = false
							break
						}
					}
					if matched {
						result = append(result, seg)
					}
				}
				return result
			})
	}

	// Build an L0 view with a couple of L0 segments whose dmlPos is
	// `realL0DmlTs`. Trigger() runs resolveLatestDeletePos under the current
	// flag value, so the returned view's latestDeletePos reflects the flag.
	buildView := func() *view.LevelZeroCompactionView {
		l0Segs := []*view.SegmentView{
			{
				ID:            100,
				Label:         label,
				DmlPos:        &msgpb.MsgPosition{ChannelName: channel, Timestamp: realL0DmlTs},
				Level:         datapb.SegmentLevel_L0,
				State:         commonpb.SegmentState_Flushed,
				DeltalogCount: 100,
				DeltaSize:     1,
				DeltaRowCount: 1,
			},
			{
				ID:            101,
				Label:         label,
				DmlPos:        &msgpb.MsgPosition{ChannelName: channel, Timestamp: realL0DmlTs},
				Level:         datapb.SegmentLevel_L0,
				State:         commonpb.SegmentState_Flushed,
				DeltalogCount: 100,
				DeltaSize:     1,
				DeltaRowCount: 1,
			},
		}
		return &view.LevelZeroCompactionView{
			Label:           label,
			L0Segments:      l0Segs,
			LatestDeletePos: &msgpb.MsgPosition{ChannelName: channel, Timestamp: realL0DmlTs},
			TriggerID:       19530,
		}
	}

	// Mirrors compaction_trigger_v2.go:406 — feed the triggered view's
	// latestDeletePos into task.Pos and run selectFlushedSegment.
	runSelectWithTriggeredPos := func() []int64 {
		installFilteringMock()
		srcView := buildView()
		triggered, reason := srcView.Trigger()
		s.Require().NotNil(triggered, "Trigger returned nil: %s", reason)
		triggeredView := triggered.(*view.LevelZeroCompactionView)

		task := ctask.NewL0CompactionTask(&datapb.CompactionTask{
			PlanID:        1,
			TriggerID:     19530,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			Channel:       channel,
			Type:          datapb.CompactionType_Level0DeleteCompaction,
			NodeID:        1,
			State:         datapb.CompactionTaskState_executing,
			InputSegments: []int64{100, 101},
			Pos:           triggeredView.LatestDeletePos,
		}, nil, s.mockMeta, s.compactionTaskMeta)

		flushed, _, err := task.SelectFlushedSegment()
		s.Require().NoError(err)
		return lo.Map(flushed, func(seg *meta.SegmentInfo, _ int) int64 { return seg.GetID() })
	}

	s.Run("flag_off_excludes_high_start_position_segment", func() {
		paramtable.Get().Save(flagKey, "false")
		defer paramtable.Get().Reset(flagKey)

		gotIDs := runSelectWithTriggeredPos()
		s.ElementsMatch([]int64{200}, gotIDs,
			"with flag off, segment 201 (StartPosition=20000 > taskPos=%d) must be excluded", realL0DmlTs)
	})

	s.Run("flag_on_includes_high_start_position_segment", func() {
		paramtable.Get().Save(flagKey, "true")
		defer paramtable.Get().Reset(flagKey)

		gotIDs := runSelectWithTriggeredPos()
		s.ElementsMatch([]int64{200, 201}, gotIDs,
			"with flag on, resolveLatestDeletePos must lift taskPos so segment 201 is included")
	})
}
