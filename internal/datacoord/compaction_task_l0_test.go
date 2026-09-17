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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestL0CompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(L0CompactionTaskSuite))
}

func TestL0CompactionCommitsDeltalogsToV3Manifest(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/200"
	oldManifest := packed.MarshalManifestPath(basePath, 7)
	newManifest := packed.MarshalManifestPath(basePath, 8)
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             200,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))

	deltaPath := basePath + "/_delta/9001"
	deltalogs := []*datapb.FieldBinlog{{
		Binlogs: []*datapb.Binlog{{LogID: 9001, LogPath: deltaPath, EntriesNum: 3, MemorySize: 128}},
	}}
	commit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (string, error) {
			require.Equal(t, basePath, base)
			require.EqualValues(t, 7, version)
			require.Equal(t, []packed.DeltaLogEntry{{Path: deltaPath, NumEntries: 3}}, updates.DeltaLogs)
			return newManifest, nil
		},
	).Build()
	defer commit.UnPatch()

	task := &datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_Level0DeleteCompaction}
	_, _, err = meta.CompleteCompactionMutation(context.Background(), task, &datapb.CompactionPlanResult{
		Segments: []*datapb.CompactionSegment{{SegmentID: 200, Deltalogs: deltalogs}},
	})
	require.NoError(t, err)

	updated := meta.GetSegment(context.Background(), 200)
	require.Equal(t, newManifest, updated.GetManifestPath())
	require.EqualValues(t, 3, updated.GetStats().GetDeleteNumRows())
	require.Empty(t, updated.GetDeltalogs()[0].GetBinlogs()[0].GetLogPath())
}

func TestL0CompactionV3ManifestCommitIsIdempotentOnRetry(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/201"
	oldManifest := packed.MarshalManifestPath(basePath, 7)
	newManifest := packed.MarshalManifestPath(basePath, 8)
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             201,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))

	deltaPath := basePath + "/_delta/9001"
	// Each attempt receives a fresh result, as a re-query of the worker would.
	freshDeltalogs := func() []*datapb.FieldBinlog {
		return []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{LogID: 9001, LogPath: deltaPath, EntriesNum: 3, MemorySize: 128}},
		}}
	}

	var commitCount int
	commit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(_ string, _ int64, _ *indexpb.StorageConfig, _ *packed.ManifestUpdates) (string, error) {
			commitCount++
			return newManifest, nil
		},
	).Build()
	defer commit.UnPatch()

	task := &datapb.CompactionTask{PlanID: 2, Type: datapb.CompactionType_Level0DeleteCompaction}
	result := func() *datapb.CompactionPlanResult {
		return &datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{{SegmentID: 201, Deltalogs: freshDeltalogs()}},
		}
	}
	// First attempt publishes the manifest and records the deltalog on the segment.
	_, _, err = meta.CompleteCompactionMutation(context.Background(), task, result())
	require.NoError(t, err)
	// A retry (saveSegmentMeta re-run after a failed meta_saved/etcd write) with
	// the same output must not append the deltalog to the manifest a second time.
	_, _, err = meta.CompleteCompactionMutation(context.Background(), task, result())
	require.NoError(t, err)

	require.Equal(t, 1, commitCount, "manifest must be committed exactly once across retries")
	updated := meta.GetSegment(context.Background(), 201)
	require.Equal(t, newManifest, updated.GetManifestPath())
	require.EqualValues(t, 3, updated.GetStats().GetDeleteNumRows(), "delete count must not double on retry")
	require.Len(t, updated.GetDeltalogs(), 1)
	require.Len(t, updated.GetDeltalogs()[0].GetBinlogs(), 1)
}

func addL0SaveMetaFixture(t *testing.T, mt *meta, inputIDs []int64, targets ...*datapb.SegmentInfo) {
	t.Helper()
	for _, id := range inputIDs {
		require.NoError(t, mt.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
			ID:    id,
			State: commonpb.SegmentState_Flushed,
			Level: datapb.SegmentLevel_L0,
		})))
	}
	for _, target := range targets {
		require.NoError(t, mt.AddSegment(context.Background(), NewSegmentInfo(target)))
	}
}

func l0DeltaOutput(segmentID int64, basePath string) *datapb.CompactionSegment {
	return &datapb.CompactionSegment{
		SegmentID: segmentID,
		Deltalogs: []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{LogID: 9001, LogPath: basePath + "/_delta/9001", EntriesNum: 3, MemorySize: 128}},
		}},
	}
}

func requireL0InputsRetired(t *testing.T, mt *meta, inputIDs []int64) {
	t.Helper()
	for _, id := range inputIDs {
		seg := mt.GetSegment(context.Background(), id)
		require.Equal(t, commonpb.SegmentState_Dropped, seg.GetState())
		require.True(t, seg.GetCompacted())
	}
}

// A V3 delta target retired (dropped) by a concurrent compaction while the L0
// plan was executing must not wedge the task: saveSegmentMeta skips it before
// any manifest I/O, and the input L0 segments are still retired so the task
// reaches meta_saved instead of re-polling a permanent error forever.
func TestL0CompactionSaveSegmentMetaSkipsDroppedV3Target(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/240"
	manifest7 := packed.MarshalManifestPath(basePath, 7)
	mt, err := newMemoryMeta(t)
	require.NoError(t, err)
	inputs := []int64{140, 141}
	addL0SaveMetaFixture(t, mt, inputs, &datapb.SegmentInfo{
		ID:             240,
		State:          commonpb.SegmentState_Dropped,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifest7,
	})

	var commitCount atomic.Int32
	mockCommit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, _ *packed.ManifestUpdates) (string, error) {
			commitCount.Add(1)
			return packed.MarshalManifestPath(base, version+1), nil
		}).Build()
	defer mockCommit.UnPatch()

	task := newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		InputSegments: inputs,
	}, nil, mt)

	require.NoError(t, task.saveSegmentMeta([]*datapb.CompactionSegment{l0DeltaOutput(240, basePath)}))

	require.Zero(t, commitCount.Load(), "dropped target must not reach CommitSegmentManifests")
	require.Equal(t, manifest7, mt.GetSegment(context.Background(), 240).GetManifestPath())
	requireL0InputsRetired(t, mt, inputs)
}

// A target that passes the saveSegmentMeta health check but drops before the
// commit lands is skipped inside CommitSegmentManifests as a benign terminal
// outcome, so the batch still returns success. saveSegmentMeta must invoke the
// batch for the healthy target and let the input segments retire on its success;
// the per-target ErrSegmentNotFound swallow now lives in the primitive
// (TestCommitSegmentManifestsSkipsDroppedSegment).
func TestL0CompactionSaveSegmentMetaSwallowsNotFoundFromManifestCommit(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/241"
	mt, err := newMemoryMeta(t)
	require.NoError(t, err)
	inputs := []int64{142, 143}
	addL0SaveMetaFixture(t, mt, inputs, &datapb.SegmentInfo{
		ID:             241,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(basePath, 7),
	})

	var batchCalls atomic.Int32
	mockCommit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, _ *packed.ManifestUpdates) (string, error) {
			batchCalls.Add(1)
			require.Equal(t, basePath, base)
			require.EqualValues(t, 7, version)
			// Drop the target after the manifest revision is prepared but before
			// the final catalog publication. The batch must skip that target while
			// still retiring the L0 inputs in the same catalog transaction.
			require.NoError(t, mt.UpdateSegmentsInfo(context.Background(),
				UpdateStatusOperator(241, commonpb.SegmentState_Dropped)))
			return packed.MarshalManifestPath(basePath, 8), nil
		}).Build()
	defer mockCommit.UnPatch()

	task := newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		InputSegments: inputs,
	}, nil, mt)

	require.NoError(t, task.saveSegmentMeta([]*datapb.CompactionSegment{l0DeltaOutput(241, basePath)}))

	require.EqualValues(t, 1, batchCalls.Load())
	requireL0InputsRetired(t, mt, inputs)
}

// Any manifest commit failure other than a vanished segment must keep failing
// the save so the scheduler retries: the input segments stay live and the task
// does not reach meta_saved on a partially published result.
func TestL0CompactionSaveSegmentMetaFailsOnManifestCommitError(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/242"
	mt, err := newMemoryMeta(t)
	require.NoError(t, err)
	inputs := []int64{144}
	addL0SaveMetaFixture(t, mt, inputs, &datapb.SegmentInfo{
		ID:             242,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(basePath, 7),
	})

	mockCommit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(string, int64, *indexpb.StorageConfig, *packed.ManifestUpdates) (string, error) {
			return "", merr.WrapErrServiceInternalMsg("manifest commit failed")
		}).Build()
	defer mockCommit.UnPatch()

	task := newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		InputSegments: inputs,
	}, nil, mt)

	require.Error(t, task.saveSegmentMeta([]*datapb.CompactionSegment{l0DeltaOutput(242, basePath)}))

	seg := mt.GetSegment(context.Background(), 144)
	require.Equal(t, commonpb.SegmentState_Flushed, seg.GetState(), "inputs must not retire on a failed save")
	require.False(t, seg.GetCompacted())
}

// The batch's manifest generation must overlap rather than run serially: each
// mocked loon transaction (CommitSegmentManifests stage 2) blocks until all
// targets have entered, so a serial implementation stalls on the first and fails
// via the timeout error instead of hanging. This drives the real primitive end to
// end — atomic multi-lock acquisition, then the parallel per-target manifest I/O.
func TestL0CompactionSaveSegmentMetaCommitsV3TargetsInParallel(t *testing.T) {
	const targets = 3
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.L0ManifestUpdatePoolSize.Key, "16")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.L0ManifestUpdatePoolSize.Key)

	mt, err := newMemoryMeta(t)
	require.NoError(t, err)
	inputs := []int64{145}
	basePaths := make(map[int64]string, targets)
	targetInfos := make([]*datapb.SegmentInfo, 0, targets)
	output := make([]*datapb.CompactionSegment, 0, targets)
	for i := int64(0); i < targets; i++ {
		segID := 243 + i
		basePath := fmt.Sprintf("/tmp/milvus/insert_log/1/10/%d", segID)
		basePaths[segID] = basePath
		targetInfos = append(targetInfos, &datapb.SegmentInfo{
			ID:             segID,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath(basePath, 7),
		})
		output = append(output, l0DeltaOutput(segID, basePath))
	}
	addL0SaveMetaFixture(t, mt, inputs, targetInfos...)

	release := make(chan struct{})
	var entered atomic.Int32
	mockCommit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, _ *packed.ManifestUpdates) (string, error) {
			if entered.Add(1) == targets {
				close(release)
			}
			select {
			case <-release:
				return packed.MarshalManifestPath(base, version+1), nil
			case <-time.After(30 * time.Second):
				return "", errors.New("v3 manifest commits did not overlap; batch fan-out is serial")
			}
		}).Build()
	defer mockCommit.UnPatch()

	task := newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		InputSegments: inputs,
	}, nil, mt)

	require.NoError(t, task.saveSegmentMeta(output))

	require.EqualValues(t, targets, entered.Load())
	requireL0InputsRetired(t, mt, inputs)
}

type L0CompactionTaskSuite struct {
	suite.Suite

	mockAlloc *allocator.MockAllocator
	mockMeta  *MockCompactionMeta
}

func (s *L0CompactionTaskSuite) SetupTest() {
	s.mockMeta = NewMockCompactionMeta(s.T())
	s.mockAlloc = allocator.NewMockAllocator(s.T())
	// s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil)
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
	s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.MatchedBy(func(result *datapb.CompactionPlanResult) bool {
		segments := result.GetSegments()
		return len(segments) == 1 &&
			segments[0].GetSegmentID() == 200 &&
			segments[0].GetDeltalogs()[0].GetBinlogs()[0].GetLogPath() == actualDeltaPath &&
			segments[0].GetDeltalogs()[0].GetBinlogs()[0].GetLogID() == 9001
	})).Return(nil, nil, nil).Once()

	s.NoError(task.saveSegmentMeta(output))
	s.Equal(datapb.CompactionTaskState_meta_saved, task.GetTask().GetState())
}

func (s *L0CompactionTaskSuite) TestSaveSegmentMetaRetriesAtomicMutation() {
	task := s.generateTestL0Task(datapb.CompactionTaskState_executing)
	output := []*datapb.CompactionSegment{{SegmentID: 200}, {SegmentID: 201}}

	first := true
	s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, task *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {
			s.Require().Len(result.GetSegments(), 2)
			if first {
				first = false
				s.Empty(result.GetSegments()[0].GetManifest())
				result.GetSegments()[0].Manifest = "prepared-manifest"
				return nil, nil, errors.New("atomic mutation failed")
			}
			s.Equal("prepared-manifest", result.GetSegments()[0].GetManifest(),
				"a retry reuses a manifest revision prepared by the partial attempt")
			s.Empty(result.GetSegments()[1].GetManifest())
			return nil, nil, nil
		},
	).Twice()

	s.Error(task.saveSegmentMeta(output))
	s.Equal(datapb.CompactionTaskState_executing, task.GetTask().GetState())

	s.NoError(task.saveSegmentMeta(output))
	s.Equal(datapb.CompactionTaskState_meta_saved, task.GetTask().GetState())
}

func (s *L0CompactionTaskSuite) TestProcessRefreshPlan_NormalL0() {
	channel := "Ch-1"
	deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

	s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(
		[]*SegmentInfo{
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

	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			Level:         datapb.SegmentLevel_L0,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Deltalogs:     deltaLogs,
		}}
	}).Times(2)
	task := newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        1,
		State:         datapb.CompactionTaskState_executing,
		InputSegments: []int64{100, 101},
	}, nil, s.mockMeta)
	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
	task.allocator = alloc
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
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
		return nil
	}).Once()
	task := newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		InputSegments: []int64{102},
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Channel:       channel,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        1,
		State:         datapb.CompactionTaskState_executing,
	}, nil, s.mockMeta)

	_, err := task.BuildCompactionRequest()
	s.Error(err)
	s.ErrorIs(err, merr.ErrSegmentNotFound)
}

func (s *L0CompactionTaskSuite) TestProcessRefreshPlan_SelectZeroSegmentsL0() {
	channel := "Ch-1"
	deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			Level:         datapb.SegmentLevel_L0,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Deltalogs:     deltaLogs,
		}}
	}).Times(2)
	s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	task := newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        1,
		State:         datapb.CompactionTaskState_executing,
		InputSegments: []int64{100, 101},
	}, nil, s.mockMeta)
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
	deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

	s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(
		[]*SegmentInfo{
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

	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			Level:         datapb.SegmentLevel_L0,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Deltalogs:     deltaLogs,
		}}
	}).Times(2)
	task := newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        1,
		State:         datapb.CompactionTaskState_executing,
		InputSegments: []int64{100, 101},
	}, s.mockAlloc, s.mockMeta)

	s.mockAlloc.EXPECT().AllocN(mock.Anything).Return(0, 0, errors.New("mock alloc err"))

	_, err := task.BuildCompactionRequest()
	s.T().Logf("err=%v", err)
	s.Error(err)
}

func (s *L0CompactionTaskSuite) generateTestL0Task(state datapb.CompactionTaskState) *l0CompactionTask {
	return newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        NullNodeID,
		State:         state,
		Channel:       "ch-1",
		InputSegments: []int64{100, 101},
	}, s.mockAlloc, s.mockMeta)
}

func (s *L0CompactionTaskSuite) TestPorcessStateTrans() {
	s.Run("test pipelining Compaction failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		s.mockAlloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.updateAndSaveTaskMeta(setNodeID(100))
		channel := "ch-1"
		deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

		s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(
			[]*SegmentInfo{
				{SegmentInfo: &datapb.SegmentInfo{
					ID:            200,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
				}},
			},
		)

		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				Level:         datapb.SegmentLevel_L0,
				InsertChannel: channel,
				State:         commonpb.SegmentState_Flushed,
				Deltalogs:     deltaLogs,
			}}
		}).Twice()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)

		cluster := session.NewMockCluster(s.T())
		// A worker-sent rejection (a milvus error) does prove the plan was not
		// accepted, but reusing this planID on another node is only safe if that
		// classification is correct. A fresh planID makes the classification
		// unnecessary: this attempt is abandoned exactly as an unreadable outcome
		// would be; see the dedicated case below.
		cluster.EXPECT().CreateCompaction(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(nodeID int64, plan *datapb.CompactionPlan, collectionID int64) error {
			s.Require().EqualValues(t.GetTask().NodeID, nodeID)
			s.Require().EqualValues(t.GetTask().GetCollectionID(), collectionID)
			return merr.WrapErrServiceInternalMsg("mock rejection")
		})

		t.CreateTaskOnWorker(100, cluster)
		s.Equal(datapb.CompactionTaskState_retrying, t.GetTask().State)
	})

	s.Run("test pipelining success", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		s.mockAlloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.updateAndSaveTaskMeta(setNodeID(100))
		channel := "ch-1"
		deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

		s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(
			[]*SegmentInfo{
				{SegmentInfo: &datapb.SegmentInfo{
					ID:            200,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
				}},
			},
		)

		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				Level:         datapb.SegmentLevel_L0,
				InsertChannel: channel,
				State:         commonpb.SegmentState_Flushed,
				Deltalogs:     deltaLogs,
			}}
		}).Twice()

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateCompaction(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(nodeID int64, plan *datapb.CompactionPlan, collectionID int64) error {
			s.Require().EqualValues(t.GetTask().NodeID, nodeID)
			s.Require().EqualValues(t.GetTask().GetCollectionID(), collectionID)
			return nil
		})

		t.CreateTaskOnWorker(100, cluster)
		s.Equal(datapb.CompactionTaskState_executing, t.GetTask().GetState())
	})

	// stay in executing state when GetCompactionPlanResults error except ErrNodeNotFound
	s.Run("test executing GetCompactionPlanResult fail NodeNotFound", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTask().NodeID, mock.Anything).Return(nil, merr.WrapErrNodeNotFound(t.GetTask().NodeID)).Once()

		t.QueryTaskOnWorker(cluster)
		// A DataNode deregisters from etcd before tearing down its running
		// compactions, so the node being gone from the registry does not prove
		// the plan stopped. Abandon the attempt instead of re-dispatching it.
		s.Equal(datapb.CompactionTaskState_retrying, t.GetTask().GetState())
		s.EqualValues(100, t.GetTask().GetNodeID())
	})

	// An unanswered round -- transport error or nil result -- ends the attempt,
	// same as the create path.
	s.Run("test executing GetCompactionPlanResult fail mock error", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTask().NodeID, mock.Anything).Return(nil, errors.New("mock error")).Once()
		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_retrying, t.GetTask().GetState())
	})

	s.Run("test executing GetCompactionPlanResult nil result", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTask().NodeID, mock.Anything).
			Return(nil, nil).Once()
		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_retrying, t.GetTask().GetState())
	})

	s.Run("test executing with result executing", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTask().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTask().GetPlanID(),
				State:  datapb.CompactionTaskState_executing,
			}, nil).Once()

		t.QueryTaskOnWorker(cluster)
	})

	s.Run("test executing with result completed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTask().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTask().GetPlanID(),
				State:  datapb.CompactionTaskState_completed,
			}, nil).Once()

		s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, nil).Once()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		// No SetSegmentsCompacting expectation: releasing the inputs belongs to
		// doClean alone, so reaching completed must not unlock them. An
		// unexpected call here fails the test.

		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_completed, t.GetTask().GetState())
	})
	s.Run("test executing with result completed save segment meta failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTask().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTask().GetPlanID(),
				State:  datapb.CompactionTaskState_completed,
			}, nil).Once()

		s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).
			Return(nil, nil, errors.New("mock error")).Once()

		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_executing, t.GetTask().GetState())
	})
	s.Run("test executing with result completed process meta_saved failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTask().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTask().GetPlanID(),
				State:  datapb.CompactionTaskState_completed,
			}, nil).Once()

		s.mockMeta.EXPECT().ValidateSegmentStateBeforeCompleteCompactionMutation(mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, nil).Once()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_meta_saved, t.GetTask().GetState())
	})

	s.Run("test executing with result failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTask().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTask().GetPlanID(),
				State:  datapb.CompactionTaskState_failed,
			}, nil).Once()

		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_retrying, t.GetTask().GetState())
	})
	s.Run("test executing with result failed save compaction meta failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(t.GetTask().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTask().GetPlanID(),
				State:  datapb.CompactionTaskState_failed,
			}, nil).Once()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

		t.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_executing, t.GetTask().GetState())
	})

	s.Run("test metaSaved success", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_meta_saved)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)

		// Reaching completed must not unlock the inputs; doClean does that once.
		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetTask().GetState())
	})

	s.Run("test metaSaved failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		t := s.generateTestL0Task(datapb.CompactionTaskState_meta_saved)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)

		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_meta_saved, t.GetTask().GetState())
	})

	s.Run("test complete drop failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_completed)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)
		// Reaching completed must not unlock the inputs; doClean does that once.
		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetTask().GetState())
	})

	s.Run("test complete success", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_completed)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)
		// Reaching completed must not unlock the inputs; doClean does that once.
		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetTask().GetState())
	})

	s.Run("test process failed success", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_failed)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_failed, t.GetTask().GetState())
	})

	s.Run("test process failed failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_failed)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTask().GetNodeID() > 0)

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_failed, t.GetTask().GetState())
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
	label := &CompactionGroupLabel{
		CollectionID: collectionID,
		PartitionID:  partitionID,
		Channel:      channel,
	}

	// Flushed L1 segments visible to selectFlushedSegment. The one with
	// StartPosition > realL0DmlTs is the case we care about — it would
	// normally be filtered out by `startPos < taskPos`.
	const realL0DmlTs = uint64(5000)
	lowPosSeg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            200,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channel,
		Level:         datapb.SegmentLevel_L1,
		State:         commonpb.SegmentState_Flushed,
		StartPosition: &msgpb.MsgPosition{ChannelName: channel, Timestamp: 3000},
	}}
	highPosSeg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
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
			RunAndReturn(func(ctx context.Context, filters ...SegmentFilter) []*SegmentInfo {
				all := []*SegmentInfo{lowPosSeg, highPosSeg}
				result := make([]*SegmentInfo, 0, len(all))
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
	buildView := func() *LevelZeroCompactionView {
		l0Segs := []*SegmentView{
			{
				ID:            100,
				label:         label,
				dmlPos:        &msgpb.MsgPosition{ChannelName: channel, Timestamp: realL0DmlTs},
				Level:         datapb.SegmentLevel_L0,
				State:         commonpb.SegmentState_Flushed,
				DeltalogCount: 100,
				DeltaSize:     1,
				DeltaRowCount: 1,
			},
			{
				ID:            101,
				label:         label,
				dmlPos:        &msgpb.MsgPosition{ChannelName: channel, Timestamp: realL0DmlTs},
				Level:         datapb.SegmentLevel_L0,
				State:         commonpb.SegmentState_Flushed,
				DeltalogCount: 100,
				DeltaSize:     1,
				DeltaRowCount: 1,
			},
		}
		return &LevelZeroCompactionView{
			label:           label,
			l0Segments:      l0Segs,
			latestDeletePos: &msgpb.MsgPosition{ChannelName: channel, Timestamp: realL0DmlTs},
			triggerID:       19530,
		}
	}

	// Mirrors compaction_trigger_v2.go:406 — feed the triggered view's
	// latestDeletePos into task.Pos and run selectFlushedSegment.
	runSelectWithTriggeredPos := func() []int64 {
		installFilteringMock()
		srcView := buildView()
		triggered, reason := srcView.Trigger()
		s.Require().NotNil(triggered, "Trigger returned nil: %s", reason)
		triggeredView := triggered.(*LevelZeroCompactionView)

		task := newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
			PlanID:        1,
			TriggerID:     19530,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			Channel:       channel,
			Type:          datapb.CompactionType_Level0DeleteCompaction,
			NodeID:        1,
			State:         datapb.CompactionTaskState_executing,
			InputSegments: []int64{100, 101},
			Pos:           triggeredView.latestDeletePos,
		}, nil, s.mockMeta)

		flushed, _, err := task.selectFlushedSegment()
		s.Require().NoError(err)
		return lo.Map(flushed, func(seg *SegmentInfo, _ int) int64 { return seg.GetID() })
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

// TestSelectFlushedSegment_RespectsCommitTimestamp verifies that import segments
// with a commit_timestamp are excluded from L0 compaction when the trigger
// position is before the commit_timestamp.
func TestSelectFlushedSegment_RespectsCommitTimestamp(t *testing.T) {
	channel := "ch-1"

	// Import segment: start_position.ts=1000, commit_ts=5000.
	// Its effective timestamp is 5000 (controlled by segmentEffectiveTs).
	importSeg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:              777,
		CollectionID:    1,
		PartitionID:     10,
		InsertChannel:   channel,
		State:           commonpb.SegmentState_Flushed,
		Level:           datapb.SegmentLevel_L1,
		CommitTimestamp: 5000,
		StartPosition:   &msgpb.MsgPosition{Timestamp: 1000},
	}}

	// applyFilters applies a SegmentFilter slice to a candidate list,
	// mirroring what meta.SelectSegments does internally.
	applyFilters := func(candidates []*SegmentInfo, filters ...SegmentFilter) []*SegmentInfo {
		var result []*SegmentInfo
		for _, seg := range candidates {
			pass := true
			for _, f := range filters {
				if !f.Match(seg) {
					pass = false
					break
				}
			}
			if pass {
				result = append(result, seg)
			}
		}
		return result
	}

	makeTask := func(triggerTs uint64) *l0CompactionTask {
		mockAlloc := allocator.NewMockAllocator(t)
		mockMeta := NewMockCompactionMeta(t)
		mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, filters ...SegmentFilter) []*SegmentInfo {
				return applyFilters([]*SegmentInfo{importSeg}, filters...)
			})
		return newL0CompactionTask(context.TODO(), &datapb.CompactionTask{
			PlanID:       1,
			TriggerID:    19530,
			CollectionID: 1,
			PartitionID:  10,
			Type:         datapb.CompactionType_Level0DeleteCompaction,
			Channel:      channel,
			Pos:          &msgpb.MsgPosition{Timestamp: triggerTs},
		}, mockAlloc, mockMeta)
	}

	t.Run("import segment not selected when trigger pos < commit_timestamp", func(t *testing.T) {
		// triggerTs=3000 < commit_ts=5000 → segment must NOT be selected
		task := makeTask(3000)
		selected, _, err := task.selectFlushedSegment()
		assert.NoError(t, err)
		assert.Empty(t, selected, "import segment with commit_ts=5000 must not be selected at triggerTs=3000")
	})

	t.Run("import segment selected when trigger pos > commit_timestamp", func(t *testing.T) {
		// triggerTs=6000 > commit_ts=5000 → segment must be selected
		task := makeTask(6000)
		selected, _, err := task.selectFlushedSegment()
		assert.NoError(t, err)
		assert.Len(t, selected, 1, "import segment with commit_ts=5000 must be selected at triggerTs=6000")
		assert.Equal(t, int64(777), selected[0].GetID())
	})
}
