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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestL0CompactionCommitsDeltalogsToV3Manifest(t *testing.T) {
	mockManifestUpdateSubmissions(t)
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
	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).To(
		func(_ context.Context, _ *packed.ManifestIOContext, base string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
			require.Equal(t, basePath, base)
			require.EqualValues(t, 7, version)
			require.Equal(t, []packed.DeltaLogEntry{{Path: deltaPath, NumEntries: 3}}, updates.DeltaLogs)
			return mockManifestUpdateResult(newManifest, updates), nil
		},
	).Build()
	defer commit.UnPatch()

	task := &l0CompactionTask{meta: meta, committedV3Manifests: make(map[int64]string)}
	require.NoError(t, task.commitL0V3DeltalogsBatch(context.Background(), map[int64][]*datapb.FieldBinlog{200: deltalogs}))

	updated := meta.GetSegment(context.Background(), 200)
	require.Equal(t, newManifest, updated.GetManifestPath())
	require.EqualValues(t, 3, updated.GetStats().GetDeleteNumRows())
	require.Empty(t, updated.GetDeltalogs()[0].GetBinlogs()[0].GetLogPath())
}

func TestL0CompactionV3ManifestCommitIsIdempotentOnRetry(t *testing.T) {
	mockManifestUpdateSubmissions(t)
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
	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).To(
		func(_ context.Context, _ *packed.ManifestIOContext, _ string, _ int64, _ *indexpb.StorageConfig, _ *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
			commitCount++
			return mockManifestUpdateResult(newManifest, nil), nil
		},
	).Build()
	defer commit.UnPatch()

	task := &l0CompactionTask{meta: meta}
	// First attempt publishes the manifest and records the deltalog on the segment.
	require.NoError(t, task.commitL0V3DeltalogsBatch(context.Background(), map[int64][]*datapb.FieldBinlog{201: freshDeltalogs()}))
	// A retry (saveSegmentMeta re-run after a failed meta_saved/etcd write) with
	// the same output must not append the deltalog to the manifest a second time.
	require.NoError(t, task.commitL0V3DeltalogsBatch(context.Background(), map[int64][]*datapb.FieldBinlog{201: freshDeltalogs()}))

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
	mockCommit := mockey.Mock((*meta).CommitSegmentManifests).To(
		func(m *meta, ctx context.Context, commits []SegmentManifestCommit, extraOps ...UpdateOperator) error {
			commitCount.Add(int32(len(commits)))
			return nil
		}).Build()
	defer mockCommit.UnPatch()

	task := newL0CompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		InputSegments: inputs,
	}, nil, mt, nil)

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
	mockCommit := mockey.Mock((*meta).CommitSegmentManifests).To(
		func(m *meta, ctx context.Context, commits []SegmentManifestCommit, extraOps ...UpdateOperator) error {
			batchCalls.Add(1)
			// The primitive skips a target that vanished during manifest I/O and
			// still returns success, but the input-segment retirement folded into
			// the same batch (extraOps) still commits — simulate both.
			return m.UpdateSegmentsInfo(ctx, extraOps...)
		}).Build()
	defer mockCommit.UnPatch()

	task := newL0CompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		InputSegments: inputs,
	}, nil, mt, nil)

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

	mockCommit := mockey.Mock((*meta).CommitSegmentManifests).To(
		func(m *meta, ctx context.Context, commits []SegmentManifestCommit, extraOps ...UpdateOperator) error {
			return merr.WrapErrServiceInternalMsg("manifest commit failed")
		}).Build()
	defer mockCommit.UnPatch()

	task := newL0CompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		InputSegments: inputs,
	}, nil, mt, nil)

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
	mockManifestUpdateSubmissions(t)
	const targets = 3
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ManifestCommitConcurrency.Key, "16")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ManifestCommitConcurrency.Key)

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
	mockCommit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).To(
		func(_ context.Context, _ *packed.ManifestIOContext, base string, version int64, _ *indexpb.StorageConfig, _ *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
			if entered.Add(1) == targets {
				close(release)
			}
			select {
			case <-release:
				return mockManifestUpdateResult(packed.MarshalManifestPath(base, version+1), nil), nil
			case <-time.After(30 * time.Second):
				return mockManifestUpdateResult("", nil), errors.New("v3 manifest commits did not overlap; batch fan-out is serial")
			}
		}).Build()
	defer mockCommit.UnPatch()

	task := newL0CompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		InputSegments: inputs,
	}, nil, mt, nil)

	require.NoError(t, task.saveSegmentMeta(output))

	require.EqualValues(t, targets, entered.Load())
	requireL0InputsRetired(t, mt, inputs)
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
		return newL0CompactionTask(&datapb.CompactionTask{
			PlanID:       1,
			TriggerID:    19530,
			CollectionID: 1,
			PartitionID:  10,
			Type:         datapb.CompactionType_Level0DeleteCompaction,
			Channel:      channel,
			Pos:          &msgpb.MsgPosition{Timestamp: triggerTs},
		}, mockAlloc, mockMeta, nil)
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
