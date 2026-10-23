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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/stats"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
)

// TestMixCompactionSaveSegmentMetaRecomputesDataView verifies that a
// completed mix compaction (outputs published, inputs retired) requests an
// asynchronous DataView reconciliation. Mix/Sort/BumpSchemaVersion
// compactions mutate SegmentMeta directly through CompleteCompactionMutation,
// so they must trigger the recompute themselves.
func TestMixCompactionSaveSegmentMetaRecomputesDataView(t *testing.T) {
	recomputeManager := &recordingDataViewManager{}
	m := &meta.Meta{Segments: meta.NewSegmentsInfo(metacache.NewMetaStore(nil)), DataViewManager: recomputeManager}

	mockComplete := mockey.Mock((*meta.Meta).CompleteCompactionMutation).To(
		func(_ *meta.Meta, _ context.Context, _ *datapb.CompactionTask, _ *datapb.CompactionPlanResult) ([]*meta.SegmentInfo, error) {
			return []*meta.SegmentInfo{{SegmentInfo: &datapb.SegmentInfo{ID: 100}}}, nil
		}).Build()
	defer mockComplete.UnPatch()
	mockSave := mockey.Mock((*ctask.CompactionTaskMeta).SaveCompactionTask).Return(nil).Build()
	defer mockSave.UnPatch()

	task := ctask.NewMixCompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     2,
		CollectionID:  7,
		PartitionID:   10,
		Type:          datapb.CompactionType_MixCompaction,
		State:         datapb.CompactionTaskState_executing,
		InputSegments: []int64{1},
		Schema:        &schemapb.CollectionSchema{Version: 1},
	}, nil, m, ctask.NewCompactionTaskMetaOverCatalog(nil), index.NewTestVersionManager())

	require.NoError(t, task.SaveSegmentMeta(&datapb.CompactionPlanResult{}))

	recomputeManager.mu.Lock()
	defer recomputeManager.mu.Unlock()
	require.Equal(t, []int64{7}, recomputeManager.calls, "mix compaction must request a DataView recompute")
}

// TestBumpSchemaVersionSaveSegmentMetaRecomputesDataView verifies that a
// completed schema-bump compaction (manifests rewritten) requests an
// asynchronous DataView reconciliation.
func TestBumpSchemaVersionSaveSegmentMetaRecomputesDataView(t *testing.T) {
	recomputeManager := &recordingDataViewManager{}
	m := &meta.Meta{Segments: meta.NewSegmentsInfo(metacache.NewMetaStore(nil)), DataViewManager: recomputeManager}

	mockComplete := mockey.Mock((*meta.Meta).CompleteCompactionMutation).To(
		func(_ *meta.Meta, _ context.Context, _ *datapb.CompactionTask, _ *datapb.CompactionPlanResult) ([]*meta.SegmentInfo, error) {
			return []*meta.SegmentInfo{{SegmentInfo: &datapb.SegmentInfo{ID: 100}}}, nil
		}).Build()
	defer mockComplete.UnPatch()
	mockSave := mockey.Mock((*ctask.CompactionTaskMeta).SaveCompactionTask).Return(nil).Build()
	defer mockSave.UnPatch()

	task := ctask.NewBumpSchemaVersionTask(&datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     2,
		CollectionID:  7,
		PartitionID:   10,
		Type:          datapb.CompactionType_BumpSchemaVersionCompaction,
		State:         datapb.CompactionTaskState_executing,
		InputSegments: []int64{1},
		Schema:        &schemapb.CollectionSchema{Version: 2},
	}, nil, m, ctask.NewCompactionTaskMetaOverCatalog(nil), index.NewTestVersionManager())

	require.NoError(t, task.SaveSegmentMeta(&datapb.CompactionPlanResult{}))

	recomputeManager.mu.Lock()
	defer recomputeManager.mu.Unlock()
	require.Equal(t, []int64{7}, recomputeManager.calls, "schema-bump compaction must request a DataView recompute")
}

// TestStatsTaskSetJobInfoManifestRecomputesDataView verifies that a stats
// task (e.g. sort) which advances a segment's manifest version requests an
// asynchronous DataView reconciliation after the SegmentMeta mutation
// commits.
func TestStatsTaskSetJobInfoManifestRecomputesDataView(t *testing.T) {
	ctx := context.Background()
	recomputeManager := &recordingDataViewManager{}
	m := &meta.Meta{Segments: meta.NewSegmentsInfo(metacache.NewMetaStore(nil)), DataViewManager: recomputeManager}

	mockUpdate := mockey.Mock((*meta.Meta).UpdateSegmentsInfo).To(
		func(_ *meta.Meta, _ context.Context, _ ...meta.UpdateOperator) error { return nil }).Build()
	defer mockUpdate.UnPatch()

	st := &stats.StatsTask{
		StatsTask: &indexpb.StatsTask{
			TaskID:          1,
			SegmentID:       10,
			TargetSegmentID: 10,
			CollectionID:    7,
			SubJobType:      indexpb.StatsSubJob_Sort,
		},
		Meta: m,
	}
	require.NoError(t, st.SetJobInfo(ctx, &workerpb.StatsResult{Manifest: "/by-dev/manifest/7/10/1"}))

	recomputeManager.mu.Lock()
	defer recomputeManager.mu.Unlock()
	require.Equal(t, []int64{7}, recomputeManager.calls, "manifest-advancing stats task must request a DataView recompute")
}
