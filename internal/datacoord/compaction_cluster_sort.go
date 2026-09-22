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
	"path"
	"slices"
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

const maxClusterSortAttempts = 3

func clusterSortReplayInput(task *datapb.CompactionTask, segment *SegmentInfo) bool {
	ref := segment.GetClusterStats()
	return task.GetType() == datapb.CompactionType_ClusterSortCompaction && task.GetClusterSortPendingResult() != nil &&
		segment.GetState() == commonpb.SegmentState_Dropped && segment.GetCompacted() &&
		ref != nil && !ref.Sorted && ref.ClusteringTaskId == task.TriggerID && ref.GroupId == task.ClusterSortGroupId
}

func clusterSortTerminal(task *datapb.CompactionTask) bool {
	switch task.GetState() {
	case datapb.CompactionTaskState_failed, datapb.CompactionTaskState_timeout, datapb.CompactionTaskState_cleaned, datapb.CompactionTaskState_completed:
		return true
	default:
		return false
	}
}

func clusterStagingSegments(ctx context.Context, meta CompactionMeta, parent *datapb.CompactionTask) (map[int64]int64, error) {
	groups := make(map[int64]int64)
	for _, id := range parent.GetTmpSegments() {
		segment := meta.GetSegment(ctx, id)
		if segment == nil {
			return nil, merr.WrapErrSegmentNotFound(id)
		}
		ref := segment.GetClusterStats()
		if ref == nil || ref.Version != 1 || ref.Sorted || ref.ClusteringTaskId != parent.PlanID || ref.FieldId != parent.GetClusteringKeyField().GetFieldID() || segment.GetCollectionID() != parent.CollectionID || segment.GetPartitionID() != parent.PartitionID {
			return nil, merr.WrapErrServiceInternalMsg("invalid staging cluster_stats for parent %d, segment %d", parent.PlanID, id)
		}
		groups[id] = ref.GroupId
	}
	return groups, nil
}

func latestClusterSort(tasks []*datapb.CompactionTask, group, inputID int64) *datapb.CompactionTask {
	var latest *datapb.CompactionTask
	for _, task := range tasks {
		if task.GetType() != datapb.CompactionType_ClusterSortCompaction || task.GetClusterSortGroupId() != group {
			continue
		}
		if len(task.GetInputSegments()) != 1 || task.InputSegments[0] != inputID {
			continue
		}
		if latest == nil || task.PlanID > latest.PlanID {
			latest = task
		}
	}
	return latest
}

// The parent's committed TmpSegments is the only staging catalog. A periodic
// scan of all "finished" segments could mix attempts or start before mapping
// completes; neither is allowed here.
func (m *CompactionTriggerManager) submitClusterSort(ctx context.Context, segment *SegmentInfo) error {
	ref := segment.GetClusterStats()
	if ref == nil || ref.Sorted {
		return nil
	}
	parent := m.meta.GetCompactionTaskMeta().GetCompactionTask(ref.ClusteringTaskId)
	if parent == nil || parent.GetState() != datapb.CompactionTaskState_statistic {
		return nil
	}
	groups, err := clusterStagingSegments(ctx, m.meta, parent)
	if err != nil {
		return err
	}
	inputID := segment.GetID()
	if group, ok := groups[inputID]; !ok || group != ref.GroupId {
		return merr.WrapErrServiceInternalMsg("segment is outside parent's committed staging set")
	}
	previous := latestClusterSort(m.meta.GetCompactionTasksByTriggerID(ctx, parent.PlanID), ref.GroupId, inputID)
	attempt := int32(1)
	if previous != nil {
		if previous.ClusterSortCompleted || !clusterSortTerminal(previous) {
			return nil
		}
		attempt = previous.RetryTimes + 1
	}
	if attempt > maxClusterSortAttempts {
		return nil
	}
	input := m.meta.GetHealthySegment(ctx, inputID)
	if input == nil {
		return merr.WrapErrSegmentNotFound(inputID)
	}
	if input.isCompacting {
		return nil // Previous attempt cleanup still owns this segment.
	}
	// Sorting preserves physical segment boundaries. Allocate exactly one
	// output ID and one plan ID, independent of the parent's target size.
	start, end, err := m.allocator.AllocN(2)
	if err != nil {
		return err
	}
	if start <= 0 || end-start != 2 {
		return merr.WrapErrServiceInternalMsg("invalid cluster sort ID allocation")
	}
	now := time.Now().Unix()
	return m.inspector.enqueueCompaction(&datapb.CompactionTask{
		PlanID: start + 1, TriggerID: parent.PlanID, Type: datapb.CompactionType_ClusterSortCompaction,
		State: datapb.CompactionTaskState_pipelining, StartTime: now, LastStateStartTime: now,
		CollectionID: parent.CollectionID, PartitionID: parent.PartitionID, Channel: parent.Channel,
		Schema: parent.Schema, CollectionTtl: parent.CollectionTtl, TotalRows: input.GetNumOfRows(), MaxSize: parent.MaxSize,
		InputSegments: []int64{inputID}, PreAllocatedSegmentIDs: &datapb.IDRange{Begin: start, End: start + 1}, RetryTimes: attempt, ClusterSortGroupId: ref.GroupId,
	})
}

func (t *clusteringCompactionTask) processClusterSort() error {
	ctx := context.TODO()
	parent := t.GetTaskProto()
	groups, err := clusterStagingSegments(ctx, t.meta, parent)
	if err != nil {
		return err
	}
	children := t.meta.GetCompactionTasksByTriggerID(ctx, parent.PlanID)
	results := make(map[int64][]int64)
	var outputIDs []int64
	pending := false
	for inputID, group := range groups {
		child := latestClusterSort(children, group, inputID)
		if child == nil || !child.ClusterSortCompleted {
			if child != nil && clusterSortTerminal(child) && child.RetryTimes >= maxClusterSortAttempts {
				return merr.WrapErrServiceInternalMsg("cluster sort segment %d (group %d) exhausted retries: %s", inputID, group, child.GetFailReason())
			}
			select {
			case getStatsTaskChSingleton() <- inputID:
			default:
			}
			pending = true
			continue
		}
		if len(child.GetResultSegments()) > 1 {
			return merr.WrapErrServiceInternalMsg("cluster sort segment %d has multiple outputs", inputID)
		}
		for _, id := range child.GetResultSegments() {
			segment := t.meta.GetHealthySegment(ctx, id)
			if segment == nil {
				return merr.WrapErrSegmentNotFound(id)
			}
			ref := segment.GetClusterStats()
			if ref == nil || !ref.Sorted || ref.ClusteringTaskId != parent.PlanID || ref.GroupId != group || ref.NumRows != segment.GetNumOfRows() {
				return merr.WrapErrServiceInternalMsg("invalid cluster sort output %d", id)
			}
		}
		results[inputID] = child.GetResultSegments()
		outputIDs = append(outputIDs, child.GetResultSegments()...)
	}
	if pending {
		return nil
	}
	slices.Sort(outputIDs)
	if len(slices.Compact(append([]int64(nil), outputIDs...))) != len(outputIDs) {
		return merr.WrapErrServiceInternalMsg("duplicate cluster sort output segment")
	}
	if err = t.updateAndSaveTaskMeta(setResultSegments(outputIDs)); err != nil {
		return err
	}
	if err = t.regenerateClusterPartitionStats(results); err != nil {
		return err
	}
	return t.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_indexing))
}

func (t *clusteringCompactionTask) regenerateClusterPartitionStats(results map[int64][]int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cli, err := storage.NewChunkManagerFactoryWithParam(Params).NewPersistentStorageChunkManager(ctx)
	if err != nil {
		return err
	}
	parent := t.GetTaskProto()
	file := path.Join(cli.RootPath(), common.PartitionStatsPath, metautil.JoinIDPath(parent.CollectionID, parent.PartitionID), parent.Channel, strconv.FormatInt(parent.PlanID, 10))
	payload, err := cli.Read(ctx, file)
	if err != nil {
		return err
	}
	snapshot, err := storage.DeserializePartitionsStatsSnapshot(payload)
	if err != nil {
		return err
	}
	updated, err := rebuildClusterPartitionStats(snapshot, results, func(id int64) *SegmentInfo { return t.meta.GetSegment(ctx, id) })
	if err != nil {
		return err
	}
	payload, err = storage.SerializePartitionStatsSnapshot(updated)
	if err != nil {
		return err
	}
	return cli.Write(ctx, file, payload)
}

func rebuildClusterPartitionStats(snapshot *storage.PartitionStatsSnapshot, results map[int64][]int64, get func(int64) *SegmentInfo) (*storage.PartitionStatsSnapshot, error) {
	updated := &storage.PartitionStatsSnapshot{SegmentStats: make(map[int64]storage.SegmentStats), Version: snapshot.Version}
	for inputID, outputs := range results {
		if len(outputs) > 1 {
			return nil, merr.WrapErrServiceInternalMsg("cluster sort segment %d has multiple outputs", inputID)
		}
		centroids := make(map[uint32]storage.VectorFieldValue)
		var field storage.FieldStats
		if stats, ok := snapshot.SegmentStats[inputID]; ok {
			segment := get(inputID)
			if segment == nil || len(stats.FieldStats) != 1 {
				return nil, merr.WrapErrServiceInternalMsg("missing cluster staging statistics")
			}
			field = stats.FieldStats[0].Clone()
			ids := segment.GetClusterStats().GetCentroidIds()
			if len(ids) != len(field.Centroids) {
				return nil, merr.WrapErrServiceInternalMsg("cluster staging centroid statistics mismatch")
			}
			for i, centroid := range ids {
				centroids[centroid] = field.Centroids[i]
			}
		}
		for _, id := range outputs {
			segment := get(id)
			if segment == nil {
				return nil, merr.WrapErrSegmentNotFound(id)
			}
			if stats, ok := snapshot.SegmentStats[id]; ok {
				// Previous attempt wrote the snapshot before saving parent's state.
				if int64(stats.NumRows) != segment.GetNumOfRows() {
					return nil, merr.WrapErrServiceInternalMsg("cluster output statistics row count mismatch")
				}
				updated.SegmentStats[id] = stats
				continue
			}
			f := field.Clone()
			f.Centroids = nil
			for _, centroid := range segment.GetClusterStats().GetCentroidIds() {
				v, ok := centroids[centroid]
				if !ok {
					return nil, merr.WrapErrServiceInternalMsg("output centroid absent from staging statistics")
				}
				f.Centroids = append(f.Centroids, v)
			}
			updated.SegmentStats[id] = storage.SegmentStats{NumRows: int(segment.GetNumOfRows()), FieldStats: []storage.FieldStats{f}}
		}
	}
	return updated, nil
}

func validateClusterSortResult(task *datapb.CompactionTask, result *datapb.CompactionPlanResult, segments *SegmentsInfo) error {
	invalid := func() error { return merr.WrapErrIllegalCompactionPlan("invalid cluster sort result or pinned inputs") }
	if result.GetPlanID() != task.PlanID || result.GetType() != task.Type || result.GetState() != datapb.CompactionTaskState_completed {
		return invalid()
	}
	allocated := task.GetPreAllocatedSegmentIDs()
	if len(task.GetInputSegments()) != 1 || len(result.GetSegments()) > 1 || allocated.GetBegin() <= 0 || allocated.GetEnd()-allocated.GetBegin() != 1 {
		return invalid()
	}
	input := segments.GetSegment(task.InputSegments[0])
	if input == nil {
		return merr.WrapErrSegmentNotFound(task.InputSegments[0])
	}
	ref := input.GetClusterStats()
	if ref == nil || ref.Version != 1 || ref.Sorted || ref.ClusteringTaskId != task.TriggerID || ref.GroupId != task.ClusterSortGroupId || ref.NumRows != input.GetNumOfRows() || input.GetCollectionID() != task.CollectionID || input.GetPartitionID() != task.PartitionID {
		return invalid()
	}
	var outputRows int64
	centroids := make(map[uint32]bool)
	for _, id := range ref.CentroidIds {
		centroids[id] = true
	}
	for _, output := range result.GetSegments() {
		r := output.GetClusterStats()
		if r == nil || r.Version != 1 || !r.Sorted || r.ClusteringTaskId != task.TriggerID || r.GroupId != task.ClusterSortGroupId || r.FieldId != ref.FieldId || r.NumRows != output.NumOfRows || output.IsSorted || output.IsSortedByNamespace {
			return invalid()
		}
		if output.SegmentID != allocated.GetBegin() || output.SegmentID == input.GetID() {
			return invalid()
		}
		if r.NumRows <= 0 || len(r.Files) == 0 || r.RangesPath == "" || len(r.CentroidIds) == 0 {
			return invalid()
		}
		outputRows += output.NumOfRows
		for i, centroid := range r.CentroidIds {
			if !centroids[centroid] || (i > 0 && r.CentroidIds[i-1] >= centroid) {
				return invalid()
			}
		}
	}
	if outputRows > input.GetNumOfRows() {
		return invalid()
	}
	return nil
}
