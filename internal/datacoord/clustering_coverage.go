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
	"maps"
	"path"
	"slices"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// clusteringInputClosure expands persisted group membership, not connectivity
// inferred from shared clusters. Legacy snapshots have no group directory;
// their full-rebuild selection uses current live L2 metadata below. Fresh
// segments do not expand snapshot-owned coverage.
func clusteringInputClosure(base *storage.PartitionStatsSnapshot, selected []int64) ([]int64, error) {
	ids := make(map[int64]struct{}, len(selected))
	for _, id := range selected {
		if id <= 0 {
			return nil, merr.WrapErrServiceInternalMsg("invalid clustering input segment %d", id)
		}
		if _, duplicate := ids[id]; duplicate {
			return nil, merr.WrapErrServiceInternalMsg("duplicate clustering input segment %d", id)
		}
		ids[id] = struct{}{}
	}
	if base != nil {
		if err := base.ValidateClusteringStats(); err != nil {
			return nil, err
		}
		if base.ClusteringStats != nil {
			for _, group := range base.ClusteringStats.GetGroups() {
				touched := false
				for _, id := range group.GetSegmentIds() {
					if _, ok := ids[id]; ok {
						touched = true
						break
					}
				}
				if touched {
					for _, id := range group.GetSegmentIds() {
						ids[id] = struct{}{}
					}
				}
			}
		}
	}
	result := slices.Collect(maps.Keys(ids))
	slices.Sort(result)
	return result, nil
}

// Legacy snapshots can refer to segments already replaced by ordinary L2
// compaction. Rebuild all current live L2 segments instead of resurrecting those
// old IDs. This is only the legacy full-rebuild path, not a partial group merge.
func legacyClusteringInputClosure(ctx context.Context, m *meta, label *CompactionGroupLabel, inputs []int64) []int64 {
	ids := make(map[int64]struct{}, len(inputs))
	for _, id := range inputs {
		ids[id] = struct{}{}
	}
	for _, segment := range m.SelectSegments(ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) && segment.GetLevel() == datapb.SegmentLevel_L2 && !segment.GetIsInvisible() &&
			segment.GetCollectionID() == label.CollectionID && segment.GetPartitionID() == label.PartitionID && segment.GetInsertChannel() == label.Channel
	})) {
		ids[segment.GetID()] = struct{}{}
	}
	result := slices.Collect(maps.Keys(ids))
	slices.Sort(result)
	return result
}

func validateClusteringInputGroups(base *storage.PartitionStatsSnapshot, inputs []int64) error {
	expanded, err := clusteringInputClosure(base, inputs)
	if err != nil {
		return err
	}
	if len(expanded) != len(inputs) {
		return merr.WrapErrCompactionPlanConflict("clustering inputs do not contain complete base groups")
	}
	return nil
}

func loadClusteringBase(ctx context.Context, m *meta, collection, partition int64, channel string, version int64, field *schemapb.FieldSchema) (*storage.PartitionStatsSnapshot, error) {
	if version == 0 {
		return nil, nil
	}
	if version < 0 || m.chunkManager == nil {
		return nil, merr.WrapErrServiceInternalMsg("invalid clustering base version or storage")
	}
	info := m.GetPartitionStatsMeta().GetPartitionStats(collection, partition, channel, version)
	if info == nil {
		return nil, merr.WrapErrCompactionPlanConflict("pinned clustering base metadata is missing")
	}
	if info.GetCollectionID() != collection || info.GetPartitionID() != partition || info.GetVChannel() != channel || info.GetVersion() != version {
		return nil, merr.WrapErrDataIntegrityMsg("clustering base catalog scope mismatch")
	}

	key := path.Join(m.chunkManager.RootPath(), common.PartitionStatsPath, metautil.JoinIDPath(collection, partition), channel, strconv.FormatInt(version, 10))
	data, err := m.chunkManager.Read(ctx, key)
	if err != nil {
		return nil, merr.Wrap(err, "read pinned clustering base")
	}
	base, err := storage.DeserializePartitionsStatsSnapshot(data)
	if err != nil {
		return nil, err
	}
	if base.GetVersion() != version {
		return nil, merr.WrapErrDataIntegrityMsg("clustering base file version mismatch")
	}
	members := make(map[int64]struct{}, len(info.GetSegmentIDs()))
	for _, id := range info.GetSegmentIDs() {
		if _, duplicate := members[id]; duplicate {
			return nil, merr.WrapErrDataIntegrityMsg("duplicate clustering base catalog segment")
		}
		if _, exists := base.SegmentStats[id]; !exists {
			return nil, merr.WrapErrDataIntegrityMsg("clustering base coverage differs from catalog")
		}
		members[id] = struct{}{}
	}
	if len(members) != len(base.SegmentStats) {
		return nil, merr.WrapErrDataIntegrityMsg("clustering base coverage differs from catalog")
	}
	if stats := base.ClusteringStats; stats != nil {
		dim, err := storage.GetDimFromParams(field.GetTypeParams())
		if err != nil {
			return nil, err
		}
		if stats.GetFieldId() != field.GetFieldID() || stats.GetDataType() != field.GetDataType() || stats.GetDim() != int64(dim) {
			return nil, merr.WrapErrDataIntegrityMsg("clustering base field differs from task schema")
		}
	}
	return base, nil
}

// Recheck every member, including those added by the closure. Do not silently
// filter an unavailable member out of a selected group.
func clusteringInputSegments(ctx context.Context, m *meta, label *CompactionGroupLabel, ids []int64, base *storage.PartitionStatsSnapshot, requireAvailable, namespace bool) ([]*SegmentInfo, error) {
	result := make([]*SegmentInfo, 0, len(ids))
	for _, id := range ids {
		segment := m.GetHealthySegment(ctx, id)
		if segment == nil || segment.GetCollectionID() != label.CollectionID || segment.GetPartitionID() != label.PartitionID || segment.GetInsertChannel() != label.Channel ||
			!isFlushed(segment) || segment.GetLevel() == datapb.SegmentLevel_L0 || segment.GetIsImporting() || segment.GetIsInvisible() ||
			(requireAvailable && segment.isCompacting) || (namespace && !segment.GetIsSortedByNamespace() && segment.GetPartitionStatsVersion() == 0) || m.isSegmentCompactionProtected(id) {
			return nil, merr.WrapErrCompactionPlanConflict("clustering group member is unavailable or outside task scope")
		}
		if base != nil {
			if stats, covered := base.SegmentStats[id]; covered && int64(stats.NumRows) != segment.GetNumOfRows() {
				return nil, merr.WrapErrDataIntegrityMsg("clustering base row count changed for segment %d", id)
			}
		}
		result = append(result, segment)
	}
	return result, nil
}

func prepareClusteringView(ctx context.Context, m *meta, view *ClusteringSegmentsView, namespace bool) (*ClusteringSegmentsView, error) {
	label := view.GetGroupLabel()
	version := m.GetPartitionStatsMeta().GetCurrentPartitionStatsVersion(label.CollectionID, label.PartitionID, label.Channel)
	base, err := loadClusteringBase(ctx, m, label.CollectionID, label.PartitionID, label.Channel, version, view.clusteringKeyField)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(view.segments))
	for _, segment := range view.segments {
		ids = append(ids, segment.ID)
	}
	expanded, err := clusteringInputClosure(base, ids)
	if err != nil {
		return nil, err
	}
	if base == nil || base.ClusteringStats == nil {
		expanded = legacyClusteringInputClosure(ctx, m, label, expanded)
	}
	segments, err := clusteringInputSegments(ctx, m, label, expanded, base, true, namespace)
	if err != nil {
		return nil, err
	}
	if m.GetPartitionStatsMeta().GetCurrentPartitionStatsVersion(label.CollectionID, label.PartitionID, label.Channel) != version {
		return nil, merr.WrapErrCompactionPlanConflict("clustering base changed during selection")
	}
	next := *view
	next.segments = GetViewsByInfo(segments...)
	next.basePartitionStatsVersion = version
	mlog.Info(ctx, "clustering input groups selected", mlog.Int64("baseVersion", version),
		mlog.Int("candidateSegments", len(ids)), mlog.Int("inputSegments", len(expanded)), mlog.Float64("groupExpansionRatio", float64(len(expanded))/float64(max(1, len(ids)))))
	return &next, nil
}

func (t *clusteringCompactionTask) validateClusteringBase(ctx context.Context) (*storage.PartitionStatsSnapshot, error) {
	task := t.GetTaskProto()
	m := t.meta.(*meta)
	version := m.GetPartitionStatsMeta().GetCurrentPartitionStatsVersion(task.GetCollectionID(), task.GetPartitionID(), task.GetChannel())
	if version != task.GetBasePartitionStatsVersion() {
		return nil, merr.WrapErrCompactionPlanConflict("clustering task base is stale")
	}
	base, err := loadClusteringBase(ctx, m, task.GetCollectionID(), task.GetPartitionID(), task.GetChannel(), version, task.GetClusteringKeyField())
	if err != nil {
		return nil, err
	}
	if err := validateClusteringInputGroups(base, task.GetInputSegments()); err != nil {
		return nil, err
	}
	if base == nil || base.ClusteringStats == nil {
		expanded := legacyClusteringInputClosure(ctx, m, &CompactionGroupLabel{CollectionID: task.GetCollectionID(), PartitionID: task.GetPartitionID(), Channel: task.GetChannel()}, task.GetInputSegments())
		if len(expanded) != len(task.GetInputSegments()) {
			return nil, merr.WrapErrCompactionPlanConflict("legacy clustering requires every current L2 segment")
		}
	}
	// Base-covered tasks need scope/physical-row validation even on recovery.
	if base != nil {
		_, err = clusteringInputSegments(ctx, m, &CompactionGroupLabel{CollectionID: task.GetCollectionID(), PartitionID: task.GetPartitionID(), Channel: task.GetChannel()},
			task.GetInputSegments(), base, false, task.GetSchema().GetEnableNamespace())
	}
	return base, err
}
