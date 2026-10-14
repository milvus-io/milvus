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

package storage

import (
	"math"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/clustercompaction"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ClusteringSnapshotDraft owns a full table and remapped physical ranges. It is
// not publishable until the full index has been built in precisely this order.
type ClusteringSnapshotDraft struct {
	Snapshot  *PartitionStatsSnapshot
	Centroids *clusteringpb.ClusteringCentroidsStats
}

// MergeClusteringSnapshot removes whole selected base groups and appends live
// output clusters. Delta ranges use Analyze offsets; retained ranges use base
// offsets. Each namespace is translated independently and neither input mutates.
func MergeClusteringSnapshot(base *PartitionStatsSnapshot, baseCentroids *clusteringpb.ClusteringCentroidsStats,
	inputs []int64, outputs map[int64]SegmentStats, outputGroups []*clusteringpb.ClusteringGroup,
	centroids *clusteringpb.ClusteringCentroidsStats, field *schemapb.FieldSchema, version int64,
) (*ClusteringSnapshotDraft, error) {
	dim, err := GetDimFromParams(field.GetTypeParams())
	if err != nil {
		return nil, err
	}
	if version <= 0 || (base != nil && version <= base.Version) {
		return nil, merr.WrapErrDataIntegrityMsg("invalid clustering merge version")
	}
	if err := clustercompaction.ValidateCentroids(centroids, field.GetDataType(), int64(dim)); err != nil {
		return nil, err
	}
	selected := make(map[int64]struct{}, len(inputs))
	for _, id := range inputs {
		if _, exists := selected[id]; exists || id <= 0 {
			return nil, merr.WrapErrDataIntegrityMsg("invalid clustering merge inputs")
		}
		selected[id] = struct{}{}
	}
	draft := &ClusteringSnapshotDraft{
		Snapshot: &PartitionStatsSnapshot{Version: version, SegmentStats: make(map[int64]SegmentStats), ClusteringStats: &clusteringpb.PartitionClusteringStats{
			FormatVersion: PartitionClusteringStatsFormatVersion, FieldId: field.GetFieldID(), DataType: field.GetDataType(), Dim: int64(dim),
		}},
		Centroids: &clusteringpb.ClusteringCentroidsStats{},
	}
	appendGroups := func(segments map[int64]SegmentStats, groups []*clusteringpb.ClusteringGroup, descriptions *clusteringpb.ClusteringCentroidsStats) error {
		live := make(map[uint32]struct{})
		for _, stats := range segments {
			for _, entry := range stats.ClusteringRanges {
				if entry == nil || uint64(entry.ClusterOffset) >= uint64(len(descriptions.GetCentroids())) {
					return merr.WrapErrDataIntegrityMsg("merge range references unknown cluster")
				}
				live[entry.ClusterOffset] = struct{}{}
			}
		}
		offsets := make([]uint32, 0, len(live))
		for offset := range live {
			offsets = append(offsets, offset)
		}
		slices.Sort(offsets)
		mapping := make(map[uint32]uint32, len(live))
		for _, offset := range offsets {
			if uint64(len(draft.Centroids.Centroids)) >= math.MaxUint32 {
				return merr.WrapErrDataIntegrityMsg("merged cluster count overflows")
			}
			mapping[offset] = uint32(len(draft.Centroids.Centroids))
			draft.Centroids.Centroids = append(draft.Centroids.Centroids, proto.Clone(descriptions.Centroids[offset]).(*schemapb.VectorField))
		}
		for id, stats := range segments {
			if _, duplicate := draft.Snapshot.SegmentStats[id]; duplicate {
				return merr.WrapErrDataIntegrityMsg("clustering output collides with retained segment")
			}
			// FieldStats contains interface pointers and BF internals. Clone via
			// its codec as well as ranges; no mutable alias to an older snapshot.
			blob, err := json.Marshal(stats)
			if err != nil {
				return merr.WrapErrSerializationFailed(err, "clone retained segment stats")
			}
			var clone SegmentStats
			if err := json.Unmarshal(blob, &clone); err != nil {
				return merr.Wrap(err, "clone retained segment stats")
			}
			for _, entry := range clone.ClusteringRanges {
				entry.ClusterOffset = mapping[entry.ClusterOffset]
			}
			draft.Snapshot.SegmentStats[id] = clone
		}
		for _, group := range groups {
			if group == nil || len(group.SegmentIds) == 0 {
				return merr.WrapErrDataIntegrityMsg("empty clustering merge group")
			}
			for _, id := range group.SegmentIds {
				if _, exists := segments[id]; !exists {
					return merr.WrapErrDataIntegrityMsg("merge group references unknown segment")
				}
			}
			draft.Snapshot.ClusteringStats.Groups = append(draft.Snapshot.ClusteringStats.Groups, proto.Clone(group).(*clusteringpb.ClusteringGroup))
		}
		return nil
	}
	if base != nil && base.ClusteringStats != nil {
		if err := base.ValidateClusteringStats(); err != nil {
			return nil, err
		}
		meta := base.ClusteringStats
		if meta.FieldId != field.GetFieldID() || meta.DataType != field.GetDataType() || meta.Dim != int64(dim) || uint64(meta.NumClusters) != uint64(len(baseCentroids.GetCentroids())) {
			return nil, merr.WrapErrDataIntegrityMsg("clustering merge base field/table mismatch")
		}
		if meta.NumClusters > 0 {
			if err := clustercompaction.ValidateCentroids(baseCentroids, meta.DataType, meta.Dim); err != nil {
				return nil, err
			}
		}
		retained := make(map[int64]SegmentStats)
		var groups []*clusteringpb.ClusteringGroup
		for _, group := range meta.Groups {
			count := 0
			for _, id := range group.SegmentIds {
				if _, ok := selected[id]; ok {
					count++
				}
			}
			if count != 0 && count != len(group.SegmentIds) {
				return nil, merr.WrapErrCompactionPlanConflict("clustering merge would split a base group")
			}
			if count == 0 {
				groups = append(groups, group)
				for _, id := range group.SegmentIds {
					retained[id] = base.SegmentStats[id]
				}
			}
		}
		if err := appendGroups(retained, groups, baseCentroids); err != nil {
			return nil, err
		}
	}
	for id := range outputs {
		if _, exists := selected[id]; exists {
			return nil, merr.WrapErrDataIntegrityMsg("clustering output reuses input identity")
		}
		if base != nil {
			if _, exists := base.SegmentStats[id]; exists {
				return nil, merr.WrapErrDataIntegrityMsg("clustering output reuses base identity")
			}
		}
	}
	if err := appendGroups(outputs, outputGroups, centroids); err != nil {
		return nil, err
	}
	draft.Snapshot.ClusteringStats.NumClusters = uint32(len(draft.Centroids.Centroids))
	if err := validateClusteringField(draft.Snapshot.ClusteringStats); err != nil {
		return nil, err
	}
	if err := draft.Snapshot.validateClusteringLayout(); err != nil {
		return nil, err
	}
	return draft, nil
}

func (d *ClusteringSnapshotDraft) Finalize(clustersFile string, index *clusteringpb.ClusteringIndexInfo) (*PartitionStatsSnapshot, error) {
	d.Snapshot.ClusteringStats.ClustersFile = clustersFile
	d.Snapshot.ClusteringStats.ClusteringIndex = index
	if err := d.Snapshot.ValidateClusteringStats(); err != nil {
		return nil, err
	}
	return d.Snapshot, nil
}
