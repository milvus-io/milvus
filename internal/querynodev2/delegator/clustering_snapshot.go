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

package delegator

import (
	"context"
	"encoding/json"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/clustercompaction"
	"github.com/milvus-io/milvus/internal/util/clusteringindex"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

type clusterSearcher interface {
	Search(context.Context, []byte, int64, int64) ([]int64, error)
}
type clusterRangeLocation struct {
	segment int64
	row     *clusteringpb.ClusteringRange
}

// Immutable metadata, directory and native cache owner travel with a segment pin.
type partitionStatsBinding struct {
	stats     *storage.PartitionStatsSnapshot
	index     clusterSearcher
	locations map[uint32][]clusterRangeLocation
}

func (sd *shardDelegator) loadPartitionStatsBinding(ctx context.Context, partition, version int64) (*partitionStatsBinding, error) {
	key := path.Join(sd.chunkManager.RootPath(), common.PartitionStatsPath, metautil.JoinIDPath(sd.Collection(), partition), sd.vchannelName, strconv.FormatInt(version, 10))
	blob, err := sd.chunkManager.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	stats, err := storage.DeserializePartitionsStatsSnapshot(blob)
	if err != nil {
		return nil, err
	}
	if stats.Version != version {
		return nil, merr.WrapErrDataIntegrityMsg("partition stats file version differs from target binding")
	}
	binding := &partitionStatsBinding{stats: stats}
	c := stats.ClusteringStats
	if c == nil || c.GetNumClusters() == 0 {
		return binding, nil
	}
	if c.GetClustersFile() != key+".clusters" {
		return nil, merr.WrapErrDataIntegrityMsg("cluster table belongs to another snapshot")
	}
	for _, object := range c.GetClusteringIndex().GetIndexFilePaths() {
		if !strings.HasPrefix(object, key+".clustering_index/") {
			return nil, merr.WrapErrDataIntegrityMsg("cluster index belongs to another snapshot")
		}
	}
	var field *schemapb.FieldSchema
	for _, f := range sd.collection.Schema().GetFields() {
		if f.GetFieldID() == c.GetFieldId() {
			field = f
			break
		}
	}
	if field == nil || field.GetDataType() != c.GetDataType() {
		return nil, merr.WrapErrDataIntegrityMsg("clustering field differs from loaded schema")
	}
	dim, err := storage.GetDimFromParams(field.GetTypeParams())
	if err != nil || int64(dim) != c.GetDim() {
		return nil, merr.WrapErrDataIntegrityMsg("clustering dimension differs from loaded schema")
	}
	blob, err = sd.chunkManager.Read(ctx, c.GetClustersFile())
	if err != nil {
		return nil, err
	}
	var centroids clusteringpb.ClusteringCentroidsStats
	if err := proto.Unmarshal(blob, &centroids); err != nil {
		return nil, err
	}
	if err := clustercompaction.ValidateCentroids(&centroids, field.GetDataType(), c.GetDim()); err != nil {
		return nil, err
	}
	if len(centroids.GetCentroids()) != int(c.GetNumClusters()) {
		return nil, merr.WrapErrDataIntegrityMsg("clustering description count mismatch")
	}
	binding.index, err = clusteringindex.Load(ctx, sd.Collection(), partition, sd.vchannelName, version, field, c.GetClusteringIndex(), int64(c.GetNumClusters()))
	if err != nil {
		return nil, err
	}
	binding.locations = clusteringLocations(stats)
	return binding, nil
}

// Only validated full snapshots may feed this directory. Resident segments are
// intersected later; they are never used to infer cluster/group ownership.
func clusteringLocations(stats *storage.PartitionStatsSnapshot) map[uint32][]clusterRangeLocation {
	locations := make(map[uint32][]clusterRangeLocation)
	for id, segment := range stats.SegmentStats {
		for _, r := range segment.ClusteringRanges {
			locations[r.GetClusterOffset()] = append(locations[r.GetClusterOffset()], clusterRangeLocation{id, r})
		}
	}
	return locations
}

func legacyStatsBindings(bindings map[int64]*partitionStatsBinding) map[int64]*storage.PartitionStatsSnapshot {
	stats := make(map[int64]*storage.PartitionStatsSnapshot)
	for partition, binding := range bindings {
		if binding != nil && binding.stats.ClusteringStats == nil {
			stats[partition] = binding.stats
		}
	}
	return stats
}

func clusteringCandidateRanges(ctx context.Context, bindings map[int64]*partitionStatsBinding,
	req *internalpb.SearchRequest, sealed []SnapshotItem, rows map[int64]int64, ratio float64,
) map[int64]*querypb.ClusteringRowRanges {
	// TODO: resolve explicit metric (especially COSINE) consistently with Analyze.
	// Iterators and range search cannot yet use a top-k cluster candidate budget.
	if req == nil || strings.ToUpper(req.GetMetricType()) != "L2" || req.GetIsIterator() || !(ratio > 0 && ratio < 1) {
		return nil
	}
	var plan planpb.PlanNode
	if proto.Unmarshal(req.GetSerializedExprPlan(), &plan) != nil || plan.GetVectorAnns() == nil {
		return nil
	}
	var searchParams map[string]interface{}
	if json.Unmarshal([]byte(plan.GetVectorAnns().GetQueryInfo().GetSearchParams()), &searchParams) != nil {
		return nil
	}
	if _, rangeSearch := searchParams["radius"]; rangeSearch {
		return nil
	}
	var placeholders commonpb.PlaceholderGroup
	if proto.Unmarshal(req.GetPlaceholderGroup(), &placeholders) != nil || len(placeholders.GetPlaceholders()) != 1 {
		return nil
	}
	queries := placeholders.GetPlaceholders()[0]
	if len(queries.GetValues()) == 0 || int64(len(queries.GetValues())) != req.GetNq() {
		return nil
	}
	eligible := make(map[int64][]int64)
	for _, item := range sealed {
		for _, segment := range item.Segments {
			eligible[segment.PartitionID] = append(eligible[segment.PartitionID], segment.SegmentID)
		}
	}
	result := make(map[int64]*querypb.ClusteringRowRanges)
	for partition, ids := range eligible {
		binding := bindings[partition]
		if binding == nil || binding.index == nil {
			continue
		}
		c := binding.stats.ClusteringStats
		if c == nil || c.GetFieldId() != req.GetFieldId() || clustercompaction.VectorElementBytes(c.GetDataType()) == 0 || c.GetDim() <= 0 {
			continue
		}
		width := clustercompaction.VectorElementBytes(c.GetDataType())
		placeholderType := commonpb.PlaceholderType_FloatVector
		switch c.GetDataType() {
		case schemapb.DataType_Float16Vector:
			placeholderType = commonpb.PlaceholderType_Float16Vector
		case schemapb.DataType_BFloat16Vector:
			placeholderType = commonpb.PlaceholderType_BFloat16Vector
		}
		if queries.GetType() != placeholderType || c.GetDim() > math.MaxInt/width {
			continue
		}
		params := funcutil.KeyValuePair2Map(c.GetClusteringIndex().GetIndexParams())
		if params[common.MetricTypeKey] != "L2" {
			continue
		}
		var vectors []byte
		valid := true
		for _, q := range queries.GetValues() {
			if len(q) != int(c.GetDim()*width) {
				valid = false
				break
			}
			vectors = append(vectors, q...)
		}
		if !valid {
			continue
		}
		budget := max(int64(1), int64(math.Ceil(float64(c.GetNumClusters())*ratio)))
		labels, err := binding.index.Search(ctx, vectors, req.GetNq(), budget)
		if err != nil {
			mlog.Warn(ctx, "skip clustering range pruning", mlog.Int64("partitionID", partition), mlog.Err(err))
			continue
		}
		candidates := make(map[int64]*querypb.ClusteringRowRanges)
		for _, id := range ids {
			stats, covered := binding.stats.SegmentStats[id]
			if !covered {
				continue
			}
			if int64(stats.NumRows) != rows[id] {
				valid = false
				break
			}
			candidates[id] = &querypb.ClusteringRowRanges{SnapshotVersion: binding.stats.Version, NumRows: int64(stats.NumRows)}
		}
		if !valid {
			continue
		}
		selected := make(map[int64]struct{}, len(labels))
		for _, label := range labels {
			if label == -1 {
				continue
			}
			if label < 0 || label >= int64(c.GetNumClusters()) {
				valid = false
				break
			}
			if _, seen := selected[label]; seen {
				continue
			}
			selected[label] = struct{}{}
			for _, location := range binding.locations[uint32(label)] {
				if hint := candidates[location.segment]; hint != nil {
					hint.Ranges = append(hint.Ranges, location.row)
				}
			}
		}
		// All -1 is an incomplete/unsupported result, not evidence of an empty scope.
		if !valid || len(selected) == 0 {
			continue
		}
		for id, hint := range candidates {
			sort.Slice(hint.Ranges, func(i, j int) bool { return hint.Ranges[i].GetRowOffset() < hint.Ranges[j].GetRowOffset() })
			result[id] = hint
		}
	}
	return result
}
