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
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/clustercompaction"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// Bound optional request metadata; exceeding the budget falls back, never truncates candidates.
const maxClusteringHintEntries = 1 << 20

func clusteringSearchHints(ctx context.Context, bindings map[int64]*partitionStatsBinding,
	req *internalpb.SearchRequest, sealed []SnapshotItem, rows map[int64]int64, ratio float64,
) map[int64]*querypb.SegmentClusterSearchHints {
	// TODO: resolve explicit metric (especially COSINE) consistently with Analyze.
	// Iterators and range search cannot yet use a top-k cluster candidate budget.
	if req == nil || strings.ToUpper(req.GetMetricType()) != "L2" || req.GetIsIterator() || req.GetIsAdvanced() || !(ratio > 0 && ratio < 1) {
		return nil
	}
	var plan planpb.PlanNode
	if proto.Unmarshal(req.GetSerializedExprPlan(), &plan) != nil || plan.GetVectorAnns() == nil || plan.GetVectorAnns().GetFieldId() != req.GetFieldId() {
		return nil
	}
	qi := plan.GetVectorAnns().GetQueryInfo()
	if qi == nil || qi.GetGroupByFieldId() > 0 || len(qi.GetGroupByFieldIds()) > 0 || qi.GetSearchIteratorV2Info() != nil || qi.GetHints() != "" || qi.GetRefineTopkRatio() != 0 {
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
	ctx, span := otel.Tracer("querynode").Start(ctx, "ClusteringSearchHints")
	defer span.End()
	fallback := func(reason string) {
		span.AddEvent("fallback", trace.WithAttributes(attribute.String("reason", reason)))
	}
	eligible := make(map[int64][]int64)
	for _, item := range sealed {
		for _, segment := range item.Segments {
			eligible[segment.PartitionID] = append(eligible[segment.PartitionID], segment.SegmentID)
		}
	}
	result := make(map[int64]*querypb.SegmentClusterSearchHints)
	metadataEntries := 0
	for partition, ids := range eligible {
		binding := bindings[partition]
		if binding == nil || binding.index == nil {
			fallback("missing_binding")
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
		labels, distances, err := binding.index.Search(ctx, vectors, req.GetNq(), budget)
		if err != nil {
			fallback("probe_error")
			mlog.RatedWarn(ctx, 1.0, "skip clustering search hints", mlog.FieldPartitionID(partition), mlog.Err(err))
			continue
		}
		if req.GetNq() > math.MaxInt/budget || len(labels) != int(req.GetNq()*budget) || len(distances) != len(labels) {
			fallback("probe_shape")
			continue
		}
		candidates := make(map[int64]*querypb.SegmentClusterSearchHints)
		for _, id := range ids {
			stats, covered := binding.stats.SegmentStats[id]
			if !covered {
				continue
			}
			if int64(stats.NumRows) != rows[id] {
				fallback("row_domain")
				valid = false
				break
			}
			if req.GetNq() > int64(maxClusteringHintEntries-metadataEntries) {
				return nil
			}
			metadataEntries += int(req.GetNq())
			candidates[id] = &querypb.SegmentClusterSearchHints{SnapshotVersion: binding.stats.Version, NumRows: int64(stats.NumRows), Queries: make([]*querypb.QueryClusterSearchHints, req.GetNq())}
		}
		if !valid {
			continue
		}
		for q := int64(0); q < req.GetNq(); q++ {
			// A bad probe is ordinary for this partition/query, never empty scope.
			valid = true
			selected := make(map[int64]float32)
			order := make([]int64, 0, budget)
			for j := q * budget; j < (q+1)*budget; j++ {
				label, distance := labels[j], distances[j]
				if label == -1 {
					continue
				}
				if label < 0 || label >= int64(c.GetNumClusters()) || math.IsNaN(float64(distance)) || math.IsInf(float64(distance), 0) || len(binding.locations[uint32(label)]) == 0 {
					valid = false
					break
				}
				previous, seen := selected[label]
				if !seen {
					order = append(order, label)
				}
				if !seen || distance < previous {
					selected[label] = distance
				}
			}
			for _, candidate := range candidates {
				candidate.Queries[q] = &querypb.QueryClusterSearchHints{}
				if valid && len(selected) > 0 {
					candidate.Queries[q].Hints = &querypb.ClusterSearchHintList{}
				}
			}
			if !valid || len(selected) == 0 {
				fallback("invalid_probe_hit")
				continue
			}
			for _, label := range order {
				for _, location := range binding.locations[uint32(label)] {
					if candidate := candidates[location.segment]; candidate != nil {
						metadataEntries++
						if metadataEntries > maxClusteringHintEntries {
							return nil
						}
						candidate.Queries[q].Hints.Ranges = append(candidate.Queries[q].Hints.Ranges, &querypb.ClusterSearchHint{
							RowOffset: location.row.GetRowOffset(), RowCount: location.row.GetRowCount(), CentroidDistance: selected[label],
						})
					}
				}
			}
		}
		for id, candidate := range candidates {
			result[id] = candidate
		}
	}
	span.SetAttributes(attribute.Int("hint_entries", metadataEntries), attribute.Int("segments", len(result)), attribute.Int64("nq", req.GetNq()))
	return result
}
