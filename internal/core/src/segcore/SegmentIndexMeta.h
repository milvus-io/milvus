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

#pragma once

#include <map>
#include <string>
#include <utility>

#include "common/EasyAssert.h"
#include "common/IndexMeta.h"
#include "knowhere/emb_list_utils.h"
#include "pb/segcore.pb.h"

namespace milvus::segcore {

// BuildSegmentIndexMeta derives a segment's index configuration from its own load
// info, replacing the collection-wide copy segments used to be handed at creation.
//
// That copy was a per-node mutable global refreshed out of band, and every consumer
// of it turned out to have a local source: search resolves the metric from the
// segment's loaded index, and interim-index construction needs only the per-field
// index params, which travel with the segment in SegmentLoadInfo.index_infos.
//
// max_index_row_cnt comes from the segment's own load info. It scales the
// interim-index build threshold (VecIndexConfig::GetBuildThreshold), and
// IndexingRecord additionally refuses to build anything when it is not positive,
// so it cannot simply be dropped: leaving it at 0 silently disables the growing
// interim index entirely. QueryNode computes it from the schema and DataCoord's
// segment size budget -- the same expression the collection-wide index meta used
// -- and stamps it onto SegmentLoadInfo.
inline IndexMetaPtr
BuildSegmentIndexMeta(
    const milvus::proto::segcore::SegmentLoadInfo* load_info) {
    std::map<FieldId, FieldIndexMeta> field_metas;
    if (load_info != nullptr) {
        for (const auto& index_info : load_info->index_infos()) {
            std::map<std::string, std::string> index_params;
            for (const auto& kv : index_info.index_params()) {
                index_params.emplace(kv.key(), kv.value());
            }
            if (index_params.empty()) {
                continue;
            }
            auto field_id = FieldId(index_info.fieldid());
            field_metas.emplace(field_id,
                                FieldIndexMeta(field_id,
                                               std::move(index_params),
                                               /*type_params=*/{}));
        }
    }
    int64_t max_index_row_cnt =
        load_info != nullptr ? load_info->max_index_row_count() : 0;
    return std::make_shared<CollectionIndexMeta>(max_index_row_cnt,
                                                 std::move(field_metas));
}

inline MetricType
ResolveMetricTypeFromIndexMeta(const IndexMetaPtr& index_meta,
                               FieldId field_id) {
    if (index_meta == nullptr || !index_meta->HasField(field_id)) {
        return {};
    }
    const auto& index_params =
        index_meta->GetFieldIndexMeta(field_id).GetIndexParams();
    auto metric = index_params.find(knowhere::meta::METRIC_TYPE);
    return metric == index_params.end() ? MetricType() : metric->second;
}

// A search may omit its metric, but the producing segment may not: score
// orientation, reduce ordering, and refinement all depend on the metric the
// segment actually searched with. When the request names one explicitly it
// must match the segment's own index configuration before any search runs.
inline MetricType
ResolveSearchMetricType(const MetricType& requested_metric,
                        const MetricType& segment_metric,
                        FieldId field_id) {
    if (segment_metric.empty()) {
        // Legacy/test-created segments may not carry index configuration. An
        // explicit request metric is still sufficient to execute them safely;
        // only an omitted request requires the segment to supply the metric.
        if (!requested_metric.empty()) {
            return requested_metric;
        }
        ThrowInfo(FieldNotLoaded,
                  "segment has no metric type for vector field {}",
                  field_id.get());
    }
    if (!requested_metric.empty() && requested_metric != segment_metric) {
        ThrowInfo(MetricTypeNotMatch,
                  "metric type of field index is not the same as search "
                  "request, field {}, field index: {}, search request: {}",
                  field_id.get(),
                  segment_metric,
                  requested_metric);
    }
    return requested_metric.empty() ? segment_metric : requested_metric;
}

// VECTOR_ARRAY supports two distinct request shapes. Plain vector placeholders
// search individual array elements and therefore require a scalar-vector metric;
// embedding-list placeholders search rows and require a MAX_SIM_* metric. The
// plan can validate this immediately only when the request names a metric. When
// it does not, validate against the segment-resolved metric at the last boundary
// before search execution.
inline void
ValidateVectorArraySearchMode(const MetricType& metric_type,
                              bool element_level,
                              FieldId field_id) {
    bool embedding_list_metric =
        knowhere::get_el_metric_type(metric_type).has_value();
    if (embedding_list_metric == element_level) {
        ThrowInfo(DataTypeInvalid,
                  "search type mismatch for VECTOR_ARRAY field {}: metric_type "
                  "{} {} embedding list search, but search data is {}",
                  field_id.get(),
                  metric_type,
                  embedding_list_metric ? "requires" : "does not support",
                  element_level ? "plain vector" : "embedding list");
    }
}

}  // namespace milvus::segcore
