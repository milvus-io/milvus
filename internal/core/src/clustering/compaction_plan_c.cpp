// Copyright (C) 2019-2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "clustering/analyze_c.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "knowhere/cluster/cluster_factory.h"
#include "knowhere/cluster/compaction_result.h"
#include "knowhere/dataset.h"
#include "monitor/scope_metric.h"
#include "pb/clustering.pb.h"

using namespace milvus;

namespace {

knowhere::DataSetPtr
BuildFloatCentroidDataset(
    const milvus::proto::clustering::ClusteringCentroidsStats& stats) {
    AssertInfo(stats.centroids_size() > 0,
               "clustering compaction centroids are empty");
    const auto dim = stats.centroids(0).dim();
    AssertInfo(dim > 0, "clustering compaction centroid dim is invalid");
    AssertInfo(static_cast<uint64_t>(dim) <=
                   std::numeric_limits<size_t>::max() /
                       static_cast<uint64_t>(stats.centroids_size()),
               "clustering compaction centroid data size overflows");

    std::vector<float> values;
    values.reserve(static_cast<size_t>(stats.centroids_size()) *
                   static_cast<size_t>(dim));
    for (int index = 0; index < stats.centroids_size(); ++index) {
        const auto& centroid = stats.centroids(index);
        AssertInfo(centroid.dim() == dim,
                   "clustering compaction centroid dimensions differ");
        AssertInfo(centroid.has_float_vector(),
                   "clustering compaction only supports float centroids");
        const auto& data = centroid.float_vector().data();
        AssertInfo(
            data.size() == dim,
            "clustering compaction centroid data length differs from dim");
        values.insert(values.end(), data.begin(), data.end());
    }

    auto owned = std::make_unique<float[]>(values.size());
    std::copy(values.begin(), values.end(), owned.get());
    return knowhere::GenResultDataSet(
        stats.centroids_size(), dim, std::move(owned));
}

template <typename T>
knowhere::DataSetPtr
BuildHalfCentroidDataset(
    const milvus::proto::clustering::ClusteringCentroidsStats& stats,
    bool is_bfloat16) {
    AssertInfo(stats.centroids_size() > 0,
               "clustering compaction centroids are empty");
    const auto dim = stats.centroids(0).dim();
    AssertInfo(dim > 0, "clustering compaction centroid dim is invalid");
    AssertInfo(static_cast<uint64_t>(dim) <=
                   std::numeric_limits<size_t>::max() /
                       static_cast<uint64_t>(stats.centroids_size()),
               "clustering compaction centroid data size overflows");

    const auto total =
        static_cast<size_t>(stats.centroids_size()) * static_cast<size_t>(dim);
    auto owned = std::make_unique<T[]>(total);
    for (int index = 0; index < stats.centroids_size(); ++index) {
        const auto& centroid = stats.centroids(index);
        AssertInfo(centroid.dim() == dim,
                   "clustering compaction centroid dimensions differ");
        AssertInfo(is_bfloat16 ? centroid.has_bfloat16_vector()
                               : centroid.has_float16_vector(),
                   "clustering compaction centroid type differs from field");
        const auto& data = is_bfloat16 ? centroid.bfloat16_vector()
                                       : centroid.float16_vector();
        AssertInfo(data.size() == static_cast<size_t>(dim) * sizeof(T),
                   "clustering compaction centroid data length differs from "
                   "dim");
        std::memcpy(owned.get() + static_cast<size_t>(index) * dim,
                    data.data(),
                    data.size());
    }
    return knowhere::GenResultDataSet(
        stats.centroids_size(), dim, std::move(owned));
}

template <typename T>
knowhere::CompactionResult
BuildTypedCompactionPlan(const char* cluster_type,
                         const knowhere::DataSetPtr& centroid_dataset,
                         const std::vector<uint64_t>& counts,
                         const knowhere::Json& params) {
    auto cluster = knowhere::ClusterFactory::Instance().Create<T>(cluster_type);
    AssertInfo(cluster.has_value(),
               "failed to create clustering implementation: {}",
               cluster.what());
    auto cluster_node = std::move(cluster.value());
    auto set_status = cluster_node.SetCentroids(*centroid_dataset);
    AssertInfo(set_status == knowhere::Status::success ||
                   set_status == knowhere::Status::not_implemented,
               "failed to inject clustering centroids");

    auto plan = cluster_node.BuildCompactionPlan(counts, params);
    AssertInfo(plan.has_value(),
               "failed to build clustering compaction plan: {}",
               plan.what());
    return std::move(plan.value());
}

knowhere::CompactionResult&
GetPlan(CClusteringCompactionPlan plan) {
    AssertInfo(plan != nullptr, "clustering compaction plan is null");
    return *reinterpret_cast<knowhere::CompactionResult*>(plan);
}

}  // namespace

CStatus
BuildClusteringCompactionPlan(CClusteringCompactionPlan* result,
                              const uint8_t* serialized_centroids,
                              uint64_t serialized_centroids_len,
                              int32_t field_type,
                              const uint64_t* centroid_counts,
                              uint64_t centroid_count,
                              const char* cluster_type,
                              const char* params_json) {
    SCOPE_CGO_CALL_METRIC();
    try {
        AssertInfo(result != nullptr,
                   "clustering compaction plan result is null");
        *result = nullptr;
        AssertInfo(serialized_centroids != nullptr,
                   "serialized clustering centroids are null");
        AssertInfo(centroid_counts != nullptr || centroid_count == 0,
                   "clustering centroid counts are null");
        AssertInfo(cluster_type != nullptr && std::strlen(cluster_type) > 0,
                   "clustering type is empty");
        AssertInfo(serialized_centroids_len <=
                       static_cast<uint64_t>(std::numeric_limits<int>::max()),
                   "serialized clustering centroids are too large");

        milvus::proto::clustering::ClusteringCentroidsStats centroids;
        AssertInfo(centroids.ParseFromArray(
                       serialized_centroids,
                       static_cast<int>(serialized_centroids_len)),
                   "failed to unmarshal clustering centroids");
        AssertInfo(
            static_cast<uint64_t>(centroids.centroids_size()) == centroid_count,
            "clustering centroid count does not match count vector");

        std::vector<uint64_t> counts;
        if (centroid_count > 0) {
            counts.assign(centroid_counts, centroid_counts + centroid_count);
        }
        const auto params =
            knowhere::Json::parse(params_json == nullptr ? "{}" : params_json);
        knowhere::CompactionResult plan;
        switch (static_cast<DataType>(field_type)) {
            case DataType::VECTOR_FLOAT:
                plan = BuildTypedCompactionPlan<knowhere::fp32>(
                    cluster_type,
                    BuildFloatCentroidDataset(centroids),
                    counts,
                    params);
                break;
            case DataType::VECTOR_FLOAT16:
                plan = BuildTypedCompactionPlan<knowhere::fp16>(
                    cluster_type,
                    BuildHalfCentroidDataset<knowhere::fp16>(centroids, false),
                    counts,
                    params);
                break;
            case DataType::VECTOR_BFLOAT16:
                plan = BuildTypedCompactionPlan<knowhere::bf16>(
                    cluster_type,
                    BuildHalfCentroidDataset<knowhere::bf16>(centroids, true),
                    counts,
                    params);
                break;
            default:
                ThrowInfo(DataTypeInvalid,
                          "invalid clustering compaction field type {}",
                          field_type);
        }
        *result = new knowhere::CompactionResult(std::move(plan));
        return milvus::SuccessCStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(milvus::UnexpectedError, error.what());
    }
}

CStatus
GetClusteringCompactionPlanMeta(CClusteringCompactionPlan plan,
                                uint64_t* row_count,
                                uint32_t* centroid_count,
                                const uint64_t** centroid_counts,
                                uint64_t* group_count) {
    SCOPE_CGO_CALL_METRIC();
    try {
        AssertInfo(row_count != nullptr && centroid_count != nullptr &&
                       centroid_counts != nullptr && group_count != nullptr,
                   "clustering compaction plan metadata output is null");
        const auto& value = GetPlan(plan);
        *row_count = value.row_count;
        *centroid_count = value.centroid_count;
        *centroid_counts = value.centroid_counts.data();
        *group_count = value.centroid_groups.size();
        return milvus::SuccessCStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(milvus::UnexpectedError, error.what());
    }
}

CStatus
GetClusteringCompactionPlanGroup(CClusteringCompactionPlan plan,
                                 uint64_t group_offset,
                                 CClusteringCentroidGroup* group) {
    SCOPE_CGO_CALL_METRIC();
    try {
        AssertInfo(group != nullptr,
                   "clustering centroid group output is null");
        const auto& value = GetPlan(plan);
        AssertInfo(group_offset < value.centroid_groups.size(),
                   "clustering centroid group offset is out of range");
        const auto& source = value.centroid_groups[group_offset];
        group->centroid_group_id = source.centroid_group_id;
        group->rows = source.rows;
        group->centroid_count = source.centroids.size();
        group->centroids = source.centroids.data();
        return milvus::SuccessCStatus();
    } catch (const std::exception& error) {
        return milvus::FailureCStatus(milvus::UnexpectedError, error.what());
    }
}

CStatus
DeleteClusteringCompactionPlan(CClusteringCompactionPlan plan) {
    SCOPE_CGO_CALL_METRIC();
    delete reinterpret_cast<knowhere::CompactionResult*>(plan);
    return milvus::SuccessCStatus();
}
