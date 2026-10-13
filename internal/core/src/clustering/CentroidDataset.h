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

#pragma once
#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include "common/EasyAssert.h"
#include "knowhere/dataset.h"
#include "pb/clustering.pb.h"

namespace milvus::clustering {
inline knowhere::DataSetPtr
BuildCentroidDataset(
    const milvus::proto::clustering::ClusteringCentroidsStats& stats) {
    AssertInfo(stats.centroids_size() > 0,
               "clustering compaction centroids are empty");
    const auto dim = stats.centroids(0).dim();
    AssertInfo(dim > 0, "clustering compaction centroid dim is invalid");
    AssertInfo(static_cast<uint64_t>(dim) <=
                   std::numeric_limits<size_t>::max() / sizeof(float) /
                       static_cast<uint64_t>(stats.centroids_size()),
               "clustering compaction centroid data size overflows");

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
        for (const auto value : data) {
            AssertInfo(std::isfinite(value), "non-finite clustering centroid");
        }
    }

    // Validate every declared shape before allocating from metadata dimensions.
    auto owned = std::make_unique<float[]>(
        static_cast<size_t>(stats.centroids_size()) * dim);
    for (int i = 0; i < stats.centroids_size(); ++i) {
        const auto& data = stats.centroids(i).float_vector().data();
        std::copy(data.begin(),
                  data.end(),
                  owned.get() + static_cast<size_t>(i) * dim);
    }
    return knowhere::GenResultDataSet(
        stats.centroids_size(), dim, std::move(owned));
}

}  // namespace milvus::clustering
