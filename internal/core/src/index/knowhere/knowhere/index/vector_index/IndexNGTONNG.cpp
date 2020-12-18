// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "knowhere/index/vector_index/IndexNGTONNG.h"

#include "NGT/lib/NGT/GraphOptimizer.h"

#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

#include <cstddef>
#include <memory>

namespace milvus {
namespace knowhere {

void
IndexNGTONNG::BuildAll(const DatasetPtr& dataset_ptr, const Config& config) {
    GET_TENSOR_DATA_DIM(dataset_ptr);

    NGT::Property prop;
    prop.setDefaultForCreateIndex();
    prop.dimension = dim;

    auto edge_size = config[IndexParams::edge_size].get<int64_t>();
    prop.edgeSizeForCreation = edge_size;
    prop.insertionRadiusCoefficient = 1.0;

    MetricType metric_type = config[Metric::TYPE];

    if (metric_type == Metric::L2) {
        prop.distanceType = NGT::Index::Property::DistanceType::DistanceTypeL2;
    } else if (metric_type == Metric::IP) {
        prop.distanceType = NGT::Index::Property::DistanceType::DistanceTypeIP;
    } else {
        KNOWHERE_THROW_MSG("Metric type not supported: " + metric_type);
    }

    index_ =
        std::shared_ptr<NGT::Index>(NGT::Index::createGraphAndTree(reinterpret_cast<const float*>(p_data), prop, rows));

    // reconstruct graph
    NGT::GraphOptimizer graphOptimizer(false);

    auto number_of_outgoing_edges = config[IndexParams::outgoing_edge_size].get<size_t>();
    auto number_of_incoming_edges = config[IndexParams::incoming_edge_size].get<size_t>();

    graphOptimizer.shortcutReduction = true;
    graphOptimizer.searchParameterOptimization = false;
    graphOptimizer.prefetchParameterOptimization = false;
    graphOptimizer.accuracyTableGeneration = false;
    graphOptimizer.margin = 0.2;
    graphOptimizer.gtEpsilon = 0.1;

    graphOptimizer.set(number_of_outgoing_edges, number_of_incoming_edges, 1000, 20);

    graphOptimizer.execute(*index_);
}

void
IndexNGTONNG::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    index_size_ = index_->memSize();
}

}  // namespace knowhere
}  // namespace milvus
