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

#include <memory>

namespace milvus {
namespace knowhere {

void
IndexNGTONNG::BuildAll(const DatasetPtr& dataset_ptr, const Config& config) {
    GETTENSOR(dataset_ptr);

    NGT::Property prop;
    prop.setDefaultForCreateIndex();
    prop.dimension = dim;
    prop.edgeSizeForCreation = 20;
    prop.insertionRadiusCoefficient = 1.0;

    MetricType metric_type = config[Metric::TYPE];

    if (metric_type == Metric::L2)
        prop.distanceType = NGT::Index::Property::DistanceType::DistanceTypeL2;
    else if (metric_type == Metric::HAMMING)
        prop.distanceType = NGT::Index::Property::DistanceType::DistanceTypeHamming;
    else if (metric_type == Metric::JACCARD)
        prop.distanceType = NGT::Index::Property::DistanceType::DistanceTypeJaccard;
    else
        KNOWHERE_THROW_MSG("Metric type not supported: " + metric_type);
    index_ =
        std::shared_ptr<NGT::Index>(NGT::Index::createGraphAndTree(reinterpret_cast<const float*>(p_data), prop, rows));

    // reconstruct graph
    NGT::GraphOptimizer graphOptimizer(true);

    size_t number_of_outgoing_edges = 5;
    size_t number_of_incoming_edges = 30;
    size_t number_of_queries = 1000;
    size_t number_of_res = 20;

    graphOptimizer.shortcutReduction = true;
    graphOptimizer.searchParameterOptimization = true;
    graphOptimizer.prefetchParameterOptimization = false;
    graphOptimizer.accuracyTableGeneration = false;
    graphOptimizer.margin = 0.2;
    graphOptimizer.gtEpsilon = 0.1;

    graphOptimizer.set(number_of_outgoing_edges, number_of_incoming_edges, number_of_queries, number_of_res);

    graphOptimizer.execute(*index_);
}

}  // namespace knowhere
}  // namespace milvus
