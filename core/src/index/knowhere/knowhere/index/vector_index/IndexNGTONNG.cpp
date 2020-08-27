#include "knowhere/index/vector_index/IndexNGTONNG.h"

#include "NGT/lib/NGT/GraphOptimizer.h"

#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus {
namespace knowhere {

void
IndexNGTONNG::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    GET_TENSOR_DATA_DIM(dataset_ptr);

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
