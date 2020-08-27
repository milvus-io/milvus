#include "knowhere/index/vector_index/IndexNGTPANNG.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus {
namespace knowhere {

void
IndexNGTPANNG::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    GET_TENSOR_DATA_DIM(dataset_ptr);

    NGT::Property prop;
    prop.setDefaultForCreateIndex();
    prop.dimension = dim;

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

    size_t force_removed_edge_size = 60;
    size_t selective_removed_edge_size = 30;

    // prune
    NGT::GraphIndex& graph = dynamic_cast<NGT::GraphIndex&>(index_->getIndex());
    for (size_t id = 1; id < graph.repository.size(); id++) {
        try {
            NGT::GraphNode& node = *graph.getNode(id);
            if (node.size() >= force_removed_edge_size) {
                node.resize(force_removed_edge_size);
            }
            if (node.size() >= selective_removed_edge_size) {
                size_t rank = 0;
                for (auto i = node.begin(); i != node.end(); ++rank) {
                    if (rank >= selective_removed_edge_size) {
                        bool found = false;
                        for (size_t t1 = 0; t1 < node.size() && found == false; ++t1) {
                            if (t1 >= selective_removed_edge_size) {
                                break;
                            }
                            if (rank == t1) {
                                continue;
                            }
                            NGT::GraphNode& node2 = *graph.getNode(node[t1].id);
                            for (size_t t2 = 0; t2 < node2.size(); ++t2) {
                                if (t2 >= selective_removed_edge_size) {
                                    break;
                                }
                                if (node2[t2].id == (*i).id) {
                                    found = true;
                                    break;
                                }
                            }  // for
                        }      // for
                        if (found) {
                            // remove
                            i = node.erase(i);
                            continue;
                        }
                    }
                    i++;
                }  // for
            }
        } catch (NGT::Exception& err) {
            std::cerr << "Graph::search: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
            continue;
        }
    }
}

}  // namespace knowhere
}  // namespace milvus
