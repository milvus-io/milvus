#include "knowhere/index/vector_index/IndexNGT.h"

#include <bits/stdint-intn.h>
#include <omp.h>

#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus {
namespace knowhere {

BinarySet
IndexNGT::Serialize(const Config& config = Config()) override {
    // TODO
}

void
IndexNGT::Load(const BinarySet& index_binary) override {
    // TODO
}

void
IndexNG::Train(const DatasetPtr& dataset_ptr, const Config& config) override {
    GET_TENSOR_DATA_DIM(dataset_ptr);

    NGT::Property prop;
    prop.setDefaultForCreateIndex();
    prop.dimension = dim;

    auto metric_type = config[Metric::TYPE];
    switch (metric_type) {
        case Metric::L2:
            prop.distanceType = NGT::Index::Property::DistanceType::DistanceTypeL2;
            break;
        case Metric::HAMMING:
            prop.distanceType = NGT::Index::Property::DistanceType::DistanceTypeHamming;
            break;
        case Metric::JACCARD:
            prop.distanceType = NGT::Index::Property::DistanceType::DistanceTypeJaccard;
            break;
        default:
            KNOWHERE_THROW_MSG("Metric type not supported: " + metric_type);
    }
    index_ = std::make_shared<NGT::Index>(NGT::Index::createGraphAndTree(p_data, prop, rows));
}

void
IndexNGT::AddWithoutIds(const DatasetPtr dataset_ptr, const Config() config) override {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    GET_TENSOR_DATA(dataset_ptr);

    index_->append(p_data, rows);
}

DatasetPtr
IndexNGT::Query(const DatasetPtr& dataset_ptr, const Config& config) override {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    GET_TENSOR_DATA(dataset_ptr);

    size_t k = config[meta::TOPK].get<int64_t>();
    size_t id_size = sizeof(int64_t) * k;
    size_t dist_size = sizeof(float) * k;
    auto p_id = (int64_t*)malloc(id_size * rows);
    auto p_dist = (float*)malloc(dist_size * rows);

    NGT::Command::SearchParameter sp;
    sp.size = k;

#pragma omp parallel for
    for (unsigned int i = 0; i < rows; ++i) {
        const float* single_query = (float*)p_data + i * Dim();

        NGT::Object* object = index_->allocateObject(single_query, Dim());
        NGT::SearchContainer sc(*object);

        size_t step = sp.step == 0 ? UINT_MAX : sp.step;
        double epsilon;
        if (sp.step != 0) {
            epsilon = sp.beginOfEpsilon + (sp.endOfEpsilon - sp.beginOfEpsilon) * n / step;
        } else {
            epsilon = sp.beginOfEpsilon + sp.stepOfEpsilon * n;
            if (epsilon > sp.endOfEpsilon) {
                break;
            }
        }

        NGT::ObjectDistances res;
        sc.setResults(&res);
        sc.setSize(sp.size);
        sc.setRadius(sp.radius);

        if (sp.accuracy > 0.0) {
            sc.setExpectedAccuracy(sp.accuracy);
        } else {
            sc.setEpsilon(epsilon);
        }
        sc.setEdgeSize(sp.edgeSize);

        try {
            index_->search(sc);
        } catch (NGT::Exception& err) {
            throw err;
        }

        auto local_id = p_id + i * k;
        auto local_dist = p_dist + i * k;

        int64_t res_num = res.size();
        for (int64_t idx = 0; idx < res_num; ++idx) {
            *(local_id + idx) = res[idx].id;
            *(local_dist + idx) = res[idx].distance;
        }
        while (res_num < k) {
            *(local_id + res_num) = 0;
            *(local_dist + res_num) = -1;
        }
        index_->deleteObject(object);
    }

    auto res_ds = std::make_shared<Dataset>();
    res_ds->Set(meta::IDS, p_id);
    res_ds->Set(meta::DISTANCE, p_dist);
    return res_ds;
}

int64_t
IndexNGT::Count() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->getNumberOfVectors();
}

int64_t
IndexNGT::Dim() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->getDimension();
}

}  // namespace knowhere
}  // namespace milvus
