#include "knowhere/index/vector_index/IndexNGT.h"

#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>
#include <omp.h>
#include <sstream>

#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus {
namespace knowhere {

BinarySet
IndexNGT::Serialize(const Config& config = Config()) override {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }
    std::stringstream obj, grp, prf, tre;
    index_->saveIndex(obj, grp, prf, tre);

    auto obj_str = obj.str();
    auto grp_str = grp.str();
    auto prf_str = prf.str();
    auto tre_str = tre.str();
    uint64_t obj_size = obj_str.size();
    uint64_t grp_size = grp_str.size();
    uint64_t prf_size = prf_str.size();
    uint64_t tre_size = tre_str.size();

    std::shared_ptr<uint8_t[]> obj_size_data(new uint8_t[sizeof(uint64_t)]);
    memcpy(obj_size_data.get(), &obj_size, sizeof(uint64_t));
    std::shared_ptr<uint8_t[]> grp_size_data(new uint8_t[sizeof(uint64_t)]);
    memcpy(grp_size_data.get(), &grp_size, sizeof(uint64_t));
    std::shared_ptr<uint8_t[]> prf_size_data(new uint8_t[sizeof(uint64_t)]);
    memcpy(prf_size_data.get(), &prf_size, sizeof(uint64_t));
    std::shared_ptr<uint8_t[]> tre_size_data(new uint8_t[sizeof(uint64_t)]);
    memcpy(tre_size_data.get(), &tre_size, sizeof(uint64_t));

    std::shared_ptr<uint8_t[]> obj_data(new uint8_t[obj_size]);
    memcpy(obj_data.get(), obj_str.data(), obj_size);
    std::shared_ptr<uint8_t[]> grp_data(new uint8_t[grp_size]);
    memcpy(grp_data.get(), grp_str.data(), grp_size);
    std::shared_ptr<uint8_t[]> prf_data(new uint8_t[prf_size]);
    memcpy(prf_data.get(), prf_str.data(), prf_size);
    std::shared_ptr<uint8_t[]> tre_data(new uint8_t[tre_size]);
    memcpy(tre_data.get(), tre_str.data(), tre_size);

    BinarySet res_set;
    res_set.Append("ngt_obj_size", obj_size_data, sizeof(uint64_t));
    res_set.Append("ngt_obj_data", obj_data, obj_size);
    res_set.Append("ngt_grp_size", grp_size_data, sizeof(uint64_t));
    res_set.Append("ngt_grp_data", grp_data, grp_size);
    res_set.Append("ngt_prf_size", prf_size_data, sizeof(uint64_t));
    res_set.Append("ngt_prf_data", prf_data, prf_size);
    res_set.Append("ngt_tre_size", tre_size_data, sizeof(uint64_t));
    res_set.Append("ngt_tre_data", tre_data, tre_size);
    return res_set;
}

void
IndexNGT::Load(const BinarySet& index_binary) override {
    auto obj_size = index_binary.GetByName("ngt_obj_size");
    uint64_t obj_size_;
    memcpy(&obj_size_, obj_size->data.get(), sizeof(uint64_t));
    auto obj_data = index_binary.GetByName("ngt_obj_data");
    std::string obj_str(obj_data->data.get(), obj_size);

    auto grp_size = index_binary.GetByName("ngt_grp_size");
    uint64_t grp_size_;
    memcpy(&grp_size_, grp_size->data.get(), sizeof(uint64_t));
    auto grp_data = index_binary.GetByName("ngt_grp_data");
    std::string grp_str(grp_data->data.get(), grp_size);

    auto prf_size = index_binary.GetByName("ngt_prf_size");
    uint64_t prf_size_;
    memcpy(&prf_size_, prf_size->data.get(), sizeof(uint64_t));
    auto prf_data = index_binary.GetByName("ngt_prf_data");
    std::string prf_str(prf_data->data.get(), prf_size);

    auto tre_size = index_binary.GetByName("ngt_tre_size");
    uint64_t tre_size_;
    memcpy(&tre_size_, tre_size->data.get(), sizeof(uint64_t));
    auto tre_data = index_binary.GetByName("ngt_tre_data");
    std::string tre_str(tre_data->data.get(), tre_size);

    std::stringstream obj(obj_str);
    std::stringstream grp(grp_str);
    std::stringstream prf(prf_str);
    std::stringstream tre(tre_str);

    index_ = NGT::Index::loadIndex(obj, grp, prf, tre);
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
