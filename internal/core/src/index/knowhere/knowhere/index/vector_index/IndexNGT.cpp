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

#include "knowhere/index/vector_index/IndexNGT.h"

#include <omp.h>
#include <sstream>
#include <string>

#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus {
namespace knowhere {

BinarySet
IndexNGT::Serialize(const Config& config) {
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

    std::shared_ptr<uint8_t[]> obj_data(new uint8_t[obj_size]);
    memcpy(obj_data.get(), obj_str.data(), obj_size);
    std::shared_ptr<uint8_t[]> grp_data(new uint8_t[grp_size]);
    memcpy(grp_data.get(), grp_str.data(), grp_size);
    std::shared_ptr<uint8_t[]> prf_data(new uint8_t[prf_size]);
    memcpy(prf_data.get(), prf_str.data(), prf_size);
    std::shared_ptr<uint8_t[]> tre_data(new uint8_t[tre_size]);
    memcpy(tre_data.get(), tre_str.data(), tre_size);

    BinarySet res_set;
    res_set.Append("ngt_obj_data", obj_data, obj_size);
    res_set.Append("ngt_grp_data", grp_data, grp_size);
    res_set.Append("ngt_prf_data", prf_data, prf_size);
    res_set.Append("ngt_tre_data", tre_data, tre_size);
    if (config.contains(INDEX_FILE_SLICE_SIZE_IN_MEGABYTE)) {
        Disassemble(config[INDEX_FILE_SLICE_SIZE_IN_MEGABYTE].get<int64_t>() * 1024 * 1024, res_set);
    }
    return res_set;
}

void
IndexNGT::Load(const BinarySet& index_binary) {
    Assemble(const_cast<BinarySet&>(index_binary));
    auto obj_data = index_binary.GetByName("ngt_obj_data");
    std::string obj_str(reinterpret_cast<char*>(obj_data->data.get()), obj_data->size);

    auto grp_data = index_binary.GetByName("ngt_grp_data");
    std::string grp_str(reinterpret_cast<char*>(grp_data->data.get()), grp_data->size);

    auto prf_data = index_binary.GetByName("ngt_prf_data");
    std::string prf_str(reinterpret_cast<char*>(prf_data->data.get()), prf_data->size);

    auto tre_data = index_binary.GetByName("ngt_tre_data");
    std::string tre_str(reinterpret_cast<char*>(tre_data->data.get()), tre_data->size);

    std::stringstream obj(obj_str);
    std::stringstream grp(grp_str);
    std::stringstream prf(prf_str);
    std::stringstream tre(tre_str);

    index_ = std::shared_ptr<NGT::Index>(NGT::Index::loadIndex(obj, grp, prf, tre));
}

void
IndexNGT::BuildAll(const DatasetPtr& dataset_ptr, const Config& config) {
    KNOWHERE_THROW_MSG("IndexNGT has no implementation of BuildAll, please use IndexNGT(PANNG/ONNG) instead!");
}

#if 0
void
IndexNGT::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    KNOWHERE_THROW_MSG("IndexNGT has no implementation of Train, please use IndexNGT(PANNG/ONNG) instead!");
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
}

void
IndexNGT::AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    GET_TENSOR_DATA(dataset_ptr);

    index_->append(reinterpret_cast<const float*>(p_data), rows);
}
#endif

DatasetPtr
IndexNGT::Query(const DatasetPtr& dataset_ptr, const Config& config, const faiss::BitsetView bitset) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    GET_TENSOR_DATA(dataset_ptr);

    int k = config[meta::TOPK].get<int>();
    auto epsilon = config[IndexParams::epsilon].get<float>();
    auto edge_size = config[IndexParams::max_search_edges].get<int>();
    if (edge_size == -1) {  // pass -1
        edge_size--;
    }
    size_t id_size = sizeof(int64_t) * k;
    size_t dist_size = sizeof(float) * k;
    auto p_id = static_cast<int64_t*>(malloc(id_size * rows));
    auto p_dist = static_cast<float*>(malloc(dist_size * rows));

    NGT::Command::SearchParameter sp;
    sp.size = k;

#pragma omp parallel for
    for (unsigned int i = 0; i < rows; ++i) {
        const float* single_query = reinterpret_cast<float*>(const_cast<void*>(p_data)) + i * Dim();

        NGT::Object* object = index_->allocateObject(single_query, Dim());
        NGT::SearchContainer sc(*object);

        //        double epsilon = sp.beginOfEpsilon;

        NGT::ObjectDistances res;
        sc.setResults(&res);
        sc.setSize(static_cast<size_t>(sp.size));
        sc.setRadius(sp.radius);

        if (sp.accuracy > 0.0) {
            sc.setExpectedAccuracy(sp.accuracy);
        } else {
            sc.setEpsilon(epsilon);
        }
        //        sc.setEdgeSize(sp.edgeSize);
        sc.setEdgeSize(edge_size);

        try {
            index_->search(sc, bitset);
        } catch (NGT::Exception& err) {
            KNOWHERE_THROW_MSG("Query failed");
        }

        auto local_id = p_id + i * k;
        auto local_dist = p_dist + i * k;

        int64_t res_num = res.size();
        float dis_coefficient = 1.0;
        if (index_->getObjectSpace().getDistanceType() == NGT::ObjectSpace::DistanceType::DistanceTypeIP) {
            dis_coefficient = -1.0;
        }
        for (int64_t idx = 0; idx < res_num; ++idx) {
            *(local_id + idx) = res[idx].id - 1;
            *(local_dist + idx) = res[idx].distance * dis_coefficient;
        }
        MapOffsetToUid(local_id, res_num);
        while (res_num < static_cast<int64_t>(k)) {
            *(local_id + res_num) = -1;
            *(local_dist + res_num) = 1.0 / 0.0;
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

void
IndexNGT::UpdateIndexSize() {
    KNOWHERE_THROW_MSG("IndexNGT has no implementation of UpdateIndexSize, please use IndexNGT(PANNG/ONNG) instead!");
}

}  // namespace knowhere
}  // namespace milvus
