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

#include "knowhere/index/vector_index/adapter/SptagAdapter.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"

namespace milvus {
namespace knowhere {

std::shared_ptr<SPTAG::MetadataSet>
ConvertToMetadataSet(const DatasetPtr& dataset_ptr) {
    auto elems = dataset_ptr->Get<int64_t>(meta::ROWS);

    auto p_id = new int64_t[elems];
    for (int64_t i = 0; i < elems; ++i) p_id[i] = i;

    auto p_offset = new int64_t[elems + 1];
    for (int64_t i = 0; i <= elems; ++i) p_offset[i] = i * 8;

    std::shared_ptr<SPTAG::MetadataSet> metaset(
        new SPTAG::MemMetadataSet(SPTAG::ByteArray((std::uint8_t*)p_id, elems * sizeof(int64_t), true),
                                  SPTAG::ByteArray((std::uint8_t*)p_offset, elems * sizeof(int64_t), true), elems));

    return metaset;
}

std::shared_ptr<SPTAG::VectorSet>
ConvertToVectorSet(const DatasetPtr& dataset_ptr) {
    GET_TENSOR_DATA_DIM(dataset_ptr)
    size_t num_bytes = rows * dim * sizeof(float);
    SPTAG::ByteArray byte_array((uint8_t*)p_data, num_bytes, false);

    auto vectorset = std::make_shared<SPTAG::BasicVectorSet>(byte_array, SPTAG::VectorValueType::Float, dim, rows);
    return vectorset;
}

std::vector<SPTAG::QueryResult>
ConvertToQueryResult(const DatasetPtr& dataset_ptr, const Config& config) {
    GET_TENSOR_DATA_DIM(dataset_ptr);

    int64_t k = config[meta::TOPK].get<int64_t>();
    std::vector<SPTAG::QueryResult> query_results(rows, SPTAG::QueryResult(nullptr, k, true));
    for (auto i = 0; i < rows; ++i) {
        query_results[i].SetTarget((float*)p_data + i * dim);
    }

    return query_results;
}

DatasetPtr
ConvertToDataset(std::vector<SPTAG::QueryResult> query_results, std::shared_ptr<std::vector<int64_t>> uid) {
    auto k = query_results[0].GetResultNum();
    auto elems = query_results.size() * k;

    size_t p_id_size = sizeof(int64_t) * elems;
    size_t p_dist_size = sizeof(float) * elems;
    auto p_id = (int64_t*)malloc(p_id_size);
    auto p_dist = (float*)malloc(p_dist_size);

#pragma omp parallel for
    for (size_t i = 0; i < query_results.size(); ++i) {
        auto results = query_results[i].GetResults();
        auto num_result = query_results[i].GetResultNum();
        for (auto j = 0; j < num_result; ++j) {
            //            p_id[i * k + j] = results[j].VID;
            auto id = *(int64_t*)query_results[i].GetMetadata(j).Data();
            if (uid != nullptr) {
                if (id >= 0) {
                    id = uid->at(id);
                }
            }
            p_id[i * k + j] = id;
            p_dist[i * k + j] = results[j].Dist;
        }
    }

    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::IDS, p_id);
    ret_ds->Set(meta::DISTANCE, p_dist);
    return ret_ds;
}

}  // namespace knowhere
}  // namespace milvus
