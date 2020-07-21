// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <memory>
#include <string>
#include <vector>

#ifdef MILVUS_GPU_VERSION
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuCloner.h>
#endif
#include <faiss/IndexFlat.h>
#include <faiss/IndexScalarQuantizer.h>
#include <faiss/clone_index.h>
#include <faiss/impl/ScalarQuantizer.h>
#include <faiss/index_factory.h>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "knowhere/index/vector_offset_index/IndexIVFSQNR_NM.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/gpu/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

namespace milvus {
namespace knowhere {

BinarySet
IVFSQNR_NM::Serialize(const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    BinarySet res_set = SerializeImpl(index_type_);

    size_t d = index_->d;
    auto ivfsq_index = dynamic_cast<faiss::IndexIVFScalarQuantizer*>(index_.get());
    auto invlists = ivfsq_index->invlists;
    auto rows = invlists->compute_ntotal();
    auto sq = ivfsq_index->sq;
    auto code_size = ivfsq_index->code_size;
    auto arranged_data = new uint8_t[code_size * rows + 2 * d * sizeof(float)];
    size_t curr_index = 0;

    // convert arranged sq8 data to sq8 data
    auto ails = dynamic_cast<faiss::ArrayInvertedLists*>(invlists);
    for (size_t i = 0; i < invlists->nlist; i++) {
        auto list_size = ails->ids[i].size();
        for (size_t j = 0; j < list_size; j++) {
            memcpy(arranged_data + code_size * ails->ids[i][j], data_.get() + code_size * (curr_index + j), code_size);
        }
        curr_index += list_size;
    }

    memcpy(arranged_data + code_size * curr_index, sq.trained.data(), 2 * d * sizeof(float));

    res_set.Append(SQ8_DATA, std::shared_ptr<uint8_t[]>(arranged_data),
                   code_size * rows * sizeof(uint8_t) + 2 * d * sizeof(float));
    return res_set;
}

void
IVFSQNR_NM::Load(const BinarySet& binary_set) {
    std::lock_guard<std::mutex> lk(mutex_);
    data_ = binary_set.GetByName(SQ8_DATA)->data;
    LoadImpl(binary_set, index_type_);
    // arrange sq8 data
    auto ivfsq_index = dynamic_cast<faiss::IndexIVFScalarQuantizer*>(index_.get());
    auto invlists = ivfsq_index->invlists;
    auto rows = invlists->compute_ntotal();
    auto sq = ivfsq_index->sq;
    auto code_size = sq.code_size;
    auto d = sq.d;
    auto arranged_data = new uint8_t[code_size * rows + 2 * d * sizeof(float)];
    prefix_sum.resize(invlists->nlist);
    size_t curr_index = 0;

#ifndef MILVUS_GPU_VERSION
    auto ails = dynamic_cast<faiss::ArrayInvertedLists*>(invlists);
    for (size_t i = 0; i < invlists->nlist; i++) {
        auto list_size = ails->ids[i].size();
        for (size_t j = 0; j < list_size; j++) {
            memcpy(arranged_data + code_size * (curr_index + j), data_.get() + code_size * ails->ids[i][j], code_size);
        }
        prefix_sum[i] = curr_index;
        curr_index += list_size;
    }
#else
    auto rol = dynamic_cast<faiss::ReadOnlyArrayInvertedLists*>(invlists);
    auto lengths = rol->readonly_length;
    auto rol_ids = (const int64_t*)rol->pin_readonly_ids->data;
    for (size_t i = 0; i < invlists->nlist; i++) {
        auto list_size = lengths[i];
        for (size_t j = 0; j < list_size; j++) {
            memcpy(arranged_data + code_size * (curr_index + j), data_.get() + code_size * rol_ids[curr_index + j],
                   code_size);
        }
        prefix_sum[i] = curr_index;
        curr_index += list_size;
    }
#endif
    memcpy(arranged_data + code_size * curr_index, sq.trained.data(), 2 * d * sizeof(float));
    data_ = std::shared_ptr<uint8_t[]>(arranged_data);
}

void
IVFSQNR_NM::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    GET_TENSOR_DATA_DIM(dataset_ptr)

    faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
    faiss::Index* coarse_quantizer = new faiss::IndexFlat(dim, metric_type);
    index_ = std::shared_ptr<faiss::Index>(
        new faiss::IndexIVFScalarQuantizer(coarse_quantizer, dim, config[IndexParams::nlist].get<int64_t>(),
                                           faiss::QuantizerType::QT_8bit, metric_type, false));

    index_->train(rows, (float*)p_data);
}

void
IVFSQNR_NM::Add(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    GET_TENSOR_DATA_ID(dataset_ptr)
    index_->add_with_ids_without_codes(rows, (float*)p_data, p_ids);

    ArrangeCodes(dataset_ptr, config);
}

void
IVFSQNR_NM::AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    GET_TENSOR_DATA(dataset_ptr)
    index_->add_without_codes(rows, (float*)p_data);

    ArrangeCodes(dataset_ptr, config);
}

VecIndexPtr
IVFSQNR_NM::CopyCpuToGpu(const int64_t device_id, const Config& config) {
#ifdef MILVUS_GPU_VERSION
    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(device_id)) {
        ResScope rs(res, device_id, false);

        auto gpu_index =
            faiss::gpu::index_cpu_to_gpu_without_codes(res->faiss_res.get(), device_id, index_.get(), data_.get());

        std::shared_ptr<faiss::Index> device_index;
        device_index.reset(gpu_index);
        return std::make_shared<GPUIVFSQ>(device_index, device_id, res);
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu_resource");
    }
#else
    KNOWHERE_THROW_MSG("Calling IVFSQNR_NM::CopyCpuToGpu when we are using CPU version");
#endif
}

void
IVFSQNR_NM::ArrangeCodes(const DatasetPtr& dataset_ptr, const Config& config) {
    GET_TENSOR_DATA_DIM(dataset_ptr)

    // Construct arranged sq8 data from original data
    const float* original_data = (const float*)p_data;
    auto ivfsq_index = dynamic_cast<faiss::IndexIVFScalarQuantizer*>(index_.get());
    auto sq = ivfsq_index->sq;
    auto invlists = ivfsq_index->invlists;
    auto code_size = sq.code_size;
    auto arranged_data = new uint8_t[code_size * rows + 2 * dim * sizeof(float)];
    std::unique_ptr<faiss::Quantizer> squant(sq.select_quantizer());
    std::vector<uint8_t> one_code(code_size);
    size_t curr_index = 0;

    auto ails = dynamic_cast<faiss::ArrayInvertedLists*>(invlists);
    for (size_t i = 0; i < invlists->nlist; i++) {
        auto list_size = ails->ids[i].size();
        for (size_t j = 0; j < list_size; j++) {
            const float* x_j = original_data + dim * ails->ids[i][j];

            memset(one_code.data(), 0, code_size);
            squant->encode_vector(x_j, one_code.data());
            memcpy(arranged_data + code_size * (curr_index + j), one_code.data(), code_size);
        }
        curr_index += list_size;
    }

    memcpy(arranged_data + code_size * curr_index, sq.trained.data(), 2 * dim * sizeof(float));
    data_ = std::shared_ptr<uint8_t[]>(arranged_data);
}

void
IVFSQNR_NM::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    auto ivfsq_index = dynamic_cast<faiss::IndexIVFScalarQuantizer*>(index_.get());
    auto nb = ivfsq_index->invlists->compute_ntotal();
    auto nlist = ivfsq_index->nlist;
    auto d = ivfsq_index->d;
    // ivf ids, sq trained vectors and quantizer
    index_size_ = nb * sizeof(int64_t) + 2 * d * sizeof(float) + nlist * d * sizeof(float);
}

}  // namespace knowhere
}  // namespace milvus
