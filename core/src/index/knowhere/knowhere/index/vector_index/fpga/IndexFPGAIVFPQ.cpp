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

#pragma once
#include "knowhere/index/vector_index/fpga/IndexFPGAIVFPQ.h"
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVF.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/index_io.h>
#include <string>
#include <vector>
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/fpga/utils.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "utils/Log.h"

namespace milvus {
namespace knowhere {

void
FPGAIVFPQ::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    GETTENSOR(dataset_ptr)
    LOG_ENGINE_DEBUG_ << " fpga ivpq train. dim:" << dim;
    faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
    faiss::Index* coarse_quantizer = new faiss::IndexFlatL2(dim);
    index_ = std::shared_ptr<faiss::Index>(
        new faiss::IndexIVFPQ(coarse_quantizer, dim, config[IndexParams::nlist].get<int64_t>(),
                              config[IndexParams::m].get<int64_t>(), config[IndexParams::nbits].get<int64_t>()));

    index_->train(rows, (float*)p_data);
}
void
FPGAIVFPQ::Add(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }
    LOG_ENGINE_DEBUG_ << " fpga ivpq add. ";
    std::lock_guard<std::mutex> lk(mutex_);
    // GET_TENSOR_DATA_ID(dataset_ptr)
    GETTENSOR(dataset_ptr)
    index_->add(rows, (float*)p_data);  // we not support add_with_id ,maybe support latter
}

void
FPGAIVFPQ::CopyIndexToFpga() {
    std::lock_guard<std::mutex> lk(mutex_);
    auto Fpga = Fpga::FpgaInst::GetInstance();
    auto ivf_index = static_cast<faiss::IndexIVFPQ*>(index_.get());
    ivf_index->make_direct_map();
    Fpga->setIndex(ivf_index);
    Fpga->CopyIndexToFPGA();

    LOG_ENGINE_DEBUG_ << " copy index to fpga end";
}
void
FPGAIVFPQ::QueryImpl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels, const Config& config) {
    std::lock_guard<std::mutex> lk(mutex_);

    try {
        LOG_ENGINE_DEBUG_ << " run fpga search QueryImpl";
        auto params = GenParams(config);
        int nprobe = params->nprobe;
        LOG_ENGINE_DEBUG_ << "nprobe:" << nprobe << "n:" << n << "k:" << k;
        auto Fpga = Fpga::FpgaInst::GetInstance();
        std::vector<faiss::Index::idx_t> vlabels(n * k);

        std::vector<float> vdistances(n * k);
        auto t0 = Elapsed();
        // do query
        Fpga->BatchAnnQuery(nprobe, n, (float*)data, k, vlabels, vdistances);
        auto t1 = Elapsed();
        LOG_ENGINE_DEBUG_ << " vlabels size:" << vlabels.size() << "search time:" << t1 - t0;
        auto elems = n * k;
        //
        size_t p_id_size = sizeof(int64_t) * elems;
        size_t p_dist_size = sizeof(float) * elems;
        memcpy(distances, vdistances.data(), p_dist_size);
        memcpy(labels, vlabels.data(), p_id_size);
        LOG_ENGINE_DEBUG_ << " copy end" << vlabels.size();
    } catch (...) {
    }
}
}  // namespace knowhere
}  // namespace milvus
