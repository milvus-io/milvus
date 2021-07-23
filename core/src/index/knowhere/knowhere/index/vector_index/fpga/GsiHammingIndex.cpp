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

#ifdef MILVUS_APU_VERSION

#include "knowhere/index/vector_index/fpga/GsiHammingIndex.h"
#include "ApuInst.h"
#include "scheduler/task/SearchTask.h"

#include <string>

namespace milvus {
namespace knowhere {

void
GsiHammingIndex::CopyIndexToFpga(uint32_t row_count, const std::string& location) {
    num_bfeatures_ = Dim();
    num_bytes_in_rec_ = num_bfeatures_ / CHAR_BITS;
    index_size_ = row_count * num_bytes_in_rec_;

    auto apu = Fpga::ApuInst::getInstance();
    apu->cleanApuResources(Fpga::APU_CLEAN_TYPE::NEW_DB);
    apu->PopulateApuParams(num_bfeatures_, row_count, location);
    apu->createBdb();
    apu->loadSeesionToApu(Fpga::APU_METRIC_TYPE::HAMMING);
}

DatasetPtr
GsiHammingIndex::Query(const DatasetPtr& dataset, const Config& config, faiss::ConcurrentBitsetPtr blacklist) {
    auto apu = Fpga::ApuInst::getInstance();

    num_bfeatures_ = Dim();
    num_queries_ = static_cast<uint32_t>(dataset.get()->Get<int64_t>(meta::ROWS));
    num_bytes_in_rec_ = num_bfeatures_ / CHAR_BITS;
    topK_ = config[meta::TOPK];

    apu->setTopK(topK_);
    AllocateMemory(dataset, config);

    apu->Query(indices_, distances_, queries_, Fpga::APU_METRIC_TYPE::HAMMING);

    auto ret_ds = std::make_shared<Dataset>();
    int64_t* ids_int64 = (int64_t*)calloc(topK_ * num_queries_, sizeof(int64_t));
    convertToInt64_t(&indices_, ids_int64);

    ret_ds->Set(meta::IDS, ids_int64);
    ret_ds->Set(meta::DISTANCE, distances_.rows_f32);
    free((void*)indices_.rows_u32);
    return ret_ds;
}
#endif
}  // namespace knowhere
}  // namespace milvus
