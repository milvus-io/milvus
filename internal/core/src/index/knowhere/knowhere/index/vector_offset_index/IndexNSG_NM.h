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

#include <memory>
#include <vector>

#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/VecIndex.h"

namespace milvus {
namespace knowhere {

namespace impl {
class NsgIndex;
}

class NSG_NM : public VecIndex {
 public:
    explicit NSG_NM(const int64_t gpu_num = -1) : gpu_(gpu_num) {
        if (gpu_ >= 0) {
            index_mode_ = IndexMode::MODE_GPU;
        }
        index_type_ = IndexEnum::INDEX_NSG;
    }

    BinarySet
    Serialize(const Config& config) override;

    void
    Load(const BinarySet&) override;

    void
    BuildAll(const DatasetPtr& dataset_ptr, const Config& config) override;

    void
    Train(const DatasetPtr&, const Config&) override {
        KNOWHERE_THROW_MSG("NSG_NM not support add item dynamically, please invoke BuildAll interface.");
    }

    void
    AddWithoutIds(const DatasetPtr&, const Config&) override {
        KNOWHERE_THROW_MSG("Addwithoutids is not supported");
    }

    DatasetPtr
    Query(const DatasetPtr&, const Config&, const faiss::BitsetView bitset) override;

    int64_t
    Count() override;

    int64_t
    Dim() override;

    void
    UpdateIndexSize() override;

 private:
    int64_t gpu_;
    std::shared_ptr<impl::NsgIndex> index_ = nullptr;
    std::shared_ptr<uint8_t[]> data_ = nullptr;
};

using NSG_NMIndexPtr = std::shared_ptr<NSG_NM>();

}  // namespace knowhere
}  // namespace milvus
