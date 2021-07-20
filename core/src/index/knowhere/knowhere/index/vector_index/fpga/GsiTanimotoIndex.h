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
#pragma once

#include "Apu.h"
#include "GsiBaseIndex.h"

#include <memory>
#include <string>

namespace milvus {
namespace knowhere {

class GsiTanimotoIndex : public GsiBaseIndex {
 public:
    explicit GsiTanimotoIndex(uint32_t dim) : GsiBaseIndex(dim) {
        index_type_ = IndexEnum::INDEX_FAISS_BIN_IDMAP;
    }

    void
    CopyIndexToFpga(uint32_t row_count, const std::string& location) override;

    DatasetPtr
    Query(const DatasetPtr& dataset, const Config& config, faiss::ConcurrentBitsetPtr blacklist) override;
};
}  // namespace knowhere
}  // namespace milvus
#endif
