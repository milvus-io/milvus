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

#pragma once

#include <memory>
#include <mutex>
#include <utility>

#include "IndexRHNSW.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/IndexType.h"

namespace milvus {
namespace knowhere {

class IndexRHNSWFlat : public IndexRHNSW {
 public:
    IndexRHNSWFlat() : IndexRHNSW() {
        index_type_ = IndexEnum::INDEX_RHNSWFlat;
    }

    explicit IndexRHNSWFlat(std::shared_ptr<faiss::Index> index) : IndexRHNSW(std::move(index)) {
        index_type_ = IndexEnum::INDEX_RHNSWFlat;
    }

    IndexRHNSWFlat(int d, int M, MetricType metric = Metric::L2);

    BinarySet
    Serialize(const Config& config) override;

    void
    Load(const BinarySet& index_binary) override;

    void
    Train(const DatasetPtr& dataset_ptr, const Config& config) override;

    void
    UpdateIndexSize() override;
};

}  // namespace knowhere
}  // namespace milvus
