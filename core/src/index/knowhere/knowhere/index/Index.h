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

#include "cache/DataObj.h"
#include "knowhere/common/BinarySet.h"
#include "knowhere/common/Config.h"

namespace milvus {
namespace knowhere {

class Index : public milvus::cache::DataObj {
 public:
    virtual BinarySet
    Serialize(const Config& config = Config()) = 0;

    virtual void
    Load(const BinarySet&) = 0;
};

using IndexPtr = std::shared_ptr<Index>;

class Blacklist : public milvus::cache::DataObj {
 public:
    Blacklist() {
    }

    int64_t
    Size() override {
        int64_t sz = sizeof(Blacklist);
        if (bitset_) {
            sz += bitset_->size();
        }
        return sz;
    }

    int64_t time_stamp_ = -1;
    faiss::ConcurrentBitsetPtr bitset_ = nullptr;
};

using BlacklistPtr = std::shared_ptr<Blacklist>;

}  // namespace knowhere
}  // namespace milvus
