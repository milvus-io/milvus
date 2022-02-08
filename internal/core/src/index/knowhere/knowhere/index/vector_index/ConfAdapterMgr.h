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

#include <functional>
#include <memory>
#include <unordered_map>
#include <mutex>

#include "knowhere/index/IndexType.h"
#include "knowhere/index/vector_index/ConfAdapter.h"

namespace milvus {
namespace knowhere {

class AdapterMgr {
 public:
    template <typename T>
    struct register_t {
        explicit register_t(const IndexType type) {
            AdapterMgr::GetInstance().collection_[type] = ([] { return std::make_shared<T>(); });
        }
    };

    static AdapterMgr&
    GetInstance() {
        static AdapterMgr instance;
        return instance;
    }

    ConfAdapterPtr
    GetAdapter(const IndexType indexType);

    void
    RegisterAdapter();

 protected:
    bool init_ = false;
    std::unordered_map<IndexType, std::function<ConfAdapterPtr()>> collection_;
    std::once_flag once_;
};

}  // namespace knowhere
}  // namespace milvus
