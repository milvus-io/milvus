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
#include <string>

#include "cache/DataObj.h"

namespace milvus {
namespace cache {

class CachePlaceholder {
 public:
    explicit CachePlaceholder(int64_t size);
    ~CachePlaceholder();

    void
    Erase();

    std::string
    ItemKey() const {
        return item_key_;
    }

 private:
    class MockDataObj : public DataObj {
     public:
        explicit MockDataObj(int64_t size) : size_(size) {
        }

        int64_t
        Size() override {
            return size_;
        }

        int64_t size_ = 0;
    };

    std::string item_key_;
};

using CachePlaceholderPtr = std::shared_ptr<CachePlaceholder>;

}  // namespace cache
}  // namespace milvus
