// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#pragma once

#include "src/wrapper/VecIndex.h"

#include <memory>

namespace zilliz {
namespace milvus {
namespace cache {

class DataObj {
public:
    DataObj(const engine::VecIndexPtr& index)
            : index_(index)
    {}

    DataObj(const engine::VecIndexPtr& index, int64_t size)
            : index_(index),
              size_(size)
    {}

    engine::VecIndexPtr data() { return index_; }
    const engine::VecIndexPtr& data() const { return index_; }

    int64_t size() const {
        if(index_ == nullptr) {
            return 0;
        }

        if(size_ > 0) {
            return size_;
        }

        return index_->Count() * index_->Dimension() * sizeof(float);
    }

private:
    engine::VecIndexPtr index_ = nullptr;
    int64_t size_ = 0;
};

using DataObjPtr = std::shared_ptr<DataObj>;

}
}
}