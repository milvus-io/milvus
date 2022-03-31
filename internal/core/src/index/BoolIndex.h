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

#include <vector>
#include <memory>
#include "index/ScalarIndexSort.h"

namespace milvus::scalar {

// TODO: optimize here.
class BoolIndex : public ScalarIndexSort<bool> {
 public:
    void
    BuildWithDataset(const DatasetPtr& dataset) override {
        auto size = dataset->Get<int64_t>(knowhere::meta::ROWS);
        auto data = dataset->Get<const void*>(knowhere::meta::TENSOR);
        proto::schema::BoolArray arr;
        arr.ParseFromArray(data, size);
        Build(arr.data().size(), arr.data().data());
    }
};
using BoolIndexPtr = std::unique_ptr<BoolIndex>;

inline BoolIndexPtr
CreateBoolIndex() {
    return std::make_unique<BoolIndex>();
}
}  // namespace milvus::scalar
