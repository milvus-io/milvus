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

#include "index/ScalarIndex.h"
#include <string>
#include <memory>
#include <vector>
#include "index/Meta.h"
#include <pb/schema.pb.h>

namespace milvus::scalar {

class StringIndex : public ScalarIndex<std::string> {
 public:
    void
    BuildWithDataset(const DatasetPtr& dataset) override {
        auto size = dataset->Get<int64_t>(knowhere::meta::ROWS);
        auto data = dataset->Get<const void*>(knowhere::meta::TENSOR);
        proto::schema::StringArray arr;
        arr.ParseFromArray(data, size);

        {
            // TODO: optimize here. avoid memory copy.
            std::vector<std::string> vecs{arr.data().begin(), arr.data().end()};
            Build(arr.data().size(), vecs.data());
        }

        {
            // TODO: test this way.
            // auto strs = (const std::string*)arr.data().data();
            // Build(arr.data().size(), strs);
        }
    }

    const TargetBitmapPtr
    Query(const DatasetPtr& dataset) override {
        auto op = dataset->Get<OpType>(OPERATOR_TYPE);
        if (op == OpType::PrefixMatch) {
            auto prefix = dataset->Get<std::string>(PREFIX_VALUE);
            return PrefixMatch(prefix);
        }
        return ScalarIndex<std::string>::Query(dataset);
    }

    virtual const TargetBitmapPtr
    PrefixMatch(std::string prefix) = 0;
};
using StringIndexPtr = std::unique_ptr<StringIndex>;
}  // namespace milvus::scalar
