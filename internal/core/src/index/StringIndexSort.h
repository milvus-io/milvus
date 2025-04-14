// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <memory>
#include <vector>
#include <string>
#include <string_view>

#include "common/Utils.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndex.h"

namespace milvus::index {
// TODO: should inherit from StringIndex?
class StringIndexSort : public ScalarIndexSort<std::string> {
 public:
    const TargetBitmap
    Query(const DatasetPtr& dataset) override {
        auto op = dataset->Get<OpType>(OPERATOR_TYPE);
        if (op == OpType::PrefixMatch) {
            auto prefix = dataset->Get<std::string>(MATCH_VALUE);
            return PrefixMatch(prefix);
        }
        return ScalarIndex<std::string>::Query(dataset);
    }

    const TargetBitmap
    PrefixMatch(std::string_view prefix) {
        auto data = GetData();
        TargetBitmap bitset(data.size());
        auto it = std::lower_bound(
            data.begin(),
            data.end(),
            prefix,
            [](const IndexStructure<std::string>& value,
               std::string_view prefix) { return value.a_ < prefix; });
        for (; it != data.end(); ++it) {
            if (!milvus::PrefixMatch(it->a_, prefix)) {
                break;
            }
            bitset[it->idx_] = true;
        }
        return bitset;
    }
};
using StringIndexSortPtr = std::unique_ptr<StringIndexSort>;

inline StringIndexSortPtr
CreateStringIndexSort() {
    return std::make_unique<StringIndexSort>();
}
}  // namespace milvus::index
