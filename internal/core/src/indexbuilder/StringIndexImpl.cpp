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

#include "StringIndexImpl.h"

namespace milvus::indexbuilder {

// TODO: optimize here.

knowhere::BinarySet
StringIndexImpl::Serialize(const knowhere::Config& config) {
    knowhere::BinarySet res_set;
    auto data = this->GetData();
    for (const auto& record : data) {
        auto idx = record.idx_;
        auto str = record.a_;
        std::shared_ptr<uint8_t[]> content(new uint8_t[str.length()]);
        memcpy(content.get(), str.c_str(), str.length());
        res_set.Append(std::to_string(idx), content, str.length());
    }
    return res_set;
}

void
StringIndexImpl::Load(const knowhere::BinarySet& index_binary) {
    std::vector<std::string> vecs;

    for (const auto& [k, v] : index_binary.binary_map_) {
        std::string str(reinterpret_cast<const char*>(v->data.get()), v->size);
        vecs.emplace_back(str);
    }

    Build(vecs.size(), vecs.data());
}

}  // namespace milvus::indexbuilder
