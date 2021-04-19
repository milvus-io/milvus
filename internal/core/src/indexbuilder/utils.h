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
#include <string>
#include <algorithm>

#include "index/knowhere/knowhere/index/IndexType.h"

namespace milvus {
namespace indexbuilder {

std::vector<std::string>
NM_List() {
    static std::vector<std::string> ret{
        milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
        milvus::knowhere::IndexEnum::INDEX_NSG,
    };
    return ret;
}

std::vector<std::string>
BIN_List() {
    static std::vector<std::string> ret{milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP,
                                        milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT};
    return ret;
}

std::vector<std::string>
Need_ID_List() {
    static std::vector<std::string> ret{
        // milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
        // milvus::knowhere::IndexEnum::INDEX_NSG
    };

    return ret;
}

std::vector<std::string>
Need_BuildAll_list() {
    static std::vector<std::string> ret{milvus::knowhere::IndexEnum::INDEX_NSG};
    return ret;
}

template <typename T>
bool
is_in_list(const T& t, std::function<std::vector<T>()> list_func) {
    auto l = list_func();
    return std::find(l.begin(), l.end(), t) != l.end();
}

bool
is_in_bin_list(const milvus::knowhere::IndexType& index_type) {
    return is_in_list<std::string>(index_type, BIN_List);
}

bool
is_in_nm_list(const milvus::knowhere::IndexType& index_type) {
    return is_in_list<std::string>(index_type, NM_List);
}

bool
is_in_need_build_all_list(const milvus::knowhere::IndexType& index_type) {
    return is_in_list<std::string>(index_type, Need_BuildAll_list);
}

bool
is_in_need_id_list(const milvus::knowhere::IndexType& index_type) {
    return is_in_list<std::string>(index_type, Need_ID_List);
}

}  // namespace indexbuilder
}  // namespace milvus
