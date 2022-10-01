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

#include "indexbuilder/utils.h"

#include <algorithm>
#include <string>
#include <tuple>
#include <vector>

namespace milvus::indexbuilder {

std::vector<knowhere::IndexType>
NM_List() {
    static std::vector<knowhere::IndexType> ret{
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
#ifdef MILVUS_SUPPORT_NSG
        knowhere::IndexEnum::INDEX_NSG,
#endif
        knowhere::IndexEnum::INDEX_RHNSWFlat,
    };
    return ret;
}

std::vector<knowhere::IndexType>
BIN_List() {
    static std::vector<knowhere::IndexType> ret{
        knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP,
        knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
    };
    return ret;
}

std::vector<knowhere::IndexType>
Need_ID_List() {
    static std::vector<knowhere::IndexType> ret{
        // knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
        // knowhere::IndexEnum::INDEX_NSG,
    };

    return ret;
}

std::vector<knowhere::IndexType>
Need_BuildAll_list() {
    static std::vector<knowhere::IndexType> ret{
#ifdef MILVUS_SUPPORT_NSG
        knowhere::IndexEnum::INDEX_NSG,
#endif
    };
    return ret;
}

std::vector<std::tuple<knowhere::IndexType, knowhere::MetricType>>
unsupported_index_combinations() {
    static std::vector<std::tuple<knowhere::IndexType, knowhere::MetricType>> ret{
        std::make_tuple(knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT, knowhere::metric::L2),
    };
    return ret;
}

template <typename T>
bool
is_in_list(const T& t, std::function<std::vector<T>()> list_func) {
    auto l = list_func();
    return std::find(l.begin(), l.end(), t) != l.end();
}

bool
is_in_bin_list(const knowhere::IndexType& index_type) {
    return is_in_list<knowhere::IndexType>(index_type, BIN_List);
}

bool
is_in_nm_list(const knowhere::IndexType& index_type) {
    return is_in_list<knowhere::IndexType>(index_type, NM_List);
}

bool
is_in_need_build_all_list(const knowhere::IndexType& index_type) {
    return is_in_list<knowhere::IndexType>(index_type, Need_BuildAll_list);
}

bool
is_in_need_id_list(const knowhere::IndexType& index_type) {
    return is_in_list<knowhere::IndexType>(index_type, Need_ID_List);
}

bool
is_unsupported(const knowhere::IndexType& index_type, const knowhere::MetricType& metric_type) {
    return is_in_list<std::tuple<knowhere::IndexType, knowhere::MetricType>>(std::make_tuple(index_type, metric_type),
                                                                             unsupported_index_combinations);
}

}  // namespace milvus::indexbuilder
