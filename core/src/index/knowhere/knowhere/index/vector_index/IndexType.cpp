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

#include "knowhere/index/vector_index/IndexType.h"
#include <unordered_map>
#include "knowhere/common/Exception.h"

namespace knowhere {

static std::unordered_map<int32_t, std::string> index_type_str_map = {
    {(int32_t)IndexType::INVALID, "INVALID"},
    {(int32_t)IndexType::INDEX_FAISS_IDMAP, "IDMAP"},
    {(int32_t)IndexType::INDEX_FAISS_IVFFLAT, "IVFFLAT"},
    {(int32_t)IndexType::INDEX_FAISS_IVFPQ, "IVFPQ"},
    {(int32_t)IndexType::INDEX_FAISS_IVFSQ8, "IVFSQ8"},
    {(int32_t)IndexType::INDEX_FAISS_IVFSQ8H, "IVFSQ8H"},
    {(int32_t)IndexType::INDEX_FAISS_BIN_IDMAP, "BIN_IDMAP"},
    {(int32_t)IndexType::INDEX_FAISS_BIN_IVFFLAT, "BIN_IVFFLAT"},
    {(int32_t)IndexType::INDEX_NSG, "NSG"},
    {(int32_t)IndexType::INDEX_SPTAG_KDT_RNT, "SPTAG_KDT_RNT"},
    {(int32_t)IndexType::INDEX_SPTAG_BKT_RNT, "SPTAG_BKT_RNT"},
    {(int32_t)IndexType::INDEX_HNSW, "HNSW"},
};

std::string
IndexTypeToStr(const IndexType type) {
    try {
        return index_type_str_map.at((int32_t)type);
    } catch (...) {
        KNOWHERE_THROW_MSG("Invalid index type " + std::to_string((int32_t)type));
    }
}

}  // namespace knowhere
