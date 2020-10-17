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

namespace milvus {
namespace knowhere {

/* for compatible with 0.7.0 */
static std::unordered_map<int32_t, std::string> old_index_type_str_map = {
    {(int32_t)OldIndexType::INVALID, "INVALID"},
    {(int32_t)OldIndexType::FAISS_IDMAP, IndexEnum::INDEX_FAISS_IDMAP},
    {(int32_t)OldIndexType::FAISS_IVFFLAT_CPU, IndexEnum::INDEX_FAISS_IVFFLAT},
    {(int32_t)OldIndexType::FAISS_IVFFLAT_GPU, IndexEnum::INDEX_FAISS_IVFFLAT},
    {(int32_t)OldIndexType::FAISS_IVFFLAT_MIX, IndexEnum::INDEX_FAISS_IVFFLAT},
    {(int32_t)OldIndexType::FAISS_IVFPQ_CPU, IndexEnum::INDEX_FAISS_IVFPQ},
    {(int32_t)OldIndexType::FAISS_IVFPQ_GPU, IndexEnum::INDEX_FAISS_IVFPQ},
    {(int32_t)OldIndexType::FAISS_IVFPQ_MIX, IndexEnum::INDEX_FAISS_IVFPQ},
    {(int32_t)OldIndexType::FAISS_IVFSQ8_MIX, IndexEnum::INDEX_FAISS_IVFSQ8},
    {(int32_t)OldIndexType::FAISS_IVFSQ8_CPU, IndexEnum::INDEX_FAISS_IVFSQ8},
    {(int32_t)OldIndexType::FAISS_IVFSQ8_GPU, IndexEnum::INDEX_FAISS_IVFSQ8},
    {(int32_t)OldIndexType::FAISS_IVFSQ8_HYBRID, IndexEnum::INDEX_FAISS_IVFSQ8H},
    {(int32_t)OldIndexType::NSG_MIX, IndexEnum::INDEX_NSG},
    {(int32_t)OldIndexType::SPTAG_KDT_RNT_CPU, IndexEnum::INDEX_SPTAG_KDT_RNT},
    {(int32_t)OldIndexType::SPTAG_BKT_RNT_CPU, IndexEnum::INDEX_SPTAG_BKT_RNT},
    {(int32_t)OldIndexType::HNSW, IndexEnum::INDEX_HNSW},
    {(int32_t)OldIndexType::ANNOY, IndexEnum::INDEX_ANNOY},
    {(int32_t)OldIndexType::FAISS_BIN_IDMAP, IndexEnum::INDEX_FAISS_BIN_IDMAP},
    {(int32_t)OldIndexType::FAISS_BIN_IVFLAT_CPU, IndexEnum::INDEX_FAISS_BIN_IVFFLAT},
};

static std::unordered_map<std::string, int32_t> str_old_index_type_map = {
    {"", (int32_t)OldIndexType::INVALID},
    {IndexEnum::INDEX_FAISS_IDMAP, (int32_t)OldIndexType::FAISS_IDMAP},
    {IndexEnum::INDEX_FAISS_IVFFLAT, (int32_t)OldIndexType::FAISS_IVFFLAT_CPU},
    {IndexEnum::INDEX_FAISS_IVFPQ, (int32_t)OldIndexType::FAISS_IVFPQ_CPU},
    {IndexEnum::INDEX_FAISS_IVFSQ8, (int32_t)OldIndexType::FAISS_IVFSQ8_CPU},
    {IndexEnum::INDEX_FAISS_IVFSQ8H, (int32_t)OldIndexType::FAISS_IVFSQ8_HYBRID},
    {IndexEnum::INDEX_NSG, (int32_t)OldIndexType::NSG_MIX},
    {IndexEnum::INDEX_SPTAG_KDT_RNT, (int32_t)OldIndexType::SPTAG_KDT_RNT_CPU},
    {IndexEnum::INDEX_SPTAG_BKT_RNT, (int32_t)OldIndexType::SPTAG_BKT_RNT_CPU},
    {IndexEnum::INDEX_HNSW, (int32_t)OldIndexType::HNSW},
    {IndexEnum::INDEX_ANNOY, (int32_t)OldIndexType::ANNOY},
    {IndexEnum::INDEX_FAISS_BIN_IDMAP, (int32_t)OldIndexType::FAISS_BIN_IDMAP},
    {IndexEnum::INDEX_FAISS_BIN_IVFFLAT, (int32_t)OldIndexType::FAISS_BIN_IVFLAT_CPU},
};

/* used in 0.8.0 */
namespace IndexEnum {
const char* INVALID = "";
const char* INDEX_FAISS_IDMAP = "IDMAP";
const char* INDEX_FAISS_IVFFLAT = "IVF_FLAT";
const char* INDEX_FAISS_IVFPQ = "IVF_PQ";
const char* INDEX_FAISS_IVFSQ8 = "IVF_SQ8";
const char* INDEX_FAISS_IVFSQ8H = "IVF_SQ8_HYBRID";
const char* INDEX_FAISS_BIN_IDMAP = "BIN_IDMAP";
const char* INDEX_FAISS_BIN_IVFFLAT = "BIN_IVF_FLAT";
const char* INDEX_NSG = "NSG";
const char* INDEX_SPTAG_KDT_RNT = "SPTAG_KDT_RNT";
const char* INDEX_SPTAG_BKT_RNT = "SPTAG_BKT_RNT";
const char* INDEX_HNSW = "HNSW";
const char* INDEX_ANNOY = "ANNOY";
}  // namespace IndexEnum

std::string
OldIndexTypeToStr(const int32_t type) {
    try {
        return old_index_type_str_map.at(type);
    } catch (...) {
        KNOWHERE_THROW_MSG("Invalid index type " + std::to_string(type));
    }
}

int32_t
StrToOldIndexType(const std::string& str) {
    try {
        return str_old_index_type_map.at(str);
    } catch (...) {
        KNOWHERE_THROW_MSG("Invalid index str " + str);
    }
}

}  // namespace knowhere
}  // namespace milvus
