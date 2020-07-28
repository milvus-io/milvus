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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "db/Constants.h"
#include "knowhere/index/IndexType.h"
#include "src/version.h"

namespace milvus {
namespace engine {

static const char* DIMENSION = "dim";

// TODO(linxj): replace with VecIndex::IndexType
enum class EngineType {
    INVALID = 0,
    FAISS_IDMAP = 1,
    FAISS_IVFFLAT = 2,
    FAISS_IVFSQ8 = 3,
    NSG_MIX = 4,
    FAISS_IVFSQ8H = 5,
    FAISS_PQ = 6,
#ifdef MILVUS_SUPPORT_SPTAG
    SPTAG_KDT = 7,
    SPTAG_BKT = 8,
#endif
    FAISS_BIN_IDMAP = 9,
    FAISS_BIN_IVFFLAT = 10,
    HNSW = 11,
    ANNOY = 12,
    FAISS_IVFSQ8NR = 13,
    HNSW_SQ8NM = 14,
    MAX_VALUE = HNSW_SQ8NM,
};

static std::map<std::string, int32_t> s_index_name2type = {
    {knowhere::IndexEnum::INDEX_FAISS_IDMAP, (int32_t)EngineType::FAISS_IDMAP},
    {knowhere::IndexEnum::INDEX_FAISS_IVFFLAT, (int32_t)EngineType::FAISS_IVFFLAT},
    {knowhere::IndexEnum::INDEX_FAISS_IVFPQ, (int32_t)EngineType::FAISS_PQ},
    {knowhere::IndexEnum::INDEX_FAISS_IVFSQ8, (int32_t)EngineType::FAISS_IVFSQ8},
    {knowhere::IndexEnum::INDEX_FAISS_IVFSQ8NR, (int32_t)EngineType::FAISS_IVFSQ8NR},
    {knowhere::IndexEnum::INDEX_FAISS_IVFSQ8H, (int32_t)EngineType::FAISS_IVFSQ8H},
    {knowhere::IndexEnum::INDEX_NSG, (int32_t)EngineType::NSG_MIX},
#ifdef MILVUS_SUPPORT_SPTAG
    {knowhere::IndexEnum::INDEX_SPTAG_KDT_RNT, (int32_t)EngineType::SPTAG_KDT},
    {knowhere::IndexEnum::INDEX_SPTAG_BKT_RNT, (int32_t)EngineType::SPTAG_BKT},
#endif
    {knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP, (int32_t)EngineType::FAISS_BIN_IDMAP},
    {knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT, (int32_t)EngineType::FAISS_BIN_IVFFLAT},
    {knowhere::IndexEnum::INDEX_HNSW, (int32_t)EngineType::HNSW},
    {knowhere::IndexEnum::INDEX_HNSW_SQ8NM, (int32_t)EngineType::HNSW_SQ8NM},
    {knowhere::IndexEnum::INDEX_ANNOY, (int32_t)EngineType::ANNOY},
};

static std::map<int32_t, std::string> s_index_type2name = {
    {(int32_t)EngineType::FAISS_IDMAP, knowhere::IndexEnum::INDEX_FAISS_IDMAP},
    {(int32_t)EngineType::FAISS_IVFFLAT, knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
    {(int32_t)EngineType::FAISS_PQ, knowhere::IndexEnum::INDEX_FAISS_IVFPQ},
    {(int32_t)EngineType::FAISS_IVFSQ8, knowhere::IndexEnum::INDEX_FAISS_IVFSQ8},
    {(int32_t)EngineType::FAISS_IVFSQ8NR, knowhere::IndexEnum::INDEX_FAISS_IVFSQ8NR},
    {(int32_t)EngineType::FAISS_IVFSQ8H, knowhere::IndexEnum::INDEX_FAISS_IVFSQ8H},
    {(int32_t)EngineType::NSG_MIX, knowhere::IndexEnum::INDEX_NSG},
#ifdef MILVUS_SUPPORT_SPTAG
    {(int32_t)EngineType::SPTAG_KDT, knowhere::IndexEnum::INDEX_SPTAG_KDT_RNT},
    {(int32_t)EngineType::SPTAG_BKT, knowhere::IndexEnum::INDEX_SPTAG_BKT_RNT},
#endif
    {(int32_t)EngineType::FAISS_BIN_IDMAP, knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP},
    {(int32_t)EngineType::FAISS_BIN_IVFFLAT, knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT},
    {(int32_t)EngineType::HNSW, knowhere::IndexEnum::INDEX_HNSW},
    {(int32_t)EngineType::HNSW_SQ8NM, knowhere::IndexEnum::INDEX_HNSW_SQ8NM},
    {(int32_t)EngineType::ANNOY, knowhere::IndexEnum::INDEX_ANNOY},
};

enum class MetricType {
    INVALID = 0,
    L2 = 1,              // Euclidean Distance
    IP = 2,              // Cosine Similarity
    HAMMING = 3,         // Hamming Distance
    JACCARD = 4,         // Jaccard Distance
    TANIMOTO = 5,        // Tanimoto Distance
    SUBSTRUCTURE = 6,    // Substructure Distance
    SUPERSTRUCTURE = 7,  // Superstructure Distance
    MAX_VALUE = SUPERSTRUCTURE
};

static std::map<std::string, MetricType> s_map_metric_type = {
    {"L2", MetricType::L2},
    {"IP", MetricType::IP},
    {"HAMMING", MetricType::HAMMING},
    {"JACCARD", MetricType::JACCARD},
    {"TANIMOTO", MetricType::TANIMOTO},
    {"SUBSTRUCTURE", MetricType::SUBSTRUCTURE},
    {"SUPERSTRUCTURE", MetricType::SUPERSTRUCTURE},
};

enum class StructuredIndexType {
    INVALID = 0,
    SORTED = 1,
};

namespace meta {

using DateT = int;

namespace hybrid {

enum DataType {
    NONE = 0,
    BOOL = 1,
    INT8 = 2,
    INT16 = 3,
    INT32 = 4,
    INT64 = 5,

    FLOAT = 10,
    DOUBLE = 11,

    STRING = 20,

    UID = 30,

    VECTOR_BINARY = 100,
    VECTOR_FLOAT = 101,
};

}  // namespace hybrid

}  // namespace meta
}  // namespace engine
}  // namespace milvus
