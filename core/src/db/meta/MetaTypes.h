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

static std::map<std::string, EngineType> s_map_engine_type = {
    {knowhere::IndexEnum::INDEX_FAISS_IDMAP, EngineType::FAISS_IDMAP},
    {knowhere::IndexEnum::INDEX_FAISS_IVFFLAT, EngineType::FAISS_IVFFLAT},
    {knowhere::IndexEnum::INDEX_FAISS_IVFPQ, EngineType::FAISS_PQ},
    {knowhere::IndexEnum::INDEX_FAISS_IVFSQ8, EngineType::FAISS_IVFSQ8},
    {knowhere::IndexEnum::INDEX_FAISS_IVFSQ8NR, EngineType::FAISS_IVFSQ8NR},
    {knowhere::IndexEnum::INDEX_FAISS_IVFSQ8H, EngineType::FAISS_IVFSQ8H},
    {knowhere::IndexEnum::INDEX_NSG, EngineType::NSG_MIX},
#ifdef MILVUS_SUPPORT_SPTAG
    {knowhere::IndexEnum::INDEX_SPTAG_KDT_RNT, EngineType::SPTAG_KDT},
    {knowhere::IndexEnum::INDEX_SPTAG_BKT_RNT, EngineType::SPTAG_BKT},
#endif
    {knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP, EngineType::FAISS_BIN_IDMAP},
    {knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT, EngineType::FAISS_BIN_IVFFLAT},
    {knowhere::IndexEnum::INDEX_HNSW, EngineType::HNSW},
    {knowhere::IndexEnum::INDEX_HNSW_SQ8NM, EngineType::HNSW_SQ8NM},
    {knowhere::IndexEnum::INDEX_ANNOY, EngineType::ANNOY},
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

constexpr int32_t DEFAULT_ENGINE_TYPE = (int)EngineType::FAISS_IDMAP;
constexpr int32_t DEFAULT_METRIC_TYPE = (int)MetricType::L2;
constexpr int32_t DEFAULT_INDEX_FILE_SIZE = 1024;
constexpr char CURRENT_VERSION[] = MILVUS_VERSION;

constexpr int64_t FLAG_MASK_NO_USERID = 0x1;
constexpr int64_t FLAG_MASK_HAS_USERID = 0x1 << 1;

using DateT = int;
const DateT EmptyDate = -1;

struct EnvironmentSchema {
    uint64_t global_lsn_ = 0;
};  // EnvironmentSchema

struct CollectionSchema {
    typedef enum {
        NORMAL,
        TO_DELETE,
    } TABLE_STATE;

    size_t id_ = 0;
    std::string collection_id_;
    int32_t state_ = (int)NORMAL;
    uint16_t dimension_ = 0;
    int64_t created_on_ = 0;
    int64_t flag_ = 0;
    int64_t index_file_size_ = DEFAULT_INDEX_FILE_SIZE;
    int32_t engine_type_ = DEFAULT_ENGINE_TYPE;
    std::string index_params_ = "{}";
    int32_t metric_type_ = DEFAULT_METRIC_TYPE;
    std::string owner_collection_;
    std::string partition_tag_;
    std::string version_ = CURRENT_VERSION;
    uint64_t flush_lsn_ = 0;
};  // CollectionSchema

struct SegmentSchema {
    typedef enum {
        NEW,
        RAW,
        TO_INDEX,
        INDEX,
        TO_DELETE,
        NEW_MERGE,
        NEW_INDEX,
        BACKUP,
    } FILE_TYPE;

    size_t id_ = 0;
    std::string collection_id_;
    std::string segment_id_;
    std::string file_id_;
    int32_t file_type_ = NEW;
    size_t file_size_ = 0;
    size_t row_count_ = 0;
    DateT date_ = EmptyDate;
    uint16_t dimension_ = 0;
    // TODO(zhiru)
    std::string location_;
    int64_t updated_time_ = 0;
    int64_t created_on_ = 0;
    int64_t index_file_size_ = DEFAULT_INDEX_FILE_SIZE;  // not persist to meta
    int32_t engine_type_ = DEFAULT_ENGINE_TYPE;
    std::string index_params_;                   // not persist to meta
    int32_t metric_type_ = DEFAULT_METRIC_TYPE;  // not persist to meta
    uint64_t flush_lsn_ = 0;
};  // SegmentSchema

using SegmentSchemaPtr = std::shared_ptr<meta::SegmentSchema>;
using SegmentsSchema = std::vector<SegmentSchema>;

using File2RefCount = std::map<uint64_t, int64_t>;
using Table2FileRef = std::map<std::string, File2RefCount>;

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
    VECTOR = 200,
};

struct VectorFieldSchema {
    std::string vector_id_;
    int64_t dimension;
    int64_t index_file_size_ = DEFAULT_INDEX_FILE_SIZE;
    int32_t engine_type_ = DEFAULT_ENGINE_TYPE;
    std::string index_params_ = "{}";
    int32_t metric_type_ = DEFAULT_METRIC_TYPE;
};

struct VectorFieldsSchema {
    std::vector<VectorFieldSchema> vector_fields_;
};
using VectorFieldSchemaPtr = std::shared_ptr<VectorFieldSchema>;

struct FieldSchema {
    // TODO(yukun): need field_id?
    std::string collection_id_;
    std::string field_name_;
    int32_t field_type_ = (int)INT8;
    std::string index_name_;
    std::string index_param_ = "{}";
    std::string field_params_ = "{}";
};

struct FieldsSchema {
    std::vector<FieldSchema> fields_schema_;
};

using FieldSchemaPtr = std::shared_ptr<FieldSchema>;

struct VectorFileSchema {
    std::string field_name_;
    int64_t index_file_size_ = DEFAULT_INDEX_FILE_SIZE;  // not persist to meta
    int32_t engine_type_ = DEFAULT_ENGINE_TYPE;
    std::string index_params_ = "{}";            // not persist to meta
    int32_t metric_type_ = DEFAULT_METRIC_TYPE;  // not persist to meta
};

using VectorFileSchemaPtr = std::shared_ptr<VectorFileSchema>;

struct CollectionFileSchema {
    typedef enum {
        NEW,
        RAW,
        TO_INDEX,
        INDEX,
        TO_DELETE,
        NEW_MERGE,
        NEW_INDEX,
        BACKUP,
    } FILE_TYPE;

    size_t id_ = 0;
    std::string collection_id_;
    std::string segment_id_;
    std::string file_id_;
    int32_t file_type_ = NEW;
    size_t file_size_ = 0;
    size_t row_count_ = 0;
    DateT date_ = EmptyDate;
    std::string location_;
    int64_t updated_time_ = 0;
    int64_t created_on_ = 0;
    uint64_t flush_lsn_ = 0;
};

using CollectionFileSchemaPtr = std::shared_ptr<CollectionFileSchema>;
}  // namespace hybrid

}  // namespace meta
}  // namespace engine
}  // namespace milvus
