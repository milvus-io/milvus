/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "db/engine/ExecutionEngine.h"
#include "db/Constants.h"

#include <vector>
#include <map>
#include <string>

namespace zilliz {
namespace milvus {
namespace engine {
namespace meta {

constexpr int32_t DEFAULT_ENGINE_TYPE = (int)EngineType::FAISS_IDMAP;
constexpr int32_t DEFAULT_NLIST = 16384;
constexpr int32_t DEFAULT_INDEX_FILE_SIZE = 1024*ONE_MB;
constexpr int32_t DEFAULT_METRIC_TYPE = (int)MetricType::L2;

typedef int DateT;
const DateT EmptyDate = -1;
typedef std::vector<DateT> DatesT;

struct TableSchema {
    typedef enum {
        NORMAL,
        TO_DELETE,
    } TABLE_STATE;

    size_t id_ = 0;
    std::string table_id_;
    int32_t state_ = (int)NORMAL;
    uint16_t dimension_ = 0;
    int64_t created_on_ = 0;
    int32_t engine_type_ = DEFAULT_ENGINE_TYPE;
    int32_t nlist_ = DEFAULT_NLIST;
    int32_t index_file_size_ = DEFAULT_INDEX_FILE_SIZE;
    int32_t metric_type_ = DEFAULT_METRIC_TYPE;
}; // TableSchema

struct TableFileSchema {
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
    std::string table_id_;
    std::string file_id_;
    int32_t file_type_ = NEW;
    size_t file_size_ = 0;
    size_t row_count_ = 0;
    DateT date_ = EmptyDate;
    uint16_t dimension_ = 0;
    std::string location_;
    int64_t updated_time_ = 0;
    int64_t created_on_ = 0;
    int32_t engine_type_ = DEFAULT_ENGINE_TYPE;
    int32_t nlist_ = DEFAULT_NLIST; //not persist to meta
    int32_t metric_type_ = DEFAULT_METRIC_TYPE; //not persist to meta
}; // TableFileSchema

typedef std::vector<TableFileSchema> TableFilesSchema;
typedef std::map<DateT, TableFilesSchema> DatePartionedTableFilesSchema;

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz
