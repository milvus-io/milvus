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
    int32_t engine_type_ = (int)EngineType::FAISS_IDMAP;
    int32_t nlist_ = 16384;
    int32_t index_file_size_ = 1024*ONE_MB;
    int32_t metric_type_ = (int)MetricType::L2;
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
    int32_t engine_type_ = (int)EngineType::FAISS_IDMAP;
    std::string file_id_;
    int32_t file_type_ = NEW;
    size_t file_size_ = 0;
    size_t row_count_ = 0;
    DateT date_ = EmptyDate;
    uint16_t dimension_ = 0;
    std::string location_;
    int64_t updated_time_ = 0;
    int64_t created_on_ = 0;
}; // TableFileSchema

typedef std::vector<TableFileSchema> TableFilesSchema;
typedef std::map<DateT, TableFilesSchema> DatePartionedTableFilesSchema;

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz
