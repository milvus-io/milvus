/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "ExecutionEngine.h"

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
    int state_ = (int)NORMAL;
    size_t files_cnt_ = 0;
    uint16_t dimension_ = 0;
    long created_on_ = 0;
    int engine_type_ = (int)EngineType::FAISS_IDMAP;
    bool store_raw_data_ = false;
}; // TableSchema

struct TableFileSchema {
    typedef enum {
        NEW,
        RAW,
        TO_INDEX,
        INDEX,
        TO_DELETE,
    } FILE_TYPE;

    size_t id_ = 0;
    std::string table_id_;
    int engine_type_ = (int)EngineType::FAISS_IDMAP;
    std::string file_id_;
    int file_type_ = NEW;
    size_t size_ = 0;
    DateT date_ = EmptyDate;
    uint16_t dimension_ = 0;
    std::string location_;
    long updated_time_ = 0;
    long created_on_ = 0;
}; // TableFileSchema

typedef std::vector<TableFileSchema> TableFilesSchema;
typedef std::map<DateT, TableFilesSchema> DatePartionedTableFilesSchema;

} // namespace meta
} // namespace engine
} // namespace milvus
} // namespace zilliz
