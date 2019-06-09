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
namespace vecwise {
namespace engine {
namespace meta {

typedef int DateT;
const DateT EmptyDate = -1;
typedef std::vector<DateT> DatesT;

struct TableSchema {
    size_t id;
    std::string table_id;
    size_t files_cnt = 0;
    uint16_t dimension;
    std::string location;
    long created_on;
    int engine_type_ = (int)EngineType::FAISS_IDMAP;
}; // TableSchema

struct TableFileSchema {
    typedef enum {
        NEW,
        RAW,
        TO_INDEX,
        INDEX,
        TO_DELETE,
    } FILE_TYPE;

    size_t id;
    std::string table_id;
    int engine_type_ = (int)EngineType::FAISS_IDMAP;
    std::string file_id;
    int file_type = NEW;
    size_t size;
    DateT date = EmptyDate;
    uint16_t dimension;
    std::string location;
    long updated_time;
    long created_on;
}; // TableFileSchema

typedef std::vector<TableFileSchema> TableFilesSchema;
typedef std::map<DateT, TableFilesSchema> DatePartionedTableFilesSchema;

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
