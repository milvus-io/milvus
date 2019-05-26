/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

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

struct GroupSchema {
    size_t id;
    std::string group_id;
    size_t files_cnt = 0;
    uint16_t dimension;
    std::string location = "";
    long created_on;
}; // GroupSchema

struct GroupFileSchema {
    typedef enum {
        NEW,
        RAW,
        TO_INDEX,
        INDEX,
        TO_DELETE,
    } FILE_TYPE;

    size_t id;
    std::string group_id;
    std::string file_id;
    int file_type = NEW;
    size_t rows;
    DateT date = EmptyDate;
    uint16_t dimension;
    std::string location = "";
    long updated_time;
    long created_on;
}; // GroupFileSchema

typedef std::vector<GroupFileSchema> GroupFilesSchema;
typedef std::map<DateT, GroupFilesSchema> DatePartionedGroupFilesSchema;

} // namespace meta
} // namespace engine
} // namespace vecwise
} // namespace zilliz
