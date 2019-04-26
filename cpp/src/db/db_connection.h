////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string>
#include <memory>

#include <sqlite_orm.h>


namespace zilliz {
namespace vecwise {
namespace engine {

struct GroupSchema {
    size_t id;
    std::string group_id;
    size_t files_cnt = 0;
    uint16_t dimension;
    std::string location = "";
    std::string next_file_location = "";
}; // GroupSchema


struct GroupFileSchema {
    typedef enum {
        RAW,
        INDEX
    } FILE_TYPE;

    size_t id;
    std::string group_id;
    std::string file_id;
    int files_type = RAW;
    size_t rows;
    std::string location = "";
}; // GroupFileSchema

inline auto initStorage(const std::string &path) {
    using namespace sqlite_orm;
    return make_storage(path,
        // Add table below
                        make_table("Groups",
                                   make_column("id", &GroupSchema::id, primary_key()),
                                   make_column("group_id", &GroupSchema::group_id, unique()),
                                   make_column("dimension", &GroupSchema::dimension),
                                   make_column("files_cnt", &GroupSchema::files_cnt, default_value(0))));
}

using SqliteDB = decltype(initStorage(""));
using SqliteDBPtr= std::shared_ptr<SqliteDB>;

class Connection {
 protected:
    static SqliteDBPtr connect_;
};

}
}
}
