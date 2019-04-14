#ifndef VECENGINE_DB_H_
#define VECENGINE_DB_H_

#include <string>
#include "options.h"
#include "db_meta.h"
#include "status.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class Env;

class DB {
public:
    static DB* Open(const Options& options_, const std::string& name_);

    virtual Status add_group(GroupOptions options_,
            const std::string& group_id_,
            GroupSchema& group_info_) = 0;
    virtual Status get_group(const std::string& group_id_, GroupSchema& group_info_) = 0;
    virtual Status has_group(const std::string& group_id_, bool& has_or_not_) = 0;
    virtual Status get_group_files(const std::string& group_id_,
                                   const int date_delta_,
                                   GroupFilesSchema& group_files_info_) = 0;

    DB() = default;
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    virtual ~DB();
}; // DB

} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif // VECENGINE_DB_H_
