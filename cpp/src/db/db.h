#ifndef VECENGINE_DB_H_
#define VECENGINE_DB_H_

#include <string>
#include "options.h"

namespace zilliz {
namespace vecwise {
namespace engine {

class Env;

class DB {
public:
    static DB* Open(const Options& options_, const std::string& name_);

    virtual std::string add_group(GroupOptions options_,
            const std::string& group_id_) = 0;

    DB() = default;
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    virtual ~DB();
}; // DB

} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif // VECENGINE_DB_H_
