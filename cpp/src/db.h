#ifndef VECENGINE_DB_H_
#define VECENGINE_DB_H_

#include "options.h"

namespace vecengine {

class Env;

class DB {
public:
    static DB* Open(const Options& options_, const std::string& name_);

    DB() = default;
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    virtual ~DB();
}; // DB

} // namespace vecengine

#endif // VECENGINE_DB_H_
