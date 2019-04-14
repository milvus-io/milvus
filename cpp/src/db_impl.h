#ifndef VECENGINE_DB_IMPL_H_
#define VECENGINE_DB_IMPL_H_

#include "db.h"

namespace vecengine {

class Env;

class DBImpl : public DB {
public:
    DBImpl(const Options& options_, const std::string& name_);

    virtual ~DBImpl();
private:
    const _dbname;
    Env* const _env;
    const Options _options;

}; // DBImpl

} // namespace vecengine

#endif // VECENGINE_DB_IMPL_H_
