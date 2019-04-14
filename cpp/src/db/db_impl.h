#ifndef VECENGINE_DB_IMPL_H_
#define VECENGINE_DB_IMPL_H_

#include "db.h"

namespace vecengine {

class Env;

class DBImpl : public DB {
public:
    DBImpl(const Options& options_, const std::string& name_);

    virtual Status add_group(GroupOptions options_,
            const std::string& group_id_,
            std::string& gid_) override;

    virtual ~DBImpl();
private:

    Status meta_add_group(const std::string& group_id_);
    Status meta_add_group_file(const std::string& group_id_);

    const _dbname;
    Env* const _env;
    const Options _options;

}; // DBImpl

} // namespace vecengine

#endif // VECENGINE_DB_IMPL_H_
