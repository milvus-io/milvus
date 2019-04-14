#include <assert.h>
#include "db_impl.h"

namespace vecengine {

DBImpl::DBImpl(const Options& options_, const std::string& name_)
    : _dbname(name_),
      _env(options_.env),
      _options(options_) {
}

Status DBImpl::add_group(const GroupOptions& options_,
        const std::string& group_id_,
        std::string& gid_) {
    assert((!options_.has_id) ||
            (options_.has_id && ("" != group_id_)));

}

/*
 *  DB
 */

DB::~DB() {}

DB* DB::Open(const Options& options_, const std::string& name_) {
    DBImpl* impl = new DBImpl(options_, name_);
    return impl;
}

} // namespace vecengine
