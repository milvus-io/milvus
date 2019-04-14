include "db_impl.h"

namespace vecengine {

DB::DB(const Options& options_, const std::string& name_)
    : _dbname(name_),
      _env(options_.env),
      _options(options_) {
}

} // namespace vecengine
