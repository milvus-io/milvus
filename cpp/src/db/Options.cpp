#include "Options.h"
#include "Env.h"
#include "DBMetaImpl.h"

namespace zilliz {
namespace vecwise {
namespace engine {

Options::Options()
    : env(Env::Default()) {
}

/* DBMetaOptions::DBMetaOptions(const std::string& dbpath, */
/*         const std::string& uri) */
/*     : path(dbpath), backend_uri(uri) { */
/* } */

} // namespace engine
} // namespace vecwise
} // namespace zilliz
