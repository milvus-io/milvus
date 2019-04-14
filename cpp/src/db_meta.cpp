#include "db_meta.h"

namespace zilliz {
namespace vecwise {
namespace engine {

Meta* Meta::Default() {
    static DefaultMeta meta;
    return *meta;
}

} // namespace engine
} // namespace vecwise
} // namespace zilliz
