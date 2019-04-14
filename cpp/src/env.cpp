#inlcude "env.h"

namespace vecengine {

DBConfig::DBConfig()
    : _mem_sync_interval(10),
      _file_merge_trigger_number(20),
      _index_file_build_trigger_size(100000) {
}

} // namespace vecengine
