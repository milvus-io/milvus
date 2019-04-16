#pragma once

#include <string>
#include <memory>

namespace zilliz {
namespace vecwise {
namespace engine {

class Env;

struct MetaOptions {
}; // MetaOptions


struct DBMetaOptions : public MetaOptions {
    std::string backend_uri;
    std::string dbname;
}; // DBMetaOptions


struct Options {
    Options();
    uint16_t  memory_sync_interval = 10;
    uint16_t  raw_file_merge_trigger_number = 100;
    size_t  raw_to_index_trigger_size = 100000;
    Env* env;
    DBMetaOptions meta;
}; // Options


struct GroupOptions {
    size_t dimension;
    bool has_id = false;
}; // GroupOptions


} // namespace engine
} // namespace vecwise
} // namespace zilliz
