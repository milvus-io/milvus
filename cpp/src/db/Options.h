#pragma once

#include <string>
#include <memory>

namespace zilliz {
namespace vecwise {
namespace engine {

class Env;

struct DBMetaOptions {
    /* DBMetaOptions(const std::string&, const std::string&); */
    std::string path;
    std::string backend_uri;
}; // DBMetaOptions


struct Options {
    Options();
    uint16_t  memory_sync_interval = 10;
    uint16_t  merge_trigger_number = 100;
    size_t  index_trigger_size = 1024*1024*256;
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
