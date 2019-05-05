/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
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
    uint16_t  memory_sync_interval = 1;
    uint16_t  merge_trigger_number = 2;
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
