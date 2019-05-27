/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <string>
#include <memory>
#include <map>

namespace zilliz {
namespace vecwise {
namespace engine {

class Env;

struct ArchiveConf {
    using CriteriaT = std::map<std::string, int>;

    ArchiveConf(const std::string& type, const std::string& criterias = "disk:512");

    const std::string& GetType() const { return type_; }
    const CriteriaT GetCriterias() const { return criterias_; }

private:
    void ParseCritirias(const std::string& type);
    void ParseType(const std::string& criterias);

    std::string type_;
    CriteriaT criterias_;
};

struct DBMetaOptions {
    std::string path;
    std::string backend_uri;
    ArchiveConf archive_conf = ArchiveConf("delete");
}; // DBMetaOptions


struct Options {
    Options();
    uint16_t  memory_sync_interval = 1;
    uint16_t  merge_trigger_number = 2;
    size_t  index_trigger_size = 1024*1024*1024;
    Env* env;
    DBMetaOptions meta;
}; // Options


} // namespace engine
} // namespace vecwise
} // namespace zilliz
