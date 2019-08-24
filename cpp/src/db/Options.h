/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "Constants.h"

#include <string>
#include <memory>
#include <map>
#include <vector>

namespace zilliz {
namespace milvus {
namespace engine {

class Env;

static const char* ARCHIVE_CONF_DISK = "disk";
static const char* ARCHIVE_CONF_DAYS = "days";

struct ArchiveConf {
    using CriteriaT = std::map<std::string, int>;

    ArchiveConf(const std::string& type, const std::string& criterias = std::string());

    const std::string& GetType() const { return type_; }
    const CriteriaT GetCriterias() const { return criterias_; }

    void SetCriterias(const ArchiveConf::CriteriaT& criterial);

private:
    void ParseCritirias(const std::string& type);
    void ParseType(const std::string& criterias);

    std::string type_;
    CriteriaT criterias_;
};

struct DBMetaOptions {
    std::string path;
    std::vector<std::string> slave_paths;
    std::string backend_uri;
    ArchiveConf archive_conf = ArchiveConf("delete");
}; // DBMetaOptions

struct Options {

    typedef enum {
        SINGLE,
        CLUSTER,
        READ_ONLY
    } MODE;

    Options();

    uint16_t  merge_trigger_number = 2;
    DBMetaOptions meta;
    int mode = MODE::SINGLE;

    size_t insert_buffer_size = 4 * ONE_GB;
    bool insert_cache_immediately_ = false;
}; // Options


} // namespace engine
} // namespace milvus
} // namespace zilliz
