// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include "Constants.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace milvus {
namespace engine {

class Env;

extern const char* ARCHIVE_CONF_DISK;
extern const char* ARCHIVE_CONF_DAYS;
extern const char* DEFAULT_PARTITON_TAG;

struct ArchiveConf {
    using CriteriaT = std::map<std::string, int64_t>;

    explicit ArchiveConf(const std::string& type, const std::string& criterias = std::string());

    const std::string&
    GetType() const {
        return type_;
    }

    const CriteriaT
    GetCriterias() const {
        return criterias_;
    }

    void
    SetCriterias(const ArchiveConf::CriteriaT& criterial);

 private:
    void
    ParseCritirias(const std::string& criterias);
    void
    ParseType(const std::string& type);

    std::string type_;
    CriteriaT criterias_;
};

struct DBMetaOptions {
    std::string path_;
    std::vector<std::string> slave_paths_;
    std::string backend_uri_;
    ArchiveConf archive_conf_ = ArchiveConf("delete");
};  // DBMetaOptions

struct DBOptions {
    typedef enum { SINGLE = 0, CLUSTER_READONLY, CLUSTER_WRITABLE } MODE;

    uint16_t merge_trigger_number_ = 2;
    DBMetaOptions meta_;
    int mode_ = MODE::SINGLE;

    size_t insert_buffer_size_ = 4 * GB;
    bool insert_cache_immediately_ = false;

    int64_t auto_flush_interval_ = 1;
    int64_t file_cleanup_timeout_ = 10;

    bool metric_enable_ = false;

    // wal relative configurations
    bool wal_enable_ = true;
    bool recovery_error_ignore_ = true;
    int64_t buffer_size_ = 256;
    std::string mxlog_path_ = "/tmp/milvus/wal/";
};  // Options

}  // namespace engine
}  // namespace milvus
