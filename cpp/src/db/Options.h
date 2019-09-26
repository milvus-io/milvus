// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
    std::string path_;
    std::vector<std::string> slave_paths_;
    std::string backend_uri_;
    ArchiveConf archive_conf_ = ArchiveConf("delete");
}; // DBMetaOptions

struct DBOptions {
    typedef enum {
        SINGLE = 0,
        CLUSTER_READONLY,
        CLUSTER_WRITABLE
    } MODE;

    uint16_t  merge_trigger_number_ = 2;
    DBMetaOptions meta_;
    int mode_ = MODE::SINGLE;

    size_t insert_buffer_size_ = 4 * ONE_GB;
    bool insert_cache_immediately_ = false;
}; // Options


} // namespace engine
} // namespace milvus
} // namespace zilliz
